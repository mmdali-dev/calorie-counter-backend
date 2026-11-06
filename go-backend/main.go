// Food Diary API: compatible replacement for main.py.
// Build: go build -trimpath -o food-diary .
// Run next to the existing food.db, preserving JWT_SECRET and APP_TIMEZONE.
// No AutoMigrate: the existing SQLite schema and rowids are part of the contract.
package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"math/big"
	"net/http/httptest"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/gofiber/fiber/v3"
	swaggerfiles "github.com/swaggo/files/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	_ "modernc.org/sqlite"
)

type object = map[string]any
type apiError struct {
	status int
	detail any
}

func (e *apiError) Error() string          { return fmt.Sprint(e.detail) }
func failure(status int, detail any) error { return &apiError{status, detail} }

type User struct {
	ID            int64  `json:"id" gorm:"primaryKey"`
	Phone         string `json:"phone"`
	PasswordHash  string `json:"-"`
	Gender        int64  `json:"gender"`
	Height        int64  `json:"height"`
	Weight        int64  `json:"weight"`
	Name          string `json:"name"`
	BirthYear     int64  `json:"birth_year"`
	ActivityLevel int64  `json:"activity_level"`
}
type Meal struct {
	ID        int64 `gorm:"primaryKey"`
	UserID    int64
	Date      string
	MealType  int64
	CreatedAt string `gorm:"autoCreateTime:false"`
	UpdatedAt string `gorm:"autoUpdateTime:false"`
}
type MealItem struct {
	ID       int64   `json:"id" gorm:"primaryKey"`
	MealID   int64   `json:"meal_id"`
	FoodID   int64   `json:"food_id"`
	FoodName string  `json:"food_name"`
	Calories float64 `json:"calories"`
	Amount   float64 `json:"amount"`
	Unit     string  `json:"unit"`
}
type server struct {
	db, writer             *gorm.DB
	tz                     *time.Location
	secret                 []byte
	birthMax               int
	idCol, nameCol, calCol string
	writeMu                sync.Mutex
	spec                   []byte
}

func openDB(path string, write bool) (*gorm.DB, error) {
	var dsn string
	if path == ":memory:" {
		return nil, errors.New("use a file-backed SQLite database; :memory: cannot be shared by independent pools")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	uriPath := filepath.ToSlash(abs)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	u := url.URL{Scheme: "file", Path: uriPath}
	q := url.Values{}
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(30000)")
	if write {
		q.Set("_txlock", "immediate")
	}
	u.RawQuery = q.Encode()
	dsn = u.String()
	db, err := gorm.Open(sqlite.New(sqlite.Config{DriverName: "sqlite", DSN: dsn}), &gorm.Config{SkipDefaultTransaction: true, Logger: logger.Default.LogMode(logger.Silent), DisableAutomaticPing: true})
	if err != nil {
		return nil, err
	}
	pool, err := db.DB()
	if err != nil {
		return nil, err
	}
	n := min(max(runtime.GOMAXPROCS(0), 2), 8)
	if write {
		n = 1
	}
	pool.SetMaxOpenConns(n)
	pool.SetMaxIdleConns(n)
	if err = pool.Ping(); err != nil {
		pool.Close()
		return nil, err
	}
	return db, nil
}

const createUsers = `CREATE TABLE users (
 id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT UNIQUE NOT NULL,
 password_hash TEXT NOT NULL, gender INTEGER NOT NULL CHECK(gender IN (0,1)),
 height INTEGER NOT NULL, weight INTEGER NOT NULL, name TEXT NOT NULL,
 birth_year INTEGER NOT NULL, activity_level INTEGER NOT NULL DEFAULT 3)`
const createMeals = `CREATE TABLE meals (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 user_id INTEGER NOT NULL REFERENCES users(id), date TEXT NOT NULL,
 meal_type INTEGER NOT NULL CHECK(meal_type BETWEEN 1 AND 4),
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(user_id, date, meal_type))`
const createItems = `CREATE TABLE meal_items (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 meal_id INTEGER NOT NULL REFERENCES meals(id) ON DELETE CASCADE,
 food_id INTEGER NOT NULL, food_name TEXT NOT NULL, calories REAL NOT NULL,
 amount REAL NOT NULL, unit TEXT NOT NULL)`

func columns(db *gorm.DB, table string) (map[string]bool, error) {
	var rows []struct{ Name string }
	e := db.Raw("PRAGMA table_info('" + table + "')").Scan(&rows).Error
	result := map[string]bool{}
	for _, r := range rows {
		result[r.Name] = true
	}
	return result, e
}
func bootstrap(db *gorm.DB) error {
	// All DDL uses one pinned connection and a transaction, including old-version upgrades.
	return db.Connection(func(conn *gorm.DB) error {
		if e := conn.Exec("PRAGMA foreign_keys=OFF").Error; e != nil {
			return e
		}
		defer conn.Exec("PRAGMA foreign_keys=ON")
		if e := conn.Exec("PRAGMA legacy_alter_table=ON").Error; e != nil {
			return e
		}
		defer conn.Exec("PRAGMA legacy_alter_table=OFF")
		return conn.Transaction(func(tx *gorm.DB) error {
			exec := func(q string) error { return tx.Exec(q).Error }
			var names []string
			if e := tx.Raw("SELECT name FROM sqlite_master WHERE type='table'").Scan(&names).Error; e != nil {
				return e
			}
			tables := map[string]bool{}
			for _, n := range names {
				tables[n] = true
			}
			if !tables["foods"] {
				if e := exec("CREATE TABLE foods (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, calories REAL NOT NULL)"); e != nil {
					return e
				}
			}
			if !tables["users"] {
				if e := exec(createUsers); e != nil {
					return e
				}
			} else {
				var ddl string
				if e := tx.Raw("SELECT sql FROM sqlite_master WHERE name='users'").Scan(&ddl).Error; e != nil {
					return e
				}
				if !strings.Contains(ddl, "gender IN (0,1)") {
					cols, e := columns(tx, "users")
					if e != nil {
						return e
					}
					activity := "3"
					if cols["activity_level"] {
						activity = "activity_level"
					}
					for _, q := range []string{"ALTER TABLE users RENAME TO users_old", createUsers,
						"INSERT INTO users(id,phone,password_hash,gender,height,weight,name,birth_year,activity_level) SELECT id,phone,password_hash,CASE WHEN gender IN (0,1) THEN gender ELSE 0 END,height,weight,name,birth_year," + activity + " FROM users_old",
						"DROP TABLE users_old"} {
						if e := exec(q); e != nil {
							return e
						}
					}
				}
				cols, e := columns(tx, "users")
				if e != nil {
					return e
				}
				if !cols["activity_level"] {
					if e = exec("ALTER TABLE users ADD COLUMN activity_level INTEGER NOT NULL DEFAULT 3"); e != nil {
						return e
					}
				}
			}
			cols, e := columns(tx, "meals")
			if e != nil {
				return e
			}
			if cols["food_name"] {
				for _, q := range []string{"ALTER TABLE meals RENAME TO meals_legacy", createMeals, createItems,
					"INSERT INTO meals(user_id,date,meal_type,created_at,updated_at) SELECT user_id,substr(eaten_at,1,10),meal_type,min(eaten_at),max(eaten_at) FROM meals_legacy GROUP BY user_id,substr(eaten_at,1,10),meal_type",
					"INSERT INTO meal_items(meal_id,food_id,food_name,calories,amount,unit) SELECT m.id,l.food_id,l.food_name,l.calories,l.amount,l.unit FROM meals_legacy l JOIN meals m ON m.user_id=l.user_id AND m.meal_type=l.meal_type AND m.date=substr(l.eaten_at,1,10) ORDER BY l.id",
					"DROP TABLE meals_legacy"} {
					if e = exec(q); e != nil {
						return e
					}
				}
			} else {
				if !tables["meals"] {
					if e = exec(createMeals); e != nil {
						return e
					}
				}
				if !tables["meal_items"] {
					if e = exec(createItems); e != nil {
						return e
					}
				}
			}
			if e = exec("CREATE INDEX IF NOT EXISTS idx_meal_items_meal_id ON meal_items(meal_id)"); e != nil {
				return e
			}
			cols, e = columns(tx, "foods")
			if e != nil {
				return e
			}
			if !cols["calories_cached"] {
				if e = exec("ALTER TABLE foods ADD COLUMN calories_cached REAL"); e != nil {
					return e
				}
			}
			rows, e := tx.Raw("PRAGMA foreign_key_check").Rows()
			if e != nil {
				return e
			}
			defer rows.Close()
			if rows.Next() {
				return errors.New("foreign_key_check failed; startup transaction rolled back")
			}
			return rows.Err()
		})
	})
}

func newServer(path string) (*server, error) {
	w, e := openDB(path, true)
	if e != nil {
		return nil, e
	}
	cleanup := func() { p, _ := w.DB(); p.Close() }
	if e = bootstrap(w); e != nil {
		cleanup()
		return nil, e
	}
	db, e := openDB(path, false)
	if e != nil {
		cleanup()
		return nil, e
	}
	tz, e := time.LoadLocation(env("APP_TIMEZONE", "Asia/Tehran"))
	if e != nil {
		tz = time.UTC
	}
	s := &server{db: db, writer: w, tz: tz, secret: []byte(env("JWT_SECRET", "change-this-secret-before-public-use")), birthMax: time.Now().Year(), idCol: "rowid", nameCol: `"name"`, calCol: `"calories"`}
	cols, e := columns(db, "foods")
	if e != nil {
		s.close()
		return nil, e
	}
	if cols["id"] {
		s.idCol = `"id"`
	}
	if cols["Food_Item"] {
		s.nameCol = `"Food_Item"`
	}
	if cols["Calories"] {
		s.calCol = `"Calories"`
	}
	if (!cols["Food_Item"] && !cols["name"]) || (!cols["Calories"] && !cols["calories"]) {
		s.close()
		return nil, errors.New("unsupported foods schema")
	}
	s.spec = s.openAPI()
	return s, nil
}
func (s *server) close() {
	for _, db := range []*gorm.DB{s.db, s.writer} {
		if db != nil {
			p, e := db.DB()
			if e == nil {
				p.Close()
			}
		}
	}
}
func env(k, d string) string {
	v, ok := os.LookupEnv(k)
	if ok {
		return v
	}
	return d
}
func (s *server) token(uid int64) string {
	head := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body := base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, `{"sub": %d, "exp": %d}`, uid, time.Now().UTC().Add(30*24*time.Hour).Unix()))
	sign := head + "." + body
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(sign))
	return sign + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func passwordHash(password string) (string, error) {
	salt := make([]byte, 16)
	if _, e := rand.Read(salt); e != nil {
		return "", e
	}
	h, e := pbkdf2.Key(sha256.New, password, salt, 200000, 32)
	if e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(salt) + "$" + base64.RawURLEncoding.EncodeToString(h), nil
}
func decode64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}
func verifyPassword(password, stored string) bool {
	p := strings.Split(stored, "$")
	if len(p) != 2 {
		return false
	}
	salt, e := decode64(p[0])
	if e != nil {
		return false
	}
	want, e := decode64(p[1])
	if e != nil {
		return false
	}
	h, e := pbkdf2.Key(sha256.New, password, salt, 200000, 32)
	return e == nil && hmac.Equal(want, h)
}
func (s *server) authenticate(c fiber.Ctx) (*User, error) {
	auth := c.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return nil, failure(401, "توکن لازم است")
	}
	bad := func() (*User, error) { return nil, failure(401, "توکن نامعتبر یا منقضی است") }
	fields := strings.FieldsFunc(auth, unicode.IsSpace)
	if len(fields) < 2 {
		return bad()
	}
	token := strings.TrimLeftFunc(auth[len(fields[0]):], unicode.IsSpace)
	p := strings.Split(token, ".")
	if len(p) != 3 {
		return bad()
	}
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(p[0] + "." + p[1]))
	if !hmac.Equal([]byte(p[2]), []byte(base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))) {
		return bad()
	}
	b, e := decode64(p[1])
	if e != nil {
		return bad()
	}
	var payload object
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if dec.Decode(&payload) != nil {
		return bad()
	}
	exp, ok := number(payload["exp"])
	if !ok || exp < float64(time.Now().UnixNano())/1e9 {
		return bad()
	}
	uid, ok := pythonInt(payload["sub"])
	if !ok {
		return bad()
	}
	var user User
	e = s.db.First(&user, uid).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return nil, failure(401, "کاربر این توکن دیگر وجود ندارد")
	}
	if e != nil {
		return nil, e
	}
	return &user, nil
}
func userJSON(u *User) object {
	return object{"id": u.ID, "phone": u.Phone, "name": u.Name, "gender": u.Gender, "height": u.Height, "weight": u.Weight, "birth_year": u.BirthYear, "activity_level": u.ActivityLevel, "age": int64(time.Now().Year()) - u.BirthYear}
}
func pythonInt(v any) (int64, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		n, e := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n, e == nil
	case json.Number:
		f, e := strconv.ParseFloat(string(x), 64)
		if e != nil || math.IsInf(f, 0) || math.IsNaN(f) || f >= math.Exp2(63) || f < -math.Exp2(63) {
			return 0, false
		}
		return int64(f), true
	}
	return 0, false
}
func number(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		n, e := strconv.ParseFloat(string(x), 64)
		return n, e == nil
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// Pydantic compatibility: strict body types, aggregate errors, field order,
// unknown fields, nullable fields, Unicode lengths and FastAPI's error envelope.
type rule struct {
	name, kind                string
	optional, nullable        bool
	min, max                  float64
	hasMin, hasMax, exclusive bool
	def                       any
}

func strRule(n string, lo, hi int, optional bool) rule {
	return rule{name: n, kind: "string", optional: optional, nullable: optional, min: float64(lo), max: float64(hi), hasMin: lo > 0, hasMax: true}
}
func intRule(n string, lo, hi int, exclusive, optional bool) rule {
	return rule{name: n, kind: "int", optional: optional, nullable: optional, min: float64(lo), max: float64(hi), hasMin: true, hasMax: true, exclusive: exclusive}
}
func (s *server) rules(model string) []rule {
	switch model {
	case "register":
		r := []rule{strRule("phone", 1, 32, false), strRule("password", 1, 256, false), intRule("gender", 0, 1, false, false), intRule("height", 0, 300, true, false), intRule("weight", 0, 700, true, false), strRule("name", 1, 100, false), intRule("birth_year", 1900, s.birthMax, true, false), intRule("activity_level", 1, 5, false, true)}
		r[7].nullable = false
		r[7].def = json.Number("3")
		return r
	case "login":
		return []rule{strRule("phone", 1, 32, false), strRule("password", 1, 256, false)}
	case "profile":
		return []rule{intRule("height", 0, 300, true, true), intRule("weight", 0, 700, true, true), intRule("age", 0, 130, true, true), intRule("gender", 0, 1, false, true), intRule("activity_level", 1, 5, false, true)}
	case "meal":
		return []rule{intRule("meal_id", 1, 4, false, false), {name: "foods", kind: "list"}, {name: "date", kind: "date", optional: true, nullable: true}}
	case "item":
		return []rule{intRule("food_id", 0, 2147483647, false, false), {name: "amount", kind: "float", hasMin: true, exclusive: true, hasMax: true, max: 100000}, {name: "calories", kind: "float", optional: true, nullable: true, hasMin: true, hasMax: true, max: 100000}, strRule("name", 0, 100, true), strRule("unit", 0, 32, true)}
	}
	return nil
}
func issue(kind string, loc []any, msg string, input any, ctx object) object {
	o := object{"type": kind, "loc": loc, "msg": msg, "input": input}
	if ctx != nil {
		o["ctx"] = ctx
	}
	return o
}
func location(loc []any, k any) []any { return append(append([]any{}, loc...), k) }
func (s *server) validate(model string, input any, loc []any, order map[string][]string) (object, []object) {
	errorsList := []object{}
	obj, ok := input.(map[string]any)
	if !ok {
		return nil, []object{issue("model_attributes_type", loc, "Input should be a valid dictionary or object to extract fields from", input, nil)}
	}
	result := object{}
	known := map[string]bool{}
	for _, r := range s.rules(model) {
		known[r.name] = true
		v, present := obj[r.name]
		l := location(loc, r.name)
		add := func(t, m string, ctx object) { errorsList = append(errorsList, issue(t, l, m, v, ctx)) }
		if !present {
			if !r.optional {
				v = obj
				add("missing", "Field required", nil)
			} else if r.def != nil {
				result[r.name] = r.def
			}
			continue
		}
		if v == nil && r.nullable {
			result[r.name] = nil
			continue
		}
		switch r.kind {
		case "string":
			text, ok := v.(string)
			if !ok {
				add("string_type", "Input should be a valid string", nil)
				continue
			}
			n := utf8.RuneCountInString(text)
			if r.hasMin && float64(n) < r.min {
				add("string_too_short", fmt.Sprintf("String should have at least %g character", r.min)+plural(r.min), object{"min_length": r.min})
				continue
			}
			if float64(n) > r.max {
				add("string_too_long", fmt.Sprintf("String should have at most %g character", r.max)+plural(r.max), object{"max_length": r.max})
				continue
			}
			result[r.name] = v
		case "int", "float":
			num, isNumber := v.(json.Number)
			if !isNumber || (r.kind == "int" && strings.ContainsAny(string(num), ".eE")) {
				typ, msg := "int_type", "Input should be a valid integer"
				if r.kind == "float" {
					typ, msg = "float_type", "Input should be a valid number"
				}
				add(typ, msg, nil)
				continue
			}
			f, e := strconv.ParseFloat(string(num), 64)
			if r.kind == "float" && (e != nil || math.IsInf(f, 0) || math.IsNaN(f)) {
				add("finite_number", "Input should be a finite number", nil)
				continue
			}
			if r.hasMin && (f < r.min || (r.exclusive && f == r.min)) {
				kind, op, key := "greater_than_equal", "greater than or equal to", "ge"
				if r.exclusive {
					kind, op, key = "greater_than", "greater than", "gt"
				}
				add(kind, "Input should be "+op+" "+strconv.FormatFloat(r.min, 'f', -1, 64), object{key: r.min})
				continue
			}
			if r.hasMax && f > r.max {
				add("less_than_equal", "Input should be less than or equal to "+strconv.FormatFloat(r.max, 'f', -1, 64), object{"le": r.max})
				continue
			}
			result[r.name] = v
		case "list":
			arr, ok := v.([]any)
			if !ok {
				add("list_type", "Input should be a valid list", nil)
				continue
			}
			if len(arr) > 100 {
				add("too_long", fmt.Sprintf("List should have at most 100 items after validation, not %d", len(arr)), object{"field_type": "List", "max_length": 100, "actual_length": len(arr)})
				continue
			}
			if len(arr) == 0 {
				add("too_short", "List should have at least 1 item after validation, not 0", object{"field_type": "List", "min_length": 1, "actual_length": 0})
				continue
			}
			values := make([]any, 0, len(arr))
			for i, item := range arr {
				value, errs := s.validate("item", item, location(l, i), order)
				values = append(values, value)
				errorsList = append(errorsList, errs...)
			}
			result[r.name] = values
		case "date":
			d, er := parseDate(v, l)
			if er != nil {
				errorsList = append(errorsList, er)
				continue
			}
			if d < "1900-01-01" {
				add("greater_than_equal", "Input should be greater than or equal to 1900-01-01", object{"ge": "1900-01-01"})
				continue
			}
			if d > "2100-12-31" {
				add("less_than_equal", "Input should be less than or equal to 2100-12-31", object{"le": "2100-12-31"})
				continue
			}
			result[r.name] = d
		}
	}
	keys := order[fmt.Sprint(loc)]
	if keys == nil {
		for k := range obj {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		if !known[k] {
			errorsList = append(errorsList, issue("extra_forbidden", location(loc, k), "Extra inputs are not permitted", obj[k], nil))
		}
	}
	return result, errorsList
}
func plural(n float64) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func parseDate(v any, loc []any) (string, object) {
	bad := func(reason string) (string, object) {
		return "", issue("date_from_datetime_parsing", loc, "Input should be a valid date or datetime, "+reason, v, object{"error": reason})
	}
	if _, isBool := v.(bool); isBool || v == nil {
		return "", issue("date_type", loc, "Input should be a valid date", v, nil)
	}
	inexact := func() (string, object) {
		return "", issue("date_from_datetime_inexact", loc, "Datetimes provided to dates should have zero time - e.g. be exact dates", v, nil)
	}
	yearZero := func() (string, object) {
		return "", issue("date_parsing", loc, "Input should be a valid date in the format YYYY-MM-DD, year 0 is out of range", v, object{"error": "year 0 is out of range"})
	}
	var stamp float64
	numeric := false
	switch x := v.(type) {
	case json.Number:
		stamp, _ = strconv.ParseFloat(string(x), 64)
		numeric = true
	case string:
		if t, e := time.Parse("2006-01-02", x); e == nil {
			if t.Year() == 0 {
				return yearZero()
			}
			return t.Format("2006-01-02"), nil
		}
		if n, e := strconv.ParseFloat(x, 64); e == nil && dateNumberRE.MatchString(x) {
			stamp = n
			numeric = true
			break
		}
		if len(x) < 10 {
			return bad("input is too short")
		}
		if _, e := strconv.Atoi(x[:4]); e != nil {
			return bad("invalid character in year")
		}
		if x[4] != '-' {
			return bad("invalid date separator, expected `-`")
		}
		if _, e := strconv.Atoi(x[5:7]); e != nil {
			return bad("invalid character in month")
		}
		if x[7] != '-' {
			return bad("invalid date separator, expected `-`")
		}
		if _, e := strconv.Atoi(x[8:10]); e != nil {
			return bad("invalid character in day")
		}
		month, _ := strconv.Atoi(x[5:7])
		if month < 1 || month > 12 {
			return bad("month value is outside expected range of 1-12")
		}
		day, _ := strconv.Atoi(x[8:10])
		if day < 1 || day > 31 {
			return bad("day value is outside expected range")
		}
		if _, e := time.Parse("2006-01-02", x[:10]); e != nil {
			return bad("day value is outside expected range")
		}
		if len(x) > 10 {
			if !strings.ContainsRune("Tt_ ", rune(x[10])) {
				return bad("invalid datetime separator, expected `T`, `t`, `_` or space")
			}
			if len(x) < 16 {
				return bad("input is too short")
			}
			digits := func(pos int) (int, bool) {
				if pos+2 > len(x) || x[pos] < '0' || x[pos] > '9' || x[pos+1] < '0' || x[pos+1] > '9' {
					return 0, false
				}
				return int(x[pos]-'0')*10 + int(x[pos+1]-'0'), true
			}
			hour, ok := digits(11)
			if !ok {
				return bad("invalid character in hour")
			}
			if hour > 23 {
				return bad("hour value is outside expected range of 0-23")
			}
			if x[13] != ':' {
				return bad("invalid time separator, expected `:`")
			}
			minute, ok := digits(14)
			if !ok {
				return bad("invalid character in minute")
			}
			if minute > 59 {
				return bad("minute value is outside expected range of 0-59")
			}
			pos, second, micro := 16, 0, 0
			if pos < len(x) && x[pos] == ':' {
				second, ok = digits(pos + 1)
				if !ok {
					return bad("invalid character in second")
				}
				if second > 59 {
					return bad("second value is outside expected range of 0-59")
				}
				pos += 3
				if pos < len(x) && (x[pos] == '.' || x[pos] == ',') {
					pos++
					start := pos
					for pos < len(x) && x[pos] >= '0' && x[pos] <= '9' {
						if pos-start < 6 {
							micro = micro*10 + int(x[pos]-'0')
						}
						pos++
					}
					if pos == start {
						return bad("second fraction digits missing after `.`")
					}
				}
			}
			if pos < len(x) {
				switch x[pos] {
				case 'Z', 'z':
					pos++
				case '+', '-':
					zh, ok := digits(pos + 1)
					if !ok {
						return bad("invalid timezone hour")
					}
					pos += 3
					if pos < len(x) && x[pos] == ':' {
						pos++
					}
					zm, ok := digits(pos)
					if !ok {
						return bad("invalid timezone minute")
					}
					pos += 2
					if zm > 59 {
						return bad("timezone minute value is outside expected range of 0-59")
					}
					if zh > 23 {
						return bad("timezone offset must be less than 24 hours")
					}
				default:
					return bad("invalid timezone sign")
				}
			}
			if pos != len(x) {
				return bad("unexpected extra characters at the end of the input")
			}
			if hour != 0 || minute != 0 || second != 0 || micro != 0 {
				return inexact()
			}
			if strings.HasPrefix(x, "0000") {
				return yearZero()
			}
			return x[:10], nil
		}
		return bad("invalid datetime")
	default:
		return "", issue("date_type", loc, "Input should be a valid date", v, nil)
	}
	if numeric {
		if math.IsNaN(stamp) || math.IsInf(stamp, 0) {
			return bad("invalid datetime")
		}
		if math.Abs(stamp) > 2e10 {
			stamp /= 1000
		}
		if stamp < -62167219200 {
			return bad("dates before 0000 are not supported as unix timestamps")
		}
		if stamp >= 253402300800 {
			return bad("dates after 9999 are not supported as unix timestamps")
		}
		seconds, frac := math.Modf(stamp)
		t := time.Unix(int64(seconds), int64(math.Round(frac*1e6))*1000).UTC()
		if t.Year() < 0 {
			return bad("dates before 0000 are not supported as unix timestamps")
		}
		if t.Year() > 9999 {
			return bad("dates after 9999 are not supported as unix timestamps")
		}
		if t.Hour() != 0 || t.Minute() != 0 || t.Second() != 0 || t.Nanosecond() != 0 {
			return "", issue("date_from_datetime_inexact", loc, "Datetimes provided to dates should have zero time - e.g. be exact dates", v, nil)
		}
		if t.Year() == 0 {
			return yearZero()
		}
		return t.Format("2006-01-02"), nil
	}
	return bad("invalid datetime")
}

var dateNumberRE = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

func orderedJSON(raw []byte) (any, map[string][]string, error) {
	raw, special := specialNumbers(raw)
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	order := map[string][]string{}
	var read func([]any) (any, error)
	read = func(loc []any) (any, error) {
		tok, e := d.Token()
		if e != nil {
			return nil, e
		}
		if n, ok := special[int(d.InputOffset())]; ok {
			return n, nil
		}
		switch tok {
		case json.Delim('{'):
			m := object{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return nil, e
				}
				key := k.(string)
				if _, ok := m[key]; !ok {
					order[fmt.Sprint(loc)] = append(order[fmt.Sprint(loc)], key)
				}
				v, e := read(location(loc, key))
				if e != nil {
					return nil, e
				}
				m[key] = v
			}
			_, e = d.Token()
			return m, e
		case json.Delim('['):
			a := []any{}
			for d.More() {
				v, e := read(location(loc, len(a)))
				if e != nil {
					return nil, e
				}
				a = append(a, v)
			}
			_, e = d.Token()
			return a, e
		default:
			if n, ok := tok.(json.Number); ok && strings.ContainsAny(string(n), ".eE") {
				if f, e := strconv.ParseFloat(string(n), 64); e != nil && math.IsInf(f, 0) {
					return f, nil
				}
			}
			return tok, nil
		}
	}
	v, e := read([]any{"body"})
	if e != nil {
		return nil, order, e
	}
	_, e = d.Token()
	if e != io.EOF {
		if e == nil {
			e = errors.New("extra data")
		}
		return nil, order, e
	}
	return v, order, nil
}
func body(c fiber.Ctx) (any, map[string][]string, error) {
	raw := c.Body()
	if len(raw) == 0 {
		return nil, nil, nil
	}
	ct := strings.ToLower(strings.Split(c.Get("Content-Type"), ";")[0])
	if ct != "" && ct != "application/json" && !strings.HasSuffix(ct, "+json") {
		return string(raw), nil, nil
	}
	var encodingErr error
	raw, encodingErr = decodeJSONBytes(raw)
	if encodingErr != nil {
		return nil, nil, failure(400, "There was an error parsing the body")
	}
	v, order, e := orderedJSON(raw)
	if e != nil {
		pos, msg := jsonError(raw)
		return nil, nil, failure(422, []object{issue("json_invalid", []any{"body", pos}, "JSON decode error", object{}, object{"error": msg})})
	}
	return v, order, nil
}

func specialNumbers(raw []byte) ([]byte, map[int]float64) {
	if !bytes.Contains(raw, []byte("NaN")) && !bytes.Contains(raw, []byte("Infinity")) {
		return raw, nil
	}
	out := make([]byte, 0, len(raw))
	special := map[int]float64{}
	quoted, escaped := false, false
	for i := 0; i < len(raw); i++ {
		b := raw[i]
		if quoted {
			out = append(out, b)
			if escaped {
				escaped = false
			} else if b == '\\' {
				escaped = true
			} else if b == '"' {
				quoted = false
			}
			continue
		}
		if b == '"' {
			quoted = true
			out = append(out, b)
			continue
		}
		found := false
		for _, literal := range []string{"-Infinity", "Infinity", "NaN"} {
			if bytes.HasPrefix(raw[i:], []byte(literal)) {
				out = append(out, '0')
				n := math.NaN()
				if literal == "Infinity" {
					n = math.Inf(1)
				} else if literal == "-Infinity" {
					n = math.Inf(-1)
				}
				special[len(out)] = n
				i += len(literal) - 1
				found = true
				break
			}
		}
		if !found {
			out = append(out, b)
		}
	}
	return out, special
}

func decodeJSONBytes(raw []byte) ([]byte, error) {
	var order binary.ByteOrder
	unit := 0
	skip := 0
	switch {
	case bytes.HasPrefix(raw, []byte{0, 0, 254, 255}):
		order = binary.BigEndian
		unit = 4
		skip = 4
	case bytes.HasPrefix(raw, []byte{255, 254, 0, 0}):
		order = binary.LittleEndian
		unit = 4
		skip = 4
	case bytes.HasPrefix(raw, []byte{254, 255}):
		order = binary.BigEndian
		unit = 2
		skip = 2
	case bytes.HasPrefix(raw, []byte{255, 254}):
		order = binary.LittleEndian
		unit = 2
		skip = 2
	case bytes.HasPrefix(raw, []byte{239, 187, 191}):
		raw = raw[3:]
	case len(raw) >= 4 && raw[0] == 0:
		order = binary.BigEndian
		unit = 2
		if raw[1] == 0 {
			unit = 4
		}
	case len(raw) >= 4 && raw[1] == 0:
		order = binary.LittleEndian
		unit = 2
		if raw[2] == 0 && raw[3] == 0 {
			unit = 4
		}
	case len(raw) == 2 && raw[0] == 0:
		order = binary.BigEndian
		unit = 2
	case len(raw) == 2 && raw[1] == 0:
		order = binary.LittleEndian
		unit = 2
	}
	if unit == 0 {
		if !utf8.Valid(raw) {
			return nil, errors.New("invalid UTF-8")
		}
		return raw, nil
	}
	raw = raw[skip:]
	if len(raw)%unit != 0 {
		return nil, errors.New("truncated Unicode")
	}
	if unit == 2 {
		codes := make([]uint16, len(raw)/2)
		for i := range codes {
			codes[i] = order.Uint16(raw[i*2:])
		}
		return []byte(string(utf16.Decode(codes))), nil
	}
	runes := make([]rune, len(raw)/4)
	for i := range runes {
		n := order.Uint32(raw[i*4:])
		if n > utf8.MaxRune {
			return nil, errors.New("invalid UTF-32")
		}
		runes[i] = rune(n)
	}
	return []byte(string(runes)), nil
}
func (s *server) validated(model string, v any, order map[string][]string) (object, error) {
	if v == nil {
		return nil, failure(422, []object{issue("missing", []any{"body"}, "Field required", nil, nil)})
	}
	out, errs := s.validate(model, v, []any{"body"}, order)
	if len(errs) > 0 {
		return nil, failure(422, errs)
	}
	return out, nil
}
func jsonError(raw []byte) (int, string) {
	// Locate JSON syntax failures using a small grammar, matching CPython's messages.
	p := 0
	var msg string
	var value func() bool
	ws := func() {
		for p < len(raw) && strings.ContainsRune(" \r\n\t", rune(raw[p])) {
			p++
		}
	}
	stringValue := func() bool {
		start := p
		p++
		for p < len(raw) {
			b := raw[p]
			if b == '"' {
				p++
				return true
			}
			if b < 32 {
				msg = "Invalid control character at"
				return false
			}
			if b == '\\' {
				p++
				if p >= len(raw) {
					p = start
					msg = "Unterminated string starting at"
					return false
				}
				if raw[p] == 'u' {
					if p+4 >= len(raw) {
						msg = "Invalid \\uXXXX escape"
						return false
					}
					for j := 1; j <= 4; j++ {
						if !strings.ContainsRune("0123456789abcdefABCDEF", rune(raw[p+j])) {
							msg = "Invalid \\uXXXX escape"
							return false
						}
					}
					p += 4
				} else if !strings.ContainsRune(`"\/bfnrt`, rune(raw[p])) {
					p--
					msg = "Invalid \\escape"
					return false
				}
			}
			p++
		}
		p = start
		msg = "Unterminated string starting at"
		return false
	}
	value = func() bool {
		ws()
		if p == len(raw) {
			msg = "Expecting value"
			return false
		}
		switch raw[p] {
		case '"':
			return stringValue()
		case '{':
			p++
			ws()
			if p < len(raw) && raw[p] == '}' {
				p++
				return true
			}
			for {
				ws()
				if p >= len(raw) || raw[p] != '"' {
					msg = "Expecting property name enclosed in double quotes"
					return false
				}
				if !stringValue() {
					return false
				}
				ws()
				if p >= len(raw) || raw[p] != ':' {
					msg = "Expecting ':' delimiter"
					return false
				}
				p++
				if !value() {
					return false
				}
				ws()
				if p < len(raw) && raw[p] == '}' {
					p++
					return true
				}
				if p >= len(raw) || raw[p] != ',' {
					msg = "Expecting ',' delimiter"
					return false
				}
				p++
			}
		case '[':
			p++
			ws()
			if p < len(raw) && raw[p] == ']' {
				p++
				return true
			}
			for {
				if !value() {
					return false
				}
				ws()
				if p < len(raw) && raw[p] == ']' {
					p++
					return true
				}
				if p >= len(raw) || raw[p] != ',' {
					msg = "Expecting ',' delimiter"
					return false
				}
				p++
			}
		default:
			for _, lit := range []string{"true", "false", "null", "NaN", "Infinity", "-Infinity"} {
				if bytes.HasPrefix(raw[p:], []byte(lit)) {
					p += len(lit)
					return true
				}
			}
			r := jsonNum.Find(raw[p:])
			if len(r) > 0 {
				p += len(r)
				return true
			}
			msg = "Expecting value"
			return false
		}
	}
	if value() {
		ws()
		msg = "Extra data"
	}
	return utf8.RuneCount(raw[:min(p, len(raw))]), msg
}

var jsonNum = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?`)

func asInt(m object, k string) int64     { n, _ := pythonInt(m[k]); return n }
func asFloat(m object, k string) float64 { n, _ := number(m[k]); return n }
func asString(m object, k string) string { s, _ := m[k].(string); return s }
func decimalDigits(s string) string {
	return strings.Map(func(r rune) rune {
		for _, range16 := range unicode.Nd.R16 {
			if r >= rune(range16.Lo) && r <= rune(range16.Hi) && (r-rune(range16.Lo))%rune(range16.Stride) == 0 {
				return '0' + (r-rune(range16.Lo))/rune(range16.Stride)%10
			}
		}
		for _, range32 := range unicode.Nd.R32 {
			if r >= rune(range32.Lo) && r <= rune(range32.Hi) && (r-rune(range32.Lo))%rune(range32.Stride) == 0 {
				return '0' + (r-rune(range32.Lo))/rune(range32.Stride)%10
			}
		}
		return r
	}, s)
}

var caloriesRE = regexp.MustCompile(`[0-9]+(?:\.[0-9]+)?`)

func parseCalories(raw any) (float64, bool) {
	rawText := fmt.Sprint(raw)
	if f, ok := raw.(float64); ok {
		format := byte('f')
		if f != 0 && (math.Abs(f) < 1e-4 || math.Abs(f) >= 1e16) {
			format = 'e'
		}
		rawText = strconv.FormatFloat(f, format, -1, 64)
	}
	text := strings.ReplaceAll(decimalDigits(rawText), ",", "")
	matches := caloriesRE.FindAllString(text, -1)
	if len(matches) == 0 {
		return 0, false
	}
	sum := 0.0
	for _, m := range matches {
		n, e := strconv.ParseFloat(m, 64)
		if e != nil {
			return 0, false
		}
		sum += n
	}
	sum /= float64(len(matches))
	return sum, !math.IsNaN(sum) && !math.IsInf(sum, 0) && sum >= 0
}
func (s *server) foodSelect() string {
	return "SELECT " + s.idCol + " AS id," + s.nameCol + " AS name," + s.calCol + " AS calories FROM foods"
}
func foodRows(db *gorm.DB, query string, args ...any) ([]object, error) {
	rows, e := db.Raw(query, args...).Rows()
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []object{}
	for rows.Next() {
		var id, name, cal any
		if e = rows.Scan(&id, &name, &cal); e != nil {
			return nil, e
		}
		out = append(out, object{"id": id, "name": name, "calories": cal})
	}
	return out, rows.Err()
}
func (s *server) search(db *gorm.DB, q string) ([]object, error) {
	n := decimalDigits(q)
	digits := n != ""
	for _, r := range n {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}
	if digits {
		id, ok := new(big.Int).SetString(n, 10)
		if !ok || !id.IsInt64() {
			return nil, errors.New("Python int too large to convert to SQLite INTEGER")
		}
		return foodRows(db, s.foodSelect()+" WHERE "+s.idCol+" = ? LIMIT 1", id.Int64())
	}
	return foodRows(db, s.foodSelect()+" WHERE "+s.nameCol+" LIKE ? LIMIT 100", "%"+q+"%")
}
func (s *server) resolve(tx *gorm.DB, m object) (MealItem, error) {
	item := MealItem{FoodID: asInt(m, "food_id"), Amount: asFloat(m, "amount")}
	if item.FoodID == 0 {
		item.FoodName = pyStrip(asString(m, "name"))
		item.Unit = pyStrip(asString(m, "unit"))
		if item.FoodName == "" || item.Unit == "" || m["calories"] == nil {
			return item, failure(422, "برای غذای دلخواه name، calories و unit لازم است")
		}
		item.Calories = asFloat(m, "calories")
		return item, nil
	}
	rows, e := s.search(tx, strconv.FormatInt(item.FoodID, 10))
	if e != nil {
		return item, e
	}
	if len(rows) == 0 {
		return item, failure(404, "غذا پیدا نشد")
	}
	if rows[0]["name"] == nil {
		return item, errors.New("NULL food_name violates existing schema")
	}
	item.FoodName = fmt.Sprint(rows[0]["name"])
	item.Unit = "serving"
	var cached sql.NullFloat64
	if e = tx.Raw("SELECT calories_cached FROM foods WHERE "+s.idCol+"=?", item.FoodID).Row().Scan(&cached); e != nil {
		return item, e
	}
	bad := failure(422, "مقدار کالری این غذا در دیتابیس معتبر نیست")
	if !cached.Valid {
		cal, ok := parseCalories(rows[0]["calories"])
		if !ok {
			return item, bad
		}
		cached = sql.NullFloat64{Float64: cal, Valid: true}
		if e = tx.Exec("UPDATE foods SET calories_cached=? WHERE "+s.idCol+"=?", cal, item.FoodID).Error; e != nil {
			return item, e
		}
	}
	if cached.Float64 > 100000 {
		return item, bad
	}
	item.Calories = cached.Float64
	return item, nil
}
func pyStrip(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f) })
}
func serializeMeal(m Meal, items []MealItem) object {
	if items == nil {
		items = []MealItem{}
	}
	total := 0.0
	for _, i := range items {
		total += i.Calories * i.Amount
	}
	return object{"id": m.ID, "user_id": m.UserID, "date": m.Date, "meal_id": m.MealType, "meal_name": map[int64]string{1: "breakfast", 2: "lunch", 3: "dinner", 4: "snack"}[m.MealType], "foods": items, "created_at": m.CreatedAt, "updated_at": m.UpdatedAt, "total_calories": total}
}

func (s *server) register(c fiber.Ctx, m object) error {
	hash, e := passwordHash(asString(m, "password"))
	if e != nil {
		return e
	}
	u := User{Phone: asString(m, "phone"), PasswordHash: hash, Gender: asInt(m, "gender"), Height: asInt(m, "height"), Weight: asInt(m, "weight"), Name: asString(m, "name"), BirthYear: asInt(m, "birth_year"), ActivityLevel: asInt(m, "activity_level")}
	s.writeMu.Lock()
	e = s.writer.Create(&u).Error
	s.writeMu.Unlock()
	if e != nil {
		if strings.Contains(strings.ToUpper(e.Error()), "UNIQUE") {
			return failure(409, "این شماره تلفن قبلا ثبت شده است")
		}
		return failure(422, "داده‌های ثبت‌نام نامعتبر است")
	}
	return c.Status(201).JSON(object{"access_token": s.token(u.ID), "token_type": "bearer"})
}
func (s *server) login(c fiber.Ctx, m object) error {
	var u User
	e := s.db.Where("phone = ?", asString(m, "phone")).First(&u).Error
	if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	if e != nil || !verifyPassword(asString(m, "password"), u.PasswordHash) {
		return failure(401, "شماره تلفن یا رمز عبور اشتباه است")
	}
	return c.JSON(object{"access_token": s.token(u.ID), "token_type": "bearer"})
}
func (s *server) profile(c fiber.Ctx, m object, u *User) error {
	changes := map[string]any{}
	for k, v := range m {
		if v != nil {
			changes[k] = asInt(m, k)
		}
	}
	if len(changes) == 0 {
		return failure(400, "حداقل یک مورد برای تغییر ارسال کنید")
	}
	if age, ok := changes["age"]; ok {
		changes["birth_year"] = int64(time.Now().Year()) - age.(int64)
		delete(changes, "age")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	e := s.writer.Transaction(func(tx *gorm.DB) error {
		if e := tx.Model(&User{}).Where("id=?", u.ID).Updates(changes).Error; e != nil {
			return e
		}
		return tx.First(u, u.ID).Error
	})
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return failure(404, "کاربر پیدا نشد")
	}
	if e != nil {
		return e
	}
	return c.JSON(userJSON(u))
}
func (s *server) addMeal(c fiber.Ctx, m object, u *User) error {
	now := time.Now().In(s.tz).Truncate(time.Microsecond)
	stamp := now.Format("2006-01-02T15:04:05.000000Z07:00")
	if now.Nanosecond() == 0 {
		stamp = now.Format("2006-01-02T15:04:05Z07:00")
	}
	if strings.HasSuffix(stamp, "Z") {
		stamp = strings.TrimSuffix(stamp, "Z") + "+00:00"
	}
	day := asString(m, "date")
	if day == "" {
		day = time.Now().In(s.tz).Format("2006-01-02")
	}
	status := 201
	var result object
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	e := s.writer.Transaction(func(tx *gorm.DB) error {
		items := []MealItem{}
		for _, v := range m["foods"].([]any) {
			item, e := s.resolve(tx, v.(object))
			if e != nil {
				return e
			}
			items = append(items, item)
		}
		var count int64
		if e := tx.Model(&Meal{}).Where("user_id=? AND date=? AND meal_type=?", u.ID, day, asInt(m, "meal_id")).Count(&count).Error; e != nil {
			return e
		}
		if count > 0 {
			status = 200
		}
		var meal Meal
		if e := tx.Raw("INSERT INTO meals(user_id,date,meal_type,created_at,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(user_id,date,meal_type) DO UPDATE SET updated_at=excluded.updated_at RETURNING *", u.ID, day, asInt(m, "meal_id"), stamp, stamp).Scan(&meal).Error; e != nil {
			return e
		}
		if e := tx.Where("meal_id=?", meal.ID).Delete(&MealItem{}).Error; e != nil {
			return e
		}
		for i := range items {
			items[i].MealID = meal.ID
		}
		if e := tx.Create(&items).Error; e != nil {
			return e
		}
		result = serializeMeal(meal, items)
		return nil
	})
	if e != nil {
		return e
	}
	return c.Status(status).JSON(result)
}
func lastQuery(c fiber.Ctx, key string) (string, bool) {
	values := c.Request().URI().QueryArgs().PeekMulti(key)
	if len(values) == 0 {
		return "", false
	}
	return string(values[len(values)-1]), true
}
func (s *server) getMeals(c fiber.Ctx, u *User) error {
	var day any
	value, present := lastQuery(c, "day")
	if present {
		d, e := parseDate(value, []any{"query", "day"})
		if e != nil {
			return failure(422, []object{e})
		}
		day = d
	}
	output := []object{}
	e := s.db.Transaction(func(tx *gorm.DB) error {
		q := tx.Where("user_id=?", u.ID)
		if day != nil {
			q = q.Where("date=?", day).Order("meal_type")
		} else {
			q = q.Order("date,meal_type")
		}
		var meals []Meal
		if e := q.Find(&meals).Error; e != nil {
			return e
		}
		if len(meals) == 0 {
			return nil
		}
		// One join replaces one items query per meal; transaction guarantees a consistent snapshot.
		var items []MealItem
		iq := tx.Table("meal_items").Select("meal_items.*").Joins("JOIN meals ON meals.id=meal_items.meal_id").Where("meals.user_id=?", u.ID)
		if day != nil {
			iq = iq.Where("meals.date=?", day)
		}
		if e := iq.Order("meal_items.id").Scan(&items).Error; e != nil {
			return e
		}
		group := map[int64][]MealItem{}
		for _, item := range items {
			group[item.MealID] = append(group[item.MealID], item)
		}
		for _, meal := range meals {
			output = append(output, serializeMeal(meal, group[meal.ID]))
		}
		return nil
	})
	if e != nil {
		return e
	}
	return c.JSON(object{"date": day, "meals": output})
}
func (s *server) listFoods(c fiber.Ctx) error {
	args := []any{int64(100), int64(0)}
	errs := []object{}
	for i, k := range []string{"limit", "offset"} {
		v, ok := lastQuery(c, k)
		if !ok {
			continue
		}
		n, valid := queryInteger(v)
		loc := []any{"query", k}
		if !valid {
			errs = append(errs, issue("int_parsing", loc, "Input should be a valid integer, unable to parse string as an integer", v, nil))
			continue
		}
		minimum := int64(0)
		if i == 0 {
			minimum = 1
		}
		if n.Cmp(big.NewInt(minimum)) < 0 {
			errs = append(errs, issue("greater_than_equal", loc, fmt.Sprintf("Input should be greater than or equal to %d", minimum), v, object{"ge": minimum}))
			continue
		}
		if i == 0 && n.Cmp(big.NewInt(1000)) > 0 {
			errs = append(errs, issue("less_than_equal", loc, "Input should be less than or equal to 1000", v, object{"le": 1000}))
			continue
		}
		if n.IsInt64() {
			args[i] = n.Int64()
		} else {
			args[i] = n
		}
	}
	if len(errs) > 0 {
		return failure(422, errs)
	}
	if _, overflow := args[1].(*big.Int); overflow {
		return errors.New("Python int too large to convert to SQLite INTEGER")
	}
	rows, e := foodRows(s.db, s.foodSelect()+" ORDER BY "+s.idCol+" LIMIT ? OFFSET ?", args...)
	if e != nil {
		return e
	}
	return c.JSON(rows)
}

var queryIntRE = regexp.MustCompile(`^[+-]?[0-9]+(?:_[0-9]+)*(?:\.0+)?$`)

func queryInteger(s string) (*big.Int, bool) {
	v := strings.TrimSpace(s)
	if !queryIntRE.MatchString(v) {
		return nil, false
	}
	v = strings.ReplaceAll(v, "_", "")
	if i := strings.IndexByte(v, '.'); i >= 0 {
		v = v[:i]
	}
	n, ok := new(big.Int).SetString(v, 10)
	return n, ok
}
func (s *server) searchFoods(c fiber.Ctx) error {
	q, ok := lastQuery(c, "q")
	loc := []any{"query", "q"}
	if !ok {
		return failure(422, []object{issue("missing", loc, "Field required", nil, nil)})
	}
	n := utf8.RuneCountInString(q)
	if n < 1 {
		return failure(422, []object{issue("string_too_short", loc, "String should have at least 1 character", q, object{"min_length": 1})})
	}
	if n > 100 {
		return failure(422, []object{issue("string_too_long", loc, "String should have at most 100 characters", q, object{"max_length": 100})})
	}
	rows, e := s.search(s.db, q)
	if e != nil {
		return e
	}
	return c.JSON(rows)
}

func (s *server) app() *fiber.App {
	app := fiber.New(fiber.Config{AppName: "Food Diary API", CaseSensitive: true, StrictRouting: true, UnescapePath: true, BodyLimit: 64 * 1024 * 1024, ReadBufferSize: 65536, ReadTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second, ErrorHandler: func(c fiber.Ctx, err error) error {
		var ae *apiError
		if errors.As(err, &ae) {
			b, e := json.Marshal(object{"detail": ae.detail})
			if e == nil {
				return c.Status(ae.status).Type("json").Send(b)
			}
		}
		log.Printf("request failed: %s %s (%T)", c.Method(), c.Path(), err)
		return c.Status(500).Type("txt").SendString("Internal Server Error")
	}})
	routes := map[string][]string{"/": {"GET"}, "/auth/register": {"POST"}, "/auth/login": {"POST"}, "/me": {"GET"}, "/profile": {"GET", "PATCH"}, "/foods": {"GET"}, "/foods/search": {"GET"}, "/meals": {"POST", "GET"}, "/docs": {"GET", "HEAD"}, "/docs/oauth2-redirect": {"GET", "HEAD"}, "/openapi.json": {"GET", "HEAD"}, "/redoc": {"GET", "HEAD"}}
	app.Use(func(c fiber.Ctx) (err error) {
		defer func() {
			if recover() != nil {
				err = errors.New("request panic")
			}
		}()
		path := c.Path()
		methods, exists := routes[path]
		if !exists {
			other := strings.TrimRight(path, "/")
			if other == "" {
				other = "/"
			}
			if _, ok := routes[other]; ok && path != other {
				target := c.BaseURL() + other
				if qs := string(c.Request().URI().QueryString()); qs != "" {
					target += "?" + qs
				}
				c.Set("Location", target)
				return c.Status(307).Send(nil)
			}
			if !strings.HasPrefix(path, "/docs/assets/") {
				return failure(404, "Not Found")
			}
			return c.Next()
		}
		for _, m := range methods {
			if m == c.Method() {
				return c.Next()
			}
		}
		allow := methods[0]
		if path == "/docs" || path == "/openapi.json" || path == "/redoc" || path == "/docs/oauth2-redirect" {
			allow = "HEAD, GET"
		}
		c.Set("Allow", allow)
		return failure(405, "Method Not Allowed")
	})
	app.Get("/", func(c fiber.Ctx) error { return c.JSON(object{"message": "Food Diary API", "docs": "/docs"}) })
	for _, route := range []struct {
		path, method, model string
		auth                bool
	}{{"/auth/register", "POST", "register", false}, {"/auth/login", "POST", "login", false}, {"/profile", "PATCH", "profile", true}, {"/meals", "POST", "meal", true}} {
		app.Add([]string{route.method}, route.path, func(c fiber.Ctx) error {
			v, order, e := body(c)
			if e != nil {
				return e
			}
			var user *User
			if route.auth {
				user, e = s.authenticate(c)
				if e != nil {
					return e
				}
			}
			m, e := s.validated(route.model, v, order)
			if e != nil {
				return e
			}
			switch route.model {
			case "register":
				return s.register(c, m)
			case "login":
				return s.login(c, m)
			case "profile":
				return s.profile(c, m, user)
			default:
				return s.addMeal(c, m, user)
			}
		})
	}
	for _, p := range []string{"/me", "/profile"} {
		app.Get(p, func(c fiber.Ctx) error {
			u, e := s.authenticate(c)
			if e != nil {
				return e
			}
			return c.JSON(userJSON(u))
		})
	}
	app.Get("/foods", s.listFoods)
	app.Get("/foods/search", s.searchFoods)
	app.Get("/meals", func(c fiber.Ctx) error {
		u, e := s.authenticate(c)
		if e != nil {
			return e
		}
		return s.getMeals(c, u)
	})
	app.Get("/openapi.json", func(c fiber.Ctx) error { return c.Type("json").Send(s.spec) })
	app.Get("/docs", func(c fiber.Ctx) error { return c.Type("html").SendString(swaggerHTML) })
	app.Get("/redoc", func(c fiber.Ctx) error { return c.Type("html").SendString(swaggerHTML) })
	app.Get("/docs/oauth2-redirect", func(c fiber.Ctx) error {
		b, e := fs.ReadFile(swaggerfiles.FS, "oauth2-redirect.html")
		if e != nil {
			return e
		}
		return c.Type("html").Send(b)
	})
	app.Get("/docs/assets/:name", func(c fiber.Ctx) error {
		name := c.Params("name")
		if name != "swagger-ui-bundle.js" && name != "swagger-ui.css" && name != "favicon-32x32.png" {
			return failure(404, "Not Found")
		}
		b, e := fs.ReadFile(swaggerfiles.FS, name)
		if e != nil {
			return e
		}
		c.Set("Cache-Control", "public, max-age=86400")
		return c.Type(strings.TrimPrefix(filepath.Ext(name), ".")).Send(b)
	})
	return app
}

const swaggerHTML = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>Food Diary API - Swagger UI</title><link rel="stylesheet" href="/docs/assets/swagger-ui.css"><link rel="icon" href="/docs/assets/favicon-32x32.png"></head><body><div id="swagger-ui"></div><script src="/docs/assets/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/openapi.json',dom_id:'#swagger-ui',deepLinking:true,displayRequestDuration:true,oauth2RedirectUrl:window.location.origin+'/docs/oauth2-redirect',presets:[SwaggerUIBundle.presets.apis],layout:'BaseLayout'});</script></body></html>`

func defaultDB() (string, error) {
	if v, ok := os.LookupEnv("DATABASE_URL"); ok {
		if !strings.HasPrefix(v, "sqlite:///") {
			return "", errors.New("DATABASE_URL must start with sqlite:///")
		}
		v = strings.TrimPrefix(v, "sqlite:///")
		if strings.Contains(v, "?") {
			return "", errors.New("SQLite URI query options are not supported; use -db with a filesystem path")
		}
		return v, nil
	}
	exe, e := os.Executable()
	if e != nil {
		return "", e
	}
	path := filepath.Join(filepath.Dir(exe), "food.db")
	if strings.Contains(filepath.ToSlash(exe), "/go-build") {
		_, source, _, ok := runtime.Caller(0)
		if ok && filepath.IsAbs(source) {
			path = filepath.Join(filepath.Dir(source), "food.db")
		} else {
			path = "food.db"
		}
	}
	return path, nil
}
func main() {
	dbFlag := flag.String("db", "", "existing SQLite file (default: food.db beside executable)")
	listen := flag.String("listen", env("HOST", "0.0.0.0")+":"+env("PORT", "8000"), "listen address")
	initNew := flag.Bool("init-empty", false, "explicitly allow creating an empty database")
	check := flag.Bool("check", false, "check database integrity and exit without starting HTTP")
	selftest := flag.Bool("selftest", false, "run isolated built-in compatibility smoke tests; never open the production database")
	flag.Parse()
	if *selftest {
		if e := runSelfTests(); e != nil {
			log.Fatal(e)
		}
		return
	}
	path := *dbFlag
	var e error
	if path == "" {
		path, e = defaultDB()
		if e != nil {
			log.Fatal(e)
		}
	}
	if _, e = os.Stat(path); e != nil && !(*initNew && os.IsNotExist(e)) {
		log.Fatalf("database not found: %s (use -db to select the existing file; -init-empty is only for a new installation)", path)
	}
	if *check {
		if e := checkDatabase(path); e != nil {
			log.Fatal(e)
		}
		fmt.Println("SQLite integrity and foreign keys: OK (read-only)")
		return
	}
	s, e := newServer(path)
	if e != nil {
		log.Fatal(e)
	}
	defer s.close()
	app := s.app()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := app.ShutdownWithContext(shutdownCtx); err != nil {
				log.Printf("shutdown: %v", err)
			}
		case <-done:
		}
	}()
	log.Printf("Food Diary API listening on %s; Swagger: /docs", *listen)
	e = app.Listen(*listen, fiber.ListenConfig{DisableStartupMessage: true})
	close(done)
	if e != nil {
		log.Printf("server: %v", e)
		os.Exit(1)
	}
}

func checkDatabase(path string) error {
	abs, e := filepath.Abs(path)
	if e != nil {
		return e
	}
	p := filepath.ToSlash(abs)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	u := url.URL{Scheme: "file", Path: p, RawQuery: "mode=ro&_pragma=busy_timeout(30000)"}
	db, e := sql.Open("sqlite", u.String())
	if e != nil {
		return e
	}
	defer db.Close()
	rows, e := db.Query("PRAGMA integrity_check")
	if e != nil {
		return e
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var result string
		if e = rows.Scan(&result); e != nil {
			return e
		}
		if result != "ok" {
			return errors.New("integrity_check failed: " + result)
		}
		count++
	}
	if e = rows.Err(); e != nil {
		return e
	}
	if count != 1 {
		return errors.New("integrity_check returned no result")
	}
	rows.Close()
	fk, e := db.Query("PRAGMA foreign_key_check")
	if e != nil {
		return e
	}
	defer fk.Close()
	if fk.Next() {
		return errors.New("foreign_key_check failed")
	}
	return fk.Err()
}

// Kept inside main.go so even a standalone release binary can verify itself.
func runSelfTests() error {
	dir, e := os.MkdirTemp("", "food-diary-selftest-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(dir)
	s, e := newServer(filepath.Join(dir, "food.db"))
	if e != nil {
		return e
	}
	defer s.close()
	if e = s.writer.Exec("INSERT INTO foods(name,calories) VALUES('apple',52),('invalid',0)").Error; e != nil {
		return e
	}
	app := s.app()
	checks := 0
	check := func(label string, ok bool) error {
		checks++
		if !ok {
			return fmt.Errorf("self-test failed: %s", label)
		}
		fmt.Println("[OK] " + label)
		return nil
	}
	call := func(method, path string, value any, token string) (int, object, error) {
		var b []byte
		var e error
		if value != nil {
			b, e = json.Marshal(value)
			if e != nil {
				return 0, nil, e
			}
		}
		r := httptest.NewRequest(method, path, bytes.NewReader(b))
		if value != nil {
			r.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		resp, e := app.Test(r, fiber.TestConfig{Timeout: 10 * time.Second})
		if e != nil {
			return 0, nil, e
		}
		defer resp.Body.Close()
		var data object
		e = json.NewDecoder(resp.Body).Decode(&data)
		return resp.StatusCode, data, e
	}
	reg := object{"phone": "selftest", "password": "secret🙂", "name": "Test", "gender": 0, "height": 178, "weight": 74, "birth_year": 1994}
	status, data, e := call("POST", "/auth/register", reg, "")
	if e != nil {
		return e
	}
	if e = check("register", status == 201); e != nil {
		return e
	}
	token, _ := data["access_token"].(string)
	status, _, e = call("POST", "/auth/register", reg, "")
	if e != nil {
		return e
	}
	if e = check("duplicate phone", status == 409); e != nil {
		return e
	}
	status, _, e = call("POST", "/auth/login", object{"phone": "selftest", "password": "secret🙂"}, "")
	if e != nil {
		return e
	}
	if e = check("login and PBKDF2", status == 200); e != nil {
		return e
	}
	status, _, e = call("GET", "/me", nil, "")
	if e != nil {
		return e
	}
	if e = check("authentication required", status == 401); e != nil {
		return e
	}
	status, data, e = call("GET", "/me", nil, token)
	if e != nil {
		return e
	}
	if e = check("JWT and profile", status == 200 && data["activity_level"] == float64(3)); e != nil {
		return e
	}
	status, _, e = call("PATCH", "/profile", object{}, token)
	if e != nil {
		return e
	}
	if e = check("empty profile patch", status == 400); e != nil {
		return e
	}
	status, data, e = call("PATCH", "/profile", object{"weight": 80, "gender": 0}, token)
	if e != nil {
		return e
	}
	if e = check("profile updates include zero values", status == 200 && data["weight"] == float64(80) && data["gender"] == float64(0)); e != nil {
		return e
	}
	meal := object{"meal_id": 1, "date": "2026-01-01", "foods": []object{{"food_id": 1, "amount": 2}, {"food_id": 0, "name": " cake ", "calories": 350, "amount": 1, "unit": " g "}}}
	status, first, e := call("POST", "/meals", meal, token)
	if e != nil {
		return e
	}
	if e = check("create meal and calorie total", status == 201 && first["total_calories"] == float64(454)); e != nil {
		return e
	}
	meal["foods"] = []object{{"food_id": 99999, "amount": 1}}
	status, _, e = call("POST", "/meals", meal, token)
	if e != nil {
		return e
	}
	if e = check("unknown food", status == 404); e != nil {
		return e
	}
	status, data, e = call("GET", "/meals?day=2026-01-01", nil, token)
	if e != nil {
		return e
	}
	meals, _ := data["meals"].([]any)
	if e = check("failed write is atomic", status == 200 && len(meals) == 1 && meals[0].(map[string]any)["total_calories"] == float64(454)); e != nil {
		return e
	}
	meal["foods"] = []object{{"food_id": 1, "amount": 3}}
	status, data, e = call("POST", "/meals", meal, token)
	if e != nil {
		return e
	}
	if e = check("replacement keeps id and created_at", status == 200 && data["id"] == first["id"] && data["created_at"] == first["created_at"] && data["total_calories"] == float64(156)); e != nil {
		return e
	}
	meal["foods"] = []object{{"food_id": 1, "amount": "3"}}
	status, _, e = call("POST", "/meals", meal, token)
	if e != nil {
		return e
	}
	if e = check("strict numeric validation", status == 422); e != nil {
		return e
	}
	for _, path := range []string{"/docs", "/openapi.json", "/docs/assets/swagger-ui-bundle.js", "/docs/assets/swagger-ui.css"} {
		resp, e := app.Test(httptest.NewRequest("GET", path, nil))
		if e != nil {
			return e
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if e = check("offline "+path, resp.StatusCode == 200); e != nil {
			return e
		}
	}
	if e = checkDatabase(filepath.Join(dir, "food.db")); e != nil {
		return e
	}
	if e = check("SQLite integrity and foreign keys", true); e != nil {
		return e
	}
	fmt.Printf("All %d self-tests passed. Production database was not opened.\n", checks)
	return nil
}

func (s *server) openAPI() []byte {
	var doc object
	if err := json.Unmarshal([]byte(openAPITemplate), &doc); err != nil {
		panic(err)
	}
	doc["components"].(map[string]any)["schemas"].(map[string]any)["RegisterIn"].(map[string]any)["properties"].(map[string]any)["birth_year"].(map[string]any)["maximum"] = s.birthMax
	b, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return b
}

const openAPITemplate = `{"openapi":"3.1.0","info":{"title":"Food Diary API","version":"1.0","description":"Compatible Food Diary API. Swagger UI assets are served locally. Keep the original JWT_SECRET, APP_TIMEZONE and SQLite database when replacing the Python service."},"paths":{"/auth/register":{"post":{"summary":"Register","operationId":"register_auth_register_post","requestBody":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/RegisterIn"}}},"required":true},"responses":{"201":{"description":"Successful Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Token"}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}},"409":{"description":"Phone already registered","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}}},"tags":["Authentication"]}},"/auth/login":{"post":{"summary":"Login","operationId":"login_auth_login_post","requestBody":{"content":{"application/json":{"schema":{"$ref":"#/components/schemas/LoginIn"}}},"required":true},"responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Token"}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}},"401":{"description":"Authentication failed","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}}},"tags":["Authentication"]}},"/me":{"get":{"summary":"Get Me","operationId":"get_me_me_get","parameters":[],"responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/User"}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}},"401":{"description":"Authentication failed","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}}},"tags":["Profile"],"security":[{"BearerAuth":[]}]}},"/profile":{"get":{"summary":"Get Profile","operationId":"get_profile_profile_get","parameters":[],"responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/User"}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}},"401":{"description":"Authentication failed","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}}},"tags":["Profile"],"security":[{"BearerAuth":[]}]},"patch":{"summary":"Update Profile","operationId":"update_profile_profile_patch","parameters":[],"requestBody":{"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/ProfileUpdate"}}}},"responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/User"}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}},"401":{"description":"Authentication failed","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}},"400":{"description":"No profile changes","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}},"404":{"description":"Record not found","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}}},"tags":["Profile"],"security":[{"BearerAuth":[]}]}},"/foods":{"get":{"summary":"List Foods","operationId":"list_foods_foods_get","parameters":[{"name":"limit","in":"query","required":false,"schema":{"type":"integer","maximum":1000,"minimum":1,"default":100,"title":"Limit"}},{"name":"offset","in":"query","required":false,"schema":{"type":"integer","minimum":0,"default":0,"title":"Offset"}}],"responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"type":"array","items":{"$ref":"#/components/schemas/Food"}}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}}},"tags":["Foods"]}},"/foods/search":{"get":{"summary":"Foods Search","operationId":"foods_search_foods_search_get","parameters":[{"name":"q","in":"query","required":true,"schema":{"type":"string","minLength":1,"maxLength":100,"title":"Q"}}],"responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"type":"array","items":{"$ref":"#/components/schemas/Food"}}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}}},"tags":["Foods"]}},"/meals":{"post":{"summary":"Add Meal","operationId":"add_meal_meals_post","parameters":[],"requestBody":{"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/MealIn"}}}},"responses":{"201":{"description":"Successful Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Meal"}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}},"200":{"description":"Existing meal replaced atomically; id and created_at remain unchanged","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Meal"}}}},"401":{"description":"Authentication failed","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}},"404":{"description":"Record not found","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}}},"tags":["Meals"],"security":[{"BearerAuth":[]}],"description":"Replaces all items for the authenticated user, date and meal type. Defaults to today in APP_TIMEZONE. Catalog foods use cached or parsed calories and unit serving. Custom foods (food_id=0) require name, calories and unit. Any failure rolls back the entire request."},"get":{"summary":"Get Meals","operationId":"get_meals_meals_get","parameters":[{"name":"day","in":"query","required":false,"schema":{"anyOf":[{"type":"string","format":"date"},{"type":"null"}],"title":"Day"}}],"responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Meals"}}}},"422":{"description":"Validation Error","content":{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/HTTPValidationError"},{"$ref":"#/components/schemas/Error"}]}}}},"401":{"description":"Authentication failed","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Error"}}}}},"tags":["Meals"],"security":[{"BearerAuth":[]}]}},"/":{"get":{"summary":"Root","operationId":"root__get","responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{"type":"object","properties":{"message":{"type":"string"},"docs":{"type":"string"}},"required":["message","docs"]}}}}},"tags":["Service"]}}},"components":{"schemas":{"HTTPValidationError":{"properties":{"detail":{"items":{"$ref":"#/components/schemas/ValidationError"},"type":"array","title":"Detail"}},"type":"object","title":"HTTPValidationError"},"LoginIn":{"properties":{"phone":{"type":"string","maxLength":32,"minLength":1,"title":"Phone"},"password":{"type":"string","maxLength":256,"minLength":1,"title":"Password"}},"additionalProperties":false,"type":"object","required":["phone","password"],"title":"LoginIn"},"MealIn":{"properties":{"meal_id":{"type":"integer","maximum":4.0,"minimum":1.0,"title":"Meal Id","description":"1 breakfast, 2 lunch, 3 dinner, 4 snack"},"foods":{"items":{"$ref":"#/components/schemas/MealItemIn"},"type":"array","maxItems":100,"minItems":1,"title":"Foods"},"date":{"anyOf":[{"type":"string","format":"date"},{"type":"null"}],"title":"Date"}},"additionalProperties":false,"type":"object","required":["meal_id","foods"],"title":"MealIn"},"MealItemIn":{"properties":{"food_id":{"type":"integer","maximum":2147483647.0,"minimum":0.0,"title":"Food Id"},"amount":{"type":"number","maximum":100000.0,"exclusiveMinimum":0.0,"title":"Amount"},"calories":{"anyOf":[{"type":"number","maximum":100000.0,"minimum":0.0},{"type":"null"}],"title":"Calories"},"name":{"anyOf":[{"type":"string","maxLength":100},{"type":"null"}],"title":"Name"},"unit":{"anyOf":[{"type":"string","maxLength":32},{"type":"null"}],"title":"Unit"}},"additionalProperties":false,"type":"object","required":["food_id","amount"],"title":"MealItemIn"},"ProfileUpdate":{"properties":{"height":{"anyOf":[{"type":"integer","maximum":300.0,"exclusiveMinimum":0.0},{"type":"null"}],"title":"Height"},"weight":{"anyOf":[{"type":"integer","maximum":700.0,"exclusiveMinimum":0.0},{"type":"null"}],"title":"Weight"},"age":{"anyOf":[{"type":"integer","maximum":130.0,"exclusiveMinimum":0.0},{"type":"null"}],"title":"Age"},"gender":{"anyOf":[{"type":"integer","maximum":1.0,"minimum":0.0},{"type":"null"}],"title":"Gender"},"activity_level":{"anyOf":[{"type":"integer","maximum":5.0,"minimum":1.0},{"type":"null"}],"title":"Activity Level"}},"additionalProperties":false,"type":"object","title":"ProfileUpdate"},"RegisterIn":{"properties":{"phone":{"type":"string","maxLength":32,"minLength":1,"title":"Phone"},"password":{"type":"string","maxLength":256,"minLength":1,"title":"Password"},"gender":{"type":"integer","maximum":1.0,"minimum":0.0,"title":"Gender"},"height":{"type":"integer","maximum":300.0,"exclusiveMinimum":0.0,"title":"Height"},"weight":{"type":"integer","maximum":700.0,"exclusiveMinimum":0.0,"title":"Weight"},"name":{"type":"string","maxLength":100,"minLength":1,"title":"Name"},"birth_year":{"type":"integer","maximum":2026.0,"exclusiveMinimum":1900.0,"title":"Birth Year"},"activity_level":{"type":"integer","maximum":5.0,"minimum":1.0,"title":"Activity Level","default":3}},"additionalProperties":false,"type":"object","required":["phone","password","gender","height","weight","name","birth_year"],"title":"RegisterIn"},"ValidationError":{"properties":{"loc":{"items":{"anyOf":[{"type":"string"},{"type":"integer"}]},"type":"array","title":"Location"},"msg":{"type":"string","title":"Message"},"type":{"type":"string","title":"Error Type"},"input":{"title":"Input"},"ctx":{"type":"object","title":"Context"}},"type":"object","required":["loc","msg","type"],"title":"ValidationError"},"Token":{"type":"object","properties":{"access_token":{"type":"string"},"token_type":{"type":"string","const":"bearer"}},"required":["access_token","token_type"]},"User":{"type":"object","properties":{"id":{"type":"integer"},"gender":{"type":"integer"},"height":{"type":"integer"},"weight":{"type":"integer"},"birth_year":{"type":"integer"},"activity_level":{"type":"integer"},"age":{"type":"integer"},"phone":{"type":"string"},"name":{"type":"string"}},"required":["id","gender","height","weight","birth_year","activity_level","age","phone","name"]},"Food":{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":["string","null"]},"calories":{"type":["string","number","null"]}},"required":["id","name","calories"]},"MealItem":{"type":"object","properties":{"id":{"type":"integer"},"meal_id":{"type":"integer"},"food_id":{"type":"integer"},"food_name":{"type":"string"},"unit":{"type":"string"},"calories":{"type":"number"},"amount":{"type":"number"}},"required":["id","meal_id","food_id","food_name","unit","calories","amount"]},"Meal":{"type":"object","properties":{"id":{"type":"integer"},"user_id":{"type":"integer"},"meal_id":{"type":"integer"},"date":{"type":"string"},"meal_name":{"type":"string"},"created_at":{"type":"string"},"updated_at":{"type":"string"},"total_calories":{"type":"number"},"foods":{"type":"array","items":{"$ref":"#/components/schemas/MealItem"}}},"required":["id","user_id","meal_id","date","meal_name","created_at","updated_at","total_calories","foods"]},"Meals":{"type":"object","properties":{"date":{"type":["string","null"],"format":"date"},"meals":{"type":"array","items":{"$ref":"#/components/schemas/Meal"}}},"required":["date","meals"]},"Error":{"type":"object","properties":{"detail":{"type":"string"}},"required":["detail"]}},"securitySchemes":{"BearerAuth":{"type":"http","scheme":"bearer","bearerFormat":"JWT"}}}}`
