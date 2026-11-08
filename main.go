// Food Diary API — a single-file Go port of main.py.
//
// Run:      go run . -db food.db
// Build:    go build -o food-diary .
// Swagger:  http://localhost:8000/docs
//
// The SQLite schema, rowids, password hashes, JWTs and API responses are kept
// compatible with the FastAPI/SQLAlchemy service. GORM is used for all data
// access; migrations are explicit so an existing food.db is never recreated.
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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
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
	"unicode/utf8"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	_ "modernc.org/sqlite"
)

const (
	tokenDays       = 30
	maxCalories     = 100_000.0
	maxAmount       = 100_000.0
	maxFoodsPerMeal = 100
	maxBodyBytes    = 64 << 20
	minDate         = "1900-01-01"
	maxDate         = "2100-12-31"
)

var mealNames = map[int64]string{1: "breakfast", 2: "lunch", 3: "dinner", 4: "snack"}

type object = map[string]any

type apiError struct {
	status int
	detail any
}

func (e *apiError) Error() string          { return fmt.Sprint(e.detail) }
func failure(status int, detail any) error { return &apiError{status, detail} }

// --------------------------------------------------------------------------- //
// GORM models
// --------------------------------------------------------------------------- //

type User struct {
	ID            int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Phone         string `gorm:"column:phone"`
	PasswordHash  string `gorm:"column:password_hash"`
	Gender        int64  `gorm:"column:gender"`
	Height        int64  `gorm:"column:height"`
	Weight        int64  `gorm:"column:weight"`
	Name          string `gorm:"column:name"`
	BirthYear     int64  `gorm:"column:birth_year"`
	ActivityLevel int64  `gorm:"column:activity_level"`
}

func (User) TableName() string { return "users" }

type Meal struct {
	ID        int64  `gorm:"column:id;primaryKey;autoIncrement"`
	UserID    int64  `gorm:"column:user_id"`
	Date      string `gorm:"column:date"`
	MealType  int64  `gorm:"column:meal_type"`
	CreatedAt string `gorm:"column:created_at"`
	UpdatedAt string `gorm:"column:updated_at"`
}

func (Meal) TableName() string { return "meals" }

type MealItem struct {
	ID       int64   `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	MealID   int64   `json:"meal_id" gorm:"column:meal_id"`
	FoodID   int64   `json:"food_id" gorm:"column:food_id"`
	FoodName string  `json:"food_name" gorm:"column:food_name"`
	Calories float64 `json:"calories" gorm:"column:calories"`
	Amount   float64 `json:"amount" gorm:"column:amount"`
	Unit     string  `json:"unit" gorm:"column:unit"`
}

func (MealItem) TableName() string { return "meal_items" }

type server struct {
	db, writer             *gorm.DB
	tz                     *time.Location
	secret                 []byte
	birthMax               int
	idCol, nameCol, calCol string
	writeMu                sync.Mutex
	spec                   []byte
}

// --------------------------------------------------------------------------- //
// Database connection and migrations
// --------------------------------------------------------------------------- //

func openDB(path string, write bool) (*gorm.DB, error) {
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

	db, err := gorm.Open(sqlite.New(sqlite.Config{DriverName: "sqlite", DSN: u.String()}), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger:                 logger.Default.LogMode(logger.Silent),
		DisableAutomaticPing:   true,
	})
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
	err := db.Raw("PRAGMA table_info('" + table + "')").Scan(&rows).Error
	result := map[string]bool{}
	for _, row := range rows {
		result[row.Name] = true
	}
	return result, err
}

// bootstrap mirrors init_db() in main.py. It is idempotent and safe to run on
// both the current schema and the older single-table "meals" schema.
func bootstrap(db *gorm.DB) error {
	return db.Connection(func(conn *gorm.DB) error {
		if err := conn.Exec("PRAGMA foreign_keys=OFF").Error; err != nil {
			return err
		}
		defer conn.Exec("PRAGMA foreign_keys=ON")
		if err := conn.Exec("PRAGMA legacy_alter_table=ON").Error; err != nil {
			return err
		}
		defer conn.Exec("PRAGMA legacy_alter_table=OFF")

		return conn.Transaction(func(tx *gorm.DB) error {
			exec := func(query string) error { return tx.Exec(query).Error }

			var names []string
			if err := tx.Raw("SELECT name FROM sqlite_master WHERE type='table'").Scan(&names).Error; err != nil {
				return err
			}
			tables := map[string]bool{}
			for _, name := range names {
				tables[name] = true
			}

			if !tables["foods"] {
				if err := exec("CREATE TABLE foods (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, calories REAL NOT NULL)"); err != nil {
					return err
				}
			}

			if !tables["users"] {
				if err := exec(createUsers); err != nil {
					return err
				}
			} else {
				var ddl string
				if err := tx.Raw("SELECT sql FROM sqlite_master WHERE name='users'").Scan(&ddl).Error; err != nil {
					return err
				}
				if !strings.Contains(ddl, "gender IN (0,1)") {
					cols, err := columns(tx, "users")
					if err != nil {
						return err
					}
					activity := "3"
					if cols["activity_level"] {
						activity = "activity_level"
					}
					for _, query := range []string{
						"ALTER TABLE users RENAME TO users_old",
						createUsers,
						"INSERT INTO users(id,phone,password_hash,gender,height,weight,name,birth_year,activity_level) " +
							"SELECT id,phone,password_hash,CASE WHEN gender IN (0,1) THEN gender ELSE 0 END,height,weight,name,birth_year," + activity + " FROM users_old",
						"DROP TABLE users_old",
					} {
						if err := exec(query); err != nil {
							return err
						}
					}
				}
				cols, err := columns(tx, "users")
				if err != nil {
					return err
				}
				if !cols["activity_level"] {
					if err := exec("ALTER TABLE users ADD COLUMN activity_level INTEGER NOT NULL DEFAULT 3"); err != nil {
						return err
					}
				}
			}

			cols, err := columns(tx, "meals")
			if err != nil {
				return err
			}
			if cols["food_name"] {
				for _, query := range []string{
					"ALTER TABLE meals RENAME TO meals_legacy",
					createMeals,
					createItems,
					"INSERT INTO meals(user_id,date,meal_type,created_at,updated_at) " +
						"SELECT user_id,substr(eaten_at,1,10),meal_type,min(eaten_at),max(eaten_at) FROM meals_legacy GROUP BY user_id,substr(eaten_at,1,10),meal_type",
					"INSERT INTO meal_items(meal_id,food_id,food_name,calories,amount,unit) " +
						"SELECT m.id,l.food_id,l.food_name,l.calories,l.amount,l.unit FROM meals_legacy l " +
						"JOIN meals m ON m.user_id=l.user_id AND m.meal_type=l.meal_type AND m.date=substr(l.eaten_at,1,10) ORDER BY l.id",
					"DROP TABLE meals_legacy",
				} {
					if err := exec(query); err != nil {
						return err
					}
				}
			} else {
				if !tables["meals"] {
					if err := exec(createMeals); err != nil {
						return err
					}
				}
				if !tables["meal_items"] {
					if err := exec(createItems); err != nil {
						return err
					}
				}
			}
			if err := exec("CREATE INDEX IF NOT EXISTS idx_meal_items_meal_id ON meal_items(meal_id)"); err != nil {
				return err
			}

			cols, err = columns(tx, "foods")
			if err != nil {
				return err
			}
			if !cols["calories_cached"] {
				if err := exec("ALTER TABLE foods ADD COLUMN calories_cached REAL"); err != nil {
					return err
				}
			}

			rows, err := tx.Raw("PRAGMA foreign_key_check").Rows()
			if err != nil {
				return err
			}
			defer rows.Close()
			violations := 0
			for rows.Next() {
				violations++
			}
			if err := rows.Err(); err != nil {
				return err
			}
			if violations > 0 {
				log.Printf("warning: PRAGMA foreign_key_check reported %d violation(s)", violations)
			}
			return nil
		})
	})
}

func newServer(path string) (*server, error) {
	writer, err := openDB(path, true)
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		if pool, err := writer.DB(); err == nil {
			pool.Close()
		}
	}
	if err = bootstrap(writer); err != nil {
		cleanup()
		return nil, err
	}
	db, err := openDB(path, false)
	if err != nil {
		cleanup()
		return nil, err
	}
	tz, err := time.LoadLocation(env("APP_TIMEZONE", "Asia/Tehran"))
	if err != nil {
		tz = time.UTC
	}
	s := &server{
		db:       db,
		writer:   writer,
		tz:       tz,
		secret:   []byte(env("JWT_SECRET", "change-this-secret-before-public-use")),
		birthMax: time.Now().Year(),
		idCol:    "rowid",
		nameCol:  `"name"`,
		calCol:   `"calories"`,
	}
	cols, err := columns(db, "foods")
	if err != nil {
		s.close()
		return nil, err
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
	s.spec = openAPISpec()
	return s, nil
}

func (s *server) close() {
	for _, db := range []*gorm.DB{s.db, s.writer} {
		if db == nil {
			continue
		}
		if pool, err := db.DB(); err == nil {
			pool.Close()
		}
	}
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// --------------------------------------------------------------------------- //
// Authentication
// --------------------------------------------------------------------------- //

func (s *server) token(uid int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, `{"sub": %d, "exp": %d}`, uid, time.Now().UTC().Add(tokenDays*24*time.Hour).Unix()))
	signing := header + "." + payload
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func passwordHash(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash, err := pbkdf2.Key(sha256.New, password, salt, 200_000, 32)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(salt) + "$" + base64.RawURLEncoding.EncodeToString(hash), nil
}

func decode64(value string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(value, "="))
}

func verifyPassword(password, stored string) bool {
	parts := strings.Split(stored, "$")
	if len(parts) != 2 {
		return false
	}
	salt, err := decode64(parts[0])
	if err != nil {
		return false
	}
	expected, err := decode64(parts[1])
	if err != nil {
		return false
	}
	hash, err := pbkdf2.Key(sha256.New, password, salt, 200_000, 32)
	return err == nil && hmac.Equal(expected, hash)
}

func (s *server) authenticate(r *http.Request) (*User, error) {
	authorization := r.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
		return nil, failure(401, "توکن لازم است")
	}
	bad := func() (*User, error) { return nil, failure(401, "توکن نامعتبر یا منقضی است") }

	fields := strings.FieldsFunc(authorization, unicode.IsSpace)
	if len(fields) < 2 {
		return bad()
	}
	token := strings.TrimLeftFunc(authorization[len(fields[0]):], unicode.IsSpace)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return bad()
	}
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal([]byte(parts[2]), []byte(base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))) {
		return bad()
	}
	payloadBytes, err := decode64(parts[1])
	if err != nil {
		return bad()
	}
	var payload object
	decoder := json.NewDecoder(bytes.NewReader(payloadBytes))
	decoder.UseNumber()
	if decoder.Decode(&payload) != nil {
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
	err = s.db.Where("id = ?", uid).First(&user).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, failure(401, "کاربر این توکن دیگر وجود ندارد")
	}
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func userJSON(user *User) object {
	return object{
		"id":             user.ID,
		"phone":          user.Phone,
		"name":           user.Name,
		"gender":         user.Gender,
		"height":         user.Height,
		"weight":         user.Weight,
		"birth_year":     user.BirthYear,
		"activity_level": user.ActivityLevel,
		"age":            int64(time.Now().Year()) - user.BirthYear,
	}
}

func pythonInt(value any) (int64, bool) {
	switch x := value.(type) {
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n, err == nil
	case json.Number:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) || f >= math.Exp2(63) || f < -math.Exp2(63) {
			return 0, false
		}
		return int64(f), true
	}
	return 0, false
}

func number(value any) (float64, bool) {
	switch x := value.(type) {
	case json.Number:
		n, err := strconv.ParseFloat(string(x), 64)
		return n, err == nil
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

// --------------------------------------------------------------------------- //
// Request body and Pydantic-compatible validation
// --------------------------------------------------------------------------- //

func readBody(w http.ResponseWriter, r *http.Request) (any, error) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return nil, failure(400, "There was an error parsing the body")
	}
	if len(raw) == 0 {
		return nil, nil
	}
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	if contentType != "application/json" && !strings.HasSuffix(contentType, "+json") {
		// FastAPI treats a non-JSON body as a plain string, which then fails
		// model validation with 422.
		return string(raw), nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, failure(422, []object{issue("json_invalid", []any{"body"}, "JSON decode error", nil, nil)})
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, failure(422, []object{issue("json_invalid", []any{"body"}, "JSON decode error", nil, nil)})
	}
	return value, nil
}

type rule struct {
	name, kind                string
	optional, nullable        bool
	min, max                  float64
	hasMin, hasMax, exclusive bool
	def                       any
}

func strRule(name string, minLength, maxLength int, optional bool) rule {
	return rule{name: name, kind: "string", optional: optional, nullable: optional,
		min: float64(minLength), max: float64(maxLength), hasMin: minLength > 0, hasMax: true}
}

func intRule(name string, minValue, maxValue int, exclusive, optional bool) rule {
	return rule{name: name, kind: "int", optional: optional, nullable: optional,
		min: float64(minValue), max: float64(maxValue), hasMin: true, hasMax: true, exclusive: exclusive}
}

func (s *server) rules(model string) []rule {
	switch model {
	case "register":
		rules := []rule{
			strRule("phone", 1, 32, false),
			strRule("password", 1, 256, false),
			intRule("gender", 0, 1, false, false),
			intRule("height", 0, 300, true, false),
			intRule("weight", 0, 700, true, false),
			strRule("name", 1, 100, false),
			intRule("birth_year", 1900, s.birthMax, true, false),
			intRule("activity_level", 1, 5, false, true),
		}
		rules[7].nullable = false
		rules[7].def = json.Number("3")
		return rules
	case "login":
		return []rule{strRule("phone", 1, 32, false), strRule("password", 1, 256, false)}
	case "profile":
		return []rule{
			intRule("height", 0, 300, true, true),
			intRule("weight", 0, 700, true, true),
			intRule("age", 0, 130, true, true),
			intRule("gender", 0, 1, false, true),
			intRule("activity_level", 1, 5, false, true),
		}
	case "meal":
		return []rule{
			intRule("meal_id", 1, 4, false, false),
			{name: "foods", kind: "list"},
			{name: "date", kind: "date", optional: true, nullable: true},
		}
	case "item":
		return []rule{
			intRule("food_id", 0, 2_147_483_647, false, false),
			{name: "amount", kind: "float", hasMin: true, exclusive: true, hasMax: true, max: maxAmount},
			{name: "calories", kind: "float", optional: true, nullable: true, hasMin: true, hasMax: true, max: maxCalories},
			strRule("name", 0, 100, true),
			strRule("unit", 0, 32, true),
		}
	}
	return nil
}

func issue(kind string, loc []any, msg string, input any, ctx object) object {
	result := object{"type": kind, "loc": loc, "msg": msg, "input": input}
	if ctx != nil {
		result["ctx"] = ctx
	}
	return result
}

func location(loc []any, key any) []any {
	return append(append([]any{}, loc...), key)
}

func plural(n float64) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func (s *server) validate(model string, input any, loc []any) (object, []object) {
	errorsList := []object{}
	obj, ok := input.(map[string]any)
	if !ok {
		return nil, []object{issue("model_attributes_type", loc, "Input should be a valid dictionary or object to extract fields from", input, nil)}
	}
	result := object{}
	known := map[string]bool{}
	for _, r := range s.rules(model) {
		known[r.name] = true
		value, present := obj[r.name]
		fieldLoc := location(loc, r.name)
		add := func(kind, msg string, ctx object) {
			errorsList = append(errorsList, issue(kind, fieldLoc, msg, value, ctx))
		}
		if !present {
			if !r.optional {
				value = obj
				add("missing", "Field required", nil)
			} else if r.def != nil {
				result[r.name] = r.def
			}
			continue
		}
		if value == nil && r.nullable {
			result[r.name] = nil
			continue
		}
		switch r.kind {
		case "string":
			text, ok := value.(string)
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
			result[r.name] = value
		case "int", "float":
			num, isNumber := value.(json.Number)
			if !isNumber || (r.kind == "int" && strings.ContainsAny(string(num), ".eE")) {
				kind, msg := "int_type", "Input should be a valid integer"
				if r.kind == "float" {
					kind, msg = "float_type", "Input should be a valid number"
				}
				add(kind, msg, nil)
				continue
			}
			f, err := strconv.ParseFloat(string(num), 64)
			if r.kind == "float" && (err != nil || math.IsInf(f, 0) || math.IsNaN(f)) {
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
			result[r.name] = value
		case "list":
			arr, ok := value.([]any)
			if !ok {
				add("list_type", "Input should be a valid list", nil)
				continue
			}
			if len(arr) > maxFoodsPerMeal {
				add("too_long", fmt.Sprintf("List should have at most %d items after validation, not %d", maxFoodsPerMeal, len(arr)),
					object{"field_type": "List", "max_length": maxFoodsPerMeal, "actual_length": len(arr)})
				continue
			}
			if len(arr) == 0 {
				add("too_short", "List should have at least 1 item after validation, not 0",
					object{"field_type": "List", "min_length": 1, "actual_length": 0})
				continue
			}
			values := make([]any, 0, len(arr))
			for i, item := range arr {
				itemValue, itemErrors := s.validate("item", item, location(fieldLoc, i))
				values = append(values, itemValue)
				errorsList = append(errorsList, itemErrors...)
			}
			result[r.name] = values
		case "date":
			dateValue, dateError := parseDate(value, fieldLoc)
			if dateError != nil {
				errorsList = append(errorsList, dateError)
				continue
			}
			if dateValue < minDate {
				add("greater_than_equal", "Input should be greater than or equal to "+minDate, object{"ge": minDate})
				continue
			}
			if dateValue > maxDate {
				add("less_than_equal", "Input should be less than or equal to "+maxDate, object{"le": maxDate})
				continue
			}
			result[r.name] = dateValue
		}
	}
	for key := range obj {
		if !known[key] {
			errorsList = append(errorsList, issue("extra_forbidden", location(loc, key), "Extra inputs are not permitted", obj[key], nil))
		}
	}
	return result, errorsList
}

func (s *server) validated(model string, value any) (object, error) {
	if value == nil {
		return nil, failure(422, []object{issue("missing", []any{"body"}, "Field required", nil, nil)})
	}
	result, errs := s.validate(model, value, []any{"body"})
	if len(errs) > 0 {
		return nil, failure(422, errs)
	}
	return result, nil
}

var dateNumericRE = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

var dateLayouts = []string{
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04Z07:00",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02t15:04:05.999999999Z07:00",
	"2006-01-02t15:04:05Z07:00",
	"2006-01-02t15:04Z07:00",
	"2006-01-02t15:04:05.999999999",
	"2006-01-02t15:04:05",
	"2006-01-02t15:04",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02_15:04:05.999999999Z07:00",
	"2006-01-02_15:04:05Z07:00",
	"2006-01-02_15:04Z07:00",
	"2006-01-02_15:04:05.999999999",
	"2006-01-02_15:04:05",
	"2006-01-02_15:04",
}

func parseDate(value any, loc []any) (string, object) {
	bad := func(reason string) (string, object) {
		return "", issue("date_from_datetime_parsing", loc, "Input should be a valid date or datetime, "+reason, value, object{"error": reason})
	}
	inexact := func() (string, object) {
		return "", issue("date_from_datetime_inexact", loc, "Datetimes provided to dates should have zero time - e.g. be exact dates", value, nil)
	}
	yearZero := func() (string, object) {
		return "", issue("date_parsing", loc, "Input should be a valid date in the format YYYY-MM-DD, year 0 is out of range", value, object{"error": "year 0 is out of range"})
	}

	var text string
	switch x := value.(type) {
	case string:
		text = x
	case json.Number:
		text = string(x)
	default:
		return "", issue("date_type", loc, "Input should be a valid date", value, nil)
	}

	if t, err := time.Parse("2006-01-02", text); err == nil {
		if t.Year() == 0 {
			return yearZero()
		}
		return t.Format("2006-01-02"), nil
	}
	for _, layout := range dateLayouts {
		t, err := time.Parse(layout, text)
		if err != nil {
			continue
		}
		// Pydantic checks the wall-clock time of the parsed datetime, not
		// its UTC equivalent, when converting a datetime to a date.
		if t.Year() == 0 {
			return yearZero()
		}
		if t.Hour() != 0 || t.Minute() != 0 || t.Second() != 0 || t.Nanosecond() != 0 {
			return inexact()
		}
		return t.Format("2006-01-02"), nil
	}
	if dateNumericRE.MatchString(text) {
		stamp, err := strconv.ParseFloat(text, 64)
		if err == nil && !math.IsNaN(stamp) && !math.IsInf(stamp, 0) {
			if math.Abs(stamp) > 2e10 {
				stamp /= 1000
			}
			if stamp < -62167219200 {
				return bad("dates before 0000 are not supported as unix timestamps")
			}
			if stamp >= 253402300800 {
				return bad("dates after 9999 are not supported as unix timestamps")
			}
			seconds, fraction := math.Modf(stamp)
			t := time.Unix(int64(seconds), int64(math.Round(fraction*1e6))*1000).UTC()
			if t.Year() == 0 {
				return yearZero()
			}
			if t.Hour() != 0 || t.Minute() != 0 || t.Second() != 0 || t.Nanosecond() != 0 {
				return inexact()
			}
			return t.Format("2006-01-02"), nil
		}
	}
	return bad("invalid datetime")
}

// --------------------------------------------------------------------------- //
// Food catalogue helpers
// --------------------------------------------------------------------------- //

func decimalDigits(value string) string {
	return strings.Map(func(r rune) rune {
		for _, r16 := range unicode.Nd.R16 {
			if r >= rune(r16.Lo) && r <= rune(r16.Hi) && (r-rune(r16.Lo))%rune(r16.Stride) == 0 {
				return '0' + (r-rune(r16.Lo))/rune(r16.Stride)%10
			}
		}
		for _, r32 := range unicode.Nd.R32 {
			if r >= rune(r32.Lo) && r <= rune(r32.Hi) && (r-rune(r32.Lo))%rune(r32.Stride) == 0 {
				return '0' + (r-rune(r32.Lo))/rune(r32.Stride)%10
			}
		}
		return r
	}, value)
}

var caloriesRE = regexp.MustCompile(`[0-9]+(?:\.[0-9]+)?`)

func parseCalories(raw any) (float64, bool) {
	rawText := fmt.Sprint(raw)
	switch x := raw.(type) {
	case []byte:
		rawText = string(x)
	case float64:
		format := byte('f')
		if x != 0 && (math.Abs(x) < 1e-4 || math.Abs(x) >= 1e16) {
			format = 'e'
		}
		rawText = strconv.FormatFloat(x, format, -1, 64)
	}
	text := strings.ReplaceAll(decimalDigits(rawText), ",", "")
	matches := caloriesRE.FindAllString(text, -1)
	if len(matches) == 0 {
		return 0, false
	}
	sum := 0.0
	for _, match := range matches {
		n, err := strconv.ParseFloat(match, 64)
		if err != nil {
			return 0, false
		}
		sum += n
	}
	sum /= float64(len(matches))
	return sum, !math.IsNaN(sum) && !math.IsInf(sum, 0) && sum >= 0
}

func normalizeDBValue(value any) any {
	if bytesValue, ok := value.([]byte); ok {
		return string(bytesValue)
	}
	return value
}

func (s *server) foodSelect() string {
	return "SELECT " + s.idCol + " AS id," + s.nameCol + " AS name," + s.calCol + " AS calories FROM foods"
}

func foodRows(db *gorm.DB, query string, args ...any) ([]object, error) {
	rows, err := db.Raw(query, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []object{}
	for rows.Next() {
		var id, name, calories any
		if err = rows.Scan(&id, &name, &calories); err != nil {
			return nil, err
		}
		result = append(result, object{
			"id":       normalizeDBValue(id),
			"name":     normalizeDBValue(name),
			"calories": normalizeDBValue(calories),
		})
	}
	return result, rows.Err()
}

func (s *server) search(db *gorm.DB, query string) ([]object, error) {
	normalized := decimalDigits(query)
	digits := normalized != ""
	for _, r := range normalized {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}
	if digits {
		id, err := strconv.ParseInt(normalized, 10, 64)
		if err != nil {
			// No SQLite rowid can match an integer outside int64.
			return []object{}, nil
		}
		return foodRows(db, s.foodSelect()+" WHERE "+s.idCol+" = ? LIMIT 1", id)
	}
	return foodRows(db, s.foodSelect()+" WHERE "+s.nameCol+" LIKE ? LIMIT 100", "%"+query+"%")
}

func pyStrip(value string) string {
	return strings.TrimFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || (r >= 0x1c && r <= 0x1f)
	})
}

func (s *server) resolve(tx *gorm.DB, item object) (MealItem, error) {
	result := MealItem{FoodID: asInt(item, "food_id"), Amount: asFloat(item, "amount")}
	if result.FoodID == 0 {
		result.FoodName = pyStrip(asString(item, "name"))
		result.Unit = pyStrip(asString(item, "unit"))
		if result.FoodName == "" || result.Unit == "" || item["calories"] == nil {
			return result, failure(422, "برای غذای دلخواه name، calories و unit لازم است")
		}
		result.Calories = asFloat(item, "calories")
		if result.Calories > maxCalories {
			return result, failure(422, "مقدار کالری غیرمنطقی است")
		}
		return result, nil
	}

	rows, err := s.search(tx, strconv.FormatInt(result.FoodID, 10))
	if err != nil {
		return result, err
	}
	if len(rows) == 0 {
		return result, failure(404, "غذا پیدا نشد")
	}
	if rows[0]["name"] == nil {
		return result, failure(422, "غذای انتخاب‌شده در دیتابیس نامعتبر است")
	}
	result.FoodName = fmt.Sprint(rows[0]["name"])
	result.Unit = "serving"

	var cached sql.NullFloat64
	err = tx.Raw("SELECT calories_cached FROM foods WHERE "+s.idCol+" = ?", result.FoodID).Row().Scan(&cached)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	bad := failure(422, "مقدار کالری این غذا در دیتابیس معتبر نیست")
	if !cached.Valid {
		calories, ok := parseCalories(rows[0]["calories"])
		if !ok {
			return result, bad
		}
		cached = sql.NullFloat64{Float64: calories, Valid: true}
		if err = tx.Exec("UPDATE foods SET calories_cached = ? WHERE "+s.idCol+" = ?", calories, result.FoodID).Error; err != nil {
			return result, err
		}
	}
	if cached.Float64 > maxCalories {
		return result, bad
	}
	result.Calories = cached.Float64
	return result, nil
}

func serializeMeal(meal Meal, items []MealItem) object {
	if items == nil {
		items = []MealItem{}
	}
	total := 0.0
	for _, item := range items {
		total += item.Calories * item.Amount
	}
	return object{
		"id":             meal.ID,
		"user_id":        meal.UserID,
		"date":           meal.Date,
		"meal_id":        meal.MealType,
		"meal_name":      mealNames[meal.MealType],
		"foods":          items,
		"created_at":     meal.CreatedAt,
		"updated_at":     meal.UpdatedAt,
		"total_calories": total,
	}
}

func asInt(values object, key string) int64 {
	n, _ := pythonInt(values[key])
	return n
}

func asFloat(values object, key string) float64 {
	n, _ := number(values[key])
	return n
}

func asString(values object, key string) string {
	value, _ := values[key].(string)
	return value
}

// --------------------------------------------------------------------------- //
// HTTP handlers
// --------------------------------------------------------------------------- //

type handlerFunc func(http.ResponseWriter, *http.Request) error

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		log.Printf("json encode failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (s *server) wrap(handler handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("panic: %s %s: %v", r.Method, r.URL.Path, recovered)
				writeJSON(w, http.StatusInternalServerError, object{"detail": "Internal Server Error"})
			}
		}()
		if err := handler(w, r); err != nil {
			var apiErr *apiError
			if errors.As(err, &apiErr) {
				writeJSON(w, apiErr.status, object{"detail": apiErr.detail})
				return
			}
			log.Printf("request failed: %s %s: %v", r.Method, r.URL.Path, err)
			writeJSON(w, http.StatusInternalServerError, object{"detail": "Internal Server Error"})
		}
	}
}

func (s *server) root(w http.ResponseWriter, _ *http.Request) error {
	writeJSON(w, http.StatusOK, object{"message": "Food Diary API", "docs": "/docs"})
	return nil
}

func (s *server) registerHandler(w http.ResponseWriter, r *http.Request) error {
	value, err := readBody(w, r)
	if err != nil {
		return err
	}
	data, err := s.validated("register", value)
	if err != nil {
		return err
	}
	return s.register(w, data)
}

func (s *server) loginHandler(w http.ResponseWriter, r *http.Request) error {
	value, err := readBody(w, r)
	if err != nil {
		return err
	}
	data, err := s.validated("login", value)
	if err != nil {
		return err
	}
	return s.login(w, data)
}

func (s *server) meHandler(w http.ResponseWriter, r *http.Request) error {
	user, err := s.authenticate(r)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, userJSON(user))
	return nil
}

func (s *server) profilePatchHandler(w http.ResponseWriter, r *http.Request) error {
	value, err := readBody(w, r)
	if err != nil {
		return err
	}
	user, err := s.authenticate(r)
	if err != nil {
		return err
	}
	data, err := s.validated("profile", value)
	if err != nil {
		return err
	}
	return s.updateProfile(w, data, user)
}

func (s *server) addMealHandler(w http.ResponseWriter, r *http.Request) error {
	value, err := readBody(w, r)
	if err != nil {
		return err
	}
	user, err := s.authenticate(r)
	if err != nil {
		return err
	}
	data, err := s.validated("meal", value)
	if err != nil {
		return err
	}
	return s.addMeal(w, data, user)
}

func (s *server) getMealsHandler(w http.ResponseWriter, r *http.Request) error {
	user, err := s.authenticate(r)
	if err != nil {
		return err
	}
	var day any
	if value, ok := lastQuery(r, "day"); ok {
		parsed, dateError := parseDate(value, []any{"query", "day"})
		if dateError != nil {
			return failure(422, []object{dateError})
		}
		day = parsed
	}
	return s.getMeals(w, user, day)
}

func (s *server) listFoodsHandler(w http.ResponseWriter, r *http.Request) error {
	limit, offset := int64(100), int64(0)
	errs := []object{}
	if value, ok := lastQuery(r, "limit"); ok {
		loc := []any{"query", "limit"}
		n, valid := queryInt(value)
		switch {
		case !valid:
			errs = append(errs, issue("int_parsing", loc, "Input should be a valid integer, unable to parse string as an integer", value, nil))
		case n < 1:
			errs = append(errs, issue("greater_than_equal", loc, "Input should be greater than or equal to 1", value, object{"ge": 1}))
		case n > 1000:
			errs = append(errs, issue("less_than_equal", loc, "Input should be less than or equal to 1000", value, object{"le": 1000}))
		default:
			limit = n
		}
	}
	if value, ok := lastQuery(r, "offset"); ok {
		loc := []any{"query", "offset"}
		n, valid := queryInt(value)
		switch {
		case !valid:
			errs = append(errs, issue("int_parsing", loc, "Input should be a valid integer, unable to parse string as an integer", value, nil))
		case n < 0:
			errs = append(errs, issue("greater_than_equal", loc, "Input should be greater than or equal to 0", value, object{"ge": 0}))
		default:
			offset = n
		}
	}
	if len(errs) > 0 {
		return failure(422, errs)
	}
	rows, err := foodRows(s.db, s.foodSelect()+" ORDER BY "+s.idCol+" LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, rows)
	return nil
}

func (s *server) searchFoodsHandler(w http.ResponseWriter, r *http.Request) error {
	query, ok := lastQuery(r, "q")
	loc := []any{"query", "q"}
	if !ok {
		return failure(422, []object{issue("missing", loc, "Field required", nil, nil)})
	}
	length := utf8.RuneCountInString(query)
	if length < 1 {
		return failure(422, []object{issue("string_too_short", loc, "String should have at least 1 character", query, object{"min_length": 1})})
	}
	if length > 100 {
		return failure(422, []object{issue("string_too_long", loc, "String should have at most 100 characters", query, object{"max_length": 100})})
	}
	rows, err := s.search(s.db, query)
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, rows)
	return nil
}

func (s *server) openAPIHandler(w http.ResponseWriter, _ *http.Request) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write(s.spec)
	return err
}

func (s *server) docsHandler(w http.ResponseWriter, _ *http.Request) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, err := io.WriteString(w, swaggerHTML)
	return err
}

func (s *server) redocHandler(w http.ResponseWriter, _ *http.Request) error {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, err := io.WriteString(w, redocHTML)
	return err
}

func (s *server) register(w http.ResponseWriter, data object) error {
	hash, err := passwordHash(asString(data, "password"))
	if err != nil {
		return err
	}
	user := User{
		Phone:         asString(data, "phone"),
		PasswordHash:  hash,
		Gender:        asInt(data, "gender"),
		Height:        asInt(data, "height"),
		Weight:        asInt(data, "weight"),
		Name:          asString(data, "name"),
		BirthYear:     asInt(data, "birth_year"),
		ActivityLevel: asInt(data, "activity_level"),
	}
	s.writeMu.Lock()
	err = s.writer.Create(&user).Error
	s.writeMu.Unlock()
	if err != nil {
		if strings.Contains(strings.ToUpper(err.Error()), "UNIQUE") {
			return failure(409, "این شماره تلفن قبلا ثبت شده است")
		}
		log.Printf("register insert failed: %v", err)
		return failure(422, "داده‌های ثبت‌نام نامعتبر است")
	}
	writeJSON(w, http.StatusCreated, object{"access_token": s.token(user.ID), "token_type": "bearer"})
	return nil
}

func (s *server) login(w http.ResponseWriter, data object) error {
	var user User
	err := s.db.Where("phone = ?", asString(data, "phone")).First(&user).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if err != nil || !verifyPassword(asString(data, "password"), user.PasswordHash) {
		return failure(401, "شماره تلفن یا رمز عبور اشتباه است")
	}
	writeJSON(w, http.StatusOK, object{"access_token": s.token(user.ID), "token_type": "bearer"})
	return nil
}

func (s *server) updateProfile(w http.ResponseWriter, data object, user *User) error {
	changes := map[string]any{}
	for key, value := range data {
		if value != nil {
			changes[key] = asInt(data, key)
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
	err := s.writer.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&User{}).Where("id = ?", user.ID).Updates(changes).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", user.ID).First(user).Error
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return failure(404, "کاربر پیدا نشد")
	}
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, userJSON(user))
	return nil
}

func (s *server) addMeal(w http.ResponseWriter, data object, user *User) error {
	now := time.Now().In(s.tz).Truncate(time.Microsecond)
	stamp := now.Format("2006-01-02T15:04:05.000000Z07:00")
	if now.Nanosecond() == 0 {
		stamp = now.Format("2006-01-02T15:04:05Z07:00")
	}
	if strings.HasSuffix(stamp, "Z") {
		stamp = strings.TrimSuffix(stamp, "Z") + "+00:00"
	}
	day := asString(data, "date")
	if day == "" {
		day = time.Now().In(s.tz).Format("2006-01-02")
	}

	status := http.StatusCreated
	var result object
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	err := s.writer.Transaction(func(tx *gorm.DB) error {
		items := []MealItem{}
		foods, _ := data["foods"].([]any)
		for _, raw := range foods {
			itemData, ok := raw.(map[string]any)
			if !ok {
				return failure(422, "داده‌های وعده نامعتبر است")
			}
			item, err := s.resolve(tx, itemData)
			if err != nil {
				return err
			}
			items = append(items, item)
		}

		var count int64
		if err := tx.Model(&Meal{}).
			Where("user_id = ? AND date = ? AND meal_type = ?", user.ID, day, asInt(data, "meal_id")).
			Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			status = http.StatusOK
		}

		var meal Meal
		if err := tx.Raw(
			"INSERT INTO meals(user_id,date,meal_type,created_at,updated_at) VALUES(?,?,?,?,?) "+
				"ON CONFLICT(user_id,date,meal_type) DO UPDATE SET updated_at=excluded.updated_at RETURNING *",
			user.ID, day, asInt(data, "meal_id"), stamp, stamp,
		).Scan(&meal).Error; err != nil {
			return err
		}
		if err := tx.Where("meal_id = ?", meal.ID).Delete(&MealItem{}).Error; err != nil {
			return err
		}
		for i := range items {
			items[i].MealID = meal.ID
		}
		if err := tx.Create(&items).Error; err != nil {
			return err
		}
		result = serializeMeal(meal, items)
		return nil
	})
	if err != nil {
		return err
	}
	writeJSON(w, status, result)
	return nil
}

func (s *server) getMeals(w http.ResponseWriter, user *User, day any) error {
	output := []object{}
	err := s.db.Transaction(func(tx *gorm.DB) error {
		query := tx.Where("user_id = ?", user.ID)
		if day != nil {
			query = query.Where("date = ?", day).Order("meal_type")
		} else {
			query = query.Order("date,meal_type")
		}
		var meals []Meal
		if err := query.Find(&meals).Error; err != nil {
			return err
		}
		if len(meals) == 0 {
			return nil
		}

		var items []MealItem
		itemQuery := tx.Table("meal_items").
			Select("meal_items.*").
			Joins("JOIN meals ON meals.id = meal_items.meal_id").
			Where("meals.user_id = ?", user.ID)
		if day != nil {
			itemQuery = itemQuery.Where("meals.date = ?", day)
		}
		if err := itemQuery.Order("meal_items.id").Scan(&items).Error; err != nil {
			return err
		}
		grouped := map[int64][]MealItem{}
		for _, item := range items {
			grouped[item.MealID] = append(grouped[item.MealID], item)
		}
		for _, meal := range meals {
			output = append(output, serializeMeal(meal, grouped[meal.ID]))
		}
		return nil
	})
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, object{"date": day, "meals": output})
	return nil
}

func lastQuery(r *http.Request, key string) (string, bool) {
	values := r.URL.Query()[key]
	if len(values) == 0 {
		return "", false
	}
	return values[len(values)-1], true
}

var queryIntRE = regexp.MustCompile(`^[+-]?[0-9]+(?:_[0-9]+)*(?:\.0+)?$`)

func queryInt(value string) (int64, bool) {
	text := strings.TrimSpace(value)
	if !queryIntRE.MatchString(text) {
		return 0, false
	}
	text = strings.ReplaceAll(text, "_", "")
	if dot := strings.IndexByte(text, '.'); dot >= 0 {
		text = text[:dot]
	}
	n, err := strconv.ParseInt(text, 10, 64)
	return n, err == nil
}

// --------------------------------------------------------------------------- //
// Routing
// --------------------------------------------------------------------------- //

var routeMethods = map[string][]string{
	"/":              {"GET"},
	"/auth/register": {"POST"},
	"/auth/login":    {"POST"},
	"/me":            {"GET"},
	"/profile":       {"GET", "PATCH"},
	"/foods":         {"GET"},
	"/foods/search":  {"GET"},
	"/meals":         {"POST", "GET"},
	"/docs":          {"GET"},
	"/redoc":         {"GET"},
	"/openapi.json":  {"GET"},
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.wrap(s.root))
	mux.HandleFunc("POST /auth/register", s.wrap(s.registerHandler))
	mux.HandleFunc("POST /auth/login", s.wrap(s.loginHandler))
	mux.HandleFunc("GET /me", s.wrap(s.meHandler))
	mux.HandleFunc("GET /profile", s.wrap(s.meHandler))
	mux.HandleFunc("PATCH /profile", s.wrap(s.profilePatchHandler))
	mux.HandleFunc("GET /foods", s.wrap(s.listFoodsHandler))
	mux.HandleFunc("GET /foods/search", s.wrap(s.searchFoodsHandler))
	mux.HandleFunc("POST /meals", s.wrap(s.addMealHandler))
	mux.HandleFunc("GET /meals", s.wrap(s.getMealsHandler))
	mux.HandleFunc("GET /docs", s.wrap(s.docsHandler))
	mux.HandleFunc("GET /redoc", s.wrap(s.redocHandler))
	mux.HandleFunc("GET /openapi.json", s.wrap(s.openAPIHandler))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		methods, known := routeMethods[path]
		if !known {
			trimmed := strings.TrimRight(path, "/")
			if trimmed == "" {
				trimmed = "/"
			}
			if _, ok := routeMethods[trimmed]; ok && path != trimmed {
				target := trimmed
				if r.URL.RawQuery != "" {
					target += "?" + r.URL.RawQuery
				}
				http.Redirect(w, r, target, http.StatusTemporaryRedirect)
				return
			}
			writeJSON(w, http.StatusNotFound, object{"detail": "Not Found"})
			return
		}
		allowed := false
		for _, method := range methods {
			if method == r.Method || (r.Method == http.MethodHead && method == http.MethodGet) {
				allowed = true
				break
			}
		}
		if !allowed {
			w.Header().Set("Allow", strings.Join(methods, ", "))
			writeJSON(w, http.StatusMethodNotAllowed, object{"detail": "Method Not Allowed"})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// --------------------------------------------------------------------------- //
// Swagger / OpenAPI
// --------------------------------------------------------------------------- //

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Food Diary API — Swagger UI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: '/openapi.json',
      dom_id: '#swagger-ui',
      deepLinking: true,
      displayRequestDuration: true,
      persistAuthorization: true,
      presets: [SwaggerUIBundle.presets.apis],
      layout: 'BaseLayout'
    });
  </script>
</body>
</html>`

const redocHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Food Diary API — ReDoc</title>
</head>
<body>
  <redoc spec-url="/openapi.json"></redoc>
  <script src="https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"></script>
</body>
</html>`

const openAPITemplate = `{
  "openapi": "3.0.3",
  "info": {
    "title": "Food Diary API",
    "version": "1.0",
    "description": "Single-file Go port of the FastAPI food diary service. Register or log in to obtain a bearer token, then use the Authorize button."
  },
  "servers": [{"url": "/"}],
  "paths": {
    "/": {
      "get": {
        "summary": "Root",
        "operationId": "root",
        "responses": {
          "200": {
            "description": "OK",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "message": {"type": "string"},
                    "docs": {"type": "string"}
                  }
                }
              }
            }
          }
        }
      }
    },
    "/auth/register": {
      "post": {
        "summary": "Register a new user",
        "operationId": "register",
        "requestBody": {
          "required": true,
          "content": {"application/json": {"schema": {"$ref": "#/components/schemas/RegisterIn"}}}
        },
        "responses": {
          "201": {
            "description": "Created",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Token"}}}
          },
          "409": {"$ref": "#/components/responses/Error"},
          "422": {"$ref": "#/components/responses/ValidationError"}
        }
      }
    },
    "/auth/login": {
      "post": {
        "summary": "Log in",
        "operationId": "login",
        "requestBody": {
          "required": true,
          "content": {"application/json": {"schema": {"$ref": "#/components/schemas/LoginIn"}}}
        },
        "responses": {
          "200": {
            "description": "OK",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Token"}}}
          },
          "401": {"$ref": "#/components/responses/Error"},
          "422": {"$ref": "#/components/responses/ValidationError"}
        }
      }
    },
    "/me": {
      "get": {
        "summary": "Current user",
        "operationId": "get_me",
        "security": [{"BearerAuth": []}],
        "responses": {
          "200": {
            "description": "OK",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}
          },
          "401": {"$ref": "#/components/responses/Error"}
        }
      }
    },
    "/profile": {
      "get": {
        "summary": "Current user profile",
        "operationId": "get_profile",
        "security": [{"BearerAuth": []}],
        "responses": {
          "200": {
            "description": "OK",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}
          },
          "401": {"$ref": "#/components/responses/Error"}
        }
      },
      "patch": {
        "summary": "Update the current user profile",
        "operationId": "update_profile",
        "security": [{"BearerAuth": []}],
        "requestBody": {
          "required": true,
          "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ProfileUpdate"}}}
        },
        "responses": {
          "200": {
            "description": "OK",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/User"}}}
          },
          "400": {"$ref": "#/components/responses/Error"},
          "401": {"$ref": "#/components/responses/Error"},
          "422": {"$ref": "#/components/responses/ValidationError"}
        }
      }
    },
    "/foods": {
      "get": {
        "summary": "List foods",
        "operationId": "list_foods",
        "parameters": [
          {
            "name": "limit",
            "in": "query",
            "required": false,
            "schema": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}
          },
          {
            "name": "offset",
            "in": "query",
            "required": false,
            "schema": {"type": "integer", "minimum": 0, "default": 0}
          }
        ],
        "responses": {
          "200": {
            "description": "OK",
            "content": {
              "application/json": {
                "schema": {"type": "array", "items": {"$ref": "#/components/schemas/Food"}}
              }
            }
          },
          "422": {"$ref": "#/components/responses/ValidationError"}
        }
      }
    },
    "/foods/search": {
      "get": {
        "summary": "Search foods by name or numeric id",
        "operationId": "search_foods",
        "parameters": [
          {
            "name": "q",
            "in": "query",
            "required": true,
            "schema": {"type": "string", "minLength": 1, "maxLength": 100}
          }
        ],
        "responses": {
          "200": {
            "description": "OK",
            "content": {
              "application/json": {
                "schema": {"type": "array", "items": {"$ref": "#/components/schemas/Food"}}
              }
            }
          },
          "422": {"$ref": "#/components/responses/ValidationError"}
        }
      }
    },
    "/meals": {
      "post": {
        "summary": "Create or fully replace a meal",
        "operationId": "add_meal",
        "security": [{"BearerAuth": []}],
        "requestBody": {
          "required": true,
          "content": {"application/json": {"schema": {"$ref": "#/components/schemas/MealIn"}}}
        },
        "responses": {
          "200": {
            "description": "Existing meal replaced",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Meal"}}}
          },
          "201": {
            "description": "Meal created",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Meal"}}}
          },
          "401": {"$ref": "#/components/responses/Error"},
          "404": {"$ref": "#/components/responses/Error"},
          "422": {"$ref": "#/components/responses/ValidationError"}
        }
      },
      "get": {
        "summary": "List meals",
        "operationId": "get_meals",
        "security": [{"BearerAuth": []}],
        "parameters": [
          {
            "name": "day",
            "in": "query",
            "required": false,
            "schema": {"type": "string", "format": "date"}
          }
        ],
        "responses": {
          "200": {
            "description": "OK",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Meals"}}}
          },
          "401": {"$ref": "#/components/responses/Error"},
          "422": {"$ref": "#/components/responses/ValidationError"}
        }
      }
    }
  },
  "components": {
    "securitySchemes": {
      "BearerAuth": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}
    },
    "responses": {
      "Error": {
        "description": "Error",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}
      },
      "ValidationError": {
        "description": "Validation error",
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/ValidationError"}}}
      }
    },
    "schemas": {
      "RegisterIn": {
        "type": "object",
        "additionalProperties": false,
        "required": ["phone", "password", "gender", "height", "weight", "name", "birth_year"],
        "properties": {
          "phone": {"type": "string", "minLength": 1, "maxLength": 32},
          "password": {"type": "string", "minLength": 1, "maxLength": 256},
          "gender": {"type": "integer", "minimum": 0, "maximum": 1},
          "height": {"type": "integer", "minimum": 1, "maximum": 300},
          "weight": {"type": "integer", "minimum": 1, "maximum": 700},
          "name": {"type": "string", "minLength": 1, "maxLength": 100},
          "birth_year": {"type": "integer", "minimum": 1901, "maximum": {{BIRTH_MAX}}},
          "activity_level": {"type": "integer", "minimum": 1, "maximum": 5, "default": 3}
        }
      },
      "LoginIn": {
        "type": "object",
        "additionalProperties": false,
        "required": ["phone", "password"],
        "properties": {
          "phone": {"type": "string", "minLength": 1, "maxLength": 32},
          "password": {"type": "string", "minLength": 1, "maxLength": 256}
        }
      },
      "ProfileUpdate": {
        "type": "object",
        "additionalProperties": false,
        "properties": {
          "height": {"type": "integer", "minimum": 1, "maximum": 300, "nullable": true},
          "weight": {"type": "integer", "minimum": 1, "maximum": 700, "nullable": true},
          "age": {"type": "integer", "minimum": 1, "maximum": 130, "nullable": true},
          "gender": {"type": "integer", "minimum": 0, "maximum": 1, "nullable": true},
          "activity_level": {"type": "integer", "minimum": 1, "maximum": 5, "nullable": true}
        }
      },
      "MealItemIn": {
        "type": "object",
        "additionalProperties": false,
        "required": ["food_id", "amount"],
        "properties": {
          "food_id": {"type": "integer", "minimum": 0, "maximum": 2147483647},
          "amount": {"type": "number", "minimum": 0, "maximum": 100000, "exclusiveMinimum": true},
          "calories": {"type": "number", "minimum": 0, "maximum": 100000, "nullable": true},
          "name": {"type": "string", "maxLength": 100, "nullable": true},
          "unit": {"type": "string", "maxLength": 32, "nullable": true}
        }
      },
      "MealIn": {
        "type": "object",
        "additionalProperties": false,
        "required": ["meal_id", "foods"],
        "properties": {
          "meal_id": {
            "type": "integer",
            "minimum": 1,
            "maximum": 4,
            "description": "1 breakfast, 2 lunch, 3 dinner, 4 snack"
          },
          "foods": {
            "type": "array",
            "minItems": 1,
            "maxItems": 100,
            "items": {"$ref": "#/components/schemas/MealItemIn"}
          },
          "date": {"type": "string", "format": "date", "nullable": true}
        }
      },
      "Token": {
        "type": "object",
        "required": ["access_token", "token_type"],
        "properties": {
          "access_token": {"type": "string"},
          "token_type": {"type": "string", "example": "bearer"}
        }
      },
      "User": {
        "type": "object",
        "properties": {
          "id": {"type": "integer"},
          "phone": {"type": "string"},
          "name": {"type": "string"},
          "gender": {"type": "integer"},
          "height": {"type": "integer"},
          "weight": {"type": "integer"},
          "birth_year": {"type": "integer"},
          "activity_level": {"type": "integer"},
          "age": {"type": "integer"}
        }
      },
      "Food": {
        "type": "object",
        "properties": {
          "id": {"type": "integer"},
          "name": {"type": "string", "nullable": true},
          "calories": {
            "description": "Raw catalogue value; may be a string or a number",
            "nullable": true
          }
        }
      },
      "MealItem": {
        "type": "object",
        "properties": {
          "id": {"type": "integer"},
          "meal_id": {"type": "integer"},
          "food_id": {"type": "integer"},
          "food_name": {"type": "string"},
          "calories": {"type": "number"},
          "amount": {"type": "number"},
          "unit": {"type": "string"}
        }
      },
      "Meal": {
        "type": "object",
        "properties": {
          "id": {"type": "integer"},
          "user_id": {"type": "integer"},
          "date": {"type": "string", "format": "date"},
          "meal_id": {"type": "integer"},
          "meal_name": {"type": "string"},
          "foods": {"type": "array", "items": {"$ref": "#/components/schemas/MealItem"}},
          "created_at": {"type": "string"},
          "updated_at": {"type": "string"},
          "total_calories": {"type": "number"}
        }
      },
      "Meals": {
        "type": "object",
        "properties": {
          "date": {"type": "string", "format": "date", "nullable": true},
          "meals": {"type": "array", "items": {"$ref": "#/components/schemas/Meal"}}
        }
      },
      "Error": {
        "type": "object",
        "required": ["detail"],
        "properties": {"detail": {}}
      },
      "ValidationError": {
        "type": "object",
        "required": ["detail"],
        "properties": {
          "detail": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "type": {"type": "string"},
                "loc": {"type": "array", "items": {}},
                "msg": {"type": "string"},
                "input": {},
                "ctx": {"type": "object"}
              }
            }
          }
        }
      }
    }
  }
}`

func openAPISpec() []byte {
	spec := strings.ReplaceAll(openAPITemplate, "{{BIRTH_MAX}}", strconv.Itoa(time.Now().Year()))
	return []byte(spec)
}

// --------------------------------------------------------------------------- //
// Entry point
// --------------------------------------------------------------------------- //

func defaultDB() (string, error) {
	if value, ok := os.LookupEnv("DATABASE_URL"); ok {
		if !strings.HasPrefix(value, "sqlite:///") {
			return "", errors.New("DATABASE_URL must start with sqlite:///")
		}
		value = strings.TrimPrefix(value, "sqlite:///")
		if strings.Contains(value, "?") {
			return "", errors.New("SQLite URI query options are not supported; use -db with a filesystem path")
		}
		return value, nil
	}
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	path := filepath.Join(filepath.Dir(executable), "food.db")
	if strings.Contains(filepath.ToSlash(executable), "/go-build") {
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
	dbFlag := flag.String("db", "", "existing SQLite file (default: food.db beside the executable)")
	listen := flag.String("listen", env("HOST", "0.0.0.0")+":"+env("PORT", "8000"), "listen address")
	initEmpty := flag.Bool("init-empty", false, "explicitly allow creating a new empty database")
	flag.Parse()

	path := *dbFlag
	var err error
	if path == "" {
		path, err = defaultDB()
		if err != nil {
			log.Fatal(err)
		}
	}
	if _, err = os.Stat(path); err != nil && !(*initEmpty && os.IsNotExist(err)) {
		log.Fatalf("database not found: %s (use -db to select the existing file; -init-empty is only for a new installation)", path)
	}

	s, err := newServer(path)
	if err != nil {
		log.Fatal(err)
	}
	defer s.close()

	httpServer := &http.Server{
		Addr:              *listen,
		Handler:           s.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Printf("Food Diary API listening on %s; Swagger: /docs", *listen)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("server: %v", err)
		os.Exit(1)
	}
}
