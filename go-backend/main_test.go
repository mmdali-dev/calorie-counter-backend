package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestOpenSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "food.db")
	db, e := openDB(path, true)
	if e != nil {
		t.Fatal(e)
	}
	if e = bootstrap(db); e != nil {
		t.Fatal(e)
	}
	p, _ := db.DB()
	defer p.Close()
	var n int
	if e = p.QueryRow("SELECT count(*) FROM users").Scan(&n); e != nil {
		t.Fatal(e)
	}
}

func testServer(t *testing.T) *server {
	t.Helper()
	t.Setenv("JWT_SECRET", "test-secret")
	t.Setenv("APP_TIMEZONE", "Asia/Tehran")
	s, e := newServer(filepath.Join(t.TempDir(), "food.db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.close)
	return s
}

func TestLegacyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, e := openDB(path, true)
	if e != nil {
		t.Fatal(e)
	}
	pool, _ := db.DB()
	defer pool.Close()
	statements := []string{
		`CREATE TABLE foods (Food_Item TEXT,Calories TEXT)`,
		`INSERT INTO foods VALUES ('سیب','52')`,
		`CREATE TABLE users(id INTEGER PRIMARY KEY AUTOINCREMENT,phone TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,gender INTEGER NOT NULL,height INTEGER NOT NULL,weight INTEGER NOT NULL,name TEXT NOT NULL,birth_year INTEGER NOT NULL)`,
		`INSERT INTO users VALUES(17,'0912','old-hash',7,178,74,'Test',1994)`,
		`CREATE TABLE meals(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL REFERENCES users(id),eaten_at TEXT NOT NULL,meal_type INTEGER NOT NULL,food_id INTEGER NOT NULL,food_name TEXT NOT NULL,calories REAL NOT NULL,amount REAL NOT NULL,unit TEXT NOT NULL)`,
		`INSERT INTO meals VALUES(1,17,'2026-10-24T16:38:32+00:00',1,1,'سیب',52,2,'serving')`,
		`INSERT INTO meals VALUES(2,17,'2026-10-24T17:00:00+00:00',1,0,'کیک',350,1,'g')`,
	}
	for _, q := range statements {
		if e = db.Exec(q).Error; e != nil {
			t.Fatal(e)
		}
	}
	for i := 0; i < 3; i++ {
		if e = bootstrap(db); e != nil {
			t.Fatal(e)
		}
	}
	var user User
	if e = db.First(&user, 17).Error; e != nil {
		t.Fatal(e)
	}
	if user.Gender != 0 || user.ActivityLevel != 3 || user.PasswordHash != "old-hash" {
		t.Fatalf("legacy user changed incorrectly: %+v", user)
	}
	var meals []Meal
	db.Find(&meals)
	if len(meals) != 1 || meals[0].UserID != 17 || meals[0].CreatedAt != "2026-10-24T16:38:32+00:00" || meals[0].UpdatedAt != "2026-10-24T17:00:00+00:00" {
		t.Fatalf("meals: %+v", meals)
	}
	var items []MealItem
	db.Order("id").Find(&items)
	if len(items) != 2 || items[0].FoodName != "سیب" || items[1].FoodName != "کیک" {
		t.Fatalf("items: %+v", items)
	}
	rows, e := pool.Query("PRAGMA foreign_key_check")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("foreign keys invalid")
	}
}

func TestFailedMigrationRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.db")
	db, e := openDB(path, true)
	if e != nil {
		t.Fatal(e)
	}
	pool, _ := db.DB()
	defer pool.Close()
	for _, q := range []string{`CREATE TABLE foods(name TEXT,calories REAL)`, `CREATE TABLE users(id INTEGER PRIMARY KEY,phone TEXT,password_hash TEXT,gender INTEGER,height INTEGER,weight INTEGER,name TEXT,birth_year INTEGER)`, `INSERT INTO users VALUES(1,'p','h',0,1,1,'n',1990)`, `CREATE TABLE meals(id INTEGER PRIMARY KEY,user_id INTEGER,eaten_at TEXT,meal_type INTEGER,food_id INTEGER,food_name TEXT,calories REAL,amount REAL,unit TEXT)`, `INSERT INTO meals VALUES(1,999,'2026-01-01',1,1,'a',1,1,'g')`} {
		if e = db.Exec(q).Error; e != nil {
			t.Fatal(e)
		}
	}
	snapshot := func() []string {
		var rows []string
		db.Raw("SELECT sql FROM sqlite_master ORDER BY name").Scan(&rows)
		return rows
	}
	before := snapshot()
	if e = bootstrap(db); e == nil {
		t.Fatal("expected orphan migration to fail")
	}
	if !reflect.DeepEqual(before, snapshot()) {
		t.Fatal("failed DDL migration was not rolled back")
	}
	var gender int
	db.Raw("SELECT gender FROM users WHERE id=1").Scan(&gender)
	if gender != 0 {
		t.Fatal("user changed")
	}
	var enabled int
	pool.QueryRow("PRAGMA foreign_keys").Scan(&enabled)
	if enabled != 1 {
		t.Fatal("foreign keys not restored")
	}
}

func TestHTTPAtomicityAndOwnership(t *testing.T) {
	s := testServer(t)
	app := s.app()
	u := User{Phone: "p", PasswordHash: "h", Gender: 0, Height: 170, Weight: 70, Name: "n", BirthYear: 1990, ActivityLevel: 3}
	if e := s.writer.Create(&u).Error; e != nil {
		t.Fatal(e)
	}
	if e := s.writer.Exec(`INSERT INTO foods(name,calories) VALUES('a',52)`).Error; e != nil {
		t.Fatal(e)
	}
	request := func(method, path, body, token string) (int, object) {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		resp, e := app.Test(r)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		var out object
		if e = json.NewDecoder(resp.Body).Decode(&out); e != nil {
			t.Fatal(e)
		}
		return resp.StatusCode, out
	}
	token := s.token(u.ID)
	status, first := request("POST", "/meals", `{"meal_id":1,"date":"2026-11-01","foods":[{"food_id":1,"amount":2}]}`, token)
	if status != 201 {
		t.Fatal(status, first)
	}
	for _, payload := range []string{`{"meal_id":1,"date":"2026-11-01","foods":[{"food_id":1,"amount":3},{"food_id":999,"amount":1}]}`, `{"meal_id":1,"date":"2026-11-01","foods":[{"food_id":0,"amount":1}]}`} {
		status, _ = request("POST", "/meals", payload, token)
		if status != 404 && status != 422 {
			t.Fatal(status)
		}
	}
	_, read := request("GET", "/meals?day=2026-11-01", "", token)
	meal := read["meals"].([]any)[0].(map[string]any)
	if !reflect.DeepEqual(first, meal) {
		t.Fatalf("failed write changed meal: %+v %+v", first, meal)
	}
	status, second := request("POST", "/meals", `{"meal_id":1,"date":"2026-11-01","foods":[{"food_id":0,"amount":1,"name":" x ","calories":80,"unit":" g "}]}`, token)
	if status != 200 || first["id"] != second["id"] || first["created_at"] != second["created_at"] {
		t.Fatal(status, second)
	}
	s.writer.Delete(&MealItem{}, "meal_id=?", first["id"])
	s.writer.Delete(&Meal{}, "user_id=?", u.ID)
	s.writer.Delete(&u)
	status, _ = request("GET", "/me", "", token)
	if status != 401 {
		t.Fatal("deleted user token", status)
	}
}

func TestCalorieFormats(t *testing.T) {
	cases := []struct {
		raw  any
		want float64
		ok   bool
	}{{"۵۲", 52, true}, {"٤٥٠ تا ٥٠٠", 475, true}, {"1,234", 1234, true}, {"-52", 52, true}, {"1e3", 2, true}, {".5", 5, true}, {"-", 0, false}, {nil, 0, false}, {"۱۰۰.۵ تا ۲۰۰.۵", 150.5, true}, {"1.2.3", 2.1, true}, {1e-7, 4, true}}
	cases = append(cases, struct {
		raw  any
		want float64
		ok   bool
	}{float64(1000000), 1000000, true}, struct {
		raw  any
		want float64
		ok   bool
	}{float64(12345678), 12345678, true})
	for _, tc := range cases {
		got, ok := parseCalories(tc.raw)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("%v => %v,%v; want %v,%v", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

func TestTokenAndPassword(t *testing.T) {
	hash, e := passwordHash("فارسی🙂")
	if e != nil {
		t.Fatal(e)
	}
	if !verifyPassword("فارسی🙂", hash) || verifyPassword("wrong", hash) || verifyPassword("x", "invalid") {
		t.Fatal("password compatibility")
	}
	s := testServer(t)
	u := User{Phone: "t", PasswordHash: hash, Gender: 0, Height: 1, Weight: 1, Name: "n", BirthYear: 1994, ActivityLevel: 3}
	s.writer.Create(&u)
	app := s.app()
	for _, token := range []string{s.token(u.ID), "invalid"} {
		r := httptest.NewRequest("GET", "/me", nil)
		r.Header.Set("Authorization", "bEaReR "+token)
		resp, e := app.Test(r)
		if e != nil {
			t.Fatal(e)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		want := 200
		if token == "invalid" {
			want = 401
		}
		if resp.StatusCode != want {
			t.Fatal(resp.StatusCode)
		}
	}
}

func TestTimezoneAndOfflineDocs(t *testing.T) {
	s := testServer(t)
	_, offset := time.Now().In(s.tz).Zone()
	if offset != 12600 {
		t.Fatal("Tehran offset", offset)
	}
	app := s.app()
	for _, path := range []string{"/docs", "/openapi.json", "/docs/assets/swagger-ui-bundle.js", "/docs/assets/swagger-ui.css"} {
		resp, e := app.Test(httptest.NewRequest("GET", path, nil))
		if e != nil {
			t.Fatal(e)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || len(b) < 50 {
			t.Fatal(path, resp.StatusCode)
		}
		if path == "/docs" && (bytes.Contains(b, []byte("cdn.")) || bytes.Contains(b, []byte("unpkg"))) {
			t.Fatal("external Swagger dependency")
		}
	}
}

func TestDriverDSN(t *testing.T) {
	path := strings.ReplaceAll(filepath.Join(t.TempDir(), "test.db"), "\\", "/")
	for _, dsn := range []string{":memory:", "file::memory:?_pragma=foreign_keys(1)", "file::memory:?_pragma=foreign_keys(1)&_pragma=busy_timeout(30000)&_txlock=immediate", path, "file:" + path, "file:/" + path, "file:///" + path} {
		db, e := sql.Open("sqlite", dsn)
		if e != nil {
			t.Fatal(e)
		}
		var v string
		e = db.QueryRow("select sqlite_version()").Scan(&v)
		db.Close()
		if e != nil {
			t.Errorf("%s: %v", dsn, e)
		}
	}
}
