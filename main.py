"""Simple food diary API. Run with: python3 -m uvicorn main:app --reload"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "food.db")
JWT_SECRET = os.environ.get("JWT_SECRET", "change-this-secret-before-public-use")
TOKEN_DAYS = 30
# Local timezone used for "today" when the client omits the date.
try:
    APP_TZ = ZoneInfo(os.environ.get("APP_TIMEZONE", "Asia/Tehran"))
except ZoneInfoNotFoundError:  # pragma: no cover - only if tzdata is missing
    APP_TZ = timezone.utc
# Upper bounds guarding against absurd/overflowing input values.
MAX_CALORIES = 100_000.0
MAX_AMOUNT = 100_000.0
MAX_FOODS_PER_MEAL = 100
MIN_DATE = date(1900, 1, 1)
app = FastAPI(title="Food Diary API", version="1.0")

# Persian (۰-۹) and Arabic-Indic (٠-٩) digits -> ASCII.
DIGIT_MAP = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


def local_now() -> datetime:
    return datetime.now(APP_TZ)


def db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def connect():
    """Yield a connection, commit on success, rollback on error, and always close."""
    conn = db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    # Schema migrations rebuild tables via RENAME, so foreign key enforcement is
    # disabled for this connection (SQLite's documented ALTER TABLE procedure).
    # All data is copied and constraints re-asserted before we commit.
    c = db()
    c.execute("PRAGMA foreign_keys = OFF")
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS foods (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, calories REAL NOT NULL)""")
        cols = {r[1] for r in c.execute("PRAGMA table_info(foods)")}
        # Preserve the supplied Food_Item/Calories table; its rowid is the food ID.
        if "Food_Item" in cols and "name" not in cols:
            pass
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL, gender INTEGER NOT NULL CHECK(gender IN (0,1)),
            height INTEGER NOT NULL, weight INTEGER NOT NULL, name TEXT NOT NULL,
            birth_year INTEGER NOT NULL)""")
        # Reinforce the gender constraint for databases created before it existed.
        users_cols = {r[1] for r in c.execute("PRAGMA table_info(users)")}
        users_sql = (c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='users'").fetchone() or [""])[0]
        if "gender" in users_cols and "gender IN (0,1)" not in (users_sql or ""):
            c.execute("UPDATE users SET gender=0 WHERE gender NOT IN (0,1) OR gender IS NULL")
            c.execute("ALTER TABLE users RENAME TO users_old")
            c.execute("""CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL, gender INTEGER NOT NULL CHECK(gender IN (0,1)),
                height INTEGER NOT NULL, weight INTEGER NOT NULL, name TEXT NOT NULL,
                birth_year INTEGER NOT NULL)""")
            c.execute("""INSERT INTO users(id,phone,password_hash,gender,height,weight,name,birth_year)
                         SELECT id,phone,password_hash,gender,height,weight,name,birth_year FROM users_old""")
            c.execute("DROP TABLE users_old")
        # Migrate the legacy flat "one row per food" meals table into the
        # header (meals) + items (meal_items) model before (re)creating it.
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        meals_cols = {r[1] for r in c.execute("PRAGMA table_info(meals)")} if "meals" in tables else set()
        if "food_name" in meals_cols:
            c.execute("ALTER TABLE meals RENAME TO meals_legacy")
        c.execute("""CREATE TABLE IF NOT EXISTS meals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            date TEXT NOT NULL,
            meal_type INTEGER NOT NULL CHECK(meal_type BETWEEN 1 AND 4),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, date, meal_type))""")
        c.execute("""CREATE TABLE IF NOT EXISTS meal_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meal_id INTEGER NOT NULL REFERENCES meals(id) ON DELETE CASCADE,
            food_id INTEGER NOT NULL,
            food_name TEXT NOT NULL,
            calories REAL NOT NULL,
            amount REAL NOT NULL,
            unit TEXT NOT NULL)""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_meal_items_meal_id ON meal_items(meal_id)")
        # Cache parsed calories for catalog foods whose source value is textual
        # (e.g. "300-500", "450 تا 500 کالری"). NULL means "not parsed yet".
        # Uses a distinct name because SQLite column names are case-insensitive
        # and the source table already has a "Calories" column.
        if "calories_cached" not in {r[1] for r in c.execute("PRAGMA table_info(foods)")}:
            c.execute("ALTER TABLE foods ADD COLUMN calories_cached REAL")
        if "meals_legacy" in {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
            c.execute("""INSERT INTO meals(user_id,date,meal_type,created_at,updated_at)
                         SELECT user_id, substr(eaten_at,1,10), meal_type, min(eaten_at), max(eaten_at)
                         FROM meals_legacy GROUP BY user_id, substr(eaten_at,1,10), meal_type""")
            c.execute("""INSERT INTO meal_items(meal_id,food_id,food_name,calories,amount,unit)
                         SELECT m.id, l.food_id, l.food_name, l.calories, l.amount, l.unit
                         FROM meals_legacy l
                         JOIN meals m ON m.user_id=l.user_id AND m.meal_type=l.meal_type
                                      AND m.date=substr(l.eaten_at,1,10)
                         ORDER BY l.id""")
            c.execute("DROP TABLE meals_legacy")
        c.execute("PRAGMA foreign_key_check")
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


init_db()


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def create_token(user_id: int) -> str:
    header = b64(b'{"alg":"HS256","typ":"JWT"}')
    payload = b64(json.dumps({"sub": user_id, "exp": int((datetime.now(timezone.utc) + timedelta(days=TOKEN_DAYS)).timestamp())}).encode())
    signing = f"{header}.{payload}"
    sig = b64(hmac.new(JWT_SECRET.encode(), signing.encode(), hashlib.sha256).digest())
    return f"{signing}.{sig}"


def current_user(authorization: Optional[str] = Header(None)) -> int:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "توکن لازم است")
    try:
        token = authorization.split(None, 1)[1]
        head, body, sig = token.split(".")
        expected = b64(hmac.new(JWT_SECRET.encode(), f"{head}.{body}".encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected):
            raise ValueError()
        payload = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        if payload["exp"] < datetime.now(timezone.utc).timestamp():
            raise ValueError()
        return int(payload["sub"])
    except Exception:
        raise HTTPException(401, "توکن نامعتبر یا منقضی است")


def existing_user(user_id: int = Depends(current_user)) -> int:
    """Like current_user, but also rejects tokens whose user no longer exists."""
    with connect() as c:
        row = c.execute("SELECT 1 FROM users WHERE id=?", (user_id,)).fetchone()
    if row is None:
        raise HTTPException(401, "کاربر این توکن دیگر وجود ندارد")
    return user_id


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return f"{b64(salt)}${b64(hashed)}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_s, hash_s = stored.split("$")
        salt = base64.urlsafe_b64decode(salt_s + "=" * (-len(salt_s) % 4))
        expected = base64.urlsafe_b64decode(hash_s + "=" * (-len(hash_s) % 4))
        return hmac.compare_digest(expected, hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000))
    except Exception:
        return False


class RegisterIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone: StrictStr = Field(min_length=1, max_length=32)
    password: StrictStr = Field(min_length=1, max_length=256)
    gender: StrictInt = Field(ge=0, le=1)
    height: StrictInt = Field(gt=0, le=300)
    weight: StrictInt = Field(gt=0, le=700)
    name: StrictStr = Field(min_length=1, max_length=100)
    birth_year: StrictInt = Field(gt=1900, le=date.today().year)


class LoginIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone: StrictStr = Field(min_length=1, max_length=32)
    password: StrictStr = Field(min_length=1, max_length=256)


class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    height: Optional[StrictInt] = Field(None, gt=0, le=300)
    weight: Optional[StrictInt] = Field(None, gt=0, le=700)
    age: Optional[StrictInt] = Field(None, gt=0, le=130)


class MealItemIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    food_id: StrictInt = Field(ge=0, le=2**31 - 1)
    amount: StrictFloat = Field(gt=0, le=MAX_AMOUNT, allow_inf_nan=False)
    # Required for a custom food (food_id=0)
    calories: Optional[StrictFloat] = Field(None, ge=0, le=MAX_CALORIES, allow_inf_nan=False)
    name: Optional[StrictStr] = Field(None, max_length=100)
    unit: Optional[StrictStr] = Field(None, max_length=32)


class MealIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    meal_id: StrictInt = Field(ge=1, le=4, description="1 breakfast, 2 lunch, 3 dinner, 4 snack")
    foods: list[MealItemIn] = Field(min_length=1, max_length=MAX_FOODS_PER_MEAL)
    # Defaults to today's date when omitted.
    meal_date: Optional[date] = Field(None, alias="date", ge=MIN_DATE, le=date(2100, 12, 31))


MEAL_NAMES = {1: "breakfast", 2: "lunch", 3: "dinner", 4: "snack"}


@app.post("/auth/register", status_code=201)
def register(data: RegisterIn):
    with connect() as c:
        try:
            cur = c.execute("INSERT INTO users(phone,password_hash,gender,height,weight,name,birth_year) VALUES(?,?,?,?,?,?,?)",
                            (data.phone, hash_password(data.password), data.gender, data.height, data.weight, data.name, data.birth_year))
        except sqlite3.IntegrityError as exc:
            if "users.phone" in str(exc) or "UNIQUE" in str(exc).upper():
                raise HTTPException(409, "این شماره تلفن قبلا ثبت شده است")
            raise HTTPException(422, "داده‌های ثبت‌نام نامعتبر است")
        return {"access_token": create_token(cur.lastrowid), "token_type": "bearer"}


@app.post("/auth/login")
def login(data: LoginIn):
    with connect() as c:
        user = c.execute("SELECT id,password_hash FROM users WHERE phone=?", (data.phone,)).fetchone()
    if not user or not verify_password(data.password, user["password_hash"]):
        raise HTTPException(401, "شماره تلفن یا رمز عبور اشتباه است")
    return {"access_token": create_token(user["id"]), "token_type": "bearer"}


@app.get("/me")
def get_me(user_id: int = Depends(existing_user)):
    with connect() as c:
        row = c.execute("SELECT id,phone,name,gender,height,weight,birth_year FROM users WHERE id=?", (user_id,)).fetchone()
    if not row:
        raise HTTPException(404, "کاربر پیدا نشد")
    return dict(row)


@app.patch("/profile")
def update_profile(data: ProfileUpdate, user_id: int = Depends(existing_user)):
    changes = data.model_dump(exclude_unset=True, exclude_none=True)
    if not changes:
        raise HTTPException(400, "حداقل یک مورد برای تغییر ارسال کنید")
    if "age" in changes:
        changes["birth_year"] = date.today().year - changes.pop("age")
    with connect() as c:
        assignments = ",".join(f"{key}=?" for key in changes)
        c.execute(f"UPDATE users SET {assignments} WHERE id=?", (*changes.values(), user_id))
        row = c.execute("SELECT id,phone,name,gender,height,weight,birth_year FROM users WHERE id=?", (user_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "کاربر پیدا نشد")
    return dict(row)


def food_columns(c):
    return {r[1] for r in c.execute("PRAGMA table_info(foods)")}


def parse_calories(raw) -> Optional[float]:
    """Extract a calorie number from messy catalog values.

    Handles ASCII/Persian/Arabic digits and formats like "300-500" or
    "450 تا 500 کالری" (averaging a range). Returns None when no number
    is present (e.g. "-").
    """
    text = str(raw).translate(DIGIT_MAP).replace(",", "")
    numbers = [float(n) for n in NUMBER_RE.findall(text)]
    if not numbers:
        return None
    value = sum(numbers) / len(numbers) if len(numbers) > 1 else numbers[0]
    return value if math.isfinite(value) and value >= 0 else None


def search_food(c, value: str):
    cols = food_columns(c)
    if "Food_Item" in cols:
        name_col, cal_col, id_col = '"Food_Item"', '"Calories"', "rowid"
    else:
        name_col, cal_col, id_col = "name", "calories", "id"
    normalized_value = value.translate(DIGIT_MAP)
    if normalized_value.isdecimal():
        row = c.execute(f"SELECT {id_col} AS id,{name_col} AS name,{cal_col} AS calories FROM foods WHERE {id_col}=? LIMIT 1", (int(normalized_value),)).fetchone()
        return [dict(row)] if row else []
    return [dict(r) for r in c.execute(f"SELECT {id_col} AS id,{name_col} AS name,{cal_col} AS calories FROM foods WHERE {name_col} LIKE ? LIMIT 100", (f"%{value}%",))]


@app.get("/foods")
def list_foods(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with connect() as c:
        cols = food_columns(c)
        name_col, cal_col, id_col = ('"Food_Item"', '"Calories"', "rowid") if "Food_Item" in cols else ("name", "calories", "id")
        rows = c.execute(f"SELECT {id_col} AS id,{name_col} AS name,{cal_col} AS calories FROM foods ORDER BY {id_col} LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    return [dict(r) for r in rows]


@app.get("/foods/search")
def foods_search(q: str = Query(min_length=1, max_length=100)):
    with connect() as c:
        return search_food(c, q)


def resolve_food(c, item: MealItemIn):
    """Return (food_id, name, calories, amount, unit) for a submitted meal item."""
    if item.food_id == 0:
        if item.name is None or not item.name.strip() or item.calories is None or item.unit is None or not item.unit.strip():
            raise HTTPException(422, "برای غذای دلخواه name، calories و unit لازم است")
        if item.calories > MAX_CALORIES:
            raise HTTPException(422, "مقدار کالری غیرمنطقی است")
        return 0, item.name.strip(), item.calories, item.amount, item.unit.strip()
    found = search_food(c, str(item.food_id))
    if not found:
        raise HTTPException(404, "غذا پیدا نشد")
    name = found[0]["name"]
    cols = food_columns(c)
    cached = None
    if "calories_cached" in cols:
        cached_row = c.execute("SELECT calories_cached FROM foods WHERE rowid=?", (item.food_id,)).fetchone()
        if cached_row is not None:
            cached = cached_row["calories_cached"]
    if cached is None:
        cached = parse_calories(found[0]["calories"])
        if cached is None:
            raise HTTPException(422, "مقدار کالری این غذا در دیتابیس معتبر نیست")
        if "calories_cached" in cols:
            c.execute("UPDATE foods SET calories_cached=? WHERE rowid=?", (cached, item.food_id))
    if cached > MAX_CALORIES:
        raise HTTPException(422, "مقدار کالری این غذا در دیتابیس معتبر نیست")
    return item.food_id, name, cached, item.amount, "serving"


def serialize_meal(c, meal_row):
    items = c.execute("SELECT * FROM meal_items WHERE meal_id=? ORDER BY id", (meal_row["id"],)).fetchall()
    return {
        "id": meal_row["id"],
        "user_id": meal_row["user_id"],
        "date": meal_row["date"],
        "meal_id": meal_row["meal_type"],
        "meal_name": MEAL_NAMES[meal_row["meal_type"]],
        "foods": [dict(i) for i in items],
        "created_at": meal_row["created_at"],
        "updated_at": meal_row["updated_at"],
        "total_calories": sum(i["calories"] * i["amount"] for i in items),
    }


@app.post("/meals", status_code=201)
def add_meal(data: MealIn, response: Response, user_id: int = Depends(existing_user)):
    """Create or fully replace the (user, date, meal_type) meal with this payload."""
    now = local_now().isoformat()
    day = (data.meal_date or local_now().date()).isoformat()
    with connect() as c:
        # Validate/resolve every submitted food before touching the database.
        resolved = [resolve_food(c, item) for item in data.foods]
        existing = c.execute("SELECT id FROM meals WHERE user_id=? AND date=? AND meal_type=?",
                             (user_id, day, data.meal_id)).fetchone()
        # Upsert in one statement so concurrent requests cannot race on the
        # UNIQUE(user_id,date,meal_type) constraint.
        row = c.execute("""INSERT INTO meals(user_id,date,meal_type,created_at,updated_at)
                           VALUES(?,?,?,?,?)
                           ON CONFLICT(user_id,date,meal_type)
                           DO UPDATE SET updated_at=excluded.updated_at
                           RETURNING *""",
                        (user_id, day, data.meal_id, now, now)).fetchone()
        meal_id = row["id"]
        # Replace-on-post: drop the previous items so the meal matches the latest payload exactly.
        c.execute("DELETE FROM meal_items WHERE meal_id=?", (meal_id,))
        for food_id, name, calories, amount, unit in resolved:
            c.execute("INSERT INTO meal_items(meal_id,food_id,food_name,calories,amount,unit) VALUES(?,?,?,?,?,?)",
                      (meal_id, food_id, name, calories, amount, unit))
        meal_row = c.execute("SELECT * FROM meals WHERE id=?", (meal_id,)).fetchone()
        result = serialize_meal(c, meal_row)
    # 200 when the meal already existed and was replaced, 201 when newly created.
    if existing is not None:
        response.status_code = 200
    return result


@app.get("/meals")
def get_meals(day: Optional[date] = Query(None), user_id: int = Depends(existing_user)):
    with connect() as c:
        if day:
            rows = c.execute("SELECT * FROM meals WHERE user_id=? AND date=? ORDER BY meal_type",
                             (user_id, day.isoformat())).fetchall()
        else:
            rows = c.execute("SELECT * FROM meals WHERE user_id=? ORDER BY date,meal_type", (user_id,)).fetchall()
        meals = [serialize_meal(c, row) for row in rows]
    return {"date": day.isoformat() if day else None, "meals": meals}


@app.get("/")
def root():
    return {"message": "Food Diary API", "docs": "/docs"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
