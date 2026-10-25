"""Simple food diary API. Run with: python3 -m uvicorn main:app --reload"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "food.db")
JWT_SECRET = os.environ.get("JWT_SECRET", "change-this-secret-before-public-use")
TOKEN_DAYS = 30
app = FastAPI(title="Food Diary API", version="1.0")


def db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    with db() as c:
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
        c.execute("""CREATE TABLE IF NOT EXISTS meals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id),
            eaten_at TEXT NOT NULL, meal_type INTEGER NOT NULL CHECK(meal_type BETWEEN 1 AND 4),
            food_id INTEGER NOT NULL, food_name TEXT NOT NULL, calories REAL NOT NULL,
            amount REAL NOT NULL, unit TEXT NOT NULL)""")


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
    phone: StrictStr = Field(min_length=1)
    password: StrictStr = Field(min_length=1)
    gender: StrictInt = Field(ge=0, le=1)
    height: StrictInt = Field(gt=0)
    weight: StrictInt = Field(gt=0)
    name: StrictStr = Field(min_length=1)
    birth_year: StrictInt = Field(gt=1900, le=date.today().year)


class LoginIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone: StrictStr = Field(min_length=1)
    password: StrictStr = Field(min_length=1)


class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    height: Optional[StrictInt] = Field(None, gt=0)
    weight: Optional[StrictInt] = Field(None, gt=0)
    age: Optional[StrictInt] = Field(None, gt=0, le=130)


class MealIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    meal_id: StrictInt = Field(ge=1, le=4, description="1 breakfast, 2 lunch, 3 dinner, 4 snack")
    food_id: StrictInt = Field(ge=0)
    amount: float = Field(gt=0, allow_inf_nan=False)
    # Required for custom food (food_id=0)
    calories: Optional[float] = Field(None, ge=0, allow_inf_nan=False)
    name: Optional[StrictStr] = None
    unit: Optional[StrictStr] = None


MEAL_NAMES = {1: "breakfast", 2: "lunch", 3: "dinner", 4: "snack"}


@app.post("/auth/register", status_code=201)
def register(data: RegisterIn):
    with db() as c:
        try:
            cur = c.execute("INSERT INTO users(phone,password_hash,gender,height,weight,name,birth_year) VALUES(?,?,?,?,?,?,?)",
                            (data.phone, hash_password(data.password), data.gender, data.height, data.weight, data.name, data.birth_year))
        except sqlite3.IntegrityError:
            raise HTTPException(409, "این شماره تلفن قبلا ثبت شده است")
        return {"access_token": create_token(cur.lastrowid), "token_type": "bearer"}


@app.post("/auth/login")
def login(data: LoginIn):
    with db() as c:
        user = c.execute("SELECT id,password_hash FROM users WHERE phone=?", (data.phone,)).fetchone()
    if not user or not verify_password(data.password, user["password_hash"]):
        raise HTTPException(401, "شماره تلفن یا رمز عبور اشتباه است")
    return {"access_token": create_token(user["id"]), "token_type": "bearer"}


@app.patch("/profile")
def update_profile(data: ProfileUpdate, user_id: int = Depends(current_user)):
    changes = data.model_dump(exclude_unset=True, exclude_none=True)
    if not changes:
        raise HTTPException(400, "حداقل یک مورد برای تغییر ارسال کنید")
    if "age" in changes:
        changes["birth_year"] = date.today().year - changes.pop("age")
    with db() as c:
        assignments = ",".join(f"{key}=?" for key in changes)
        c.execute(f"UPDATE users SET {assignments} WHERE id=?", (*changes.values(), user_id))
        row = c.execute("SELECT id,phone,name,gender,height,weight,birth_year FROM users WHERE id=?", (user_id,)).fetchone()
    return dict(row)


def food_columns(c):
    return {r[1] for r in c.execute("PRAGMA table_info(foods)")}


def search_food(c, value: str):
    cols = food_columns(c)
    if "Food_Item" in cols:
        name_col, cal_col, id_col = '"Food_Item"', '"Calories"', "rowid"
    else:
        name_col, cal_col, id_col = "name", "calories", "id"
    digit_map = {ord(ch): str(i) for i, ch in enumerate("۰۱۲۳۴۵۶۷۸۹")}
    digit_map.update({ord(ch): str(i) for i, ch in enumerate("٠١٢٣٤٥٦٧٨٩")})
    normalized_value = value.translate(digit_map)
    if normalized_value.isdecimal():
        row = c.execute(f"SELECT {id_col} AS id,{name_col} AS name,{cal_col} AS calories FROM foods WHERE {id_col}=? LIMIT 1", (int(normalized_value),)).fetchone()
        return [dict(row)] if row else []
    return [dict(r) for r in c.execute(f"SELECT {id_col} AS id,{name_col} AS name,{cal_col} AS calories FROM foods WHERE {name_col} LIKE ? LIMIT 100", (f"%{value}%",))]


@app.get("/foods")
def list_foods(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with db() as c:
        cols = food_columns(c)
        name_col, cal_col, id_col = ('"Food_Item"', '"Calories"', "rowid") if "Food_Item" in cols else ("name", "calories", "id")
        rows = c.execute(f"SELECT {id_col} AS id,{name_col} AS name,{cal_col} AS calories FROM foods ORDER BY {id_col} LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    return [dict(r) for r in rows]


@app.get("/foods/search")
def foods_search(q: str = Query(min_length=1)):
    with db() as c:
        return search_food(c, q)


@app.post("/meals", status_code=201)
def add_meal(data: MealIn, user_id: int = Depends(current_user)):
    now = datetime.now(timezone.utc).isoformat()
    with db() as c:
        if data.food_id == 0:
            if data.name is None or not data.name.strip() or data.calories is None or data.unit is None or not data.unit.strip():
                raise HTTPException(422, "برای غذای دلخواه name، calories و unit لازم است")
            name, calories, unit = data.name.strip(), data.calories, data.unit.strip()
        else:
            found = search_food(c, str(data.food_id))
            if not found:
                raise HTTPException(404, "غذا پیدا نشد")
            name = found[0]["name"]
            try:
                calorie_text = str(found[0]["calories"]).translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")).replace(",", "")
                calories = float(calorie_text)
                if not math.isfinite(calories) or calories < 0:
                    raise ValueError()
            except (TypeError, ValueError):
                raise HTTPException(422, "مقدار کالری این غذا در دیتابیس معتبر نیست")
            unit = "serving"
        cur = c.execute("INSERT INTO meals(user_id,eaten_at,meal_type,food_id,food_name,calories,amount,unit) VALUES(?,?,?,?,?,?,?,?)",
                        (user_id, now, data.meal_id, data.food_id, name, calories, data.amount, unit))
        row = c.execute("SELECT * FROM meals WHERE id=?", (cur.lastrowid,)).fetchone()
    return dict(row)


@app.get("/meals")
def get_meals(day: Optional[date] = Query(None), user_id: int = Depends(current_user)):
    with db() as c:
        if day:
            rows = c.execute("SELECT * FROM meals WHERE user_id=? AND eaten_at LIKE ? ORDER BY eaten_at,id", (user_id, day.isoformat() + "%")).fetchall()
        else:
            rows = c.execute("SELECT * FROM meals WHERE user_id=? ORDER BY eaten_at,id", (user_id,)).fetchall()
    result = {key: [] for key in MEAL_NAMES.values()}
    for row in rows:
        item = dict(row)
        item["meal_name"] = MEAL_NAMES[item.pop("meal_type")]
        result[item["meal_name"]].append(item)
    return {"date": day.isoformat() if day else None, "meals": result}


@app.get("/")
def root():
    return {"message": "Food Diary API", "docs": "/docs"}
