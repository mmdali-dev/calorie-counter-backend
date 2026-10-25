"""Simple food diary API — single-file FastAPI + SQLAlchemy app.

Run the server:      python main.py
Or with uvicorn:     python -m uvicorn main:app --reload
Run self-tests:      python main.py --selftest

Everything lives in this one module: the SQLAlchemy engine, ORM models, schema
migrations, the FastAPI application, and a self-contained test suite.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Iterator, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr
from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    REAL,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    event,
    literal_column,
    select,
    text,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    mapped_column,
    sessionmaker,
)
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "food.db")
DATABASE_URL = os.environ.get("DATABASE_URL", f"sqlite:///{DB_FILE}")
JWT_SECRET = os.environ.get("JWT_SECRET", "change-this-secret-before-public-use")
TOKEN_DAYS = 30

try:
    APP_TZ = ZoneInfo(os.environ.get("APP_TIMEZONE", "Asia/Tehran"))
except ZoneInfoNotFoundError:
    APP_TZ = timezone.utc

MAX_CALORIES = 100_000.0
MAX_AMOUNT = 100_000.0
MAX_FOODS_PER_MEAL = 100
MIN_DATE = date(1900, 1, 1)
MAX_DATE = date(2100, 12, 31)

DIGIT_MAP = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")

MEAL_NAMES = {1: "breakfast", 2: "lunch", 3: "dinner", 4: "snack"}

app = FastAPI(title="Food Diary API", version="1.0")


def local_now() -> datetime:
    return datetime.now(APP_TZ)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    __table_args__ = (CheckConstraint("gender IN (0,1)", name="ck_users_gender"),)

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    phone = mapped_column(Text, unique=True, nullable=False)
    password_hash = mapped_column(Text, nullable=False)
    gender = mapped_column(Integer, nullable=False)
    height = mapped_column(Integer, nullable=False)
    weight = mapped_column(Integer, nullable=False)
    name = mapped_column(Text, nullable=False)
    birth_year = mapped_column(Integer, nullable=False)
    activity_level = mapped_column(Integer, nullable=False, default=3)


class Meal(Base):
    __tablename__ = "meals"
    __table_args__ = (
        UniqueConstraint("user_id", "date", "meal_type", name="uq_meals_user_date_type"),
        CheckConstraint("meal_type BETWEEN 1 AND 4", name="ck_meals_type"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id = mapped_column(ForeignKey("users.id"), nullable=False)
    date = mapped_column(Text, nullable=False)
    meal_type = mapped_column(Integer, nullable=False)
    created_at = mapped_column(Text, nullable=False)
    updated_at = mapped_column(Text, nullable=False)


class MealItem(Base):
    __tablename__ = "meal_items"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    meal_id = mapped_column(ForeignKey("meals.id", ondelete="CASCADE"), nullable=False)
    food_id = mapped_column(Integer, nullable=False)
    food_name = mapped_column(Text, nullable=False)
    calories = mapped_column(REAL, nullable=False)
    amount = mapped_column(REAL, nullable=False)
    unit = mapped_column(Text, nullable=False)


Index("idx_meal_items_meal_id", MealItem.meal_id)


def _set_sqlite_pragmas(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.close()


engine: Engine = create_engine(DATABASE_URL, future=True)
event.listen(engine, "connect", _set_sqlite_pragmas)

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)

_foods_md = MetaData()
_foods: Optional[Table] = None
_foods_lock = threading.Lock()


def foods_table() -> Table:
    global _foods
    if _foods is None:
        with _foods_lock:
            if _foods is None:
                _foods_md.clear()
                _foods = Table("foods", _foods_md, autoload_with=engine)
    return _foods


def reset_reflection() -> None:
    global _foods
    _foods = None
    _foods_md.clear()


def food_columns() -> set[str]:
    return set(foods_table().c.keys())


def food_id_col():
    t = foods_table()
    return t.c.id if "id" in t.c else literal_column("rowid")


def food_name_col():
    t = foods_table()
    return t.c["Food_Item"] if "Food_Item" in t.c else t.c.name


def food_calories_col():
    t = foods_table()
    return t.c["Calories"] if "Calories" in t.c else t.c.calories


def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# --------------------------------------------------------------------------- #
# Schema bootstrap and migrations
# --------------------------------------------------------------------------- #


def _table_exists(conn, name: str) -> bool:
    return conn.execute(
        text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:n"), {"n": name}
    ).fetchone() is not None


def _column_names(conn, table: str) -> set[str]:
    return {row[1] for row in conn.execute(text(f"PRAGMA table_info('{table}')"))}


def _rebuild_users_with_gender_check(conn) -> None:
    users_sql = conn.execute(
        text("SELECT sql FROM sqlite_master WHERE type='table' AND name='users'")
    ).scalar() or ""
    if "gender IN (0,1)" in users_sql:
        return
    conn.execute(text("UPDATE users SET gender=0 WHERE gender NOT IN (0,1) OR gender IS NULL"))
    conn.exec_driver_sql("ALTER TABLE users RENAME TO users_old")
    conn.exec_driver_sql(
        """CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL, gender INTEGER NOT NULL CHECK(gender IN (0,1)),
            height INTEGER NOT NULL, weight INTEGER NOT NULL, name TEXT NOT NULL,
            birth_year INTEGER NOT NULL)"""
    )
    conn.exec_driver_sql(
        """INSERT INTO users(id,phone,password_hash,gender,height,weight,name,birth_year)
           SELECT id,phone,password_hash,gender,height,weight,name,birth_year FROM users_old"""
    )
    conn.exec_driver_sql("DROP TABLE users_old")


def _migrate_legacy_meals(conn) -> None:
    cols = _column_names(conn, "meals")
    if not cols or "food_name" not in cols:
        return
    conn.exec_driver_sql("ALTER TABLE meals RENAME TO meals_legacy")
    conn.exec_driver_sql(
        """CREATE TABLE meals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            date TEXT NOT NULL,
            meal_type INTEGER NOT NULL CHECK(meal_type BETWEEN 1 AND 4),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, date, meal_type))"""
    )
    conn.exec_driver_sql(
        """CREATE TABLE meal_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meal_id INTEGER NOT NULL REFERENCES meals(id) ON DELETE CASCADE,
            food_id INTEGER NOT NULL,
            food_name TEXT NOT NULL,
            calories REAL NOT NULL,
            amount REAL NOT NULL,
            unit TEXT NOT NULL)"""
    )
    conn.exec_driver_sql(
        """INSERT INTO meals(user_id,date,meal_type,created_at,updated_at)
           SELECT user_id, substr(eaten_at,1,10), meal_type, min(eaten_at), max(eaten_at)
           FROM meals_legacy GROUP BY user_id, substr(eaten_at,1,10), meal_type"""
    )
    conn.exec_driver_sql(
        """INSERT INTO meal_items(meal_id,food_id,food_name,calories,amount,unit)
           SELECT m.id, l.food_id, l.food_name, l.calories, l.amount, l.unit
           FROM meals_legacy l
           JOIN meals m ON m.user_id=l.user_id AND m.meal_type=l.meal_type
                        AND m.date=substr(l.eaten_at,1,10)
           ORDER BY l.id"""
    )
    conn.exec_driver_sql("DROP TABLE meals_legacy")


def init_db() -> None:
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA foreign_keys = OFF")
        try:
            if not _table_exists(conn, "foods"):
                conn.exec_driver_sql(
                    """CREATE TABLE foods (
                        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                        calories REAL NOT NULL)"""
                )
            _rebuild_users_with_gender_check(conn)
            if "activity_level" not in _column_names(conn, "users"):
                conn.exec_driver_sql(
                    "ALTER TABLE users ADD COLUMN activity_level INTEGER NOT NULL DEFAULT 3"
                )
            _migrate_legacy_meals(conn)
            Base.metadata.create_all(conn)
            if "calories_cached" not in _column_names(conn, "foods"):
                conn.exec_driver_sql("ALTER TABLE foods ADD COLUMN calories_cached REAL")
            conn.exec_driver_sql("PRAGMA foreign_key_check")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.exec_driver_sql("PRAGMA foreign_keys = ON")
    foods_table()


def configure(url: str) -> None:
    global engine, SessionLocal
    engine.dispose()
    engine = create_engine(url, future=True)
    event.listen(engine, "connect", _set_sqlite_pragmas)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)
    reset_reflection()
    init_db()


init_db()


# --------------------------------------------------------------------------- #
# Auth helpers
# --------------------------------------------------------------------------- #


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


def existing_user(user_id: int = Depends(current_user), session: Session = Depends(get_session)) -> int:
    if session.get(User, user_id) is None:
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


# --------------------------------------------------------------------------- #
# Request models
# --------------------------------------------------------------------------- #


class RegisterIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone: StrictStr = Field(min_length=1, max_length=32)
    password: StrictStr = Field(min_length=1, max_length=256)
    gender: StrictInt = Field(ge=0, le=1)
    height: StrictInt = Field(gt=0, le=300)
    weight: StrictInt = Field(gt=0, le=700)
    name: StrictStr = Field(min_length=1, max_length=100)
    birth_year: StrictInt = Field(gt=1900, le=date.today().year)
    activity_level: StrictInt = Field(3, ge=1, le=5)


class LoginIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone: StrictStr = Field(min_length=1, max_length=32)
    password: StrictStr = Field(min_length=1, max_length=256)


class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    height: Optional[StrictInt] = Field(None, gt=0, le=300)
    weight: Optional[StrictInt] = Field(None, gt=0, le=700)
    age: Optional[StrictInt] = Field(None, gt=0, le=130)
    gender: Optional[StrictInt] = Field(None, ge=0, le=1)
    activity_level: Optional[StrictInt] = Field(None, ge=1, le=5)


class MealItemIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    food_id: StrictInt = Field(ge=0, le=2**31 - 1)
    amount: StrictFloat = Field(gt=0, le=MAX_AMOUNT, allow_inf_nan=False)
    calories: Optional[StrictFloat] = Field(None, ge=0, le=MAX_CALORIES, allow_inf_nan=False)
    name: Optional[StrictStr] = Field(None, max_length=100)
    unit: Optional[StrictStr] = Field(None, max_length=32)


class MealIn(BaseModel):
    model_config = ConfigDict(extra="forbid")
    meal_id: StrictInt = Field(ge=1, le=4, description="1 breakfast, 2 lunch, 3 dinner, 4 snack")
    foods: list[MealItemIn] = Field(min_length=1, max_length=MAX_FOODS_PER_MEAL)
    meal_date: Optional[date] = Field(None, alias="date", ge=MIN_DATE, le=MAX_DATE)


# --------------------------------------------------------------------------- #
# Food helpers
# --------------------------------------------------------------------------- #


def parse_calories(raw) -> Optional[float]:
    text = str(raw).translate(DIGIT_MAP).replace(",", "")
    numbers = [float(n) for n in NUMBER_RE.findall(text)]
    if not numbers:
        return None
    value = sum(numbers) / len(numbers) if len(numbers) > 1 else numbers[0]
    return value if math.isfinite(value) and value >= 0 else None


def search_food(session: Session, value: str) -> list[dict]:
    id_col = food_id_col().label("id")
    name_col = food_name_col().label("name")
    cal_col = food_calories_col().label("calories")
    normalized = value.translate(DIGIT_MAP)
    if normalized.isdecimal():
        stmt = select(id_col, name_col, cal_col).where(food_id_col() == int(normalized)).limit(1)
    else:
        stmt = select(id_col, name_col, cal_col).where(food_name_col().like(f"%{value}%")).limit(100)
    return [dict(row._mapping) for row in session.execute(stmt)]


def food_cached_calories(session: Session, food_id: int) -> Optional[float]:
    if "calories_cached" not in food_columns():
        return None
    stmt = select(foods_table().c.calories_cached).where(food_id_col() == food_id)
    return session.execute(stmt).scalar()


def set_food_cached_calories(session: Session, food_id: int, value: float) -> None:
    if "calories_cached" not in food_columns():
        return
    session.execute(foods_table().update().where(food_id_col() == food_id).values(calories_cached=value))


def resolve_food(session: Session, item: MealItemIn) -> tuple[int, str, float, float, str]:
    if item.food_id == 0:
        if item.name is None or not item.name.strip() or item.calories is None or item.unit is None or not item.unit.strip():
            raise HTTPException(422, "برای غذای دلخواه name، calories و unit لازم است")
        if item.calories > MAX_CALORIES:
            raise HTTPException(422, "مقدار کالری غیرمنطقی است")
        return 0, item.name.strip(), item.calories, item.amount, item.unit.strip()
    found = search_food(session, str(item.food_id))
    if not found:
        raise HTTPException(404, "غذا پیدا نشد")
    name = found[0]["name"]
    cached = food_cached_calories(session, item.food_id)
    if cached is None:
        cached = parse_calories(found[0]["calories"])
        if cached is None:
            raise HTTPException(422, "مقدار کالری این غذا در دیتابیس معتبر نیست")
        set_food_cached_calories(session, item.food_id, cached)
    if cached > MAX_CALORIES:
        raise HTTPException(422, "مقدار کالری این غذا در دیتابیس معتبر نیست")
    return item.food_id, name, cached, item.amount, "serving"


def serialize_meal(session: Session, meal: Meal) -> dict:
    items = session.execute(
        select(MealItem).where(MealItem.meal_id == meal.id).order_by(MealItem.id)
    ).scalars().all()
    return {
        "id": meal.id,
        "user_id": meal.user_id,
        "date": meal.date,
        "meal_id": meal.meal_type,
        "meal_name": MEAL_NAMES[meal.meal_type],
        "foods": [
            {
                "id": i.id, "meal_id": i.meal_id, "food_id": i.food_id, "food_name": i.food_name,
                "calories": i.calories, "amount": i.amount, "unit": i.unit,
            }
            for i in items
        ],
        "created_at": meal.created_at,
        "updated_at": meal.updated_at,
        "total_calories": sum(i.calories * i.amount for i in items),
    }


def _serialize_user(user: User) -> dict:
    return {
        "id": user.id,
        "phone": user.phone,
        "name": user.name,
        "gender": user.gender,
        "height": user.height,
        "weight": user.weight,
        "birth_year": user.birth_year,
        "activity_level": user.activity_level,
        "age": date.today().year - user.birth_year,
    }


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #


@app.post("/auth/register", status_code=201)
def register(data: RegisterIn, session: Session = Depends(get_session)):
    user = User(
        phone=data.phone, password_hash=hash_password(data.password), gender=data.gender,
        height=data.height, weight=data.weight, name=data.name, birth_year=data.birth_year,
        activity_level=data.activity_level,
    )
    session.add(user)
    try:
        session.flush()
    except Exception as exc:
        session.rollback()
        if "UNIQUE" in str(getattr(exc, "orig", exc)).upper():
            raise HTTPException(409, "این شماره تلفن قبلا ثبت شده است")
        raise HTTPException(422, "داده‌های ثبت‌نام نامعتبر است")
    session.commit()
    return {"access_token": create_token(user.id), "token_type": "bearer"}


@app.post("/auth/login")
def login(data: LoginIn, session: Session = Depends(get_session)):
    user = session.execute(select(User).where(User.phone == data.phone)).scalar_one_or_none()
    if not user or not verify_password(data.password, user.password_hash):
        raise HTTPException(401, "شماره تلفن یا رمز عبور اشتباه است")
    return {"access_token": create_token(user.id), "token_type": "bearer"}


@app.get("/me")
def get_me(user_id: int = Depends(existing_user), session: Session = Depends(get_session)):
    user = session.get(User, user_id)
    if user is None:
        raise HTTPException(404, "کاربر پیدا نشد")
    return _serialize_user(user)


@app.get("/profile")
def get_profile(user_id: int = Depends(existing_user), session: Session = Depends(get_session)):
    user = session.get(User, user_id)
    if user is None:
        raise HTTPException(404, "کاربر پیدا نشد")
    return _serialize_user(user)


@app.patch("/profile")
def update_profile(data: ProfileUpdate, user_id: int = Depends(existing_user), session: Session = Depends(get_session)):
    changes = data.model_dump(exclude_unset=True, exclude_none=True)
    if not changes:
        raise HTTPException(400, "حداقل یک مورد برای تغییر ارسال کنید")
    if "age" in changes:
        changes["birth_year"] = date.today().year - changes.pop("age")
    user = session.get(User, user_id)
    if user is None:
        raise HTTPException(404, "کاربر پیدا نشد")
    for key, value in changes.items():
        setattr(user, key, value)
    session.commit()
    session.refresh(user)
    return _serialize_user(user)


@app.get("/foods")
def list_foods(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0), session: Session = Depends(get_session)):
    id_col = food_id_col().label("id")
    name_col = food_name_col().label("name")
    cal_col = food_calories_col().label("calories")
    stmt = select(id_col, name_col, cal_col).order_by(food_id_col()).limit(limit).offset(offset)
    return [dict(row._mapping) for row in session.execute(stmt)]


@app.get("/foods/search")
def foods_search(q: str = Query(min_length=1, max_length=100), session: Session = Depends(get_session)):
    return search_food(session, q)


@app.post("/meals", status_code=201)
def add_meal(data: MealIn, response: Response, user_id: int = Depends(existing_user), session: Session = Depends(get_session)):
    now = local_now().isoformat()
    day = (data.meal_date or local_now().date()).isoformat()
    resolved = [resolve_food(session, item) for item in data.foods]
    existed = session.execute(
        select(Meal.id).where(Meal.user_id == user_id, Meal.date == day, Meal.meal_type == data.meal_id)
    ).first() is not None
    stmt = (
        sqlite_insert(Meal)
        .values(user_id=user_id, date=day, meal_type=data.meal_id, created_at=now, updated_at=now)
        .on_conflict_do_update(index_elements=["user_id", "date", "meal_type"], set_={"updated_at": now})
        .returning(Meal)
    )
    meal = session.execute(stmt).scalar_one()
    session.execute(delete(MealItem).where(MealItem.meal_id == meal.id))
    for food_id, name, calories, amount, unit in resolved:
        session.add(MealItem(meal_id=meal.id, food_id=food_id, food_name=name, calories=calories, amount=amount, unit=unit))
    session.flush()
    session.refresh(meal)
    result = serialize_meal(session, meal)
    session.commit()
    if existed:
        response.status_code = 200
    return result


@app.get("/meals")
def get_meals(day: Optional[date] = Query(None), user_id: int = Depends(existing_user), session: Session = Depends(get_session)):
    stmt = select(Meal).where(Meal.user_id == user_id)
    if day:
        stmt = stmt.where(Meal.date == day.isoformat()).order_by(Meal.meal_type)
    else:
        stmt = stmt.order_by(Meal.date, Meal.meal_type)
    meals = session.execute(stmt).scalars().all()
    return {"date": day.isoformat() if day else None, "meals": [serialize_meal(session, m) for m in meals]}


@app.get("/")
def root():
    return {"message": "Food Diary API", "docs": "/docs"}


# --------------------------------------------------------------------------- #
# Self-tests (python main.py --selftest)
# --------------------------------------------------------------------------- #


def _run_self_tests() -> int:
    import shutil
    import sqlite3
    import tempfile

    from fastapi.testclient import TestClient

    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(("[OK ] " if condition else "[FAIL] ") + label)
        if not condition:
            failures.append(label)

    tmpdir = tempfile.mkdtemp(prefix="calorie-selftest-")
    temp_db = os.path.join(tmpdir, "food.db")
    shutil.copy(DB_FILE, temp_db)
    configure(f"sqlite:///{temp_db}")
    client = TestClient(app, raise_server_exceptions=False)

    def auth(phone: str) -> dict:
        r = client.post("/auth/register", json={
            "phone": phone, "password": "secret123", "gender": 0,
            "height": 178, "weight": 74, "name": "Test", "birth_year": 1994,
        })
        assert r.status_code == 201, r.text
        return {"Authorization": f"Bearer {r.json()['access_token']}"}

    day = "2026-10-25"

    print("== auth ==")
    check("register 201", client.post("/auth/register", json={
        "phone": "09550000001", "password": "pw", "gender": 0, "height": 180,
        "weight": 80, "name": "n", "birth_year": 1990}).status_code == 201)
    check("register duplicate 409", client.post("/auth/register", json={
        "phone": "09550000001", "password": "pw", "gender": 0, "height": 180,
        "weight": 80, "name": "n", "birth_year": 1990}).status_code == 409)
    check("register bad gender 422", client.post("/auth/register", json={
        "phone": "09550000002", "password": "pw", "gender": 9, "height": 1,
        "weight": 1, "name": "n", "birth_year": 1990}).status_code == 422)
    check("login ok", client.post("/auth/login", json={"phone": "09550000001", "password": "pw"}).status_code == 200)
    check("login wrong pw 401", client.post("/auth/login", json={"phone": "09550000001", "password": "x"}).status_code == 401)
    check("me no token 401", client.get("/me").status_code == 401)
    check("me garbage token 401", client.get("/me", headers={"Authorization": "Bearer a.b.c"}).status_code == 401)

    headers = auth("09550000010")
    check("me ok", client.get("/me", headers=headers).status_code == 200)
    check("profile ok", client.get("/profile", headers=headers).status_code == 200)
    check("profile has activity_level", client.get("/profile", headers=headers).json().get("activity_level") is not None)
    check("profile has age", client.get("/profile", headers=headers).json().get("age") is not None)
    check("profile patch ok", client.patch("/profile", headers=headers, json={"weight": 75}).status_code == 200)
    check("profile patch activity_level", client.patch("/profile", headers=headers, json={"activity_level": 4}).json().get("activity_level") == 4)
    check("profile empty 400", client.patch("/profile", headers=headers, json={}).status_code == 400)

    print("== foods ==")
    check("list foods", len(client.get("/foods", params={"limit": 5}).json()) == 5)
    check("search by name", len(client.get("/foods/search", params={"q": "سیب"}).json()) > 0)
    check("search by id", client.get("/foods/search", params={"q": "1"}).json()[0]["id"] == 1)
    check("search persian digit id", client.get("/foods/search", params={"q": "۱"}).json()[0]["id"] == 1)
    check("search q bounded 422", client.get("/foods/search", params={"q": "x" * 101}).status_code == 422)

    print("== meals core ==")
    r1 = client.post("/meals", headers=headers, json={
        "meal_id": 1, "date": day,
        "foods": [{"food_id": 1, "amount": 2}, {"food_id": 0, "amount": 1, "name": "کیک", "calories": 350, "unit": "g"}]})
    check("create 201 total 454", r1.status_code == 201 and r1.json()["total_calories"] == 454)
    r2 = client.post("/meals", headers=headers, json={
        "meal_id": 1, "date": day, "foods": [{"food_id": 0, "amount": 2, "name": "تخم مرغ", "calories": 155, "unit": "عدد"}]})
    check("replace 200 same id", r2.status_code == 200 and r2.json()["id"] == r1.json()["id"] and len(r2.json()["foods"]) == 1)
    check("created_at stable", r1.json()["created_at"] == r2.json()["created_at"])
    check("shrink/grow array", all(
        len(client.post("/meals", headers=headers, json={"meal_id": 2, "date": "2026-10-21", "foods": foods}).json()["foods"]) == expected
        for foods, expected in [
            ([{"food_id": 1, "amount": 1}], 1),
            ([{"food_id": 1, "amount": 1}, {"food_id": 9, "amount": 1}, {"food_id": 0, "amount": 1, "name": "نان", "calories": 250, "unit": "برش"}], 3),
            ([{"food_id": 9, "amount": 3}], 1)]))

    print("== messy calories ==")
    for food_id, expected in [(989, 400.0), (1243, 475.0), (1353, 274.0), (1, 52.0)]:
        r = client.post("/meals", headers=headers, json={"meal_id": 3, "date": f"2026-09-{food_id % 9 + 1:02d}", "foods": [{"food_id": food_id, "amount": 1}]})
        check(f"food {food_id} ~ {expected}", abs(r.json()["foods"][0]["calories"] - expected) < 0.01)
    check("no-number food 422", client.post("/meals", headers=headers, json={"meal_id": 3, "date": "2026-09-20", "foods": [{"food_id": 1487, "amount": 1}]}).status_code == 422)

    print("== timezone / defaults ==")
    check("default date is Tehran today", client.post("/meals", headers=headers, json={"meal_id": 4, "foods": [{"food_id": 1, "amount": 1}]}).json()["date"] == local_now().date().isoformat())
    check("boundary dates accepted", all(client.post("/meals", headers=headers, json={"meal_id": 1, "date": d, "foods": [{"food_id": 1, "amount": 1}]}).status_code == 201 for d in ("1900-01-01", "2100-12-31")))

    print("== validation ==")
    for payload, expected in [
        ({"meal_id": 0, "foods": [{"food_id": 1, "amount": 1}]}, 422),
        ({"meal_id": 5, "foods": [{"food_id": 1, "amount": 1}]}, 422),
        ({"meal_id": 1, "foods": []}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 1, "amount": 1}] * 101}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 1, "amount": 0}]}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 1, "amount": "2"}]}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 1, "amount": 10**9}]}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 999999, "amount": 1}]}, 404),
        ({"meal_id": 1, "foods": [{"food_id": 0, "amount": 1, "calories": 10, "unit": "g"}]}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 0, "amount": 1, "name": "x", "calories": 10, "unit": "g" * 33}]}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 1, "amount": 1, "x": 1}]}, 422),
        ({"meal_id": 1, "foods": [{"food_id": 1, "amount": 1}], "x": 1}, 422),
        ({"meal_id": 1, "date": "25/10/2026", "foods": [{"food_id": 1, "amount": 1}]}, 422),
        ({"meal_id": 1, "date": "9999-12-31", "foods": [{"food_id": 1, "amount": 1}]}, 422),
        ({"meal_id": 1, "date": "0001-01-01", "foods": [{"food_id": 1, "amount": 1}]}, 422),
    ]:
        check(f"reject {payload.get('meal_id')} {len(payload.get('foods', []))} -> {expected}", client.post("/meals", headers=headers, json=payload).status_code == expected)

    print("== atomicity / isolation / GET ==")
    before = client.get("/meals", headers=headers, params={"day": day}).json()["meals"][0]["foods"]
    client.post("/meals", headers=headers, json={"meal_id": 1, "date": day, "foods": [{"food_id": 999999, "amount": 1}]})
    after = client.get("/meals", headers=headers, params={"day": day}).json()["meals"][0]["foods"]
    check("failed repost intact", [f["food_name"] for f in before] == [f["food_name"] for f in after])
    other = auth("09550000099")
    client.post("/meals", headers=other, json={"meal_id": 1, "date": day, "foods": [{"food_id": 1, "amount": 1}]})
    mine = [m for m in client.get("/meals", headers=headers, params={"day": day}).json()["meals"] if m["meal_id"] == 1][0]
    theirs = client.get("/meals", headers=other, params={"day": day}).json()["meals"][0]
    check("per-user isolation", mine["foods"][0]["food_name"] != theirs["foods"][0]["food_name"])
    check("all meals date null", client.get("/meals", headers=headers).json()["date"] is None)

    print("== concurrency ==")
    conc_headers = auth("09550000077")
    codes: list[int] = []
    lock = threading.Lock()

    def worker(amount: int) -> None:
        resp = client.post("/meals", headers=conc_headers, json={"meal_id": 1, "date": "2026-12-01", "foods": [{"food_id": 1, "amount": amount}]})
        with lock:
            codes.append(resp.status_code)

    threads = [threading.Thread(target=worker, args=(i + 1,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("concurrent all 200/201", all(c in (200, 201) for c in codes))
    conn = sqlite3.connect(temp_db)
    check("no duplicate meal rows", conn.execute("SELECT COUNT(*) FROM meals WHERE date='2026-12-01' AND meal_type=1").fetchone()[0] == 1)

    print("== deleted user ==")
    dead = auth("09550000055")
    with session_scope() as s:
        uid = s.execute(select(User.id).where(User.phone == "09550000055")).scalar_one()
        s.execute(delete(MealItem).where(MealItem.meal_id.in_(select(Meal.id).where(Meal.user_id == uid))))
        s.execute(delete(Meal).where(Meal.user_id == uid))
        s.execute(delete(User).where(User.id == uid))
    check("meals deleted-user 401", client.post("/meals", headers=dead, json={"meal_id": 1, "foods": [{"food_id": 1, "amount": 1}]}).status_code == 401)
    check("me deleted-user 401", client.get("/me", headers=dead).status_code == 401)
    check("profile deleted-user 401", client.patch("/profile", headers=dead, json={"weight": 80}).status_code == 401)

    print("== schema / integrity ==")
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    check("expected tables", {"users", "foods", "meals", "meal_items"} <= tables)
    check("users gender check", "gender IN (0,1)" in (conn.execute("SELECT sql FROM sqlite_master WHERE name='users'").fetchone()[0]))
    check("users activity_level column", "activity_level" in {r[1] for r in conn.execute("PRAGMA table_info(users)")})
    check("meals unique constraint", "UNIQUE(user_id, date, meal_type)" in (conn.execute("SELECT sql FROM sqlite_master WHERE name='meals'").fetchone()[0]))
    check("foods cache column", "calories_cached" in {r[1] for r in conn.execute("PRAGMA table_info(foods)")})
    check("no FK violations", conn.execute("PRAGMA foreign_key_check").fetchall() == [])
    check("integrity ok", conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok")

    print("== legacy migration ==")
    legacy = os.path.join(tmpdir, "legacy.db")
    lc = sqlite3.connect(legacy)
    lc.execute('CREATE TABLE "foods" ("Food_Item" TEXT, "Calories" TEXT)')
    lc.execute("INSERT INTO foods VALUES ('سیب','52')")
    lc.execute("""CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, phone TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL, gender INTEGER NOT NULL, height INTEGER NOT NULL, weight INTEGER NOT NULL,
        name TEXT NOT NULL, birth_year INTEGER NOT NULL)""")
    lc.execute("INSERT INTO users(phone,password_hash,gender,height,weight,name,birth_year) VALUES('0912','h',7,1,1,'n',1990)")
    lc.execute("""CREATE TABLE meals (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES users(id),
        eaten_at TEXT NOT NULL, meal_type INTEGER NOT NULL, food_id INTEGER NOT NULL, food_name TEXT NOT NULL,
        calories REAL NOT NULL, amount REAL NOT NULL, unit TEXT NOT NULL)""")
    lc.execute("INSERT INTO meals(user_id,eaten_at,meal_type,food_id,food_name,calories,amount,unit) VALUES (1,'2026-10-24T16:38:32+00:00',1,1,'سیب',52,2,'serving')")
    lc.execute("INSERT INTO meals(user_id,eaten_at,meal_type,food_id,food_name,calories,amount,unit) VALUES (1,'2026-10-24T17:00:00+00:00',1,0,'کیک',350,1,'g')")
    lc.commit()
    lc.close()
    configure(f"sqlite:///{legacy}")
    init_db()
    init_db()  # idempotent
    lc = sqlite3.connect(legacy)
    check("legacy migrated to 1 header", lc.execute("SELECT COUNT(*) FROM meals").fetchone()[0] == 1)
    check("legacy migrated to 2 items", lc.execute("SELECT COUNT(*) FROM meal_items").fetchone()[0] == 2)
    check("legacy gender coerced", lc.execute("SELECT gender FROM users").fetchone()[0] == 0)
    check("legacy activity_level added", "activity_level" in {r[1] for r in lc.execute("PRAGMA table_info(users)")})
    check("legacy FK clean", lc.execute("PRAGMA foreign_key_check").fetchall() == [])

    shutil.rmtree(tmpdir, ignore_errors=True)

    print(f"\n{'ALL TESTS PASSED' if not failures else str(len(failures)) + ' FAILED: ' + str(failures)}")
    return 0 if not failures else 1


if __name__ == "__main__":
    import sys
    if "--selftest" in sys.argv:
        raise SystemExit(_run_self_tests())
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)