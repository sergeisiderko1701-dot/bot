import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from aiogram.utils import executor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID", "511314867"))

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не знайдено. Додай його в Environment Variables.")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не знайдено. Додай PostgreSQL URL в Environment Variables.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)


# ======================
# CONFIG
# ======================
CLIENT_ORDER_COOLDOWN = 30
MASTER_OFFER_COOLDOWN = 15
MAX_ACTIVE_CLIENT_ORDERS = 3
MAX_ACTIVE_MASTER_ORDERS = 3
ONLINE_TIMEOUT = 300
PAGE_SIZE = 5

CATEGORIES = [
    ("🚰 Сантехнік", "Сантехнік"),
    ("⚡ Електрик", "Електрик"),
    ("🛠 Ремонт", "Ремонт"),
]
CATEGORY_LABEL_TO_VALUE = {label: value for label, value in CATEGORIES}
CATEGORY_VALUE_TO_LABEL = {value: label for label, value in CATEGORIES}

ORDER_STATUSES = {
    "new": "нова",
    "offered": "є пропозиції",
    "matched": "майстер обраний",
    "in_progress": "в роботі",
    "done": "завершена",
    "cancelled": "скасована",
    "expired": "прострочена",
}


# ======================
# DATABASE + PERSISTENT STATE
# ======================
class Database:
    def __init__(self, dsn: str):
        self.pool = pool.SimpleConnectionPool(1, 10, dsn=dsn)
        self._init_schema()

    @contextmanager
    def connection(self):
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    @contextmanager
    def cursor(self):
        with self.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur

    def _init_schema(self):
        with self.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT,
                    category TEXT,
                    district TEXT,
                    problem TEXT,
                    media_type TEXT,
                    media_file_id TEXT,
                    status TEXT DEFAULT 'new',
                    selected_master_id BIGINT,
                    rating INTEGER,
                    review_text TEXT,
                    created_at BIGINT,
                    updated_at BIGINT
                );

                CREATE TABLE IF NOT EXISTS masters (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE,
                    name TEXT,
                    category TEXT,
                    district TEXT,
                    phone TEXT,
                    description TEXT,
                    experience TEXT,
                    photo TEXT,
                    rating DOUBLE PRECISION DEFAULT 0,
                    reviews_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'pending',
                    availability TEXT DEFAULT 'offline',
                    last_seen BIGINT DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS offers (
                    id BIGSERIAL PRIMARY KEY,
                    order_id BIGINT,
                    master_user_id BIGINT,
                    price TEXT,
                    eta TEXT,
                    comment TEXT,
                    status TEXT DEFAULT 'active',
                    created_at BIGINT
                );

                CREATE TABLE IF NOT EXISTS chats (
                    id BIGSERIAL PRIMARY KEY,
                    order_id BIGINT,
                    client_user_id BIGINT,
                    master_user_id BIGINT,
                    status TEXT DEFAULT 'active',
                    created_at BIGINT
                );

                CREATE TABLE IF NOT EXISTS complaints (
                    id BIGSERIAL PRIMARY KEY,
                    order_id BIGINT,
                    from_user_id BIGINT,
                    against_user_id BIGINT,
                    against_role TEXT,
                    text TEXT,
                    created_at BIGINT
                );

                CREATE TABLE IF NOT EXISTS support_messages (
                    id BIGSERIAL PRIMARY KEY,
                    from_user_id BIGINT,
                    text TEXT,
                    created_at BIGINT
                );

                CREATE TABLE IF NOT EXISTS user_states (
                    user_id BIGINT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS app_runtime (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at BIGINT NOT NULL
                );
                """
            )

            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_user_status ON orders(user_id, status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_category_status ON orders(category, status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_masters_status_category ON masters(status, category)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_order_status ON offers(order_id, status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_chats_order_status ON chats(order_id, status)")

    # state storage
    def get_state(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute("SELECT state_json FROM user_states WHERE user_id=%s", (user_id,))
            row = cur.fetchone()
            return json.loads(row["state_json"]) if row else None

    def set_state(self, user_id: int, data: Dict[str, Any]):
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_states (user_id, state_json, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET state_json=EXCLUDED.state_json, updated_at=EXCLUDED.updated_at
                """,
                (user_id, json.dumps(data, ensure_ascii=False), now_ts()),
            )

    def reset_state(self, user_id: int):
        with self.cursor() as cur:
            cur.execute("DELETE FROM user_states WHERE user_id=%s", (user_id,))

    def get_runtime_map(self, key: str) -> Dict[str, int]:
        with self.cursor() as cur:
            cur.execute("SELECT value_json FROM app_runtime WHERE key=%s", (key,))
            row = cur.fetchone()
            if not row:
                return {}
            raw = json.loads(row["value_json"])
            return {str(k): int(v) for k, v in raw.items()}

    def set_runtime_map(self, key: str, value: Dict[str, int]):
        with self.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_runtime (key, value_json, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (key)
                DO UPDATE SET value_json=EXCLUDED.value_json, updated_at=EXCLUDED.updated_at
                """,
                (key, json.dumps(value), now_ts()),
            )


DB = Database(DATABASE_URL)


class RuntimeStore:
    def __init__(self, db: Database):
        self.db = db

    def get_timestamp(self, key: str, user_id: int) -> int:
        data = self.db.get_runtime_map(key)
        return int(data.get(str(user_id), 0))

    def set_timestamp(self, key: str, user_id: int, value: int):
        data = self.db.get_runtime_map(key)
        data[str(user_id)] = value
        self.db.set_runtime_map(key, data)


RUNTIME = RuntimeStore(DB)


# ======================
# HELPERS
# ======================
def now_ts() -> int:
    return int(time.time())


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def category_label(value: str) -> str:
    return CATEGORY_VALUE_TO_LABEL.get(value, value)


def status_label(status: str) -> str:
    return ORDER_STATUSES.get(status, status)


def get_state(user_id: int):
    return DB.get_state(user_id)


def set_state(user_id: int, data: Dict[str, Any]):
    DB.set_state(user_id, data)


def reset_state(user_id: int):
    DB.reset_state(user_id)


def touch_master_presence(user_id: int):
    with DB.cursor() as cur:
        cur.execute(
            """
            UPDATE masters
            SET last_seen=%s, availability='online'
            WHERE user_id=%s AND status='approved'
            """,
            (now_ts(), user_id),
        )


def refresh_master_online_statuses():
    threshold = now_ts() - ONLINE_TIMEOUT
    with DB.cursor() as cur:
        cur.execute(
            """
            UPDATE masters
            SET availability='offline'
            WHERE status='approved' AND last_seen < %s
            """,
            (threshold,),
        )


def approved_master_row(user_id: int):
    refresh_master_online_statuses()
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM masters WHERE user_id=%s AND status='approved'", (user_id,))
        return cur.fetchone()


def master_any_row(user_id: int):
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM masters WHERE user_id=%s", (user_id,))
        return cur.fetchone()


def client_active_orders_count(user_id: int) -> int:
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM orders
            WHERE user_id=%s AND status IN ('new', 'offered', 'matched', 'in_progress')
            """,
            (user_id,),
        )
        return cur.fetchone()["c"]


def master_active_orders_count(user_id: int) -> int:
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM orders
            WHERE selected_master_id=%s AND status IN ('matched', 'in_progress')
            """,
            (user_id,),
        )
        return cur.fetchone()["c"]


def get_master_name(master_user_id: Optional[int]) -> str:
    if not master_user_id:
        return "-"
    with DB.cursor() as cur:
        cur.execute("SELECT name FROM masters WHERE user_id=%s", (master_user_id,))
        row = cur.fetchone()
        return row["name"] if row else "-"


def get_order_row(order_id: int):
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE id=%s", (order_id,))
        return cur.fetchone()


def get_chat_for_order(order_id: int):
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM chats
            WHERE order_id=%s
            ORDER BY id DESC LIMIT 1
            """,
            (order_id,),
        )
        return cur.fetchone()


# ======================
# KEYBOARDS
# ======================
def main_menu_kb(is_admin_user: bool = False):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("👤 Клієнт"), KeyboardButton("🔧 Майстер"))
    kb.add(KeyboardButton("🆘 Допомога"))
    if is_admin_user:
        kb.add(KeyboardButton("👑 Адмін"))
    return kb


def back_menu_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("⬅️ Назад"), KeyboardButton("🏠 У меню"))
    return kb


def categories_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    for label, _ in CATEGORIES:
        kb.add(KeyboardButton(label))
    kb.add(KeyboardButton("⬅️ Назад"), KeyboardButton("🏠 У меню"))
    return kb


def client_actions_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📨 Створити заявку"))
    kb.add(KeyboardButton("👷 Переглянути майстрів"))
    kb.add(KeyboardButton("📦 Мої заявки"))
    kb.add(KeyboardButton("⬅️ Назад"), KeyboardButton("🏠 У меню"))
    return kb


def master_menu_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("👤 Мій профіль"))
    kb.add(KeyboardButton("✏️ Редагувати профіль"))
    kb.add(KeyboardButton("📦 Нові заявки"))
    kb.add(KeyboardButton("💬 Активні чати"))
    kb.add(KeyboardButton("⬅️ Назад"), KeyboardButton("🏠 У меню"))
    return kb


def admin_menu_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("👷 База майстрів"))
    kb.add(KeyboardButton("📝 Заявки майстрів"))
    kb.add(KeyboardButton("📦 Заявки клієнтів"))
    kb.add(KeyboardButton("📊 Статистика"))
    kb.add(KeyboardButton("⬅️ Назад"), KeyboardButton("🏠 У меню"))
    return kb


def admin_orders_filter_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📋 Усі заявки"))
    kb.add(KeyboardButton("🆕 Нові"), KeyboardButton("📬 Є пропозиції"))
    kb.add(KeyboardButton("🤝 Обрано майстра"), KeyboardButton("🛠 В роботі"))
    kb.add(KeyboardButton("✅ Завершені"), KeyboardButton("❌ Скасовані"))
    kb.add(KeyboardButton("⌛ Прострочені"))
    kb.add(KeyboardButton("⬅️ Назад"), KeyboardButton("🏠 У меню"))
    return kb


def master_categories_inline_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    for label, value in CATEGORIES:
        kb.add(InlineKeyboardButton(label, callback_data=f"master_cat_{value}"))
    return kb


def edit_profile_inline_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("👤 Ім'я", callback_data="edit_name"),
        InlineKeyboardButton("📍 Район", callback_data="edit_district"),
        InlineKeyboardButton("📞 Телефон", callback_data="edit_phone"),
        InlineKeyboardButton("🧾 Опис", callback_data="edit_description"),
        InlineKeyboardButton("🛠 Досвід", callback_data="edit_experience"),
        InlineKeyboardButton("📸 Фото", callback_data="edit_photo"),
    )
    return kb


def order_card_master_actions(order_id: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📨 Відгукнутись", callback_data=f"offer_start_{order_id}"))
    return kb


def offer_select_inline(offer_id: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✅ Обрати цього майстра", callback_data=f"choose_offer_{offer_id}"))
    return kb


def client_order_actions_inline(order_id: int, status: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📬 Пропозиції майстрів", callback_data=f"client_offers_{order_id}"))
    if status in ["offered", "matched"]:
        kb.add(InlineKeyboardButton("✅ Обрати майстра", callback_data=f"client_choose_{order_id}"))
    if status in ["matched", "in_progress"]:
        kb.add(InlineKeyboardButton("💬 Чат по заявці", callback_data=f"client_chat_{order_id}"))
        kb.add(InlineKeyboardButton("⚠️ Скарга на майстра", callback_data=f"complain_master_{order_id}"))
    if status in ["new", "offered", "matched"]:
        kb.add(InlineKeyboardButton("❌ Скасувати заявку", callback_data=f"client_cancel_{order_id}"))
    return kb


def selected_order_master_actions(order_id: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("💬 Відкрити чат", callback_data=f"master_chat_open_{order_id}"),
        InlineKeyboardButton("🏁 Завершити заявку", callback_data=f"finish_order_{order_id}"),
        InlineKeyboardButton("❌ Відмовитись", callback_data=f"refuse_order_{order_id}"),
        InlineKeyboardButton("⚠️ Скарга на клієнта", callback_data=f"complain_client_{order_id}"),
    )
    return kb


def admin_master_card_inline(master_id: int, status: str):
    kb = InlineKeyboardMarkup(row_width=1)
    if status == "approved":
        kb.add(InlineKeyboardButton("🚫 Заблокувати", callback_data=f"admin_block_master_{master_id}"))
    elif status == "blocked":
        kb.add(InlineKeyboardButton("✅ Розблокувати", callback_data=f"admin_unblock_master_{master_id}"))
    kb.add(InlineKeyboardButton("🗑 Видалити майстра", callback_data=f"admin_delete_master_{master_id}"))
    return kb


def admin_pending_master_inline(master_id: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ Підтвердити", callback_data=f"admin_approve_master_{master_id}"),
        InlineKeyboardButton("❌ Відхилити", callback_data=f"admin_reject_master_{master_id}"),
    )
    return kb


def admin_order_actions_inline(order_id: int, status: str):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📄 Деталі заявки", callback_data=f"admin_order_detail_{order_id}"))
    if status not in ["done", "cancelled", "expired"]:
        kb.add(InlineKeyboardButton("❌ Закрити як неактуальну", callback_data=f"admin_expire_order_{order_id}"))
    if status == "matched":
        kb.add(InlineKeyboardButton("🛠 Позначити 'в роботі'", callback_data=f"admin_progress_order_{order_id}"))
    if status in ["matched", "in_progress"]:
        kb.add(InlineKeyboardButton("🏁 Завершити", callback_data=f"admin_done_order_{order_id}"))
    if status in ["offered", "matched", "in_progress"]:
        kb.add(InlineKeyboardButton("🔄 Повернути в нові", callback_data=f"admin_reset_order_{order_id}"))
    return kb


def pagination_inline(prefix: str, page: int, has_prev: bool, has_next: bool):
    kb = InlineKeyboardMarkup(row_width=2)
    row = []
    if has_prev:
        row.append(InlineKeyboardButton("⬅️ Попередня", callback_data=f"{prefix}_{page - 1}"))
    if has_next:
        row.append(InlineKeyboardButton("Наступна ➡️", callback_data=f"{prefix}_{page + 1}"))
    if row:
        kb.add(*row)
        return kb
    return None


def support_reply_inline(user_id: int):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("↩️ Відповісти", callback_data=f"support_reply_{user_id}"))
    return kb


def exit_chat_inline():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("❌ Вийти з чату", callback_data="exit_chat"))
    return kb


# ======================
# CARD SENDERS
# ======================
async def send_master_card(chat_id: int, master_row, title="📄 Картка майстра", reply_markup=None):
    text = (
        f"{title}\n\n"
        f"🆔 ID в базі: {master_row['id']}\n"
        f"👤 Ім'я: {master_row['name']}\n"
        f"🔧 Категорія: {category_label(master_row['category'])}\n"
        f"📍 Район: {master_row['district'] or '-'}\n"
        f"📞 Телефон: {master_row['phone']}\n"
        f"🧾 Опис: {master_row['description'] or '-'}\n"
        f"🛠 Досвід: {master_row['experience'] or '-'}\n"
        f"⭐ Рейтинг: {round(master_row['rating'] or 0, 2)}\n"
        f"💬 Відгуків: {master_row['reviews_count']}\n"
        f"📌 Статус: {master_row['status']}\n"
        f"🟢 Доступність: {master_row['availability']}\n"
        f"👤 Telegram ID: {master_row['user_id']}"
    )
    if master_row.get("photo"):
        await bot.send_photo(chat_id, master_row["photo"], caption=text, reply_markup=reply_markup)
    else:
        await bot.send_message(chat_id, text, reply_markup=reply_markup)


async def send_order_card(chat_id: int, order_row, title="📄 Картка заявки", reply_markup=None):
    master_name = get_master_name(order_row["selected_master_id"])
    text = (
        f"{title}\n\n"
        f"🆔 ID заявки: {order_row['id']}\n"
        f"👤 Клієнт Telegram ID: {order_row['user_id']}\n"
        f"🔧 Категорія: {category_label(order_row['category'])}\n"
        f"📍 Район / адреса: {order_row['district'] or '-'}\n"
        f"📝 Проблема: {order_row['problem']}\n"
        f"📌 Статус: {status_label(order_row['status'])}\n"
        f"👷 Обраний майстер: {master_name}\n"
        f"⭐ Оцінка: {order_row['rating'] if order_row['rating'] is not None else '-'}"
    )
    if order_row.get("media_type") == "photo" and order_row.get("media_file_id"):
        await bot.send_photo(chat_id, order_row["media_file_id"], caption=text, reply_markup=reply_markup)
    elif order_row.get("media_type") == "video" and order_row.get("media_file_id"):
        await bot.send_video(chat_id, order_row["media_file_id"], caption=text, reply_markup=reply_markup)
    else:
        await bot.send_message(chat_id, text, reply_markup=reply_markup)


async def send_admin_order_detail(chat_id: int, order_id: int):
    order = get_order_row(order_id)
    if not order:
        await bot.send_message(chat_id, "❌ Заявку не знайдено.")
        return

    chat = get_chat_for_order(order_id)
    chat_info = "так" if chat and chat["status"] in ["active", "closed"] else "ні"
    media_info = "так" if order["media_file_id"] else "ні"
    selected_master_name = get_master_name(order["selected_master_id"])

    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT offers.*, masters.name
            FROM offers
            LEFT JOIN masters ON offers.master_user_id = masters.user_id
            WHERE offers.order_id=%s
            ORDER BY offers.id DESC
            """,
            (order_id,),
        )
        offers = cur.fetchall()

    offers_text = "немає"
    if offers:
        offers_text = "\n".join(
            f"• {offer['name'] or offer['master_user_id']} | {offer['price']} | {offer['eta']} | {offer['comment']} | статус: {offer['status']}"
            for offer in offers
        )

    detail_text = (
        f"🧾 Деталі заявки\n\n"
        f"🆔 ID: {order['id']}\n"
        f"👤 Клієнт: {order['user_id']}\n"
        f"🔧 Категорія: {category_label(order['category'])}\n"
        f"📍 Район / адреса: {order['district'] or '-'}\n"
        f"📝 Проблема: {order['problem']}\n"
        f"📌 Статус: {status_label(order['status'])}\n"
        f"👷 Обраний майстер: {selected_master_name}\n"
        f"💬 Чат був/є: {chat_info}\n"
        f"📎 Є медіа: {media_info}\n"
        f"⭐ Оцінка: {order['rating'] if order['rating'] is not None else '-'}\n"
        f"🗒 Відгук: {order['review_text'] or '-'}\n\n"
        f"📬 Пропозиції майстрів:\n{offers_text}"
    )

    markup = admin_order_actions_inline(order_id, order["status"])
    if order.get("media_type") == "photo" and order.get("media_file_id"):
        await bot.send_photo(chat_id, order["media_file_id"], caption=detail_text, reply_markup=markup)
    elif order.get("media_type") == "video" and order.get("media_file_id"):
        await bot.send_video(chat_id, order["media_file_id"], caption=detail_text, reply_markup=markup)
    else:
        await bot.send_message(chat_id, detail_text, reply_markup=markup)


# ======================
# LIST VIEWS
# ======================
async def show_client_categories(message: types.Message):
    set_state(message.from_user.id, {"flow": "client_categories"})
    await message.answer("Оберіть спеціальність:", reply_markup=categories_kb())


async def show_client_actions(message: types.Message, category: str):
    set_state(message.from_user.id, {"flow": "client_actions", "category": category})
    await message.answer(
        f"Категорія: {category_label(category)}\n\nОберіть дію:",
        reply_markup=client_actions_kb(),
    )


async def show_master_profile(message: types.Message, master_row):
    text = (
        f"👤 Ваш профіль майстра\n\n"
        f"👤 Ім'я: {master_row['name']}\n"
        f"🔧 Категорія: {category_label(master_row['category'])}\n"
        f"📍 Район: {master_row['district'] or '-'}\n"
        f"📞 Телефон: {master_row['phone']}\n"
        f"🧾 Про себе: {master_row['description'] or '-'}\n"
        f"🛠 Досвід: {master_row['experience'] or '-'}\n"
        f"⭐ Рейтинг: {round(master_row['rating'] or 0, 2)}\n"
        f"💬 Відгуків: {master_row['reviews_count']}\n"
        f"🟢 Статус: {master_row['availability']}"
    )
    if master_row.get("photo"):
        await bot.send_photo(message.chat.id, master_row["photo"], caption=text)
    else:
        await message.answer(text)
    await message.answer("Меню майстра:", reply_markup=master_menu_kb())


async def notify_masters_about_order(order_row):
    refresh_master_online_statuses()
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT user_id
            FROM masters
            WHERE status='approved' AND category=%s AND availability='online'
            """,
            (order_row["category"],),
        )
        masters = cur.fetchall()

    for master in masters:
        try:
            await send_order_card(
                master["user_id"],
                order_row,
                title="📢 Нова заявка!",
                reply_markup=order_card_master_actions(order_row["id"]),
            )
            await bot.send_message(master["user_id"], "Натисніть «📨 Відгукнутись», якщо хочете взяти цю заявку.")
        except Exception:
            logger.exception("Не вдалося повідомити майстра %s", master["user_id"])


async def notify_admin_about_order(order_row):
    await send_order_card(ADMIN_ID, order_row, title="📦 Нова заявка клієнта")


async def show_client_orders(message: types.Message):
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE user_id=%s ORDER BY id DESC", (message.from_user.id,))
        rows = cur.fetchall()
    if not rows:
        await message.answer("У вас поки немає заявок.", reply_markup=client_actions_kb())
        return
    await message.answer("📦 Ваші заявки:", reply_markup=client_actions_kb())
    for row in rows:
        await send_order_card(
            message.chat.id,
            row,
            title="📄 Ваша заявка",
            reply_markup=client_order_actions_inline(row["id"], row["status"]),
        )


async def show_order_offers(chat_id: int, client_user_id: int, order_id: int):
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM orders WHERE id=%s AND user_id=%s", (order_id, client_user_id))
        order = cur.fetchone()
    if not order:
        await bot.send_message(chat_id, "❌ Вашу заявку не знайдено.")
        return

    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT offers.id, offers.price, offers.eta, offers.comment,
                   masters.name, masters.rating, masters.reviews_count
            FROM offers
            JOIN masters ON offers.master_user_id = masters.user_id
            WHERE offers.order_id=%s AND offers.status='active'
            ORDER BY offers.id DESC
            """,
            (order_id,),
        )
        offers = cur.fetchall()

    if not offers:
        await bot.send_message(chat_id, "По цій заявці поки немає пропозицій.")
        return

    await bot.send_message(chat_id, f"📬 Пропозиції по заявці #{order_id}:")
    for offer in offers:
        text = (
            f"💼 Пропозиція\n\n"
            f"👤 Майстер: {offer['name']}\n"
            f"⭐ {round(offer['rating'] or 0, 2)} | відгуків: {offer['reviews_count']}\n"
            f"💰 Ціна: {offer['price']}\n"
            f"⏱ Коли зможе: {offer['eta']}\n"
            f"📝 Коментар: {offer['comment']}"
        )
        await bot.send_message(chat_id, text, reply_markup=offer_select_inline(offer["id"]))


async def show_new_orders_for_master(message: types.Message, master_row):
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM orders
            WHERE status IN ('new', 'offered') AND category=%s
            ORDER BY id DESC
            """,
            (master_row["category"],),
        )
        rows = cur.fetchall()
    if not rows:
        await message.answer("Наразі нових заявок у вашій категорії немає.", reply_markup=master_menu_kb())
        return
    await message.answer(f"📦 Нові заявки ({category_label(master_row['category'])}):", reply_markup=master_menu_kb())
    for row in rows:
        await send_order_card(message.chat.id, row, title="📢 Доступна заявка", reply_markup=order_card_master_actions(row["id"]))


async def show_active_orders_for_master(message: types.Message):
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM orders
            WHERE selected_master_id=%s AND status IN ('matched', 'in_progress')
            ORDER BY id DESC
            """,
            (message.from_user.id,),
        )
        rows = cur.fetchall()
    if not rows:
        await message.answer("У вас немає активних заявок.", reply_markup=master_menu_kb())
        return
    await message.answer("✅ Ваші активні заявки:", reply_markup=master_menu_kb())
    for row in rows:
        await send_order_card(message.chat.id, row, title="📄 Активна заявка", reply_markup=selected_order_master_actions(row["id"]))


async def show_admin_pending_masters_page(chat_id: int, page: int = 0):
    offset = page * PAGE_SIZE
    with DB.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM masters WHERE status='pending'")
        total = cur.fetchone()["c"]
        cur.execute(
            """
            SELECT * FROM masters
            WHERE status='pending'
            ORDER BY id DESC
            LIMIT %s OFFSET %s
            """,
            (PAGE_SIZE, offset),
        )
        rows = cur.fetchall()
    if not rows:
        await bot.send_message(chat_id, "Немає заявок майстрів на модерацію.", reply_markup=admin_menu_kb())
        return
    await bot.send_message(chat_id, f"📝 Заявки майстрів (сторінка {page + 1}):")
    for row in rows:
        await send_master_card(chat_id, row, title="📝 Заявка майстра", reply_markup=admin_pending_master_inline(row["id"]))
    pag = pagination_inline("page_pending_masters", page, page > 0, offset + PAGE_SIZE < total)
    if pag:
        await bot.send_message(chat_id, "Навігація:", reply_markup=pag)


async def show_admin_masters_page(chat_id: int, page: int = 0):
    offset = page * PAGE_SIZE
    with DB.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS c FROM masters WHERE status IN ('approved', 'blocked')")
        total = cur.fetchone()["c"]
        cur.execute(
            """
            SELECT * FROM masters
            WHERE status IN ('approved', 'blocked')
            ORDER BY rating DESC, reviews_count DESC, name ASC
            LIMIT %s OFFSET %s
            """,
            (PAGE_SIZE, offset),
        )
        rows = cur.fetchall()
    if not rows:
        await bot.send_message(chat_id, "У базі немає майстрів.", reply_markup=admin_menu_kb())
        return
    await bot.send_message(chat_id, f"👷 База майстрів (сторінка {page + 1}):")
    for row in rows:
        await send_master_card(chat_id, row, title="📄 Майстер", reply_markup=admin_master_card_inline(row["id"], row["status"]))
    pag = pagination_inline("page_masters", page, page > 0, offset + PAGE_SIZE < total)
    if pag:
        await bot.send_message(chat_id, "Навігація:", reply_markup=pag)


async def show_admin_orders_page(chat_id: int, page: int = 0, status_filter: Optional[str] = None):
    offset = page * PAGE_SIZE
    with DB.cursor() as cur:
        if status_filter:
            cur.execute("SELECT COUNT(*) AS c FROM orders WHERE status=%s", (status_filter,))
            total = cur.fetchone()["c"]
            cur.execute(
                """
                SELECT * FROM orders
                WHERE status=%s
                ORDER BY id DESC
                LIMIT %s OFFSET %s
                """,
                (status_filter, PAGE_SIZE, offset),
            )
        else:
            cur.execute("SELECT COUNT(*) AS c FROM orders")
            total = cur.fetchone()["c"]
            cur.execute(
                """
                SELECT * FROM orders
                ORDER BY id DESC
                LIMIT %s OFFSET %s
                """,
                (PAGE_SIZE, offset),
            )
        rows = cur.fetchall()

    if not rows:
        await bot.send_message(chat_id, "Заявок немає.", reply_markup=admin_orders_filter_kb())
        return

    title = "📦 Заявки клієнтів"
    if status_filter:
        title += f" — {status_label(status_filter)}"
    title += f" (сторінка {page + 1})"
    await bot.send_message(chat_id, title)
    for row in rows:
        await send_order_card(chat_id, row, title="📄 Заявка", reply_markup=admin_order_actions_inline(row["id"], row["status"]))
    prefix = f"page_orders_{status_filter or 'all'}"
    pag = pagination_inline(prefix, page, page > 0, offset + PAGE_SIZE < total)
    if pag:
        await bot.send_message(chat_id, "Навігація:", reply_markup=pag)


async def show_admin_stats(message: types.Message):
    with DB.cursor() as cur:
        counters = {}
        queries = {
            "masters_total": "SELECT COUNT(*) AS c FROM masters",
            "masters_approved": "SELECT COUNT(*) AS c FROM masters WHERE status='approved'",
            "masters_pending": "SELECT COUNT(*) AS c FROM masters WHERE status='pending'",
            "masters_blocked": "SELECT COUNT(*) AS c FROM masters WHERE status='blocked'",
            "orders_total": "SELECT COUNT(*) AS c FROM orders",
            "orders_new": "SELECT COUNT(*) AS c FROM orders WHERE status='new'",
            "orders_offered": "SELECT COUNT(*) AS c FROM orders WHERE status='offered'",
            "orders_matched": "SELECT COUNT(*) AS c FROM orders WHERE status='matched'",
            "orders_progress": "SELECT COUNT(*) AS c FROM orders WHERE status='in_progress'",
            "orders_done": "SELECT COUNT(*) AS c FROM orders WHERE status='done'",
            "orders_cancelled": "SELECT COUNT(*) AS c FROM orders WHERE status='cancelled'",
            "orders_expired": "SELECT COUNT(*) AS c FROM orders WHERE status='expired'",
        }
        for key, sql in queries.items():
            cur.execute(sql)
            counters[key] = cur.fetchone()["c"]

    text = (
        f"📊 Статистика\n\n"
        f"👷 Усього майстрів: {counters['masters_total']}\n"
        f"✅ Підтверджені: {counters['masters_approved']}\n"
        f"📝 На модерації: {counters['masters_pending']}\n"
        f"🚫 Заблоковані: {counters['masters_blocked']}\n\n"
        f"📦 Усього заявок: {counters['orders_total']}\n"
        f"🆕 Нові: {counters['orders_new']}\n"
        f"📬 Є пропозиції: {counters['orders_offered']}\n"
        f"🤝 Обрано майстра: {counters['orders_matched']}\n"
        f"🛠 В роботі: {counters['orders_progress']}\n"
        f"✅ Завершені: {counters['orders_done']}\n"
        f"❌ Скасовані: {counters['orders_cancelled']}\n"
        f"⌛ Прострочені: {counters['orders_expired']}"
    )
    await message.answer(text, reply_markup=admin_menu_kb())


# ======================
# START
# ======================
@dp.message_handler(commands=["start"])
async def start_handler(message: types.Message):
    reset_state(message.from_user.id)
    if approved_master_row(message.from_user.id):
        touch_master_presence(message.from_user.id)
    await message.answer(
        "Вітаємо в боті сервісу майстрів.",
        reply_markup=main_menu_kb(is_admin_user=is_admin(message.from_user.id)),
    )


# ======================
# CALLBACKS
# ======================
@dp.callback_query_handler(lambda c: c.data.startswith("master_cat_"))
async def cb_master_cat(call: types.CallbackQuery):
    value = call.data.split("master_cat_", 1)[1]
    st = get_state(call.from_user.id)
    if not st or st.get("flow") != "master_reg":
        await call.answer("Сценарій неактивний")
        return
    st["data"]["category"] = value
    st["history"].append("category")
    st["step"] = "district"
    set_state(call.from_user.id, st)
    await call.message.answer("📍 Вкажіть район міста, де ви берете замовлення:", reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("offer_start_"))
async def cb_offer_start(call: types.CallbackQuery):
    master = approved_master_row(call.from_user.id)
    if not master:
        await call.answer("Ви не підтверджений майстер", show_alert=True)
        return
    if master_active_orders_count(call.from_user.id) >= MAX_ACTIVE_MASTER_ORDERS:
        await call.answer("У вас уже забагато активних заявок", show_alert=True)
        return

    order_id = int(call.data.split("_")[-1])
    order = get_order_row(order_id)
    if not order:
        await call.answer("Заявку не знайдено", show_alert=True)
        return
    if order["category"] != master["category"]:
        await call.answer("Це не ваша категорія", show_alert=True)
        return
    if order["status"] not in ["new", "offered"]:
        await call.answer("Заявка вже недоступна", show_alert=True)
        return

    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM offers
            WHERE order_id=%s AND master_user_id=%s AND status IN ('active', 'selected')
            """,
            (order_id, call.from_user.id),
        )
        if cur.fetchone():
            await call.answer("Ви вже відгукнулися на цю заявку", show_alert=True)
            return

    if now_ts() - RUNTIME.get_timestamp("last_master_offer_time", call.from_user.id) < MASTER_OFFER_COOLDOWN:
        await call.answer("Зачекайте перед новим відгуком", show_alert=True)
        return

    set_state(call.from_user.id, {"flow": "offer_price", "order_id": order_id})
    await call.message.answer("💰 Вкажіть вашу ціну:", reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("client_offers_"))
async def cb_client_offers(call: types.CallbackQuery):
    await show_order_offers(call.message.chat.id, call.from_user.id, int(call.data.split("_")[-1]))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("client_choose_"))
async def cb_client_choose(call: types.CallbackQuery):
    await show_order_offers(call.message.chat.id, call.from_user.id, int(call.data.split("_")[-1]))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("choose_offer_"))
async def cb_choose_offer(call: types.CallbackQuery):
    offer_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT offers.*, orders.user_id AS client_user_id, orders.id AS order_id
            FROM offers
            JOIN orders ON offers.order_id = orders.id
            WHERE offers.id=%s AND offers.status='active'
            """,
            (offer_id,),
        )
        offer = cur.fetchone()
    if not offer or offer["client_user_id"] != call.from_user.id:
        await call.answer("Пропозиція недоступна", show_alert=True)
        return

    if master_active_orders_count(offer["master_user_id"]) >= MAX_ACTIVE_MASTER_ORDERS:
        await call.answer("У майстра вже забагато активних заявок", show_alert=True)
        return

    order_id = offer["order_id"]
    with DB.cursor() as cur:
        cur.execute("UPDATE offers SET status='rejected' WHERE order_id=%s AND status='active'", (order_id,))
        cur.execute("UPDATE offers SET status='selected' WHERE id=%s", (offer_id,))
        cur.execute(
            "UPDATE orders SET selected_master_id=%s, status='matched', updated_at=%s WHERE id=%s",
            (offer["master_user_id"], now_ts(), order_id),
        )
        cur.execute(
            """
            INSERT INTO chats (order_id, client_user_id, master_user_id, status, created_at)
            VALUES (%s, %s, %s, 'active', %s)
            """,
            (order_id, call.from_user.id, offer["master_user_id"], now_ts()),
        )

    try:
        await bot.send_message(offer["master_user_id"], f"🎉 Вас обрали по заявці #{order_id}.", reply_markup=master_menu_kb())
    except Exception:
        logger.exception("Не вдалося повідомити майстра про вибір")

    await call.message.answer("✅ Майстра обрано.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("client_chat_"))
async def cb_client_chat(call: types.CallbackQuery):
    order_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM orders
            WHERE id=%s AND user_id=%s AND selected_master_id IS NOT NULL AND status IN ('matched', 'in_progress')
            """,
            (order_id, call.from_user.id),
        )
        order = cur.fetchone()
        if not order:
            await call.answer("Чат недоступний", show_alert=True)
            return
        cur.execute(
            """
            SELECT * FROM chats
            WHERE order_id=%s AND client_user_id=%s AND status='active'
            ORDER BY id DESC LIMIT 1
            """,
            (order_id, call.from_user.id),
        )
        chat = cur.fetchone()
    if not chat:
        await call.answer("Чат не знайдено", show_alert=True)
        return

    set_state(call.from_user.id, {"flow": "chat", "role": "client", "order_id": order_id, "target_user_id": chat["master_user_id"]})
    await call.message.answer("💬 Чат відкрито. Напишіть повідомлення.", reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("client_cancel_"))
async def cb_client_cancel(call: types.CallbackQuery):
    order_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM orders
            WHERE id=%s AND user_id=%s AND status IN ('new', 'offered', 'matched')
            """,
            (order_id, call.from_user.id),
        )
        order = cur.fetchone()
    if not order:
        await call.answer("Заявку не можна скасувати", show_alert=True)
        return

    with DB.cursor() as cur:
        cur.execute("UPDATE orders SET status='cancelled', updated_at=%s WHERE id=%s", (now_ts(), order_id))
        cur.execute("UPDATE offers SET status='rejected' WHERE order_id=%s AND status='active'", (order_id,))
        cur.execute("UPDATE chats SET status='closed' WHERE order_id=%s", (order_id,))

    if order["selected_master_id"]:
        try:
            await bot.send_message(order["selected_master_id"], f"❌ Клієнт скасував заявку #{order_id}.")
        except Exception:
            logger.exception("Не вдалося повідомити майстра про скасування")

    await call.message.answer("✅ Заявку скасовано.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("master_chat_open_"))
async def cb_master_chat_open(call: types.CallbackQuery):
    order_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM orders
            WHERE id=%s AND selected_master_id=%s AND status IN ('matched', 'in_progress')
            """,
            (order_id, call.from_user.id),
        )
        order = cur.fetchone()
        if not order:
            await call.answer("Чат недоступний", show_alert=True)
            return
        cur.execute(
            """
            SELECT * FROM chats
            WHERE order_id=%s AND master_user_id=%s AND status='active'
            ORDER BY id DESC LIMIT 1
            """,
            (order_id, call.from_user.id),
        )
        chat = cur.fetchone()
    if not chat:
        await call.answer("Чат не знайдено", show_alert=True)
        return

    set_state(call.from_user.id, {"flow": "chat", "role": "master", "order_id": order_id, "target_user_id": chat["client_user_id"]})
    await call.message.answer("💬 Чат відкрито. Напишіть повідомлення.", reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("finish_order_"))
async def cb_finish_order(call: types.CallbackQuery):
    order_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM orders
            WHERE id=%s AND selected_master_id=%s AND status IN ('matched', 'in_progress')
            """,
            (order_id, call.from_user.id),
        )
        order = cur.fetchone()
    if not order:
        await call.answer("Заявка недоступна", show_alert=True)
        return

    with DB.cursor() as cur:
        cur.execute("UPDATE orders SET status='done', updated_at=%s WHERE id=%s", (now_ts(), order_id))
        cur.execute("UPDATE chats SET status='closed' WHERE order_id=%s", (order_id,))

    set_state(order["user_id"], {"flow": "rating", "master_user_id": call.from_user.id, "order_id": order_id})
    await call.message.answer("✅ Заявку завершено.")
    await bot.send_message(order["user_id"], "⭐ Оцініть майстра цифрою від 1 до 5")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("refuse_order_"))
async def cb_refuse_order(call: types.CallbackQuery):
    order_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM orders
            WHERE id=%s AND selected_master_id=%s AND status IN ('matched', 'in_progress')
            """,
            (order_id, call.from_user.id),
        )
        order = cur.fetchone()
    if not order:
        await call.answer("Заявка недоступна", show_alert=True)
        return

    with DB.cursor() as cur:
        cur.execute("UPDATE orders SET selected_master_id=NULL, status='offered', updated_at=%s WHERE id=%s", (now_ts(), order_id))
        cur.execute("UPDATE chats SET status='closed' WHERE order_id=%s", (order_id,))

    try:
        await bot.send_message(order["user_id"], f"⚠️ Майстер відмовився від заявки #{order_id}.")
    except Exception:
        logger.exception("Не вдалося повідомити клієнта про відмову")

    await call.message.answer("✅ Ви відмовились від заявки.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("complain_master_"))
async def cb_complain_master(call: types.CallbackQuery):
    order_id = int(call.data.split("_")[-1])
    order = get_order_row(order_id)
    if not order or order["user_id"] != call.from_user.id or not order["selected_master_id"]:
        await call.answer("Скарга недоступна", show_alert=True)
        return

    set_state(call.from_user.id, {"flow": "complaint_write", "order_id": order_id, "against_user_id": order["selected_master_id"], "against_role": "master"})
    await call.message.answer("Напишіть текст скарги на майстра:", reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("complain_client_"))
async def cb_complain_client(call: types.CallbackQuery):
    order_id = int(call.data.split("_")[-1])
    order = get_order_row(order_id)
    if not order or order["selected_master_id"] != call.from_user.id:
        await call.answer("Скарга недоступна", show_alert=True)
        return

    set_state(call.from_user.id, {"flow": "complaint_write", "order_id": order_id, "against_user_id": order["user_id"], "against_role": "client"})
    await call.message.answer("Напишіть текст скарги на клієнта:", reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_approve_master_"))
async def cb_admin_approve_master(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    master_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM masters WHERE id=%s AND status='pending'", (master_id,))
        master = cur.fetchone()
    if not master:
        await call.answer("Заявку не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("UPDATE masters SET status='approved', last_seen=%s WHERE id=%s", (now_ts(), master_id))
    try:
        await bot.send_message(master["user_id"], "✅ Вашу заявку схвалено. Ви тепер майстер.", reply_markup=master_menu_kb())
    except Exception:
        logger.exception("Не вдалося повідомити майстра про схвалення")
    await call.message.answer("✅ Майстра підтверджено.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_reject_master_"))
async def cb_admin_reject_master(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    master_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM masters WHERE id=%s AND status='pending'", (master_id,))
        master = cur.fetchone()
    if not master:
        await call.answer("Заявку не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("DELETE FROM masters WHERE id=%s", (master_id,))
    try:
        await bot.send_message(master["user_id"], "❌ Вашу заявку майстра відхилено.")
    except Exception:
        logger.exception("Не вдалося повідомити майстра про відхилення")
    await call.message.answer("❌ Заявку майстра відхилено.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_block_master_"))
async def cb_admin_block_master(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    master_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM masters WHERE id=%s", (master_id,))
        master = cur.fetchone()
    if not master:
        await call.answer("Майстра не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("UPDATE masters SET status='blocked', availability='offline' WHERE id=%s", (master_id,))
    try:
        await bot.send_message(master["user_id"], "🚫 Ваш профіль майстра заблоковано.")
    except Exception:
        logger.exception("Не вдалося повідомити майстра про блокування")
    await call.message.answer("🚫 Майстра заблоковано.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_unblock_master_"))
async def cb_admin_unblock_master(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    master_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM masters WHERE id=%s", (master_id,))
        master = cur.fetchone()
    if not master:
        await call.answer("Майстра не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("UPDATE masters SET status='approved' WHERE id=%s", (master_id,))
    try:
        await bot.send_message(master["user_id"], "✅ Ваш профіль майстра розблоковано.")
    except Exception:
        logger.exception("Не вдалося повідомити майстра про розблокування")
    await call.message.answer("✅ Майстра розблоковано.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_delete_master_"))
async def cb_admin_delete_master(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    master_id = int(call.data.split("_")[-1])
    with DB.cursor() as cur:
        cur.execute("SELECT * FROM masters WHERE id=%s", (master_id,))
        master = cur.fetchone()
    if not master:
        await call.answer("Майстра не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("DELETE FROM masters WHERE id=%s", (master_id,))
    try:
        await bot.send_message(master["user_id"], "🗑 Ваш профіль майстра видалено адміністратором.")
    except Exception:
        logger.exception("Не вдалося повідомити майстра про видалення")
    await call.message.answer("🗑 Майстра видалено.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_order_detail_"))
async def cb_admin_order_detail(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    await send_admin_order_detail(call.message.chat.id, int(call.data.split("_")[-1]))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_expire_order_"))
async def cb_admin_expire_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    order_id = int(call.data.split("_")[-1])
    order = get_order_row(order_id)
    if not order:
        await call.answer("Заявку не знайдено", show_alert=True)
        return

    with DB.cursor() as cur:
        cur.execute("UPDATE orders SET status='expired', updated_at=%s WHERE id=%s", (now_ts(), order_id))
        cur.execute("UPDATE chats SET status='closed' WHERE order_id=%s", (order_id,))

    try:
        await bot.send_message(order["user_id"], f"⌛ Адміністратор позначив заявку #{order_id} як неактуальну / прострочену.")
    except Exception:
        logger.exception("Не вдалося повідомити клієнта")
    if order["selected_master_id"]:
        try:
            await bot.send_message(order["selected_master_id"], f"⌛ Адміністратор позначив заявку #{order_id} як неактуальну / прострочену.")
        except Exception:
            logger.exception("Не вдалося повідомити майстра")

    await call.message.answer("⌛ Заявку закрито як неактуальну.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_progress_order_"))
async def cb_admin_progress_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    order_id = int(call.data.split("_")[-1])
    order = get_order_row(order_id)
    if not order:
        await call.answer("Заявку не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("UPDATE orders SET status='in_progress', updated_at=%s WHERE id=%s", (now_ts(), order_id))
    await call.message.answer("🛠 Заявку переведено в статус «в роботі».")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_done_order_"))
async def cb_admin_done_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    order_id = int(call.data.split("_")[-1])
    order = get_order_row(order_id)
    if not order:
        await call.answer("Заявку не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("UPDATE orders SET status='done', updated_at=%s WHERE id=%s", (now_ts(), order_id))
        cur.execute("UPDATE chats SET status='closed' WHERE order_id=%s", (order_id,))
    await call.message.answer("🏁 Заявку завершено адміністратором.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("admin_reset_order_"))
async def cb_admin_reset_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    order_id = int(call.data.split("_")[-1])
    order = get_order_row(order_id)
    if not order:
        await call.answer("Заявку не знайдено", show_alert=True)
        return
    with DB.cursor() as cur:
        cur.execute("UPDATE orders SET status='new', selected_master_id=NULL, updated_at=%s WHERE id=%s", (now_ts(), order_id))
        cur.execute("UPDATE chats SET status='closed' WHERE order_id=%s", (order_id,))
    await call.message.answer("🔄 Заявку повернуто в нові.")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("support_reply_"))
async def cb_support_reply(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    target_user_id = int(call.data.split("_")[-1])
    set_state(call.from_user.id, {"flow": "support_reply", "target_user_id": target_user_id})
    await call.message.answer("Напишіть відповідь користувачу:", reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("edit_"))
async def cb_edit_profile(call: types.CallbackQuery):
    master = approved_master_row(call.from_user.id)
    if not master:
        await call.answer("Профіль недоступний", show_alert=True)
        return
    field = call.data.replace("edit_", "", 1)
    prompts = {
        "name": "Введіть нове ім'я:",
        "district": "Введіть новий район:",
        "phone": "Введіть новий телефон:",
        "description": "Введіть новий опис:",
        "experience": "Введіть новий досвід:",
        "photo": "Надішліть нове фото або напишіть 'пропустити':",
    }
    if field not in prompts:
        await call.answer()
        return
    set_state(call.from_user.id, {"flow": "edit_profile", "field": field})
    await call.message.answer(prompts[field], reply_markup=back_menu_kb())
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("page_masters_"))
async def cb_page_masters(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    await show_admin_masters_page(call.message.chat.id, int(call.data.split("_")[-1]))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("page_pending_masters_"))
async def cb_page_pending_masters(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    await show_admin_pending_masters_page(call.message.chat.id, int(call.data.split("_")[-1]))
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("page_orders_"))
async def cb_page_orders(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Недоступно", show_alert=True)
        return
    data = call.data.replace("page_orders_", "", 1)
    status_part, page_str = data.rsplit("_", 1)
    status_filter = None if status_part == "all" else status_part
    await show_admin_orders_page(call.message.chat.id, int(page_str), status_filter)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "exit_chat")
async def cb_exit_chat(call: types.CallbackQuery):
    reset_state(call.from_user.id)
    await call.message.answer("Чат завершено.", reply_markup=main_menu_kb(is_admin_user=is_admin(call.from_user.id)))
    await call.answer()


# ======================
# MAIN ROUTER
# ======================
@dp.message_handler(content_types=types.ContentTypes.ANY)
async def router(message: types.Message):
    user_id = message.from_user.id
    text = message.text.strip() if message.text else None
    st = get_state(user_id)

    if approved_master_row(user_id):
        touch_master_presence(user_id)

    # CHAT
    if st and st.get("flow") == "chat":
        if text == "❌ Вийти з чату":
            reset_state(user_id)
            await message.answer("Чат завершено.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return

        target_id = st["target_user_id"]
        role = st["role"]
        order_id = st["order_id"]
        prefix = f"💬 Повідомлення по заявці #{order_id}\nВід {'клієнта' if role == 'client' else 'майстра'}:"

        try:
            if message.photo:
                caption = message.caption or ""
                await bot.send_photo(target_id, message.photo[-1].file_id, caption=f"{prefix}\n{caption}".strip(), reply_markup=exit_chat_inline())
                await message.answer("✅ Фото надіслано.", reply_markup=back_menu_kb())
                return
            if message.video:
                caption = message.caption or ""
                await bot.send_video(target_id, message.video.file_id, caption=f"{prefix}\n{caption}".strip(), reply_markup=exit_chat_inline())
                await message.answer("✅ Відео надіслано.", reply_markup=back_menu_kb())
                return

            msg = text or "Без тексту"
            await bot.send_message(target_id, f"{prefix}\n{msg}", reply_markup=exit_chat_inline())
            await message.answer("✅ Повідомлення надіслано.", reply_markup=back_menu_kb())
            return
        except Exception:
            logger.exception("Не вдалося переслати повідомлення")
            await message.answer("Не вдалося переслати повідомлення.")
            return

    # SUPPORT REPLY
    if st and st.get("flow") == "support_reply":
        if not text:
            await message.answer("Напишіть текст відповіді.", reply_markup=back_menu_kb())
            return
        try:
            await bot.send_message(st["target_user_id"], f"💬 Відповідь служби підтримки:\n\n{text}")
            await message.answer("✅ Відповідь надіслано.")
        except Exception:
            logger.exception("Не вдалося надіслати відповідь")
            await message.answer("Не вдалося надіслати відповідь.")
        reset_state(user_id)
        return

    # SUPPORT
    if text == "🆘 Допомога":
        set_state(user_id, {"flow": "support_write"})
        await message.answer("Напишіть повідомлення для підтримки.", reply_markup=back_menu_kb())
        return

    if st and st.get("flow") == "support_write":
        support_text = text if text else "Без тексту"
        with DB.cursor() as cur:
            cur.execute(
                "INSERT INTO support_messages (from_user_id, text, created_at) VALUES (%s, %s, %s)",
                (user_id, support_text, now_ts()),
            )
        await bot.send_message(
            ADMIN_ID,
            f"🆘 Повідомлення в підтримку\n\n👤 user_id: {user_id}\n📝 {support_text}",
            reply_markup=support_reply_inline(user_id),
        )
        reset_state(user_id)
        await message.answer("✅ Повідомлення відправлено адміністратору.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    # COMPLAINT
    if st and st.get("flow") == "complaint_write":
        complaint_text = text if text else "Без тексту"
        with DB.cursor() as cur:
            cur.execute(
                """
                INSERT INTO complaints (order_id, from_user_id, against_user_id, against_role, text, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (st["order_id"], user_id, st["against_user_id"], st["against_role"], complaint_text, now_ts()),
            )
        await bot.send_message(
            ADMIN_ID,
            f"⚠️ Нова скарга\n\n🆔 Заявка: {st['order_id']}\n👤 Від: {user_id}\n🎯 На кого: {st['against_user_id']} ({st['against_role']})\n📝 {complaint_text}",
        )
        reset_state(user_id)
        await message.answer("✅ Скаргу відправлено адміністратору.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    # REVIEW
    if st and st.get("flow") == "review_text":
        review_text = None if (text and text.lower() == "пропустити") else text
        with DB.cursor() as cur:
            cur.execute("UPDATE orders SET review_text=%s WHERE id=%s", (review_text, st["order_id"]))
        reset_state(user_id)
        await message.answer("✅ Дякуємо за відгук!", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    # EDIT PROFILE
    if st and st.get("flow") == "edit_profile":
        master = approved_master_row(user_id)
        if not master:
            reset_state(user_id)
            await message.answer("Профіль недоступний.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return
        field = st["field"]
        with DB.cursor() as cur:
            if field == "photo":
                if message.photo:
                    value = message.photo[-1].file_id
                elif text and text.lower() == "пропустити":
                    value = None
                else:
                    await message.answer("Надішліть фото або напишіть 'пропустити'.", reply_markup=back_menu_kb())
                    return
                cur.execute("UPDATE masters SET photo=%s WHERE user_id=%s", (value, user_id))
            else:
                if not text:
                    await message.answer("Надішліть текстове значення.", reply_markup=back_menu_kb())
                    return
                field_map = {
                    "name": "name",
                    "district": "district",
                    "phone": "phone",
                    "description": "description",
                    "experience": "experience",
                }
                cur.execute(f"UPDATE masters SET {field_map[field]}=%s WHERE user_id=%s", (text, user_id))
        reset_state(user_id)
        await message.answer("✅ Профіль оновлено.", reply_markup=master_menu_kb())
        return

    # GLOBAL NAV
    if text == "🏠 У меню":
        reset_state(user_id)
        await message.answer("Головне меню:", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    if text == "⬅️ Назад":
        if not st:
            await message.answer("Головне меню:", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return

        flow = st.get("flow")
        if flow in ["client_categories", "client_actions", "master_menu", "admin_menu", "support_write", "master_pending"]:
            reset_state(user_id)
            await message.answer("Головне меню:", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return

        if flow == "client_problem":
            await show_client_actions(message, st["category"])
            return

        if flow in ["offer_price", "offer_eta", "offer_comment", "edit_profile", "support_reply", "complaint_write", "review_text"]:
            reset_state(user_id)
            await message.answer("Головне меню:", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return

        if flow == "master_reg":
            history = st.get("history", [])
            if not history:
                reset_state(user_id)
                await message.answer("Головне меню:", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
                return

            prev_step = history.pop()
            st["step"] = prev_step
            set_state(user_id, st)

            prompts = {
                "name": "👤 Введіть ім'я:",
                "category": "🔧 Оберіть спеціальність кнопкою:",
                "district": "📍 Вкажіть район міста:",
                "description": "🧾 Напишіть коротко про себе:",
                "experience": "🛠 Напишіть про досвід роботи:",
                "phone": "📞 Введіть телефон:",
                "photo": "📸 Надішліть фото або напишіть 'пропустити':",
            }

            if prev_step == "category":
                await message.answer(prompts[prev_step], reply_markup=back_menu_kb())
                await message.answer("Категорії:", reply_markup=master_categories_inline_kb())
            else:
                await message.answer(prompts[prev_step], reply_markup=back_menu_kb())
            return

        reset_state(user_id)
        await message.answer("Головне меню:", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    # MAIN MENU
    if text == "👤 Клієнт":
        reset_state(user_id)
        await show_client_categories(message)
        return

    if text == "🔧 Майстер":
        reset_state(user_id)
        master = master_any_row(user_id)
        if master and master["status"] == "approved":
            await show_master_profile(message, master)
            return
        if master and master["status"] == "pending":
            set_state(user_id, {"flow": "master_pending"})
            await message.answer("Ваша заявка вже на модерації.", reply_markup=back_menu_kb())
            return
        if master and master["status"] == "blocked":
            await message.answer("Ваш профіль майстра заблокований. Зверніться в підтримку.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return
        set_state(user_id, {"flow": "master_reg", "step": "name", "history": [], "data": {}})
        await message.answer("👤 Введіть ім'я:", reply_markup=back_menu_kb())
        return

    if text == "👑 Адмін" and is_admin(user_id):
        set_state(user_id, {"flow": "admin_menu"})
        await message.answer("👑 Адмін панель:", reply_markup=admin_menu_kb())
        return

    # CLIENT FLOW
    if st and st.get("flow") == "client_categories":
        if text in CATEGORY_LABEL_TO_VALUE:
            await show_client_actions(message, CATEGORY_LABEL_TO_VALUE[text])
            return

    if st and st.get("flow") == "client_actions":
        category = st["category"]

        if text == "📨 Створити заявку":
            current = now_ts()
            prev = RUNTIME.get_timestamp("last_client_order_time", user_id)
            if current - prev < CLIENT_ORDER_COOLDOWN:
                await message.answer(f"Зачекайте {CLIENT_ORDER_COOLDOWN - (current - prev)} сек перед новою заявкою.", reply_markup=client_actions_kb())
                return
            if client_active_orders_count(user_id) >= MAX_ACTIVE_CLIENT_ORDERS:
                await message.answer("У вас уже занадто багато активних заявок. Завершіть або скасуйте одну з них.", reply_markup=client_actions_kb())
                return
            set_state(user_id, {"flow": "client_problem", "category": category})
            await message.answer(f"Категорія: {category_label(category)}\n\nВведіть район / адресу:", reply_markup=back_menu_kb())
            return

        if text == "👷 Переглянути майстрів":
            with DB.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM masters
                    WHERE status='approved' AND category=%s
                    ORDER BY rating DESC, reviews_count DESC, name ASC
                    """,
                    (category,),
                )
                rows = cur.fetchall()
            if not rows:
                await message.answer("У цій категорії поки немає підтверджених майстрів.", reply_markup=client_actions_kb())
                return
            await message.answer(f"👷 Майстри в категорії: {category_label(category)}", reply_markup=client_actions_kb())
            for row in rows:
                await send_master_card(message.chat.id, row, title="👷 Майстер")
            return

        if text == "📦 Мої заявки":
            await show_client_orders(message)
            return

    if st and st.get("flow") == "client_problem":
        if "district" not in st:
            st["district"] = text if text else ""
            set_state(user_id, st)
            await message.answer("Тепер опишіть проблему одним повідомленням:", reply_markup=back_menu_kb())
            return

        if "problem" not in st:
            st["problem"] = text if text else "Без тексту"
            set_state(user_id, st)
            await message.answer("Надішліть фото або відео проблеми, або напишіть 'пропустити':", reply_markup=back_menu_kb())
            return

        media_type = None
        media_file_id = None
        if message.photo:
            media_type = "photo"
            media_file_id = message.photo[-1].file_id
        elif message.video:
            media_type = "video"
            media_file_id = message.video.file_id
        elif text and text.lower() == "пропустити":
            pass
        else:
            await message.answer("Надішліть фото, відео або напишіть 'пропустити'.", reply_markup=back_menu_kb())
            return

        created = now_ts()
        with DB.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    user_id, category, district, problem,
                    media_type, media_file_id, status,
                    created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'new', %s, %s)
                RETURNING id
                """,
                (user_id, st["category"], st["district"], st["problem"], media_type, media_file_id, created, created),
            )
            order_id = cur.fetchone()["id"]

        RUNTIME.set_timestamp("last_client_order_time", user_id, created)
        order_row = get_order_row(order_id)
        await notify_admin_about_order(order_row)
        await notify_masters_about_order(order_row)

        reset_state(user_id)
        await message.answer("✅ Заявку створено. Майстри вже отримали сповіщення.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    if text == "📦 Мої заявки":
        await show_client_orders(message)
        return

    # MASTER REG
    if st and st.get("flow") == "master_reg":
        step = st["step"]
        data = st["data"]

        if step == "name":
            if not text:
                await message.answer("Будь ласка, введіть ім'я текстом.", reply_markup=back_menu_kb())
                return
            data["name"] = text
            st["history"].append("name")
            st["step"] = "category"
            set_state(user_id, st)
            await message.answer("🔧 Оберіть спеціальність кнопкою:", reply_markup=back_menu_kb())
            await message.answer("Категорії:", reply_markup=master_categories_inline_kb())
            return

        if step == "district":
            if not text:
                await message.answer("Будь ласка, введіть район текстом.", reply_markup=back_menu_kb())
                return
            data["district"] = text
            st["history"].append("district")
            st["step"] = "description"
            set_state(user_id, st)
            await message.answer("🧾 Напишіть коротко про себе:", reply_markup=back_menu_kb())
            return

        if step == "description":
            if not text:
                await message.answer("Будь ласка, введіть опис текстом.", reply_markup=back_menu_kb())
                return
            data["description"] = text
            st["history"].append("description")
            st["step"] = "experience"
            set_state(user_id, st)
            await message.answer("🛠 Напишіть про досвід роботи:", reply_markup=back_menu_kb())
            return

        if step == "experience":
            if not text:
                await message.answer("Будь ласка, введіть досвід текстом.", reply_markup=back_menu_kb())
                return
            data["experience"] = text
            st["history"].append("experience")
            st["step"] = "phone"
            set_state(user_id, st)
            await message.answer("📞 Введіть телефон:", reply_markup=back_menu_kb())
            return

        if step == "phone":
            if not text:
                await message.answer("Будь ласка, введіть телефон текстом.", reply_markup=back_menu_kb())
                return
            data["phone"] = text
            st["history"].append("phone")
            st["step"] = "photo"
            set_state(user_id, st)
            await message.answer("📸 Надішліть фото або напишіть 'пропустити':", reply_markup=back_menu_kb())
            return

        if step == "photo":
            if message.photo:
                data["photo"] = message.photo[-1].file_id
            elif text and text.lower() == "пропустити":
                data["photo"] = None
            else:
                await message.answer("Надішліть фото або напишіть 'пропустити'.", reply_markup=back_menu_kb())
                return

            with DB.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO masters (
                        user_id, name, category, district, phone, description, experience, photo,
                        rating, reviews_count, status, availability, last_seen
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 'pending', 'offline', %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET
                        name=EXCLUDED.name,
                        category=EXCLUDED.category,
                        district=EXCLUDED.district,
                        phone=EXCLUDED.phone,
                        description=EXCLUDED.description,
                        experience=EXCLUDED.experience,
                        photo=EXCLUDED.photo,
                        status='pending',
                        availability='offline',
                        last_seen=EXCLUDED.last_seen
                    RETURNING *
                    """,
                    (
                        user_id,
                        data["name"],
                        data["category"],
                        data["district"],
                        data["phone"],
                        data["description"],
                        data["experience"],
                        data["photo"],
                        now_ts(),
                    ),
                )
                master_row = cur.fetchone()

            await send_master_card(ADMIN_ID, master_row, title="📝 Нова заявка майстра", reply_markup=admin_pending_master_inline(master_row["id"]))
            reset_state(user_id)
            await message.answer("⏳ Заявка відправлена адміну.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return

    # OFFER FLOW
    if st and st.get("flow") in ["offer_price", "offer_eta", "offer_comment"]:
        if st["flow"] == "offer_price":
            if not text:
                await message.answer("Вкажіть ціну текстом.", reply_markup=back_menu_kb())
                return
            st["price"] = text.strip()
            st["flow"] = "offer_eta"
            set_state(user_id, st)
            await message.answer("⏱ Коли зможете взятись? Напишіть час текстом.", reply_markup=back_menu_kb())
            return

        if st["flow"] == "offer_eta":
            if not text:
                await message.answer("Вкажіть час текстом.", reply_markup=back_menu_kb())
                return
            st["eta"] = text.strip()
            st["flow"] = "offer_comment"
            set_state(user_id, st)
            await message.answer("📝 Напишіть короткий коментар / пропозицію:", reply_markup=back_menu_kb())
            return

        if st["flow"] == "offer_comment":
            if not text:
                await message.answer("Напишіть коментар текстом або хоча б '-'.", reply_markup=back_menu_kb())
                return

            comment = text.strip()
            order_id = st["order_id"]

            try:
                with DB.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO offers (order_id, master_user_id, price, eta, comment, status, created_at)
                        VALUES (%s, %s, %s, %s, %s, 'active', %s)
                        """,
                        (order_id, user_id, st["price"], st["eta"], comment, now_ts()),
                    )
                    cur.execute(
                        "UPDATE orders SET status='offered', updated_at=%s WHERE id=%s",
                        (now_ts(), order_id),
                    )

                RUNTIME.set_timestamp("last_master_offer_time", user_id, now_ts())

                with DB.cursor() as cur:
                    cur.execute("SELECT user_id FROM orders WHERE id=%s", (order_id,))
                    client_row = cur.fetchone()
                if client_row:
                    client_id = client_row["user_id"]
                    try:
                        await bot.send_message(
                            client_id,
                            f"📬 На вашу заявку #{order_id} надійшла нова пропозиція від майстра {get_master_name(user_id)}.",
                            reply_markup=main_menu_kb(is_admin_user=is_admin(client_id)),
                        )
                    except Exception:
                        logger.exception("Не вдалося повідомити клієнта про нову пропозицію")

                reset_state(user_id)
                await message.answer("✅ Пропозицію відправлено клієнту.", reply_markup=master_menu_kb())
                return

            except Exception as e:
                logger.exception("Помилка при відправці пропозиції")
                await message.answer(f"❌ Помилка при відправці пропозиції: {e}", reply_markup=master_menu_kb())
                reset_state(user_id)
                return

    # MASTER MENU
    if text == "👤 Мій профіль":
        master = approved_master_row(user_id)
        if not master:
            await message.answer("Профіль майстра недоступний.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return
        await show_master_profile(message, master)
        return

    if text == "✏️ Редагувати профіль":
        master = approved_master_row(user_id)
        if not master:
            await message.answer("Профіль майстра недоступний.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return
        await message.answer("Оберіть, що хочете змінити:", reply_markup=back_menu_kb())
        await message.answer("Поля профілю:", reply_markup=edit_profile_inline_kb())
        return

    if text == "📦 Нові заявки":
        master = approved_master_row(user_id)
        if not master:
            await message.answer("❌ Ви не підтверджений майстер.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
            return
        await show_new_orders_for_master(message, master)
        return

    if text == "💬 Активні чати":
        await show_active_orders_for_master(message)
        return

    # ADMIN
    if text == "👷 База майстрів" and is_admin(user_id):
        await show_admin_masters_page(message.chat.id, 0)
        return
    if text == "📝 Заявки майстрів" and is_admin(user_id):
        await show_admin_pending_masters_page(message.chat.id, 0)
        return
    if text == "📦 Заявки клієнтів" and is_admin(user_id):
        await message.answer("Оберіть фільтр заявок:", reply_markup=admin_orders_filter_kb())
        return
    if text == "📋 Усі заявки" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, None)
        return
    if text == "🆕 Нові" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, "new")
        return
    if text == "📬 Є пропозиції" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, "offered")
        return
    if text == "🤝 Обрано майстра" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, "matched")
        return
    if text == "🛠 В роботі" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, "in_progress")
        return
    if text == "✅ Завершені" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, "done")
        return
    if text == "❌ Скасовані" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, "cancelled")
        return
    if text == "⌛ Прострочені" and is_admin(user_id):
        await show_admin_orders_page(message.chat.id, 0, "expired")
        return
    if text == "📊 Статистика" and is_admin(user_id):
        await show_admin_stats(message)
        return

    # RATING
    if st and st.get("flow") == "rating":
        if text not in ["1", "2", "3", "4", "5"]:
            await message.answer("Будь ласка, надішліть оцінку цифрою від 1 до 5.")
            return
        rating_value = int(text)
        with DB.cursor() as cur:
            cur.execute(
                """
                UPDATE masters
                SET rating = (rating * reviews_count + %s) / (reviews_count + 1),
                    reviews_count = reviews_count + 1
                WHERE user_id=%s
                """,
                (rating_value, st["master_user_id"]),
            )
            cur.execute("UPDATE orders SET rating=%s WHERE id=%s", (rating_value, st["order_id"]))
        set_state(user_id, {"flow": "review_text", "order_id": st["order_id"]})
        await message.answer("📝 Напишіть короткий текстовий відгук або напишіть 'пропустити':", reply_markup=back_menu_kb())
        return

    # FALLBACK
    if message.photo or message.video:
        if not st:
            await message.answer("Скористайтесь меню /start", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    if not st:
        await message.answer("Скористайтесь меню нижче.", reply_markup=main_menu_kb(is_admin_user=is_admin(user_id)))
        return

    await message.answer("Не зрозумів дію. Скористайтесь кнопками меню.", reply_markup=back_menu_kb())


if __name__ == "__main__":
    logger.info("Bot started")
    executor.start_polling(dp, skip_updates=True)
