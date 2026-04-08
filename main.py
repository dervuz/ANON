import asyncio
import logging
import os
import re
import time
from collections import deque
from typing import Callable, Any, Awaitable

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, TelegramObject,
    InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile,
)
import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  КОНФИГ
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN    = os.getenv("BOT_TOKEN", "8683488542:AAFJFiZGl5af_fYuowAnz9Xburd-RGZrI3g")
ADMIN_IDS    = [int(i) for i in os.getenv("ADMIN_IDS", "6708567261").split(",")]
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:yagay@localhost:5432/postgres")

REQUIRED_CHANNEL     = "@ANONCASES"
REQUIRED_CHANNEL_URL = "https://t.me/ANONCASES"

# Антиспам
ANTISPAM_MAX_MESSAGES = 5    # макс сообщений за окно
ANTISPAM_WINDOW_SEC   = 3    # окно в секундах
ANTISPAM_WARN_AT      = 3    # варн после N нарушений
ANTISPAM_BAN_AT       = 5    # автобан после N нарушений

# Автомодерация
AUTOMOD_BLOCK_LINKS      = True
AUTOMOD_BLOCK_PHONES     = True
AUTOMOD_BLOCK_USERNAMES  = False

# ═══════════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════

pool: asyncpg.Pool | None = None


async def init_db():
    global pool
    ssl_setting = "require" if os.getenv("RAILWAY_ENVIRONMENT") else None
    pool = await asyncpg.create_pool(DATABASE_URL, ssl=ssl_setting)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id       BIGINT PRIMARY KEY,
                username      TEXT,
                name          TEXT,
                gender        TEXT      DEFAULT NULL,
                age           INTEGER   DEFAULT NULL,
                about         TEXT      DEFAULT NULL,
                banned        BOOLEAN   DEFAULT FALSE,
                shadowbanned  BOOLEAN   DEFAULT FALSE,
                ban_reason    TEXT      DEFAULT NULL,
                warn_count    INTEGER   DEFAULT 0,
                chat_count    INTEGER   DEFAULT 0,
                spam_strikes  INTEGER   DEFAULT 0,
                created_at    TIMESTAMP DEFAULT NOW(),
                last_seen     TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS reports (
                id          SERIAL PRIMARY KEY,
                from_user   BIGINT,
                on_user     BIGINT,
                reason      TEXT,
                created_at  TIMESTAMP DEFAULT NOW(),
                reviewed    BOOLEAN   DEFAULT FALSE
            );

            CREATE TABLE IF NOT EXISTS bans (
                user_id     BIGINT PRIMARY KEY,
                reason      TEXT,
                banned_by   BIGINT,
                banned_at   TIMESTAMP DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS chat_sessions (
                id          SERIAL PRIMARY KEY,
                user1       BIGINT,
                user2       BIGINT,
                started_at  TIMESTAMP DEFAULT NOW(),
                ended_at    TIMESTAMP DEFAULT NULL,
                duration_s  INTEGER   DEFAULT NULL
            );
        """)


async def upsert_user(user_id: int, username: str, name: str):
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, username, name, last_seen)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                username  = EXCLUDED.username,
                name      = EXCLUDED.name,
                last_seen = NOW()
        """, user_id, username or "", name)


async def get_user(user_id: int):
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)


async def get_all_user_ids() -> list[int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users WHERE banned = FALSE")
        return [r["user_id"] for r in rows]


async def is_banned(user_id: int) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT banned FROM users WHERE user_id = $1", user_id)
        return bool(row and row["banned"])


async def is_shadowbanned(user_id: int) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT shadowbanned FROM users WHERE user_id = $1", user_id)
        return bool(row and row["shadowbanned"])


async def ban_user(user_id: int, reason: str, admin_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET banned = TRUE, ban_reason = $1 WHERE user_id = $2",
            reason, user_id
        )
        await conn.execute("""
            INSERT INTO bans (user_id, reason, banned_by)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE SET
                reason    = EXCLUDED.reason,
                banned_by = EXCLUDED.banned_by,
                banned_at = NOW()
        """, user_id, reason, admin_id)


async def unban_user(user_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET banned = FALSE, ban_reason = NULL WHERE user_id = $1", user_id
        )
        await conn.execute("DELETE FROM bans WHERE user_id = $1", user_id)


async def shadowban_user(user_id: int):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET shadowbanned = TRUE WHERE user_id = $1", user_id)


async def unshadowban_user(user_id: int):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET shadowbanned = FALSE WHERE user_id = $1", user_id)


async def add_warn(user_id: int) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE users SET warn_count = warn_count + 1 WHERE user_id = $1 RETURNING warn_count",
            user_id
        )
        return row["warn_count"] if row else 0


async def add_spam_strike(user_id: int) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE users SET spam_strikes = spam_strikes + 1 WHERE user_id = $1 RETURNING spam_strikes",
            user_id
        )
        return row["spam_strikes"] if row else 0


async def add_report(from_user: int, on_user: int, reason: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO reports (from_user, on_user, reason) VALUES ($1, $2, $3)",
            from_user, on_user, reason
        )


async def get_pending_reports():
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT r.*, u.username, u.name
            FROM reports r
            LEFT JOIN users u ON r.on_user = u.user_id
            WHERE r.reviewed = FALSE
            ORDER BY r.created_at DESC LIMIT 20
        """)


async def mark_report_reviewed(report_id: int):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE reports SET reviewed = TRUE WHERE id = $1", report_id)


async def get_stats() -> dict:
    async with pool.acquire() as conn:
        total        = await conn.fetchval("SELECT COUNT(*) FROM users")
        banned       = await conn.fetchval("SELECT COUNT(*) FROM users WHERE banned = TRUE")
        shadowbanned = await conn.fetchval("SELECT COUNT(*) FROM users WHERE shadowbanned = TRUE")
        reports      = await conn.fetchval("SELECT COUNT(*) FROM reports WHERE reviewed = FALSE")
        new_today    = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '1 day'"
        )
        new_week     = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE created_at >= NOW() - INTERVAL '7 days'"
        )
        avg_duration = await conn.fetchval(
            "SELECT ROUND(AVG(duration_s)) FROM chat_sessions WHERE duration_s IS NOT NULL"
        )
        total_sessions = await conn.fetchval(
            "SELECT COUNT(*) FROM chat_sessions WHERE ended_at IS NOT NULL"
        )
        return {
            "total": total, "banned": banned, "shadowbanned": shadowbanned,
            "reports": reports, "new_today": new_today, "new_week": new_week,
            "avg_duration": avg_duration or 0, "total_sessions": total_sessions,
        }


async def update_profile(user_id: int, field: str, value):
    if field not in {"gender", "age", "about"}:
        return
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE users SET {field} = $1 WHERE user_id = $2", value, user_id
        )


async def increment_chat_count(user_id: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET chat_count = chat_count + 1 WHERE user_id = $1", user_id
        )


async def start_session(user1: int, user2: int) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO chat_sessions (user1, user2) VALUES ($1, $2) RETURNING id",
            user1, user2
        )
        return row["id"]


async def end_session(session_id: int, duration_s: int):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE chat_sessions SET ended_at = NOW(), duration_s = $1 WHERE id = $2",
            duration_s, session_id
        )


async def export_users_csv() -> str:
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT user_id, username, name, gender, age, banned, shadowbanned,
                   warn_count, chat_count, created_at, last_seen
            FROM users ORDER BY created_at DESC
        """)
    lines = ["user_id,username,name,gender,age,banned,shadowbanned,warn_count,chat_count,created_at,last_seen"]
    for r in rows:
        lines.append(
            f"{r['user_id']},{r['username']},{r['name']},{r['gender'] or ''},"
            f"{r['age'] or ''},{r['banned']},{r['shadowbanned']},"
            f"{r['warn_count']},{r['chat_count']},{r['created_at']},{r['last_seen']}"
        )
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
#  АНТИСПАМ
# ═══════════════════════════════════════════════════════════════

_spam_windows: dict[int, deque] = {}


def check_spam(user_id: int) -> bool:
    """True = спам (превышен лимит)."""
    now = time.monotonic()
    if user_id not in _spam_windows:
        _spam_windows[user_id] = deque()
    dq = _spam_windows[user_id]
    while dq and now - dq[0] > ANTISPAM_WINDOW_SEC:
        dq.popleft()
    dq.append(now)
    return len(dq) > ANTISPAM_MAX_MESSAGES


# ═══════════════════════════════════════════════════════════════
#  АВТОМОДЕРАЦИЯ
# ═══════════════════════════════════════════════════════════════

_RE_LINKS    = re.compile(r"(https?://|www\.|t\.me/)", re.IGNORECASE)
_RE_PHONES   = re.compile(r"(\+?\d[\d\s\-\(\)]{7,}\d)")
_RE_USERNAME = re.compile(r"@[a-zA-Z0-9_]{4,}")


def check_automod(text: str) -> str | None:
    """Возвращает причину блокировки или None."""
    if not text:
        return None
    if AUTOMOD_BLOCK_LINKS and _RE_LINKS.search(text):
        return "🔗 Отправка ссылок запрещена."
    if AUTOMOD_BLOCK_PHONES and _RE_PHONES.search(text):
        return "📞 Отправка номеров телефонов запрещена."
    if AUTOMOD_BLOCK_USERNAMES and _RE_USERNAME.search(text):
        return "🚫 Отправка @username запрещена."
    return None


# ═══════════════════════════════════════════════════════════════
#  КЛАВИАТУРЫ
# ═══════════════════════════════════════════════════════════════

def kb_subscribe() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=REQUIRED_CHANNEL_URL)],
        [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")],
    ])

def kb_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔍 Найти собеседника", callback_data="find"),
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
        ],
    ])

def kb_chat() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="❌ Завершить", callback_data="stop"),
            InlineKeyboardButton(text="🚨 Пожаловаться", callback_data="report"),
        ],
    ])

def kb_cancel_search() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить поиск", callback_data="cancel_search")],
    ])

def kb_report_reasons() -> InlineKeyboardMarkup:
    reasons = [
        "🔞 Неприемлемый контент",
        "💬 Спам / реклама",
        "🤬 Оскорбления",
        "🤖 Бот / автоответчик",
    ]
    rows = [[InlineKeyboardButton(text=r, callback_data=f"report_reason:{r}")] for r in reasons]
    rows.append([InlineKeyboardButton(text="◀️ Отмена", callback_data="report_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_profile_edit() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚥ Пол", callback_data="edit:gender"),
            InlineKeyboardButton(text="🎂 Возраст", callback_data="edit:age"),
        ],
        [InlineKeyboardButton(text="📝 О себе", callback_data="edit:about")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_main")],
    ])

def kb_gender() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👦 Парень", callback_data="set_gender:Парень"),
            InlineKeyboardButton(text="👧 Девушка", callback_data="set_gender:Девушка"),
        ],
        [InlineKeyboardButton(text="🌈 Другое", callback_data="set_gender:Другое")],
    ])

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="📋 Жалобы", callback_data="admin:reports")],
        [InlineKeyboardButton(text="🔍 Найти пользователя", callback_data="admin:find_user")],
        [
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin:broadcast"),
            InlineKeyboardButton(text="📥 Экспорт CSV", callback_data="admin:export"),
        ],
    ])

def kb_admin_user(user_id: int, is_banned_: bool, is_shadow: bool) -> InlineKeyboardMarkup:
    rows = []
    if is_banned_:
        rows.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"admin_unban:{user_id}")])
    else:
        rows.append([
            InlineKeyboardButton(text="🚫 Бан", callback_data=f"admin_ban:{user_id}"),
            InlineKeyboardButton(text="⚠️ Варн", callback_data=f"admin_warn:{user_id}"),
        ])
    shadow_text = "👁 Снять теневой бан" if is_shadow else "👁 Теневой бан"
    shadow_cb   = f"admin_unshadow:{user_id}" if is_shadow else f"admin_shadow:{user_id}"
    rows.append([InlineKeyboardButton(text=shadow_text, callback_data=shadow_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def kb_admin_report(on_user: int, report_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚫 Бан", callback_data=f"admin_ban:{on_user}"),
            InlineKeyboardButton(text="⚠️ Варн", callback_data=f"admin_warn:{on_user}"),
        ],
        [InlineKeyboardButton(text="👁 Теневой бан", callback_data=f"admin_shadow:{on_user}")],
        [InlineKeyboardButton(text="✅ Закрыть жалобу", callback_data=f"admin_close_report:{report_id}")],
    ])


# ═══════════════════════════════════════════════════════════════
#  MIDDLEWARE — ПРОВЕРКА ПОДПИСКИ
# ═══════════════════════════════════════════════════════════════

class SubscriptionMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        bot = data["bot"]

        if isinstance(event, Message):
            user_id = event.from_user.id
            reply_target = event
        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id
            reply_target = event.message
            if event.data == "check_sub":
                return await handler(event, data)
        else:
            return await handler(event, data)

        if user_id in ADMIN_IDS:
            return await handler(event, data)

        try:
            member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
            subscribed = member.status not in ("left", "kicked", "banned")
        except Exception:
            subscribed = True   # если бот не в канале — пропускаем

        if not subscribed:
            await reply_target.answer(
                "📢 *Для использования бота нужно подписаться на канал* @ANONCASES\n\n"
                "После подписки нажми кнопку ✅ Я подписался",
                parse_mode="Markdown",
                reply_markup=kb_subscribe(),
            )
            return

        return await handler(event, data)


# ═══════════════════════════════════════════════════════════════
#  IN-MEMORY СОСТОЯНИЕ ЧАТОВ
# ═══════════════════════════════════════════════════════════════

active_chats:  dict[int, int]   = {}   # user_id -> partner_id
waiting_queue: list[int]        = []   # очередь поиска
chat_start:    dict[int, float] = {}   # user_id -> unix timestamp начала
session_ids:   dict[int, int]   = {}   # user_id -> id в chat_sessions


# ═══════════════════════════════════════════════════════════════
#  FSM СОСТОЯНИЯ
# ═══════════════════════════════════════════════════════════════

class ProfileState(StatesGroup):
    waiting_age   = State()
    waiting_about = State()

class AdminState(StatesGroup):
    waiting_user_id    = State()
    waiting_ban_reason = State()
    waiting_broadcast  = State()


# ═══════════════════════════════════════════════════════════════
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}с"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}м {s}с"
    h, m = divmod(m, 60)
    return f"{h}ч {m}м {s}с"


def get_chat_duration(user_id: int) -> str:
    started = chat_start.get(user_id)
    if not started:
        return "—"
    return fmt_duration(int(time.time() - started))


def format_profile(row) -> str:
    return (
        f"👤 *Мой профиль*\n\n"
        f"⚥ Пол: {row['gender'] or '—'}\n"
        f"🎂 Возраст: {row['age'] or '—'}\n"
        f"📝 О себе: {row['about'] or '—'}\n\n"
        f"💬 Диалогов: {row['chat_count']}\n"
        f"⚠️ Предупреждений: {row['warn_count']}"
    )


async def check_subscription(bot: Bot, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status not in ("left", "kicked", "banned")
    except Exception:
        return True


async def end_chat(bot: Bot, user_id: int, notify_self: bool = True):
    partner_id = active_chats.pop(user_id, None)
    if partner_id:
        active_chats.pop(partner_id, None)

    started    = chat_start.pop(user_id, None)
    session_id = session_ids.pop(user_id, None)
    if partner_id:
        chat_start.pop(partner_id, None)
        session_ids.pop(partner_id, None)

    if started and session_id:
        await end_session(session_id, int(time.time() - started))

    if partner_id:
        try:
            await bot.send_message(partner_id, "🔴 Собеседник завершил диалог.", reply_markup=kb_main())
        except Exception:
            pass
    if notify_self:
        try:
            await bot.send_message(user_id, "🔴 Диалог завершён.", reply_markup=kb_main())
        except Exception:
            pass


async def send_user_info(chat_id: int, target_id: int, bot: Bot):
    row = await get_user(target_id)
    if not row:
        await bot.send_message(chat_id, "❌ Пользователь не найден.")
        return
    status   = "🚫 Заблокирован" if row["banned"] else "✅ Активен"
    shadow   = " | 👁 Теневой бан" if row["shadowbanned"] else ""
    in_chat  = "💬 Сейчас в диалоге" if target_id in active_chats else "🔴 Не в диалоге"
    duration = get_chat_duration(target_id) if target_id in active_chats else "—"
    text = (
        f"👤 *Пользователь {target_id}*\n\n"
        f"Имя: {row['name']}\n"
        f"Username: @{row['username'] or '—'}\n"
        f"Пол: {row['gender'] or '—'}\n"
        f"Возраст: {row['age'] or '—'}\n"
        f"О себе: {row['about'] or '—'}\n\n"
        f"💬 Диалогов: {row['chat_count']}\n"
        f"⚠️ Варнов: {row['warn_count']}\n"
        f"Статус: {status}{shadow}\n"
        f"Сейчас: {in_chat}\n"
        f"Длит. чата: {duration}\n"
        f"Причина бана: {row['ban_reason'] or '—'}\n"
        f"Регистрация: {row['created_at']}"
    )
    await bot.send_message(
        chat_id, text,
        parse_mode="Markdown",
        reply_markup=kb_admin_user(target_id, row["banned"], row["shadowbanned"]),
    )


# ═══════════════════════════════════════════════════════════════
#  РОУТЕР И ХЭНДЛЕРЫ
# ═══════════════════════════════════════════════════════════════

router = Router()


# ── /start ────────────────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(message: Message):
    user = message.from_user
    await upsert_user(user.id, user.username, user.full_name)

    if await is_banned(user.id):
        row = await get_user(user.id)
        reason = row["ban_reason"] if row else "нарушение правил"
        await message.answer(f"🚫 Ты заблокирован.\nПричина: {reason}")
        return

    if user.id in active_chats:
        await message.answer("⚠️ Ты уже в диалоге.", reply_markup=kb_chat())
        return

    await message.answer(
        "👋 *Добро пожаловать в анонимный чат!*\n\n"
        "Общайся анонимно — никто не узнает кто ты.\n\n"
        "Нажми 🔍 чтобы найти собеседника.",
        parse_mode="Markdown",
        reply_markup=kb_main(),
    )


# ── Проверка подписки ─────────────────────────────────────────

@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery, bot: Bot):
    user_id = call.from_user.id
    await upsert_user(user_id, call.from_user.username, call.from_user.full_name)
    subscribed = await check_subscription(bot, user_id)
    if subscribed:
        await call.answer("✅ Подписка подтверждена!")
        await call.message.edit_text(
            "✅ Отлично! Теперь ты можешь пользоваться ботом.",
            reply_markup=kb_main(),
        )
    else:
        await call.answer("❌ Ты ещё не подписался!", show_alert=True)


# ── Поиск ─────────────────────────────────────────────────────

@router.callback_query(F.data == "find")
async def cb_find(call: CallbackQuery, bot: Bot):
    user_id = call.from_user.id
    await call.answer()

    if await is_banned(user_id):
        await call.message.answer("🚫 Ты заблокирован.")
        return
    if user_id in active_chats:
        await call.message.answer("⚠️ Ты уже в диалоге.", reply_markup=kb_chat())
        return
    if user_id in waiting_queue:
        await call.message.answer("⏳ Ты уже в очереди поиска.")
        return

    if waiting_queue:
        partner_id = waiting_queue.pop(0)
        if partner_id in active_chats:
            waiting_queue.append(user_id)
            await call.message.answer("⏳ Ищем собеседника...", reply_markup=kb_cancel_search())
            return

        active_chats[user_id]     = partner_id
        active_chats[partner_id]  = user_id
        now = time.time()
        chat_start[user_id]       = now
        chat_start[partner_id]    = now

        sid = await start_session(user_id, partner_id)
        session_ids[user_id]      = sid
        session_ids[partner_id]   = sid

        await increment_chat_count(user_id)
        await increment_chat_count(partner_id)

        text = (
            "✅ *Собеседник найден!* Можешь писать.\n\n"
            "_Никто не знает кто ты — оставайся анонимным._"
        )
        await bot.send_message(partner_id, text, parse_mode="Markdown", reply_markup=kb_chat())
        await call.message.answer(text,          parse_mode="Markdown", reply_markup=kb_chat())
    else:
        waiting_queue.append(user_id)
        await call.message.answer("⏳ Ищем собеседника...", reply_markup=kb_cancel_search())


@router.callback_query(F.data == "cancel_search")
async def cb_cancel_search(call: CallbackQuery):
    await call.answer()
    if call.from_user.id in waiting_queue:
        waiting_queue.remove(call.from_user.id)
    await call.message.edit_text("🚫 Поиск отменён.", reply_markup=kb_main())


@router.callback_query(F.data == "stop")
async def cb_stop(call: CallbackQuery, bot: Bot):
    await call.answer()
    await end_chat(bot, call.from_user.id)


@router.message(Command("stop"))
async def cmd_stop(message: Message, bot: Bot):
    await end_chat(bot, message.from_user.id)


# ── Счётчик времени ───────────────────────────────────────────

@router.message(Command("time"))
async def cmd_time(message: Message):
    user_id = message.from_user.id
    if user_id not in active_chats:
        await message.answer("⚠️ Ты не в диалоге.")
        return
    await message.answer(
        f"⏱ Длительность диалога: *{get_chat_duration(user_id)}*",
        parse_mode="Markdown"
    )


# ── Профиль ───────────────────────────────────────────────────

@router.callback_query(F.data == "profile")
async def cb_profile(call: CallbackQuery):
    await call.answer()
    uid = call.from_user.id
    row = await get_user(uid)
    if not row:
        await upsert_user(uid, call.from_user.username, call.from_user.full_name)
        row = await get_user(uid)
    await call.message.answer(format_profile(row), parse_mode="Markdown", reply_markup=kb_profile_edit())


@router.callback_query(F.data == "back_main")
async def cb_back_main(call: CallbackQuery):
    await call.answer()
    await call.message.answer("Главное меню:", reply_markup=kb_main())


@router.callback_query(F.data.startswith("edit:"))
async def cb_edit_profile(call: CallbackQuery, state: FSMContext):
    await call.answer()
    field = call.data.split(":")[1]
    if field == "gender":
        await call.message.answer("⚥ Выбери свой пол:", reply_markup=kb_gender())
    elif field == "age":
        await state.set_state(ProfileState.waiting_age)
        await call.message.answer("🎂 Введи свой возраст (10–100):")
    elif field == "about":
        await state.set_state(ProfileState.waiting_about)
        await call.message.answer("📝 Напиши о себе (до 150 символов):")


@router.callback_query(F.data.startswith("set_gender:"))
async def cb_set_gender(call: CallbackQuery):
    await call.answer()
    gender = call.data.split(":")[1]
    await update_profile(call.from_user.id, "gender", gender)
    await call.message.answer(f"✅ Пол сохранён: {gender}", reply_markup=kb_profile_edit())


@router.message(ProfileState.waiting_age)
async def fsm_age(message: Message, state: FSMContext):
    text = message.text.strip()
    if text.isdigit() and 10 <= int(text) <= 100:
        await update_profile(message.from_user.id, "age", int(text))
        await state.clear()
        await message.answer(f"✅ Возраст сохранён: {text}", reply_markup=kb_profile_edit())
    else:
        await message.answer("❌ Введи корректный возраст (10–100).")


@router.message(ProfileState.waiting_about)
async def fsm_about(message: Message, state: FSMContext):
    text = message.text.strip()
    if len(text) > 150:
        await message.answer("❌ Слишком длинно, максимум 150 символов.")
    else:
        await update_profile(message.from_user.id, "about", text)
        await state.clear()
        await message.answer("✅ Описание сохранено!", reply_markup=kb_profile_edit())


# ── Жалобы ────────────────────────────────────────────────────

@router.callback_query(F.data == "report")
async def cb_report(call: CallbackQuery):
    await call.answer()
    if call.from_user.id not in active_chats:
        await call.message.answer("⚠️ Ты не в диалоге.")
        return
    await call.message.answer("🚨 Выбери причину жалобы:", reply_markup=kb_report_reasons())


@router.callback_query(F.data == "report_cancel")
async def cb_report_cancel(call: CallbackQuery):
    await call.answer()
    await call.message.edit_text("Жалоба отменена.", reply_markup=kb_chat())


@router.callback_query(F.data.startswith("report_reason:"))
async def cb_report_reason(call: CallbackQuery, bot: Bot):
    await call.answer()
    reason     = call.data.split(":", 1)[1]
    user_id    = call.from_user.id
    partner_id = active_chats.get(user_id)

    if not partner_id:
        await call.message.answer("⚠️ Ты уже не в диалоге.", reply_markup=kb_main())
        return

    await add_report(user_id, partner_id, reason)
    await call.message.edit_text("✅ Жалоба отправлена. Модераторы рассмотрят её.", reply_markup=kb_chat())

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"🚨 *Новая жалоба*\n\nОт: `{user_id}`\nНа: `{partner_id}`\nПричина: {reason}",
                parse_mode="Markdown",
                reply_markup=kb_admin_report(partner_id, 0),
            )
        except Exception:
            pass


# ── Пересылка сообщений ───────────────────────────────────────

RELAY_TYPES = F.content_type.in_({
    "text", "photo", "video", "audio", "voice",
    "document", "sticker", "animation", "video_note",
})


@router.message(RELAY_TYPES)
async def relay_message(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id

    # Команды не пересылаем
    if message.text and message.text.startswith("/"):
        return

    await upsert_user(user_id, message.from_user.username, message.from_user.full_name)

    if await is_banned(user_id):
        await message.answer("🚫 Ты заблокирован.")
        return

    # Если активен FSM — не перехватываем
    if await state.get_state():
        return

    partner_id = active_chats.get(user_id)
    if not partner_id:
        await message.answer("⚠️ Ты не в диалоге.", reply_markup=kb_main())
        return

    # Антиспам
    if check_spam(user_id):
        strikes = await add_spam_strike(user_id)
        await message.answer(
            f"⚡ Не так быстро! Ты отправляешь сообщения слишком часто.\n"
            f"Нарушение {strikes}/{ANTISPAM_BAN_AT}"
        )
        if strikes >= ANTISPAM_BAN_AT:
            await ban_user(user_id, "автобан за спам", 0)
            await end_chat(bot, user_id)
            await message.answer("🚫 Ты заблокирован за спам.")
        elif strikes >= ANTISPAM_WARN_AT:
            await add_warn(user_id)
            await message.answer("⚠️ Предупреждение за флуд.")
        return

    # Автомодерация (только текст)
    if message.content_type == "text" and message.text:
        block_reason = check_automod(message.text)
        if block_reason:
            await message.answer(f"🚫 {block_reason}")
            return

    # Теневой бан — делаем вид что всё ок, но ничего не отправляем
    if await is_shadowbanned(user_id):
        return

    # Статус "печатает..."
    try:
        await bot.send_chat_action(partner_id, action="typing")
    except Exception:
        pass

    # Пересылка
    try:
        ct = message.content_type
        if ct == "text":
            await bot.send_message(partner_id, message.text)
        elif ct == "photo":
            await bot.send_photo(partner_id, message.photo[-1].file_id, caption=message.caption)
        elif ct == "video":
            await bot.send_video(partner_id, message.video.file_id, caption=message.caption)
        elif ct == "audio":
            await bot.send_audio(partner_id, message.audio.file_id, caption=message.caption)
        elif ct == "voice":
            await bot.send_voice(partner_id, message.voice.file_id)
        elif ct == "document":
            await bot.send_document(partner_id, message.document.file_id, caption=message.caption)
        elif ct == "sticker":
            await bot.send_sticker(partner_id, message.sticker.file_id)
        elif ct == "animation":
            await bot.send_animation(partner_id, message.animation.file_id, caption=message.caption)
        elif ct == "video_note":
            await bot.send_video_note(partner_id, message.video_note.file_id)
    except Exception as e:
        logger.error(f"Relay error: {e}")
        await message.answer("⚠️ Не удалось отправить сообщение.")


# ═══════════════════════════════════════════════════════════════
#  АДМИН ПАНЕЛЬ
# ═══════════════════════════════════════════════════════════════

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("🛡 *Админ панель*", parse_mode="Markdown", reply_markup=kb_admin_main())


@router.message(Command("ban"))
async def cmd_ban(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: /ban <user_id> [причина]")
        return
    try:
        target_id = int(parts[1])
        reason    = parts[2] if len(parts) > 2 else "нарушение правил"
        await ban_user(target_id, reason, message.from_user.id)
        if target_id in active_chats:
            await end_chat(bot, target_id)
        await message.answer(f"✅ `{target_id}` заблокирован.\nПричина: {reason}", parse_mode="Markdown")
        try:
            await bot.send_message(target_id, f"🚫 Ты заблокирован.\nПричина: {reason}")
        except Exception:
            pass
    except ValueError:
        await message.answer("❌ Неверный user_id")


@router.message(Command("unban"))
async def cmd_unban(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /unban <user_id>")
        return
    try:
        target_id = int(parts[1])
        await unban_user(target_id)
        await message.answer(f"✅ `{target_id}` разблокирован.", parse_mode="Markdown")
        try:
            await bot.send_message(target_id, "✅ Ты разблокирован!", reply_markup=kb_main())
        except Exception:
            pass
    except ValueError:
        await message.answer("❌ Неверный user_id")


@router.message(Command("warn"))
async def cmd_warn(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: /warn <user_id> [причина]")
        return
    try:
        target_id = int(parts[1])
        reason    = parts[2] if len(parts) > 2 else "нарушение правил"
        count     = await add_warn(target_id)
        await message.answer(f"⚠️ `{target_id}` получил варн ({count}/3).", parse_mode="Markdown")
        try:
            await bot.send_message(target_id, f"⚠️ Предупреждение {count}/3.\nПричина: {reason}")
        except Exception:
            pass
        if count >= 3:
            await ban_user(target_id, "3 предупреждения", message.from_user.id)
            if target_id in active_chats:
                await end_chat(bot, target_id)
            await message.answer(
                f"🚫 `{target_id}` автоматически заблокирован (3 варна).", parse_mode="Markdown"
            )
    except ValueError:
        await message.answer("❌ Неверный user_id")


@router.message(Command("shadow"))
async def cmd_shadow(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /shadow <user_id>")
        return
    try:
        target_id = int(parts[1])
        await shadowban_user(target_id)
        await message.answer(f"👁 `{target_id}` теперь в теневом бане.", parse_mode="Markdown")
    except ValueError:
        await message.answer("❌ Неверный user_id")


@router.message(Command("unshadow"))
async def cmd_unshadow(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /unshadow <user_id>")
        return
    try:
        target_id = int(parts[1])
        await unshadowban_user(target_id)
        await message.answer(f"✅ Теневой бан снят с `{target_id}`.", parse_mode="Markdown")
    except ValueError:
        await message.answer("❌ Неверный user_id")


@router.message(Command("user"))
async def cmd_user_info(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /user <user_id>")
        return
    try:
        await send_user_info(message.chat.id, int(parts[1]), bot)
    except ValueError:
        await message.answer("❌ Неверный user_id")


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(AdminState.waiting_broadcast)
    await message.answer("📢 Введи текст рассылки (поддерживается Markdown).\n/cancel — отменить.")


@router.message(Command("export"))
async def cmd_export(message: Message, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await message.answer("⏳ Генерирую CSV...")
    csv_data = await export_users_csv()
    file = BufferedInputFile(csv_data.encode("utf-8"), filename="users_export.csv")
    await bot.send_document(message.chat.id, file, caption="📥 Экспорт пользователей")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Действие отменено.", reply_markup=kb_main())


# ── Callbacks админа ──────────────────────────────────────────

@router.callback_query(F.data == "admin:stats")
async def admin_cb_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    s      = await get_stats()
    active = len(active_chats) // 2
    waiting = len(waiting_queue)
    avg    = fmt_duration(s["avg_duration"]) if s["avg_duration"] else "—"
    await call.message.answer(
        f"📊 *Детальная статистика*\n\n"
        f"👥 Всего пользователей: {s['total']}\n"
        f"🆕 За сегодня: {s['new_today']}\n"
        f"📅 За неделю: {s['new_week']}\n\n"
        f"🚫 Заблокировано: {s['banned']}\n"
        f"👁 Теневой бан: {s['shadowbanned']}\n"
        f"📋 Жалоб на рассмотрении: {s['reports']}\n\n"
        f"💬 Активных диалогов: {active}\n"
        f"⏳ В очереди: {waiting}\n"
        f"📈 Всего сессий: {s['total_sessions']}\n"
        f"⏱ Средняя длит. диалога: {avg}",
        parse_mode="Markdown",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data == "admin:reports")
async def admin_cb_reports(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    rows = await get_pending_reports()
    if not rows:
        await call.message.answer("✅ Жалоб нет.", reply_markup=kb_admin_main())
        return
    for row in rows:
        name  = row["name"] or "—"
        uname = f"@{row['username']}" if row["username"] else "—"
        await call.message.answer(
            f"📋 *Жалоба #{row['id']}*\n\n"
            f"На: {name} ({uname})\n"
            f"ID: `{row['on_user']}`\n"
            f"Причина: {row['reason']}\n"
            f"Дата: {row['created_at']}",
            parse_mode="Markdown",
            reply_markup=kb_admin_report(row["on_user"], row["id"]),
        )


@router.callback_query(F.data == "admin:find_user")
async def admin_cb_find_user(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    await state.set_state(AdminState.waiting_user_id)
    await call.message.answer("🔍 Введи user_id пользователя:")


@router.callback_query(F.data == "admin:broadcast")
async def admin_cb_broadcast(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    await state.set_state(AdminState.waiting_broadcast)
    await call.message.answer("📢 Введи текст рассылки (поддерживается Markdown):")


@router.callback_query(F.data == "admin:export")
async def admin_cb_export(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return
    await call.answer("⏳ Генерирую...")
    csv_data = await export_users_csv()
    file = BufferedInputFile(csv_data.encode("utf-8"), filename="users_export.csv")
    await bot.send_document(call.from_user.id, file, caption="📥 Экспорт пользователей")


@router.message(AdminState.waiting_user_id)
async def admin_fsm_user_id(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    text = message.text.strip()
    if text.isdigit():
        await state.clear()
        await send_user_info(message.chat.id, int(text), bot)
    else:
        await message.answer("❌ Введи числовой user_id.")


@router.message(AdminState.waiting_broadcast)
async def admin_fsm_broadcast(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    text = message.text or ""
    if not text:
        await message.answer("❌ Пустое сообщение.")
        return

    user_ids = await get_all_user_ids()
    await message.answer(f"📢 Начинаю рассылку для {len(user_ids)} пользователей...")

    sent, failed = 0, 0
    for uid in user_ids:
        try:
            await bot.send_message(uid, text, parse_mode="Markdown")
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)   # ~20 msg/s

    await message.answer(
        f"✅ Рассылка завершена.\n✉️ Доставлено: {sent}\n❌ Не доставлено: {failed}",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data.startswith("admin_ban:"))
async def admin_cb_ban(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    target_id = int(call.data.split(":")[1])
    await state.set_state(AdminState.waiting_ban_reason)
    await state.update_data(target_id=target_id)
    await call.message.answer(f"Введи причину бана для `{target_id}`:", parse_mode="Markdown")


@router.message(AdminState.waiting_ban_reason)
async def admin_fsm_ban_reason(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data      = await state.get_data()
    target_id = data["target_id"]
    reason    = message.text.strip()
    await state.clear()
    await ban_user(target_id, reason, message.from_user.id)
    if target_id in active_chats:
        await end_chat(bot, target_id)
    await message.answer(f"✅ `{target_id}` заблокирован.\nПричина: {reason}", parse_mode="Markdown")
    try:
        await bot.send_message(target_id, f"🚫 Ты заблокирован.\nПричина: {reason}")
    except Exception:
        pass


@router.callback_query(F.data.startswith("admin_unban:"))
async def admin_cb_unban(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    target_id = int(call.data.split(":")[1])
    await unban_user(target_id)
    await call.message.answer(f"✅ `{target_id}` разблокирован.", parse_mode="Markdown")
    try:
        await bot.send_message(target_id, "✅ Ты разблокирован!", reply_markup=kb_main())
    except Exception:
        pass


@router.callback_query(F.data.startswith("admin_warn:"))
async def admin_cb_warn(call: CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    target_id = int(call.data.split(":")[1])
    count     = await add_warn(target_id)
    await call.message.answer(f"⚠️ `{target_id}` получил варн ({count}/3).", parse_mode="Markdown")
    try:
        await bot.send_message(target_id, f"⚠️ Предупреждение {count}/3.")
    except Exception:
        pass
    if count >= 3:
        await ban_user(target_id, "3 предупреждения", call.from_user.id)
        if target_id in active_chats:
            await end_chat(bot, target_id)
        await call.message.answer(
            f"🚫 `{target_id}` автоматически заблокирован.", parse_mode="Markdown"
        )


@router.callback_query(F.data.startswith("admin_shadow:"))
async def admin_cb_shadow(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    target_id = int(call.data.split(":")[1])
    await shadowban_user(target_id)
    await call.message.answer(f"👁 `{target_id}` теперь в теневом бане.", parse_mode="Markdown")


@router.callback_query(F.data.startswith("admin_unshadow:"))
async def admin_cb_unshadow(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    target_id = int(call.data.split(":")[1])
    await unshadowban_user(target_id)
    await call.message.answer(f"✅ Теневой бан снят с `{target_id}`.", parse_mode="Markdown")


@router.callback_query(F.data.startswith("admin_close_report:"))
async def admin_cb_close_report(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        return
    await call.answer()
    report_id = int(call.data.split(":")[1])
    await mark_report_reviewed(report_id)
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer(f"✅ Жалоба #{report_id} закрыта.")


# ═══════════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════════

async def main():
    await init_db()
    logger.info("✅ БД инициализирована")

    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(SubscriptionMiddleware())
    dp.callback_query.middleware(SubscriptionMiddleware())

    dp.include_router(router)

    logger.info("🚀 Бот запущен")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())