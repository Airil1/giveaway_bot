"""
Telegram-бот для розыгрышей (aiogram 3 + PostgreSQL).

Установка:
  pip install aiogram "psycopg[binary,pool]" python-dotenv

В .env или в системе нужен BOT_TOKEN.
Опционально: START_WELCOME_PHOTO — путь к PNG главного экрана (абсолютный или относительно каталога скрипта; иначе ./start_welcome.png).
ID премиум-эмодзи в кнопках/шаблоне (_PE_*): с этого бота (см. /emojiid).

Логика: участвовать может кто угодно; создавать и постить — только админы выбранного канала/группы;
управлять конкретным розыгрышем может только тот, кто его создал.

Бот должен уметь писать в чаты постов; для проверки подписок ему нужен доступ к участникам.

В личке с пользователем одно сообщение обычно редактируется (меню, шаги мастера). Посты конкурсов
и итоги уходят отдельными сообщениями в каналы/группы.

Время окончания приёма заявок вводится и показывается по Москве (UTC+3), в базе хранится в UTC.
"""
from __future__ import annotations

import asyncio
import calendar
import html
import io
import json
import logging
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.state import State, StatesGroup, default_state
from aiogram.types import (
    CallbackQuery,
    Chat,
    ChatMemberUpdated,
    ChatMemberOwner,
    ChatAdministratorRights,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    KeyboardButtonRequestChat,
    LinkPreviewOptions,
    Message,
    MessageEntity,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
try:
    from telethon import TelegramClient
    from telethon import Button as TlButton
except Exception:
    TelegramClient = None  # type: ignore[assignment]
    TlButton = None  # type: ignore[assignment]

# Явно отключаем предпросмотр ссылок в тексте (в т.ч. t.me) для поддерживающих методов API.
_LINK_PREVIEW_OFF = LinkPreviewOptions(is_disabled=True)

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("giveaway")

from contextlib import asynccontextmanager

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

_POOL: AsyncConnectionPool | None = None


def _database_url() -> str:
    url = (os.environ.get("DATABASE_URL") or "").strip()
    if url:
        return url
    user = (os.environ.get("POSTGRES_USER") or "giveaway").strip()
    password = (os.environ.get("POSTGRES_PASSWORD") or "").strip()
    host = (os.environ.get("POSTGRES_HOST") or "127.0.0.1").strip()
    port = (os.environ.get("POSTGRES_PORT") or "5432").strip()
    dbn = (os.environ.get("POSTGRES_DB") or "giveawaybot").strip()
    if not password:
        raise SystemExit(
            "Задайте DATABASE_URL или POSTGRES_PASSWORD (и при необходимости POSTGRES_*)."
        )
    return f"postgresql://{user}:{password}@{host}:{port}/{dbn}"


async def _get_pool() -> AsyncConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = AsyncConnectionPool(
            conninfo=_database_url(),
            min_size=1,
            max_size=15,
            open=False,
            max_waiting=64,
            timeout=120.0,
        )
        await _POOL.open()
    return _POOL


class _PgCursorShim:
    __slots__ = ("_cur", "lastrowid")

    def __init__(self, cur):
        self._cur = cur
        self.lastrowid: int | None = None

    async def fetchone(self):
        return await self._cur.fetchone()

    async def fetchall(self):
        return await self._cur.fetchall()


class PgConnAdapter:
    # Shim aiosqlite: ? -> %s, fetch*, commit.
    # Закрываем предыдущий курсор перед новым execute: иначе на одном соединении
    # накапливаются порталы и ломаются цепочки SELECT → много DELETE (purge) / RETURNING.

    __slots__ = ("_conn", "row_factory", "_cur")

    def __init__(self, conn):
        self._conn = conn
        self.row_factory = None
        self._cur = None

    async def _discard_cursor(self) -> None:
        c = self._cur
        self._cur = None
        if c is None:
            return
        try:
            await c.close()
        except Exception:
            pass

    async def execute(self, sql: str, params=None):
        await self._discard_cursor()
        if params is None:
            params = ()
        sql_pg = sql.replace("?", "%s")
        cur = self._conn.cursor(row_factory=dict_row)
        await cur.execute(sql_pg, params)
        self._cur = cur
        shim = _PgCursorShim(cur)
        return shim

    async def commit(self):
        await self._discard_cursor()
        await self._conn.commit()


@asynccontextmanager
async def _pg_conn():
    pool = await _get_pool()
    async with pool.connection() as raw:
        db = PgConnAdapter(raw)
        try:
            yield db
        finally:
            await db._discard_cursor()



TOKEN = os.environ.get("BOT_TOKEN", "").strip()
USERBOT_API_ID = os.environ.get("USERBOT_API_ID", "").strip()
USERBOT_API_HASH = os.environ.get("USERBOT_API_HASH", "").strip()
USERBOT_SESSION = os.environ.get("USERBOT_SESSION", "").strip()
# Необязательно: числовой user_id аккаунта userbot (если не задан — берётся из Telethon get_me()).
USERBOT_USER_ID_ENV = os.environ.get("USERBOT_USER_ID", "").strip()

if not TOKEN:
    raise SystemExit("Задайте BOT_TOKEN в переменных окружения или .env")
_database_url()  # ранняя проверка DATABASE_URL / POSTGRES_*


def _start_welcome_photo_path() -> Path:
    """Картинка главного экрана: START_WELCOME_PHOTO в окружении или start_welcome.png рядом со скриптом.

    Относительный путь в переменной считается от каталога скрипта (не от cwd процесса).
    """
    base = Path(__file__).resolve().parent
    custom = (os.environ.get("START_WELCOME_PHOTO") or "").strip()
    if custom:
        p = Path(custom).expanduser()
        if not p.is_absolute():
            p = base / p
        return p.resolve()
    return (base / "start_welcome.png").resolve()


REF_PREFIX = "ref_"
JOIN_PREFIX = "join_"

# request_chat у reply-кнопок «добавить канал / группу»
REQ_CHAT_ADD_CHANNEL = 1
REQ_CHAT_ADD_GROUP = 2

# Premium custom emoji (Telegram HTML: tg-emoji) — лотерея и сохранённые чаты
_PE_LOT_STEP = "5778647930038653243"
_PE_LOT_PROMPT = "6039614175917903752"
_PE_LOT_DOCWARN = "6030563507299160824"
_PE_LOT_PREMIUM_HINT = "5413721644277441416"
_PE_LOT_COLOR = "5884106131822875141"
_PE_LOT_CHANEL_PICK = "6039422865189638057"
_PE_LOT_WHERE_PUBLISH = "5773677501825945508"
_PE_SAVED_CHANNEL = "6032609071373226027"
_PE_SAVED_GROUP = "5904248647972820334"
_PE_BACK_BTN = "5258236805890710909"
_PE_MENU_SAVED_CHANNELS = "6028171274939797252"
_PE_MENU_MY_GIVEAWAYS = "6030425896546996257"
_PE_MENU_CREATE_GIVEAWAY = "5805298713211447980"
_PE_PICK_PUBLISH_CONTINUE = "6037622221625626773"
_PE_MY_JOINED = "6041731551845159060"
_PE_MY_OWN = "6021792097454002931"
_PE_OWN_DRAFT = "6034969813032374911"
_PE_OWN_ACTIVE = "5983150113483134607"
_PE_OWN_FINISHED = "5774022692642492953"
_PE_DRAFT_PUBLISH = "6039422865189638057"
_PE_DRAFT_EXTRA = "5904258298764334001"
_PE_DRAFT_INVITE_CHANNELS = "6032653721853234759"
_PE_DRAFT_DELETE = "6039522349517115015"
_PE_REF_TICKET = "5922272602784534896"
_PE_ADMIN_DRAW = "5836907383292436018"
_PE_ADMIN_REROLL = "5850346984501680054"
_PE_ADMIN_EDIT_DESC = "6039779802741739617"
_PE_ADMIN_PARTICIPANTS = "6037397706505195857"
_PE_GW_STEP1 = "5773677501825945508"
_PE_GW_STEP = "5778647930038653243"
_PE_GW_PROMPT = "6039614175917903752"
_PE_GW_PUBLISH_TITLE = "6041731551845159060"
_PE_GW_PUBLISH_DESC = "5774022692642492953"
_PE_POST_WINNERS = "5316979941181496594"
_PE_POST_DEADLINE = "5017179932451668652"
_PE_POST_CTA = "5774022692642492953"
_PE_DM_SUBSCRIBE = "6039422865189638057"
_PE_DM_NEEDS = "6037286673010660132"
_PE_REF_INVITE = "6033108709213736873"
_PE_REF_WEIGHT = "6032949275732742941"
_PE_REF_CONFIRMED = "5891207662678317861"
_PE_REF_NEXT = "6041685260687642937"


def _pe_icon(custom_emoji_id: Optional[str]) -> dict[str, Any]:
    """Как в giveaway.py: иконка кнопки из набора бота."""
    e = (str(custom_emoji_id) if custom_emoji_id is not None else "").strip()
    if not e:
        return {}
    return {"icon_custom_emoji_id": e}


def _ikb_back(callback_data: str) -> InlineKeyboardButton:
    """Кнопка назад: premium emoji + текст."""
    return InlineKeyboardButton(
        text="Назад",
        callback_data=callback_data,
        **_pe_icon(_PE_BACK_BTN),
    )


def _reply_btn_my_giveaways() -> KeyboardButton:
    return KeyboardButton(text="Мои розыгрыши", **_pe_icon(_PE_MENU_MY_GIVEAWAYS))


def _reply_btn_create_giveaway() -> KeyboardButton:
    return KeyboardButton(text="Создать розыгрыш", **_pe_icon(_PE_MENU_CREATE_GIVEAWAY))


def _reply_btn_saved_channels() -> KeyboardButton:
    return KeyboardButton(text="Сохранёные каналы", **_pe_icon(_PE_MENU_SAVED_CHANNELS))


def _ikb_menu_my() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text="Мои розыгрыши",
        callback_data="menu:my",
        **_pe_icon(_PE_MENU_MY_GIVEAWAYS),
    )


def _ikb_menu_create() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text="Создать розыгрыш",
        callback_data="adm_new",
        **_pe_icon(_PE_MENU_CREATE_GIVEAWAY),
    )


def _ikb_menu_saved_channels() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text="Сохранёные каналы",
        callback_data="menu:saved_chats",
        **_pe_icon(_PE_MENU_SAVED_CHANNELS),
    )


def _ikb_menu_own() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text="Созданные тобой",
        callback_data="my:own",
        **_pe_icon(_PE_MY_OWN),
    )


def _tg_pe(emoji_id: str, placeholder: str) -> str:
    """Фрагмент HTML для кастомного (premium) emoji. Внутри — ровно один символ."""
    ph = (placeholder or "·")[:1]
    return f'<tg-emoji emoji-id="{emoji_id}">{ph}</tg-emoji>'


def _lottery_media_prompt_html() -> str:
    """Шаг 1 из 8: вопрос про медиа (Да/Нет) — как раньше, с премиум-эмодзи."""
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 1 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_PROMPT, '✏️')} Добавить к посту <b>фото, GIF или видео</b>?"
    )


def _lottery_media_step1_html() -> str:
    """После «Да»: тот же текст, что и раньше на шаге загрузки файла."""
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 1 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_PROMPT, '✏️')} Отправь одно фото, GIF или видео - оно будет использовано в посте.\n\n"
        f"{_tg_pe(_PE_LOT_DOCWARN, '📎')} Документы и альбомы не поддерживаются."
    )


def _lottery_step2_description_html() -> str:
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 2 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_PROMPT, '✏️')} Введите описание лотереи (на что лотерея)."
    )


def _lottery_step3_tickets_html() -> str:
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 3 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_PROMPT, '✏️')} Выберите количество билетов в лотерее (от 5 до 100)."
    )


def _lottery_step4_winners_html() -> str:
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 4 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_PROMPT, '✏️')} Укажите количество победителей в лотерее (от 1 до 15)."
    )


def _lottery_step5_emoji_html() -> str:
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 5 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_PROMPT, '✏️')} Выберите эмоджи из списка или отправьте в чат свой "
        "(Премиум эмоджи пока поддерживаются только\n"
        f"если добавить нашего бота-помощника @AniGive в администраторы. {_tg_pe(_PE_LOT_PREMIUM_HINT, '⭐')})"
    )


def _lottery_step6_color_html() -> str:
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 6 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_COLOR, '🎨')} Выберите базовый цвет ячеек."
    )


def _lottery_publish_pick_html(has_saved_chats: bool) -> str:
    admin = (
        f"\n\n{_tg_pe(_PE_LOT_DOCWARN, '⚠️')} Убедись, что у тебя есть права администратора, "
        f"а бот добавлен в администраторы выбранного канала или чата."
    )
    if has_saved_chats:
        return (
            f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 7 из 8</b>\n\n"
            f"{_tg_pe(_PE_LOT_CHANEL_PICK, '📣')} Выберите канал(ы), где опубликовать лотерею.\n"
            f"Можно выбрать несколько каналов из списка ниже."
        )
    return (
        f"{_tg_pe(_PE_LOT_STEP, '✨')} <b>Шаг 8 из 8</b>\n\n"
        f"{_tg_pe(_PE_LOT_WHERE_PUBLISH, '📌')} Где опубликовать лотерею?\n\n"
        f"Выбери один или несколько каналов из списка ниже "
        f"или добавь новый через кнопки «Добавить канал» / «Добавить группу»."
    ) + admin


TZ_UTC3 = timezone(timedelta(hours=3))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_stored_ends_at(s: str) -> datetime:
    """Сохранённый ends_at → aware UTC (для сравнения с _utc_now)."""
    raw = (s or "").strip()
    if not raw:
        return datetime.min.replace(tzinfo=timezone.utc)
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_ends_at_user(iso: str) -> str:
    """Отображение времени окончания в UTC+3 (как вводит пользователь)."""
    dt = _parse_stored_ends_at(iso)
    return dt.astimezone(TZ_UTC3).strftime("%d.%m.%Y %H:%M")


def _parse_dt(s: str) -> Optional[datetime]:
    """Разбор даты/времени, введённых как локальное время UTC+3; возвращает UTC для хранения."""
    s = s.strip()
    for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M:%S"):
        try:
            dt_naive = datetime.strptime(s, fmt)
            dt_local = dt_naive.replace(tzinfo=TZ_UTC3)
            return dt_local.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _normalize_channel_token(t: str) -> str:
    t = t.strip()
    if not t:
        return ""
    tl = t.lower()
    if "t.me/+" in tl or "t.me/joinchat/" in tl:
        return t
    if t.startswith("https://t.me/"):
        t = t.split("/")[-1]
    if t.startswith("@"):
        t = t[1:]
    return t


def _is_private_invite_link(text: str) -> bool:
    s = (text or "").strip().lower()
    return ("t.me/+" in s) or ("t.me/joinchat/" in s)


def _forwarded_chat_id(message: Message) -> Optional[int]:
    """Извлекает chat_id из пересланного сообщения (старый и новый форматы Telegram)."""
    if getattr(message, "forward_from_chat", None):
        try:
            return int(message.forward_from_chat.id)
        except Exception:
            pass
    fo = getattr(message, "forward_origin", None)
    ch = getattr(fo, "chat", None) if fo is not None else None
    if ch is not None:
        try:
            return int(ch.id)
        except Exception:
            pass
    return None


def _telegram_numeric_chat_id_candidates(n: int) -> list[int]:
    """Короткий положительный id из клиентов (напр. канал 3727559356) в API — это -1003727559356."""
    if n > 0:
        return [int(f"-100{n}"), n]
    return [n]


async def _get_chat_by_numeric_id(bot: Bot, n: int):
    """get_chat по числу: для n > 0 сначала -100<n>, затем n (обманки группы, личка)."""
    last_exc: Exception | None = None
    for cid in _telegram_numeric_chat_id_candidates(n):
        try:
            return await bot.get_chat(cid)
        except Exception as e:
            last_exc = e
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("get_chat: no candidates")


async def init_db() -> None:
    pool = await _get_pool()
    async with pool.connection() as conn:
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS giveaways (
                id BIGSERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                giveaway_type TEXT NOT NULL DEFAULT 'giveaway',
                winners_count INTEGER NOT NULL,
                ends_at TEXT NOT NULL,
                channels_json TEXT NOT NULL,
                require_username INTEGER NOT NULL DEFAULT 0,
                one_entry_only INTEGER NOT NULL DEFAULT 1,
                referral_enabled INTEGER NOT NULL DEFAULT 0,
                referral_ticket_step INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active',
                post_chat_id BIGINT,
                post_message_id BIGINT,
                results_chat_id BIGINT,
                results_message_id BIGINT,
                created_by BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                last_winners_json TEXT,
                subscribe_only_chat_ids TEXT NOT NULL DEFAULT '[]',
                lottery_ticket_count INTEGER NOT NULL DEFAULT 0,
                lottery_button_text TEXT NOT NULL DEFAULT '🎟',
                lottery_winning_tickets_json TEXT NOT NULL DEFAULT '[]',
                lottery_cell_color TEXT NOT NULL DEFAULT 'blue',
                lottery_button_custom_emoji_id TEXT NOT NULL DEFAULT '',
                mirror_posts_json TEXT,
                mirror_target_chat_ids TEXT,
                mirror_results_json TEXT,
                post_media_kind TEXT,
                post_media_file_id TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS bot_users (
                user_id BIGINT PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                username TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS pending_referral (
                user_id BIGINT PRIMARY KEY,
                giveaway_id BIGINT NOT NULL REFERENCES giveaways(id),
                referrer_id BIGINT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS participants (
                giveaway_id BIGINT NOT NULL REFERENCES giveaways(id),
                user_id BIGINT NOT NULL,
                username TEXT,
                first_name TEXT,
                joined_at TEXT NOT NULL,
                PRIMARY KEY (giveaway_id, user_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS valid_referrals (
                giveaway_id BIGINT NOT NULL REFERENCES giveaways(id),
                referrer_id BIGINT NOT NULL,
                referee_id BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (giveaway_id, referee_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS referral_queue (
                giveaway_id BIGINT NOT NULL REFERENCES giveaways(id),
                referrer_id BIGINT NOT NULL,
                referee_id BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (giveaway_id, referee_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS saved_chats (
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                chat_type TEXT NOT NULL DEFAULT 'channel',
                title TEXT,
                username TEXT,
                added_at TEXT NOT NULL,
                PRIMARY KEY (user_id, chat_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS crosspost_pending (
                id BIGSERIAL PRIMARY KEY,
                creator_id BIGINT NOT NULL,
                snapshot_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                giveaway_id BIGINT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS crosspost_vote (
                pending_id BIGINT NOT NULL REFERENCES crosspost_pending(id),
                channel_id BIGINT NOT NULL,
                admin_id BIGINT NOT NULL,
                vote TEXT NOT NULL,
                PRIMARY KEY (pending_id, channel_id, admin_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS crosspost_notif (
                id BIGSERIAL PRIMARY KEY,
                pending_id BIGINT NOT NULL REFERENCES crosspost_pending(id),
                channel_id BIGINT NOT NULL,
                admin_id BIGINT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS crosspost_creator_notified (
                pending_id BIGINT NOT NULL REFERENCES crosspost_pending(id),
                channel_id BIGINT NOT NULL,
                PRIMARY KEY (pending_id, channel_id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS lottery_picks (
                giveaway_id BIGINT NOT NULL REFERENCES giveaways(id),
                user_id BIGINT NOT NULL,
                ticket_no INTEGER NOT NULL,
                is_winner INTEGER NOT NULL DEFAULT 0,
                won_at TEXT,
                PRIMARY KEY (giveaway_id, user_id),
                UNIQUE (giveaway_id, ticket_no)
            );
            """,
        ]
        for stmt in ddl:
            await conn.execute(stmt.strip())
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, snapshot_json FROM crosspost_pending WHERE giveaway_id IS NULL"
            )
            rows = await cur.fetchall()
            for r in rows:
                try:
                    gj = json.loads(r["snapshot_json"]).get("giveaway_id")
                    if gj is not None:
                        await cur.execute(
                            "UPDATE crosspost_pending SET giveaway_id = %s WHERE id = %s",
                            (int(gj), int(r["id"])),
                        )
                except Exception:
                    pass
        await conn.commit()


def _is_giveaway_owner(g: dict[str, Any], user_id: int) -> bool:
    return int(g.get("created_by", 0)) == int(user_id)


async def get_giveaway(db: PgConnAdapter, gid: int) -> Optional[dict[str, Any]]:
    cur = await db.execute("SELECT * FROM giveaways WHERE id = ?", (gid,))
    row = await cur.fetchone()
    return dict(row) if row else None


async def purge_giveaway_from_db(db: PgConnAdapter, gid: int) -> None:
    await _crosspost_pending_delete_for_giveaway_db(db, gid)
    await db.execute("DELETE FROM lottery_picks WHERE giveaway_id = ?", (gid,))
    await db.execute("DELETE FROM valid_referrals WHERE giveaway_id = ?", (gid,))
    await db.execute("DELETE FROM referral_queue WHERE giveaway_id = ?", (gid,))
    await db.execute("DELETE FROM participants WHERE giveaway_id = ?", (gid,))
    await db.execute("DELETE FROM pending_referral WHERE giveaway_id = ?", (gid,))
    await db.execute("DELETE FROM giveaways WHERE id = ?", (gid,))
    await db.commit()


async def _crosspost_pending_delete_bundle(db: PgConnAdapter, pending_id: int) -> None:
    await db.execute("DELETE FROM crosspost_vote WHERE pending_id = ?", (pending_id,))
    await db.execute("DELETE FROM crosspost_notif WHERE pending_id = ?", (pending_id,))
    await db.execute(
        "DELETE FROM crosspost_creator_notified WHERE pending_id = ?", (pending_id,)
    )
    await db.execute("DELETE FROM crosspost_pending WHERE id = ?", (pending_id,))
    await db.commit()


async def _crosspost_pending_delete_for_giveaway_db(
    db: PgConnAdapter, giveaway_id: int
) -> None:
    """Удаляет все pending по розыгрышу; без commit (вызывающий делает commit)."""
    gid = int(giveaway_id)
    await db.execute(
        "DELETE FROM crosspost_vote WHERE pending_id IN "
        "(SELECT id FROM crosspost_pending WHERE giveaway_id = ?)",
        (gid,),
    )
    await db.execute(
        "DELETE FROM crosspost_notif WHERE pending_id IN "
        "(SELECT id FROM crosspost_pending WHERE giveaway_id = ?)",
        (gid,),
    )
    await db.execute(
        "DELETE FROM crosspost_creator_notified WHERE pending_id IN "
        "(SELECT id FROM crosspost_pending WHERE giveaway_id = ?)",
        (gid,),
    )
    await db.execute("DELETE FROM crosspost_pending WHERE giveaway_id = ?", (gid,))


async def _crosspost_approved_extra_chat_ids(
    db: PgConnAdapter, pending_id: int, requested: list[int]
) -> list[int]:
    approved: list[int] = []
    for ch_id in requested:
        cur = await db.execute(
            "SELECT vote FROM crosspost_vote WHERE pending_id = ? AND channel_id = ?",
            (pending_id, ch_id),
        )
        votes = [row["vote"] for row in await cur.fetchall()]
        if any(v == "no" for v in votes):
            continue
        if any(v == "yes" for v in votes):
            approved.append(ch_id)
    return approved


async def _chat_title_html(bot: Bot, chat_id: int) -> str:
    try:
        ch = await bot.get_chat(chat_id)
        return html.escape(ch.title or (f"@{ch.username}" if ch.username else "") or str(chat_id))
    except Exception:
        return html.escape(str(chat_id))


async def _crosspost_vote_kind(
    db: PgConnAdapter, pending_id: int, channel_id: int
) -> str:
    """no — отказ; yes — согласие; pending — ответа ещё не было."""
    cur = await db.execute(
        "SELECT vote FROM crosspost_vote WHERE pending_id = ? AND channel_id = ?",
        (pending_id, channel_id),
    )
    votes = [row["vote"] for row in await cur.fetchall()]
    if any(v == "no" for v in votes):
        return "no"
    if any(v == "yes" for v in votes):
        return "yes"
    return "pending"


async def _crosspost_pending_vote_groups(
    db: PgConnAdapter, pending_id: int, extras: list[int]
) -> tuple[bool, list[int], list[int]]:
    """Все согласились; ждём ответа; отказались."""
    if not extras:
        return False, [], []
    kinds = [(cid, await _crosspost_vote_kind(db, pending_id, cid)) for cid in extras]
    pending_cids = [cid for cid, k in kinds if k == "pending"]
    refused_cids = [cid for cid, k in kinds if k == "no"]
    all_yes = all(k == "yes" for _, k in kinds)
    return all_yes, pending_cids, refused_cids


def _callback_answer_alert_text(text: str, max_len: int = 200) -> str:
    """Лимит Telegram на текст answerCallbackQuery."""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


async def _append_crosspost_alert_vote_lists(
    bot: Bot,
    parts: list[str],
    pending_cids: list[int],
    refused_cids: list[int],
) -> None:
    if pending_cids:
        parts.append("Ожидаем ответа от администраторов каналов:")
        for cid in pending_cids:
            parts.append(f"• {html.unescape(await _chat_title_html(bot, cid))}")
    if refused_cids:
        parts.append("Отказались:")
        for cid in refused_cids:
            parts.append(f"• {html.unescape(await _chat_title_html(bot, cid))}")


async def _fetch_pending_row_for_giveaway(gid: int) -> Optional[dict[str, Any]]:
    async with _pg_conn() as db:
        cur = await db.execute(
            "SELECT id, snapshot_json FROM crosspost_pending WHERE giveaway_id = ? ORDER BY id DESC LIMIT 1",
            (gid,),
        )
        return await cur.fetchone()


def _mirror_subscribe_from_g(g: dict[str, Any]) -> tuple[list[int], list[int]]:
    try:
        mir = [int(x) for x in json.loads(g.get("mirror_target_chat_ids") or "[]")]
        so = [int(x) for x in json.loads(g.get("subscribe_only_chat_ids") or "[]")]
    except Exception:
        return [], []
    return mir, so


def _ordered_extra_chat_ids(g: dict[str, Any]) -> list[int]:
    mir, so = _mirror_subscribe_from_g(g)
    so_only = [c for c in so if c not in mir]
    return mir + so_only


def _publish_chat_ids_from_g(g: dict[str, Any]) -> list[int]:
    """Список чатов, куда публикуем пост: post_chat_id + все mirror_target_chat_ids."""
    out: list[int] = []
    pc = g.get("post_chat_id")
    if pc is not None:
        out.append(int(pc))
    mir, _so = _mirror_subscribe_from_g(g)
    for cid in mir:
        c = int(cid)
        if c not in out:
            out.append(c)
    return out


_MGMT_SUB_DONE = "\u2705"  # ✅
_MGMT_SUB_BTN_LABEL = "подписка"
_MGMT_DEL_BTN = "\U0001f5d1\ufe0f"  # 🗑️ только эмодзи — узкая колонка, больше места для «подписка»
_MGMT_NAME_BTN_MAXLEN = 9  # в ряду 3 кнопки: короче имя — шире середина (API ширину не задаёт)


def _mgmt_sub_only_btn_text(*, active: bool) -> str:
    if active:
        return f"{_MGMT_SUB_BTN_LABEL} {_MGMT_SUB_DONE}"
    return _MGMT_SUB_BTN_LABEL


async def _chat_button_label(bot: Bot, cid: int, maxlen: int = 14) -> str:
    t = await _chat_title_html(bot, cid)
    plain = t.replace("<b>", "").replace("</b>", "")
    if len(plain) > maxlen:
        return plain[: maxlen - 1] + "…"
    return plain


async def _persist_mirror_subscribe(
    bot: Bot,
    g: dict[str, Any],
    mir: list[int],
    so: list[int],
    *,
    post_chat_id: Optional[int] = None,
) -> dict[str, Any]:
    pc = int(post_chat_id if post_chat_id is not None else g["post_chat_id"])
    gid = int(g["id"])
    extras = list(dict.fromkeys(mir + so))
    all_ids = [pc] + extras
    tokens = await _subscription_tokens_for_chat_ids(bot, all_ids)
    async with _pg_conn() as db:
        await db.execute(
            """UPDATE giveaways SET post_chat_id = ?, channels_json = ?, mirror_target_chat_ids = ?,
               subscribe_only_chat_ids = ? WHERE id = ?""",
            (
                pc,
                json.dumps(tokens, ensure_ascii=False),
                json.dumps(mir, ensure_ascii=False),
                json.dumps(so, ensure_ascii=False),
                gid,
            ),
        )
        await db.commit()
        return await get_giveaway(db, gid)


async def _invite_channels_full_kb(
    bot: Bot, g: dict[str, Any], *, pending_id: Optional[int]
) -> InlineKeyboardMarkup:
    gid = int(g["id"])
    ordered = _ordered_extra_chat_ids(g)
    rows: list[list[InlineKeyboardButton]] = []
    if pending_id is not None:
        rows.append(
            [
                InlineKeyboardButton(
                    text="▶️ Проверить согласия",
                    callback_data=f"xcont:{pending_id}",
                )
            ]
        )
    if ordered or g.get("post_chat_id") is not None:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⚙️ Управление каналами",
                    callback_data=f"dchmgmt:{gid}",
                )
            ]
        )
    # «Отменить приглашения» — только пока кто-то ещё не принял; если все согласились, отмена не нужна.
    show_cancel_pending = False
    if pending_id is not None:
        async with _pg_conn() as db:
            cur = await db.execute(
                "SELECT snapshot_json FROM crosspost_pending WHERE id = ?", (pending_id,)
            )
            prow = await cur.fetchone()
        if prow:
            snap = json.loads(prow["snapshot_json"])
            extras = [int(x) for x in snap.get("extra_chat_ids", [])]
            async with _pg_conn() as db:
                if extras:
                    all_yes, _, _ = await _crosspost_pending_vote_groups(db, pending_id, extras)
                    show_cancel_pending = not all_yes
                else:
                    show_cancel_pending = True
    rows.extend(
        _draft_channels_pick_kb(gid, show_cancel_pending=show_cancel_pending).inline_keyboard
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _invite_channels_screen_body(bot: Bot, g: dict[str, Any]) -> str:
    gid = int(g["id"])
    lines: list[str] = [
        f"{_tg_pe(_PE_DRAFT_INVITE_CHANNELS, '📎')} <b>Пригласить каналы</b>\n\n",
        "Ниже — статус приглашений и кто уже добавлен в розыгрыш.\n"
        "Приглашения админам действуют, пока черновик <b>не опубликован</b> — можно вернуться и нажать "
        "«Проверить согласия», когда придут ответы.\n",
    ]
    prow = await _fetch_pending_row_for_giveaway(gid)
    has_pending = prow is not None
    if prow:
        snap = json.loads(prow["snapshot_json"])
        pid = int(prow["id"])
        extras = [int(x) for x in snap.get("extra_chat_ids", [])]
        lines.append("\n<b>В процессе приглашения</b>")
        async with _pg_conn() as db:
            all_accepted, pending_cids, refused_cids = await _crosspost_pending_vote_groups(
                db, pid, extras
            )

        if all_accepted:
            lines.append("\nВсе каналы/группы приняли приглашение.")
        else:
            if pending_cids:
                lines.append("\nОжидаем ответа от администраторов каналов:")
                for cid in pending_cids:
                    t = await _chat_title_html(bot, cid)
                    lines.append(f"\n• <b>{t}</b>")
            if refused_cids:
                lines.append("\nОтказались:")
                for cid in refused_cids:
                    t = await _chat_title_html(bot, cid)
                    lines.append(f"\n• <b>{t}</b>")
    mir, so = _mirror_subscribe_from_g(g)
    mir_set = set(mir)
    pc = g.get("post_chat_id")
    main_cid = int(pc) if pc is not None else None
    ordered = _ordered_extra_chat_ids(g)
    all_rows = ([main_cid] if main_cid is not None else []) + [c for c in ordered if c != main_cid]
    if all_rows:
        lines.append("\n<b>В условиях розыгрыша</b>:")
        for cid in all_rows:
            t = await _chat_title_html(bot, cid)
            if main_cid is not None and cid == main_cid:
                lines.append(
                    f"\n• <b>{t}</b>\n"
                    "📣 Пост + обязательная подписка\n"
                )
            elif cid in mir_set:
                lines.append(
                    f"\n• <b>{t}</b>\n"
                    "📣 Пост + обязательная подписка\n"
                )
            else:
                lines.append(
                    f"\n• <b>{t}</b>\n"
                    "📎 Подписка (без поста) — в управлении кнопка <code>подписка ✅</code>\n"
                )
        lines.append("\n<i>Режим публикации и удаление — «⚙️ Управление каналами».</i>")
    if not has_pending and not all_rows:
        lines.append(
            "\n\n<i>Каналов пока нет — нажми «Добавить / пригласить каналы».</i>"
        )
    return "".join(lines)


async def _invite_channels_mgmt_body(bot: Bot, g: dict[str, Any]) -> str:
    """Текст без режима по каждому чату — высота сообщения не меняется при переключении кнопок."""
    ordered = _ordered_extra_chat_ids(g)
    pc = g.get("post_chat_id")
    main_cid = int(pc) if pc is not None else None
    all_rows = ([main_cid] if main_cid is not None else []) + [c for c in ordered if c != main_cid]
    lines: list[str] = [
        "⚙️ <b>Управление каналами</b>\n\n"
        "У каждого чата <b>один ряд</b>: название · средняя кнопка <code>подписка</code>: без галочки — пост + дубль, "
        "с <code>✅</code> — подписка без поста · справа <code>🗑️</code> — убрать канал из условий.\n",
    ]
    if all_rows:
        lines.append("\n<b>Подключённые чаты</b>\n")
        for cid in all_rows:
            li = await _chat_title_html(bot, cid)
            lines.append(f"• {li}\n")
    else:
        lines.append("\n<i>Пока нет приглашённых каналов в условиях.</i>")
    return "".join(lines)


async def _invite_channels_mgmt_kb(bot: Bot, g: dict[str, Any]) -> InlineKeyboardMarkup:
    gid = int(g["id"])
    mir, so = _mirror_subscribe_from_g(g)
    ordered = _ordered_extra_chat_ids(g)
    pc = g.get("post_chat_id")
    main_cid = int(pc) if pc is not None else None
    all_rows = ([main_cid] if main_cid is not None else []) + [c for c in ordered if c != main_cid]
    rows: list[list[InlineKeyboardButton]] = []
    for cid in all_rows:
        name_btn = await _chat_button_label(bot, cid, _MGMT_NAME_BTN_MAXLEN)
        in_sub_only = cid in so and cid not in mir
        sub_btn = _mgmt_sub_only_btn_text(active=in_sub_only)
        rows.append(
            [
                InlineKeyboardButton(text=name_btn, callback_data=f"dchcap:{gid}:{cid}"),
                InlineKeyboardButton(text=sub_btn, callback_data=f"dchtog:{gid}:{cid}"),
                InlineKeyboardButton(text=_MGMT_DEL_BTN, callback_data=f"dchrm:{gid}:{cid}"),
            ]
        )
    rows.append(
        [_ikb_back(f"dchinv:{gid}")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def list_active_giveaways(db: PgConnAdapter) -> list[dict[str, Any]]:
    cur = await db.execute(
        "SELECT * FROM giveaways WHERE status = 'active' ORDER BY id DESC"
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def list_my_giveaways(db: PgConnAdapter, user_id: int) -> list[dict[str, Any]]:
    cur = await db.execute(
        "SELECT * FROM giveaways WHERE created_by = ? ORDER BY id DESC LIMIT 40",
        (user_id,),
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def list_joined_giveaways(db: PgConnAdapter, user_id: int) -> list[dict[str, Any]]:
    """Розыгрыши, где пользователь участвует (включая завершённые)."""
    cur = await db.execute(
        """SELECT g.*
           FROM giveaways g
           JOIN participants p ON p.giveaway_id = g.id
           WHERE p.user_id = ? AND g.status = 'active'
           ORDER BY g.id DESC LIMIT 80""",
        (user_id,),
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def list_saved_chats(db: PgConnAdapter, user_id: int) -> list[dict[str, Any]]:
    cur = await db.execute(
        "SELECT * FROM saved_chats WHERE user_id = ? ORDER BY "
        "LOWER(COALESCE(title, '')), chat_id",
        (user_id,),
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def upsert_saved_chat(
    db: PgConnAdapter,
    user_id: int,
    chat_id: int,
    chat_type: str,
    title: Optional[str],
    username: Optional[str],
) -> None:
    await db.execute(
        """INSERT INTO saved_chats (user_id, chat_id, chat_type, title, username, added_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(user_id, chat_id) DO UPDATE SET
             chat_type = EXCLUDED.chat_type,
             title = EXCLUDED.title,
             username = EXCLUDED.username,
             added_at = EXCLUDED.added_at""",
        (
            user_id,
            chat_id,
            chat_type,
            title,
            username,
            _utc_now().isoformat(),
        ),
    )
    await db.commit()


async def delete_saved_chat(db: PgConnAdapter, user_id: int, chat_id: int) -> None:
    await db.execute("DELETE FROM saved_chats WHERE user_id = ? AND chat_id = ?", (user_id, chat_id))
    await db.commit()


# promote_members у бота нужен, чтобы после добавления бот мог назначить userbot админом (promoteChatMember).
_DL_ADMIN_CH = (
    "post_messages+edit_messages+delete_messages+invite_users+pin_messages+promote_members"
)
_DL_ADMIN_GR = (
    "manage_chat+delete_messages+invite_users+pin_messages+restrict_members+promote_members"
)


def _normalize_bot_uname(bot_username: str) -> str:
    return (bot_username or "").strip().lstrip("@")


def _invite_channel_kb(bot_username: str) -> InlineKeyboardMarkup:
    """Одна кнопка — как 🚀 «Добавить в канал»."""
    u = _normalize_bot_uname(bot_username)
    if not u:
        return InlineKeyboardMarkup(inline_keyboard=[])
    base = f"https://t.me/{u}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Добавить в канал",
                    url=f"{base}?startchannel=add&admin={_DL_ADMIN_CH}",
                )
            ],
        ]
    )


def _invite_group_kb(bot_username: str) -> InlineKeyboardMarkup:
    """Одна кнопка — как 🚀 «Добавить в группу»."""
    u = _normalize_bot_uname(bot_username)
    if not u:
        return InlineKeyboardMarkup(inline_keyboard=[])
    base = f"https://t.me/{u}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Добавить в группу",
                    url=f"{base}?startgroup=add&admin={_DL_ADMIN_GR}",
                )
            ],
        ]
    )


def _invite_deep_link_kb(bot_username: str) -> InlineKeyboardMarkup:
    """Обе ссылки 🚀 (если нужны на одном экране)."""
    u = _normalize_bot_uname(bot_username)
    if not u:
        return InlineKeyboardMarkup(inline_keyboard=[])
    base = f"https://t.me/{u}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Добавить в канал",
                    url=f"{base}?startchannel=add&admin={_DL_ADMIN_CH}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🚀 Добавить в группу",
                    url=f"{base}?startgroup=add&admin={_DL_ADMIN_GR}",
                )
            ],
        ]
    )


_ADD_CHANNEL_INVITE_HTML = (
    "<b>Добавляем канал.</b> Нажми кнопку ниже — Telegram добавит бота в канал с нужными правами."
)
_ADD_GROUP_INVITE_HTML = (
    "<b>Добавляем группу.</b> Нажми кнопку ниже — Telegram добавит бота в группу с нужными правами."
)
_NO_BOT_USERNAME_HTML = (
    "⚠️ У бота нет имени <code>@username</code> в Telegram — добавление по ссылке недоступно. "
    "Обратись к администратору бота."
)


async def _send_add_channel_invite(bot: Bot, user_id: int) -> None:
    me = await bot.get_me()
    if not me.username:
        await bot.send_message(user_id, _NO_BOT_USERNAME_HTML, parse_mode="HTML")
        return
    await bot.send_message(
        user_id,
        _ADD_CHANNEL_INVITE_HTML,
        reply_markup=_invite_channel_kb(me.username),
        parse_mode="HTML",
    )


async def _send_add_group_invite(bot: Bot, user_id: int) -> None:
    me = await bot.get_me()
    if not me.username:
        await bot.send_message(user_id, _NO_BOT_USERNAME_HTML, parse_mode="HTML")
        return
    await bot.send_message(
        user_id,
        _ADD_GROUP_INVITE_HTML,
        reply_markup=_invite_group_kb(me.username),
        parse_mode="HTML",
    )


async def _saved_chats_list_body_and_keyboard(
    db: PgConnAdapter,
    user_id: int,
) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    items = await list_saved_chats(db, user_id)
    lines = (
        "\n".join(f"• {_saved_chat_line_html(s)}" for s in items)
        if items
        else "Пока нет сохранённых каналов и групп."
    )
    body = f"{_tg_pe(_PE_MENU_SAVED_CHANNELS, '📌')} <b>Сохранёные каналы</b>\n\n{lines}"
    if not items:
        return body, None
    kb_rows: list[list[InlineKeyboardButton]] = []
    for s in items:
        suffix = " 🗑"
        title = _saved_chat_title_for_button(s, max_len=max(1, 64 - len(suffix)))
        text = title + suffix
        kb_rows.append(
            [
                InlineKeyboardButton(
                    text=text,
                    callback_data=f"delsc:{int(s['chat_id'])}",
                    **_pe_icon(_saved_chat_pick_icon_id(s)),
                )
            ]
        )
    return body, InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def _refresh_saved_chats_panel_message(
    bot: Bot,
    state: FSMContext,
    user_id: int,
    chat_id: int,
    message_id: int,
) -> None:
    """Перерисовать сообщение со списком сохранённых чатов; при ошибке edit — новое сообщение."""
    async with _pg_conn() as db:
        body, kb = await _saved_chats_list_body_and_keyboard(db, user_id)
    ok = await _edit_screen(bot, chat_id, message_id, body, kb, parse_mode="HTML")
    if ok:
        return
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass
    sent = await bot.send_message(user_id, body, reply_markup=kb, parse_mode="HTML")
    await state.update_data(
        saved_chats_msg_chat_id=sent.chat.id,
        saved_chats_msg_message_id=sent.message_id,
    )


async def _refresh_publish_step_message_if_open(
    bot: Bot, state: FSMContext, user_id: int
) -> None:
    """Если у пользователя открыт шаг 5 мастера, перерисовать список каналов."""
    if await state.get_state() != CreateGiveaway.publish_chat.state:
        return
    data = await state.get_data()
    ui_cid = data.get("ui_chat_id")
    ui_mid = data.get("ui_message_id")
    if ui_cid is None or ui_mid is None:
        return
    selected = [int(x) for x in (data.get("publish_chat_ids") or [])]
    async with _pg_conn() as db:
        items = await list_saved_chats(db, user_id)
        kb_inner = await _build_pick_publish_markup(bot, db, user_id, selected)
    is_lot = (data.get("giveaway_type") or "") == "lottery"
    screen = _publish_pick_screen_html(is_lot, bool(items))
    await _edit_screen(
        bot,
        int(ui_cid),
        int(ui_mid),
        screen,
        _merge_pick_publish_under_nav(kb_inner),
        parse_mode="HTML",
    )


def _saved_chat_line_html(row: dict[str, Any]) -> str:
    """Строка списка в HTML с premium emoji (для текста сообщений, не для inline-кнопок)."""
    t = (row.get("title") or "").strip()
    un = (row.get("username") or "").strip()
    kind = (row.get("chat_type") or "channel").lower()
    pe = (
        _tg_pe(_PE_SAVED_CHANNEL, "📢")
        if kind == "channel"
        else _tg_pe(_PE_SAVED_GROUP, "👥")
    )
    if t:
        return f"{pe} {html.escape(t, quote=False)}"
    if un:
        u = html.escape(un.lstrip("@"), quote=False)
        return f"{pe} @{u}"
    return f"{pe} <code>{int(row['chat_id'])}</code>"


def _saved_chat_title_for_button(row: dict[str, Any], *, max_len: int = 64) -> str:
    """Подпись чата без префикса: при публикации иконка задаётся через icon_custom_emoji_id (как в лотерее)."""
    t = (row.get("title") or "").strip()
    un = (row.get("username") or "").strip()
    if t:
        label = t
    elif un:
        label = f"@{un.lstrip('@')}"
    else:
        label = str(int(row["chat_id"]))
    if len(label) > max_len:
        return label[: max_len - 1] + "…"
    return label


def _saved_chat_pick_icon_id(row: dict[str, Any]) -> str:
    kind = (row.get("chat_type") or "channel").lower()
    return _PE_SAVED_CHANNEL if kind == "channel" else _PE_SAVED_GROUP


def _saved_chat_label(row: dict[str, Any]) -> str:
    """Короткая подпись с обычными emoji (удаление из списка, прочее без icon_custom_emoji_id)."""
    t = (row.get("title") or "").strip()
    un = (row.get("username") or "").strip()
    kind = (row.get("chat_type") or "channel").lower()
    icon = "📢" if kind == "channel" else "👥"
    if t:
        label = f"{icon} {t}"
        return label if len(label) <= 64 else label[:61] + "…"
    if un:
        u = un.lstrip("@")
        label = f"{icon} @{u}"
        return label if len(label) <= 64 else label[:61] + "…"
    return f"{icon} {row['chat_id']}"


async def _publish_pick_invite_rows(bot: Bot) -> list[list[InlineKeyboardButton]]:
    """Шаг 5: добавить бота в канал/группу (как в главном меню)."""
    me = await bot.get_me()
    u = _normalize_bot_uname(me.username or "")
    if u:
        base = f"https://t.me/{u}"
        return [
            [
                InlineKeyboardButton(
                    text="Добавить канал",
                    url=f"{base}?startchannel=add&admin={_DL_ADMIN_CH}",
                    **_pe_icon(_PE_SAVED_CHANNEL),
                ),
                InlineKeyboardButton(
                    text="Добавить группу",
                    url=f"{base}?startgroup=add&admin={_DL_ADMIN_GR}",
                    **_pe_icon(_PE_SAVED_GROUP),
                ),
            ]
        ]
    return [
        [
            InlineKeyboardButton(
                text="Добавить канал",
                callback_data="pubsel:invite_ch",
                **_pe_icon(_PE_SAVED_CHANNEL),
            ),
            InlineKeyboardButton(
                text="Добавить группу",
                callback_data="pubsel:invite_gr",
                **_pe_icon(_PE_SAVED_GROUP),
            ),
        ]
    ]


async def _build_pick_publish_markup(
    bot: Bot,
    db: PgConnAdapter,
    user_id: int,
    selected_chat_ids: Optional[list[int]] = None,
) -> InlineKeyboardMarkup:
    """Клавиатура шага 5: сохранённые чаты + добавить канал/группу (без «Указать другой чат»)."""
    selected = [int(x) for x in (selected_chat_ids or [])]
    items = await list_saved_chats(db, user_id)
    rows: list[list[InlineKeyboardButton]] = []
    for s in items:
        cid = int(s["chat_id"])
        is_on = cid in selected
        suffix = " ✅" if is_on else ""
        text = _saved_chat_title_for_button(s, max_len=max(1, 64 - len(suffix))) + suffix
        rows.append(
            [
                InlineKeyboardButton(
                    text=text,
                    callback_data=f"pubsel:{cid}",
                    **_pe_icon(_saved_chat_pick_icon_id(s)),
                )
            ]
        )
    rows.extend(await _publish_pick_invite_rows(bot))
    if selected:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Продолжить",
                    callback_data="pubsel:done",
                    **_pe_icon(_PE_PICK_PUBLISH_CONTINUE),
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _channels_list(channels_json: str) -> list[str]:
    try:
        data = json.loads(channels_json)
        return [str(x) for x in data if str(x).strip()]
    except Exception:
        return []


def _channel_subscription_line_html(token: str) -> str:
    """Одна строка списка «подпишись» для поста/лички.

    Без URL в разметке: для подписей к медиа Telegram API не отключает предпросмотр,
    а ссылки t.me дают большую карточку канала даже при disable_web_page_preview у текста.
    """
    t = (token or "").strip()
    if not t:
        return ""
    if t.startswith("@"):
        t = t[1:]
    if t.lstrip("-").isdigit():
        try:
            n = int(t)
        except ValueError:
            return f"• <code>{html.escape(token)}</code>"
        s = str(n)
        if s.startswith("-100") and len(s) > 4:
            inner = s[4:]
            return f"• канал / группа <code>{html.escape(inner)}</code>"
        if n > 0:
            return f"• канал / группа <code>{html.escape(str(n))}</code>"
        return f"• <code>{html.escape(t)}</code>"
    uname = t
    return f"• @{html.escape(uname)}"


async def _channel_subscription_line_clickable_html(bot: Bot, token: str) -> str:
    """Кликабельная строка подписки для личной карточки участника с реальным именем/ссылкой."""
    t = (token or "").strip()
    if not t:
        return ""
    ch_emoji = _tg_pe(_PE_SAVED_CHANNEL, "📢")
    gr_emoji = _tg_pe(_PE_SAVED_GROUP, "👥")
    tl = t.lower()
    # invite links (private groups/channels): keep full clickable URL
    if "t.me/+" in tl or "t.me/joinchat/" in tl:
        url = t if t.startswith("http") else ("https://" + t.lstrip("/"))
        code = ""
        if "t.me/+" in url.lower():
            code = url.split("t.me/+", 1)[1].split("?", 1)[0].strip("/")
        elif "t.me/joinchat/" in url.lower():
            code = url.split("t.me/joinchat/", 1)[1].split("?", 1)[0].strip("/")
        deep = f"tg://join?invite={code}" if code else url
        safe = html.escape(deep, quote=True)
        label = html.escape(t, quote=False)
        # Тип для invite-ссылки заранее не определить, используем нейтрально как "канал/группа".
        return f'• {ch_emoji} <a href="{safe}">{label}</a>'
    if t.startswith("@"):
        uname = t[1:].strip()
        if not uname:
            return ""
        kind_emoji = ch_emoji
        try:
            ch = await bot.get_chat(f"@{uname}")
            if getattr(ch, "type", "") in ("group", "supergroup"):
                kind_emoji = gr_emoji
        except Exception:
            pass
        safe_un = html.escape(f"tg://resolve?domain={uname}", quote=True)
        label = html.escape("@" + uname, quote=False)
        return f'• {kind_emoji} <a href="{safe_un}">{label}</a>'
    if t.lstrip("-").isdigit():
        # Для числовых chat_id пробуем получить публичную ссылку/инвайт.
        try:
            n = int(t)
        except ValueError:
            return f"• <code>{html.escape(t, quote=False)}</code>"
        chat = None
        kind_emoji = ch_emoji
        try:
            chat = await _get_chat_by_numeric_id(bot, n)
        except Exception:
            chat = None
        if chat is not None:
            if getattr(chat, "type", "") in ("group", "supergroup"):
                kind_emoji = gr_emoji
            title = html.escape((chat.title or f"chat {t}"), quote=False)
            uname = (getattr(chat, "username", None) or "").strip()
            if uname:
                safe_un = html.escape(f"tg://resolve?domain={uname}", quote=True)
                return f'• {kind_emoji} <a href="{safe_un}">{title}</a>'
            try:
                inv = await bot.export_chat_invite_link(chat.id)
                if inv:
                    code = ""
                    if "t.me/+" in inv.lower():
                        code = inv.split("t.me/+", 1)[1].split("?", 1)[0].strip("/")
                    elif "t.me/joinchat/" in inv.lower():
                        code = inv.split("t.me/joinchat/", 1)[1].split("?", 1)[0].strip("/")
                    deep_inv = f"tg://join?invite={code}" if code else inv
                    safe_inv = html.escape(deep_inv, quote=True)
                    return f'• {kind_emoji} <a href="{safe_inv}">{title}</a>'
            except Exception:
                pass
        return f"• {kind_emoji} <code>{html.escape(t, quote=False)}</code>"
    safe_t = html.escape(t, quote=True)
    if t.startswith("http://") or t.startswith("https://"):
        label = html.escape(t, quote=False)
        return f'• {ch_emoji} <a href="{safe_t}">{label}</a>'
    # plain username token without '@' (common for channels in storage)
    if re.match(r"^[A-Za-z][A-Za-z0-9_]{3,}$", t):
        kind_emoji = ch_emoji
        try:
            ch = await bot.get_chat(f"@{t}")
            if getattr(ch, "type", "") in ("group", "supergroup"):
                kind_emoji = gr_emoji
        except Exception:
            pass
        deep = html.escape(f"tg://resolve?domain={t}", quote=True)
        label = html.escape("@" + t, quote=False)
        return f'• {kind_emoji} <a href="{deep}">{label}</a>'
    return f"• {ch_emoji} <code>{html.escape(t, quote=False)}</code>"


async def _missing_channels_links_inline_html(bot: Bot, missing: list[str]) -> str:
    parts: list[str] = []
    for m in missing:
        line = await _channel_subscription_line_clickable_html(bot, m)
        if line.startswith("• "):
            line = line[2:]
        if line:
            parts.append(line)
    return ", ".join(parts)


def _giveaway_conditions_channels_html(g: dict[str, Any]) -> str:
    """Блок HTML: на какие чаты нужна подписка (пусто, если список пуст)."""
    channels = _channels_list(g.get("channels_json") or "[]")
    if not channels:
        return ""
    lines = ["📣 <b>Подпишись:</b>"]
    for tok in channels:
        line = _channel_subscription_line_html(tok)
        if line:
            lines.append(line)
    return "\n".join(lines) + "\n\n"


def _mirror_posts_list(g: dict[str, Any]) -> list[dict[str, Any]]:
    raw = g.get("mirror_posts_json")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _mirror_results_list(g: dict[str, Any]) -> list[dict[str, Any]]:
    raw = g.get("mirror_results_json")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


async def _broadcast_results_to_mirrors(bot: Bot, g: dict[str, Any], text: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ent in _mirror_posts_list(g):
        try:
            cid = int(ent["chat_id"])
            sent = await bot.send_message(cid, text)
            out.append({"chat_id": sent.chat.id, "message_id": sent.message_id})
        except Exception as e:
            log.warning("mirror results to %s: %s", ent, e)
    return out


def _parse_multi_channel_tokens(text: str) -> list[str]:
    parts = re.split(r"[\n,;]+", text or "")
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        t = _normalize_channel_token(p)
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


async def _subscription_tokens_for_chat_ids(bot: Bot, chat_ids: list[int]) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for cid in chat_ids:
        try:
            ch = await bot.get_chat(cid)
            tok = ch.username if ch.username else str(ch.id)
            if tok not in seen:
                seen.add(tok)
                tokens.append(tok)
        except Exception:
            tok = str(cid)
            if tok not in seen:
                seen.add(tok)
                tokens.append(tok)
    return tokens


async def check_user_subscribed(bot: Bot, user_id: int, channels: list[str]) -> tuple[bool, list[str]]:
    """Проверка подписки: каналы, супергруппы — по @username или числовому id."""
    missing: list[str] = []
    for ch in channels:
        ch = ch.strip()
        if not ch:
            continue
        try:
            if ch.lstrip("-").isdigit():
                n = int(ch)
                member = None
                for cid in _telegram_numeric_chat_id_candidates(n):
                    try:
                        member = await bot.get_chat_member(cid, user_id)
                        break
                    except Exception:
                        continue
                if member is None:
                    missing.append(ch)
                    continue
            else:
                member = await bot.get_chat_member(f"@{ch}", user_id)
            if member.status in ("left", "kicked"):
                missing.append(ch)
        except Exception as e:
            log.warning("get_chat_member %s %s: %s", ch, user_id, e)
            missing.append(ch)
    return (len(missing) == 0, missing)


async def _verify_bot_can_post(bot: Bot, chat_id: int) -> str:
    """Пустая строка = ок, иначе текст ошибки для пользователя."""
    try:
        me = await bot.get_me()
        chat = await bot.get_chat(chat_id)
        m = await bot.get_chat_member(chat_id, me.id)
    except TelegramBadRequest:
        return "С этим чатом что-то не так — добавь бота в канал или группу, чтобы он его видел."
    st = m.status
    if st == "left":
        return "Бота ещё нет в этом чате. Добавь его: в канале дай право постить, в группе — чтобы мог писать."
    if st == "administrator":
        # can_post_messages относится только к каналам; в супергруппах часто приходит False — не путать с запретом.
        if chat.type == "channel":
            if getattr(m, "can_post_messages", None) is False:
                return "В канале включи боту право «публиковать сообщения»."
        else:
            csm = getattr(m, "can_send_messages", None)
            if csm is False:
                return (
                    "В группе включи боту право отправлять сообщения "
                    "(в настройках администратора бота)."
                )
        return ""
    if st in ("member", "creator", "owner"):
        return ""
    if st == "restricted":
        if getattr(m, "can_send_messages", True):
            return ""
        return "Боту запрещено писать в этом чате — сними ограничения."
    return ""


async def _resolve_publish_chat(bot: Bot, message: Message) -> tuple[Optional[int], str]:
    """Только разбор id чата (без проверки прав пользователя)."""
    fcid = _forwarded_chat_id(message)
    if fcid is not None:
        return fcid, ""

    raw = (message.text or "").strip()
    if not raw:
        return (
            None,
            "Перешли <b>любое сообщение</b> из нужного канала или группы "
            "или напиши @username, ссылку <code>t.me/…</code>, числовой id канала "
            "(например <code>3727559356</code>) или полный id <code>-100…</code>.",
        )

    ident: str | int
    if raw.lstrip("-").isdigit():
        ident = int(raw)
    else:
        if _is_private_invite_link(raw):
            return (
                None,
                "Ссылки вида <code>t.me/+...</code> Telegram Bot API не резолвит в id чата. "
                "Добавь группу кнопкой «Добавить группу» (или перешли сообщение из чата), "
                "после этого чат можно выбрать из списка.",
            )
        token = _normalize_channel_token(raw)
        if not token:
            return None, "Не понял, о каком чате речь. Перешли сообщение из канала/группы или напиши @username."
        ident = (
            f"@{token}"
            if not token.lstrip("-").isdigit()
            else int(token)
        )

    try:
        if isinstance(ident, int):
            chat = await _get_chat_by_numeric_id(bot, ident)
        else:
            chat = await bot.get_chat(ident)
        cid = chat.id
    except Exception:
        return None, "Чат не нашёлся. Проверь @username / id или перешли сообщение оттуда."

    return cid, ""


async def _verify_user_is_chat_admin(bot: Bot, chat_id: int, user_id: int) -> str:
    """Пустая строка, если пользователь — админ/владелец чата."""
    try:
        m = await bot.get_chat_member(chat_id, user_id)
    except TelegramBadRequest:
        return (
            "Не смог проверить, что ты админ. Зайди в этот чат и попробуй снова — нужны права администратора."
        )
    st = m.status
    if st in ("creator", "administrator", "owner"):
        return ""
    return "Пост с розыгрышем может выложить только <b>админ</b> этого канала или группы."


async def _validate_publish_destination(bot: Bot, chat_id: int, user_id: int) -> str:
    """Тип чата, права бота на постинг, права пользователя как админа. Пусто = ок."""
    try:
        chat = await bot.get_chat(chat_id)
    except TelegramBadRequest:
        return "Не достучаться до чата — проверь id и что бот там есть."
    if chat.type not in ("channel", "group", "supergroup"):
        return "Пост можно публиковать только в <b>канал</b> или <b>группу</b>, не в личные сообщения."
    err = await _verify_bot_can_post(bot, chat_id)
    if err:
        return err
    return await _verify_user_is_chat_admin(bot, chat_id, user_id)


async def _validate_publish_target_for_send(bot: Bot, chat_id: int) -> str:
    """Проверка для фактической отправки: тип чата + право бота постить."""
    try:
        chat = await bot.get_chat(chat_id)
    except TelegramBadRequest:
        return "Не достучаться до чата — проверь id и что бот там есть."
    if chat.type not in ("channel", "group", "supergroup"):
        return "Пост можно публиковать только в канал или группу."
    return await _verify_bot_can_post(bot, chat_id)


async def _user_is_owner_of_chat(bot: Bot, chat_id: int, user_id: int) -> bool:
    """True, если user_id — владелец (creator) чата."""
    try:
        admins = await bot.get_chat_administrators(chat_id)
    except Exception:
        return False
    for m in admins:
        if m.user.is_bot or m.user.id != user_id:
            continue
        if isinstance(m, ChatMemberOwner):
            return True
        if getattr(m, "status", None) == "creator":
            return True
    return False


async def _resolve_crosspost_invite_recipients(bot: Bot, chat_id: int) -> tuple[str, list[Any]]:
    """Для приглашения стороннего канала: организатору не нужно быть админом.
    Возвращает (ошибка, []) или ('', список ChatMember — владелец(цы), иначе все люди-админы)."""
    try:
        chat = await bot.get_chat(chat_id)
    except TelegramBadRequest:
        return ("Чат недоступен — проверь id и что бот добавлен.", [])
    if chat.type not in ("channel", "group", "supergroup"):
        return ("Нужен именно <b>канал</b> или <b>группа</b>, личка не подойдёт.", [])
    err = await _verify_bot_can_post(bot, chat_id)
    if err:
        return (err, [])
    try:
        admins = await bot.get_chat_administrators(chat_id)
    except TelegramBadRequest:
        return (
            "Не выходит получить список админов — дай боту права админа с доступом к участникам.",
            [],
        )
    owners = [
        m
        for m in admins
        if not m.user.is_bot
        and (isinstance(m, ChatMemberOwner) or getattr(m, "status", None) == "creator")
    ]
    if owners:
        return ("", owners)
    humans = [m for m in admins if not m.user.is_bot]
    if not humans:
        return ("Не нашлось людей среди админов, кому отправить приглашение.", [])
    return ("", humans)


class CreateGiveaway(StatesGroup):
    kind = State()
    title = State()
    description = State()
    winners_count = State()
    ends_at = State()
    publish_chat = State()
    lottery_ticket_count = State()
    lottery_button_text = State()
    lottery_cell_color = State()
    media_prompt = State()
    media_file = State()


class EditGiveawayInvite(StatesGroup):
    waiting_extra = State()
    crosswait = State()


class EditPublishedDescription(StatesGroup):
    """Смена описания у уже опубликованного (active) розыгрыша."""
    text = State()


router = Router()


def _main_menu_text() -> str:
    """Текст главного экрана (подпись к фото) в личке."""
    return (
        "🎁 Добро пожаловать в AniGive!\n\n"
        "        AniGive - это удобный бот, созданный для проведения розыгрышей в каналах и чатах.\n\n"
        "✨ Что умеет бот:\n"
        "<blockquote>"
        "• Создание розыгрышей за пару кликов\n"
        "• Честный и случайный выбор победителей\n"
        "• Поддержка каналов и чатов\n"
        "• Удобное управление и быстрый запуск"
        "</blockquote>\n\n"
        "Запускайте розыгрыши, привлекайте аудиторию и радуйте своих подписчиков вместе с AniGive 🚀"
    )


async def _strip_reply_keyboard_in_chat(bot: Bot, chat_id: int) -> None:
    """Убирает reply keyboard из чата (нужно для групп, где её случайно показали)."""
    try:
        await bot.send_message(chat_id, "\u2060", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass


def _channel_bot_rights_for_request_chat() -> ChatAdministratorRights:
    """Права бота при добавлении в канал (как в deep link admin=…)."""
    return ChatAdministratorRights(
        is_anonymous=False,
        can_manage_chat=False,
        can_delete_messages=True,
        can_manage_video_chats=False,
        can_restrict_members=False,
        can_promote_members=True,
        can_change_info=False,
        can_invite_users=True,
        can_post_stories=False,
        can_edit_stories=False,
        can_delete_stories=False,
        can_post_messages=True,
        can_edit_messages=True,
        can_pin_messages=True,
    )


def _group_bot_rights_for_request_chat() -> ChatAdministratorRights:
    """Права бота при добавлении в группу/супергруппу (как в deep link admin=…)."""
    return ChatAdministratorRights(
        is_anonymous=False,
        can_manage_chat=True,
        can_delete_messages=True,
        can_manage_video_chats=False,
        can_restrict_members=True,
        can_promote_members=True,
        can_change_info=False,
        can_invite_users=True,
        can_post_stories=False,
        can_edit_stories=False,
        can_delete_stories=False,
        can_pin_messages=True,
    )


def _reply_btn_add_channel() -> KeyboardButton:
    # В request_chat нужны и user_*, и bot_*: права бота ⊆ прав пользователя; без user_* — USER_RIGHTS_MISSING.
    return KeyboardButton(
        text="Добавить канал",
        request_chat=KeyboardButtonRequestChat(
            request_id=REQ_CHAT_ADD_CHANNEL,
            chat_is_channel=True,
            user_administrator_rights=_channel_bot_rights_for_request_chat(),
            bot_administrator_rights=_channel_bot_rights_for_request_chat(),
        ),
        **_pe_icon(_PE_SAVED_CHANNEL),
    )


def _reply_btn_add_group() -> KeyboardButton:
    return KeyboardButton(
        text="Добавить группу",
        request_chat=KeyboardButtonRequestChat(
            request_id=REQ_CHAT_ADD_GROUP,
            chat_is_channel=False,
            user_administrator_rights=_group_bot_rights_for_request_chat(),
            bot_administrator_rights=_group_bot_rights_for_request_chat(),
        ),
        **_pe_icon(_PE_SAVED_GROUP),
    )


def _main_menu_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                _reply_btn_my_giveaways(),
                _reply_btn_create_giveaway(),
            ],
            [
                _reply_btn_add_channel(),
                _reply_btn_add_group(),
            ],
            [_reply_btn_saved_channels()],
        ],
        resize_keyboard=True,
    )


async def _send_fresh_main_menu_private(bot: Bot, chat_id: int, state: FSMContext) -> None:
    """Одно сообщение главного меню в личке (reply keyboard). Вызывать после state.clear()."""
    kb = _main_menu_reply_kb()
    text = _main_menu_text()
    photo_path = _start_welcome_photo_path()
    if photo_path.is_file():
        sent = await bot.send_photo(
            chat_id,
            FSInputFile(photo_path),
            caption=text,
            reply_markup=kb,
        )
    else:
        log.warning("welcome photo missing: %s", photo_path)
        sent = await bot.send_message(chat_id, text, reply_markup=kb)
    await state.update_data(ui_chat_id=sent.chat.id, ui_message_id=sent.message_id)


def _main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                _ikb_menu_my(),
                _ikb_menu_create(),
            ],
            [
                InlineKeyboardButton(
                    text="Добавить канал",
                    callback_data="menu:add_channel",
                    **_pe_icon(_PE_SAVED_CHANNEL),
                ),
                InlineKeyboardButton(
                    text="Добавить группу",
                    callback_data="menu:add_group",
                    **_pe_icon(_PE_SAVED_GROUP),
                ),
            ],
            [_ikb_menu_saved_channels()],
        ]
    )


def _my_giveaways_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [_ikb_menu_create()],
            [InlineKeyboardButton(text="📂 Список моих розыгрышей", callback_data="adm_list")],
            [_ikb_menu_saved_channels()],
        ]
    )


def _my_giveaways_empty_kb() -> InlineKeyboardMarkup:
    """Нет ни одного розыгрыша — только создать и домой."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [_ikb_menu_create()],
        ]
    )


def _wizard_nav_kb(*, show_back: bool = False) -> InlineKeyboardMarkup:
    """Навигация мастера: «Назад» на всех шагах, кроме первого (название)."""
    rows: list[list[InlineKeyboardButton]] = []
    if show_back:
        rows.append([_ikb_back("adm:back")])
    rows.append([InlineKeyboardButton(text="❌ Отменить", callback_data="adm:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _lottery_ticket_count_kb() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    nums = list(range(5, 101, 5))
    for i in range(0, len(nums), 3):
        chunk = nums[i:i + 3]
        rows.append(
            [
                InlineKeyboardButton(text=str(n), callback_data=f"lott:tc:{n}")
                for n in chunk
            ]
        )
    rows.extend(_wizard_nav_kb(show_back=True).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _lottery_winners_kb(ticket_count: int) -> InlineKeyboardMarkup:
    """Победителей не больше числа билетов (и не больше 15)."""
    tc = max(5, min(100, int(ticket_count or 5)))
    max_w = min(15, tc)
    rows: list[list[InlineKeyboardButton]] = []
    nums = list(range(1, max_w + 1))
    for i in range(0, len(nums), 3):
        chunk = nums[i:i + 3]
        rows.append(
            [
                InlineKeyboardButton(text=str(n), callback_data=f"lott:wc:{n}")
                for n in chunk
            ]
        )
    rows.extend(_wizard_nav_kb(show_back=True).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _lottery_button_text_kb() -> InlineKeyboardMarkup:
    variants = ["🎟", "💎", "🎁", "⭐️", "🍀", "⚡️", "❓"]
    rows = [[InlineKeyboardButton(text=v, callback_data=f"lott:bt:{v}") for v in variants[:4]],
            [InlineKeyboardButton(text=v, callback_data=f"lott:bt:{v}") for v in variants[4:]]]
    rows.extend(_wizard_nav_kb(show_back=True).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _lottery_color_kb() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="🔵 Синий", callback_data="lott:cc:blue"),
            InlineKeyboardButton(text="⚪ Стандартный", callback_data="lott:cc:default"),
        ],
    ]
    rows.extend(_wizard_nav_kb(show_back=True).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _lottery_base_color_emoji(color: str) -> str:
    c = (color or "blue").strip().lower()
    mapping = {
        "blue": "🔵",
        "yellow": "🟡",
        "purple": "🟣",
        "black": "⚫",
        "white": "⚪",
        "brown": "🟤",
    }
    return mapping.get(c, "🔵")


def _lottery_button_style(color: str) -> str:
    c = (color or "blue").strip().lower()
    if c == "default":
        return ""
    return "primary"


def _extract_custom_emoji_id_from_message(message: Message) -> Optional[str]:
    entities = list(getattr(message, "entities", None) or []) + list(
        getattr(message, "caption_entities", None) or []
    )
    for ent in entities:
        if not _is_custom_emoji_entity(ent):
            continue
        cid = getattr(ent, "custom_emoji_id", None)
        if cid:
            return str(cid)
    return None


def _is_custom_emoji_entity(ent: Any) -> bool:
    et = str(getattr(ent, "type", "") or "").strip().lower()
    compact = re.sub(r"[^a-z]", "", et)
    # Covers: custom_emoji, customEmoji, MessageEntityType.CUSTOM_EMOJI, etc.
    return ("custom_emoji" in et) or ("customemoji" in compact)


def _extract_all_custom_emoji_ids_from_message(message: Message) -> list[str]:
    out: list[str] = []
    entities = list(getattr(message, "entities", None) or []) + list(
        getattr(message, "caption_entities", None) or []
    )
    for ent in entities:
        if not _is_custom_emoji_entity(ent):
            continue
        cid = getattr(ent, "custom_emoji_id", None)
        if cid:
            out.append(str(cid))
    # preserve order, remove duplicates
    uniq: list[str] = []
    seen: set[str] = set()
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def _utf16_offset_to_py_index(text: str, utf16_offset: int) -> int:
    """Convert Telegram UTF-16 offset to Python string index."""
    if utf16_offset <= 0:
        return 0
    cur = 0
    for i, ch in enumerate(text):
        cur += 2 if ord(ch) > 0xFFFF else 1
        if cur >= utf16_offset:
            return i + 1
    return len(text)


def _utf16_len(text: str) -> int:
    n = 0
    for ch in text:
        n += 2 if ord(ch) > 0xFFFF else 1
    return n


def _html_to_plain_text_keep_linebreaks(src: str) -> str:
    s = re.sub(r"<\s*br\s*/?\s*>", "\n", src, flags=re.IGNORECASE)
    s = re.sub(r"</\s*(p|div|blockquote)\s*>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s)


def _html_to_text_and_custom_entities(src_html: str) -> tuple[str, list[MessageEntity]]:
    """
    Convert HTML with <tg-emoji> to plain text + custom_emoji entities.
    Other HTML formatting is flattened to plain text for channel entity-mode sends.
    Как в giveaway.py (sqlite).
    """
    out_parts: list[str] = []
    out_entities: list[MessageEntity] = []
    pos = 0
    pat = re.compile(
        r'<tg-emoji\s+emoji-id="(\d+)"\s*>(.*?)</tg-emoji>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    src = _restore_escaped_tg_emoji_html(src_html or "")
    for m in pat.finditer(src):
        out_parts.append(_html_to_plain_text_keep_linebreaks(src[pos:m.start()]))
        eid = (m.group(1) or "").strip()
        glyph = _html_to_plain_text_keep_linebreaks(m.group(2) or "")[:1] or "•"
        offset = _utf16_len("".join(out_parts))
        out_parts.append(glyph)
        if eid:
            out_entities.append(
                MessageEntity(
                    type="custom_emoji",
                    offset=offset,
                    length=_utf16_len(glyph),
                    custom_emoji_id=eid,
                )
            )
        pos = m.end()
    out_parts.append(_html_to_plain_text_keep_linebreaks(src[pos:]))
    text = "".join(out_parts)
    return text, out_entities


def _restore_escaped_tg_emoji_html(src: str) -> str:
    """
    Some legacy rows may store tg-emoji tags escaped as text.
    Restore only tg-emoji tags back to HTML form.
    """
    s = src or ""
    # &lt;tg-emoji emoji-id=&quot;123&quot;&gt;X&lt;/tg-emoji&gt;
    s = re.sub(
        r"&lt;tg-emoji\s+emoji-id=&quot;(\d+)&quot;&gt;(.*?)&lt;/tg-emoji&gt;",
        r'<tg-emoji emoji-id="\1">\2</tg-emoji>',
        s,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # &lt;tg-emoji emoji-id="123"&gt;X&lt;/tg-emoji&gt;
    s = re.sub(
        r"&lt;tg-emoji\s+emoji-id=\"(\d+)\"&gt;(.*?)&lt;/tg-emoji&gt;",
        r'<tg-emoji emoji-id="\1">\2</tg-emoji>',
        s,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # Как в html_text клиента / разные варианты разметки:
    s = re.sub(
        r"<emoji\s+id=(['\"])(\d+)\1\s*>(.*?)</emoji>",
        r'<tg-emoji emoji-id="\2">\3</tg-emoji>',
        s,
        flags=re.IGNORECASE | re.DOTALL,
    )
    s = re.sub(
        r"<emoji\s+id=(\d+)\s*>(.*?)</emoji>",
        r'<tg-emoji emoji-id="\1">\2</tg-emoji>',
        s,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return s


_TG_EMOJI_OPEN_TAG = re.compile(r"<tg-emoji([^>]*)>(.*?)</tg-emoji>", re.IGNORECASE | re.DOTALL)


def _tg_open_attrs_emoji_id(attrs: str) -> str:
    """Достаёт emoji-id из атрибутов открывающего тега <tg-emoji ...>."""
    a = attrs or ""
    for pat in (
        r'emoji-id\s*=\s*"([^"]*)"',
        r"emoji-id\s*=\s*'([^']*)'",
        r"emoji-id\s*=\s*(\d+)",
    ):
        m = re.search(pat, a, re.IGNORECASE)
        if m:
            return (m.group(1) or "").strip()
    return ""


def _sanitize_tg_emoji_html_for_send(html: str, _depth: int = 0) -> str:
    """
    Bot API с parse_mode=HTML падает на пустом/нечисловом/битом emoji-id в <tg-emoji>.
    Убираем такие теги (оставляем один видимый символ); у валидных id сжимаем содержимое до одного глифа.
    """
    if not html or _depth > 16:
        return html or ""

    def repl(m: re.Match) -> str:
        attrs, inner = m.group(1) or "", m.group(2) or ""
        inner_s = _sanitize_tg_emoji_html_for_send(inner, _depth + 1)
        glyph_src = _html_to_plain_text_keep_linebreaks(inner_s).strip()
        glyph = (glyph_src[:1] if glyph_src else "·")
        eid = _tg_open_attrs_emoji_id(attrs)
        if eid.isdigit() and 1 <= len(eid) <= 32:
            return f'<tg-emoji emoji-id="{eid}">{glyph}</tg-emoji>'
        return glyph

    return _TG_EMOJI_OPEN_TAG.sub(repl, html)


def _strip_all_tg_emoji_for_send(html: str) -> str:
    """Последний шаг: убрать все <tg-emoji>, оставить только заменитель-символ (жирный/blockquote и т.д. сохраняются)."""
    s = html or ""
    for _ in range(24):
        new_s = _TG_EMOJI_OPEN_TAG.sub(
            lambda m: (
                _html_to_plain_text_keep_linebreaks((m.group(2) or ""))[:1] or "·"
            ),
            s,
        )
        if new_s == s:
            break
        s = new_s
    return s


def _is_tg_parse_or_custom_emoji_error(exc: BaseException) -> bool:
    msg = (str(exc) or "").lower()
    return any(
        x in msg
        for x in (
            "custom emoji",
            "entities",
            "parse",
            "can't parse",
            "cannot parse",
        )
    )


def _inline_button_without_custom_emoji_icon(b: InlineKeyboardButton) -> InlineKeyboardButton:
    """Новая кнопка с тем же действием, без icon_custom_emoji_id (model_dump на разных aiogram может не выкинуть поле)."""
    args: dict[str, Any] = {"text": b.text or "·"}
    if b.url:
        args["url"] = b.url
    elif b.callback_data is not None:
        args["callback_data"] = b.callback_data
    if b.web_app is not None:
        args["web_app"] = b.web_app
    if b.login_url is not None:
        args["login_url"] = b.login_url
    if b.switch_inline_query is not None:
        args["switch_inline_query"] = b.switch_inline_query
    if b.switch_inline_query_current_chat is not None:
        args["switch_inline_query_current_chat"] = b.switch_inline_query_current_chat
    if b.switch_inline_query_chosen_chat is not None:
        args["switch_inline_query_chosen_chat"] = b.switch_inline_query_chosen_chat
    ct = getattr(b, "copy_text", None)
    if ct is not None:
        args["copy_text"] = ct
    cg = getattr(b, "callback_game", None)
    if cg is not None:
        args["callback_game"] = cg
    if getattr(b, "pay", False):
        args["pay"] = True
    return InlineKeyboardButton(**args)


def _strip_inline_kb_custom_emoji(kb: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [_inline_button_without_custom_emoji_icon(b) for b in row] for row in kb.inline_keyboard
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _message_html_preserve_custom_emoji(message: Message) -> str:
    """
    Prefer Telegram-provided html_text, but if custom emoji entities exist and
    tg-emoji tags are missing, rebuild text with explicit <tg-emoji>.
    Как в giveaway.py (sqlite).
    """
    html_text = ((getattr(message, "html_text", None) or "").strip()
                 or (getattr(message, "html_caption", None) or "").strip())
    if html_text and "<emoji id=" in html_text:
        html_text = re.sub(
            r'<emoji id="(\d+)">(.*?)</emoji>',
            r'<tg-emoji emoji-id="\1">\2</tg-emoji>',
            html_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    entities = list(getattr(message, "entities", None) or []) + list(
        getattr(message, "caption_entities", None) or []
    )
    plain = (message.text or message.caption or "")
    if not plain:
        return _restore_escaped_tg_emoji_html(html_text or html.escape(message.text or ""))
    has_custom = any(_is_custom_emoji_entity(e) for e in entities)
    if not has_custom:
        return _restore_escaped_tg_emoji_html(html_text or html.escape(plain))
    if "<tg-emoji" in html_text:
        return html_text

    out = html.escape(plain, quote=False)
    custom_entities: list[tuple[int, int, str]] = []
    for e in entities:
        if not _is_custom_emoji_entity(e):
            continue
        cid = str(getattr(e, "custom_emoji_id", "") or "").strip()
        if not cid:
            continue
        off = int(getattr(e, "offset", 0) or 0)
        ln = int(getattr(e, "length", 0) or 0)
        if ln <= 0:
            continue
        s = _utf16_offset_to_py_index(plain, off)
        t = _utf16_offset_to_py_index(plain, off + ln)
        custom_entities.append((s, t, cid))
    if not custom_entities:
        return html_text or out

    parts: list[str] = []
    cursor = 0
    for s, t, cid in sorted(custom_entities, key=lambda x: x[0]):
        if s < cursor:
            continue
        parts.append(html.escape(plain[cursor:s], quote=False))
        glyph = plain[s:t] or "•"
        parts.append(f'<tg-emoji emoji-id="{cid}">{html.escape(glyph[:1], quote=False)}</tg-emoji>')
        cursor = t
    parts.append(html.escape(plain[cursor:], quote=False))
    return _restore_escaped_tg_emoji_html("".join(parts))


async def _dispatch_crosspost_admin_requests(
    bot: Bot,
    pending_id: int,
    extra_chat_ids: list[int],
    creator_label: str,
    creator_uid: int,
) -> None:
    intro = (
        f"Привет! {creator_label} (<code>{creator_uid}</code>) делает розыгрыш и хочет подключить <b>твой чат</b>.\n\n"
        "Если согласишься — участникам нужно будет подписаться и на этот канал/группу, "
        "а пост о конкурсе продублируют у тебя.\n\n"
        "Пишу тебе как владельцу (если бот не видит владельца — как одному из админов).\n\n"
        "«Согласен» — ок, пока никто из админов этого чата не нажал «Отказ».\n"
        "«Отказ» — ваш чат не участвует.\n\n"
        "Итог: чат попадёт в розыгрыш, если есть хотя бы одно «да» и ни одного «нет» среди ответов по нему."
    )
    async with _pg_conn() as db:
        for ch_id in extra_chat_ids:
            try:
                chat = await bot.get_chat(ch_id)
            except Exception:
                continue
            ch_title = html.escape(chat.title or str(ch_id))
            _err, invitees = await _resolve_crosspost_invite_recipients(bot, ch_id)
            if not invitees:
                continue
            if await _user_is_owner_of_chat(bot, ch_id, creator_uid):
                await db.execute(
                    """INSERT INTO crosspost_vote (pending_id, channel_id, admin_id, vote)
                       VALUES (?, ?, ?, 'yes')
                       ON CONFLICT (pending_id, channel_id, admin_id)
                       DO UPDATE SET vote = EXCLUDED.vote""",
                    (pending_id, ch_id, creator_uid),
                )
                await db.commit()
                continue
            for mem in invitees:
                user = mem.user
                if user.is_bot:
                    continue
                cur = await db.execute(
                    "INSERT INTO crosspost_notif (pending_id, channel_id, admin_id) VALUES (?, ?, ?) RETURNING id",
                    (pending_id, ch_id, user.id),
                )
                row_nid = await cur.fetchone()
                await db.commit()
                nid = int(row_nid["id"])
                text = f"{intro}\n\nЧат: <b>{ch_title}</b>"
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(text="✅ Согласен", callback_data=f"xv:{nid}:y"),
                            InlineKeyboardButton(text="❌ Не участвуем", callback_data=f"xv:{nid}:n"),
                        ]
                    ]
                )
                try:
                    await bot.send_message(user.id, text, reply_markup=kb)
                except Exception as e:
                    log.debug("crosspost notify %s: %s", user.id, e)


def _giveaway_step1_title_html() -> str:
    return (
        f"{_tg_pe(_PE_GW_STEP1, '✨')} <b>Создание розыгрыша - шаг 1 из 6</b>\n\n"
        f"{_tg_pe(_PE_GW_PROMPT, '✏️')} Введите название розыгрыша -\n"
        "именно его увидят участники в заголовке.\n\n"
        f"{_tg_pe(_PE_LOT_DOCWARN, '📎')} Чтобы использовать премиум-эмодзи,\n"
        "добавьте нашего бота-помощника @AniGive в администраторы."
    )


def _giveaway_step2_desc_html() -> str:
    return (
        f"{_tg_pe(_PE_GW_STEP, '✨')} <b>Шаг 2 из 6</b>\n\n"
        f"{_tg_pe(_PE_GW_PROMPT, '✏️')} Опишите приз одним сообщением.\n"
        "Этот текст будет отображаться в розыгрыше."
    )


def _giveaway_step3_winners_html() -> str:
    return (
        f"{_tg_pe(_PE_GW_STEP, '✨')} <b>Шаг 3 из 6</b>\n\n"
        f"{_tg_pe(_PE_GW_PROMPT, '✏️')} Введите количество победителей.\n"
        "Просто отправьте число (например: <code>3</code>)"
    )


def _giveaway_step4_ends_html() -> str:
    return (
        f"{_tg_pe(_PE_GW_STEP, '✨')} <b>Шаг 4 из 6</b>\n\n"
        f"{_tg_pe(_PE_GW_PROMPT, '✏️')} Укажите дату окончания розыгрыша (UTC +3, Мск).\n"
        "Формат: <code>31.12.2026 20:00</code>.\n\n"
        "Или нажмите кнопку ниже, чтобы выбрать дату и время в календаре."
    )


def _giveaway_step5_publish_html() -> str:
    return (
        f"{_tg_pe(_PE_GW_STEP, '✨')} <b>Шаг 5 из 6</b>\n\n"
        f"{_tg_pe(_PE_GW_PUBLISH_TITLE, '📣')} Где опубликовать розыгрыш?\n\n"
        f"{_tg_pe(_PE_GW_PUBLISH_DESC, '📂')} Выбери один или несколько каналов из списка ниже, "
        "или добавь новый кнопками «Добавить канал» / «Добавить группу».\n\n"
        f"{_tg_pe(_PE_LOT_DOCWARN, '⚠️')} Убедись, что ты админ канала/чата, "
        "и бот добавлен в администраторы."
    )


def _giveaway_step6_media_intro_html() -> str:
    return (
        f"{_tg_pe(_PE_GW_STEP, '✨')} <b>Шаг 6 из 6</b>\n\n"
        "Добавить к посту <b>фото, GIF или видео</b>?"
    )


def _publish_pick_screen_html(is_lottery: bool, has_saved_chats: bool) -> str:
    if is_lottery:
        return _lottery_publish_pick_html(has_saved_chats)
    return _giveaway_step5_publish_html()


def _merge_pick_publish_under_nav(pick: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=pick.inline_keyboard + _wizard_nav_kb(show_back=True).inline_keyboard
    )


def _ends_calendar_kb(year: int, month: int) -> InlineKeyboardMarkup:
    cal = calendar.monthcalendar(year, month)
    rows: list[list[InlineKeyboardButton]] = []
    rows.append(
        [
            InlineKeyboardButton(text="◀️", callback_data=f"dt:m:{year}:{month}:prev"),
            InlineKeyboardButton(text=f"{month:02d}.{year}", callback_data="noop"),
            InlineKeyboardButton(text="▶️", callback_data=f"dt:m:{year}:{month}:next"),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(text="Пн", callback_data="noop"),
            InlineKeyboardButton(text="Вт", callback_data="noop"),
            InlineKeyboardButton(text="Ср", callback_data="noop"),
            InlineKeyboardButton(text="Чт", callback_data="noop"),
            InlineKeyboardButton(text="Пт", callback_data="noop"),
            InlineKeyboardButton(text="Сб", callback_data="noop"),
            InlineKeyboardButton(text="Вс", callback_data="noop"),
        ]
    )
    for week in cal:
        wrow: list[InlineKeyboardButton] = []
        for d in week:
            if d == 0:
                wrow.append(InlineKeyboardButton(text="·", callback_data="noop"))
            else:
                wrow.append(
                    InlineKeyboardButton(
                        text=str(d),
                        callback_data=f"dt:d:{year}:{month}:{d}",
                    )
                )
        rows.append(wrow)
    rows.extend(_wizard_nav_kb(show_back=True).inline_keyboard)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_back_to_giveaway(gid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [_ikb_back(f"g:{gid}")],
        ]
    )


def _user_stored_html_as_safe_plain_escape(
    raw: str, *, max_len: int = 900
) -> str:
    """Из БД (title/description как Telegram HTML) — плоский текст + html.escape для вставки в наши HTML-экраны.
    Иначе битые <tg-emoji> в названии ломают edit_message и блокируют удаление и др. действия."""
    plain = _html_to_plain_text_keep_linebreaks(
        _restore_escaped_tg_emoji_html((raw or "").strip())
    ).strip()
    if len(plain) > max_len:
        plain = plain[: max_len - 1] + "…"
    return html.escape(plain)


def _giveaway_title_button_caption(g: dict[str, Any]) -> str:
    """Одна строка для текста кнопки без HTML-тегов из названия."""
    t = _html_to_plain_text_keep_linebreaks(
        _restore_escaped_tg_emoji_html((g.get("title") or "").strip())
    ).replace("\n", " ").strip()
    return t


def _giveaway_user_text(g: dict[str, Any]) -> str:
    ch = _giveaway_conditions_channels_html(g)
    title_esc = _user_stored_html_as_safe_plain_escape(g.get("title") or "", max_len=400)
    desc_esc = _user_stored_html_as_safe_plain_escape(g.get("description") or "", max_len=1200)
    return (
        f"<b>{title_esc}</b>\n\n"
        f"{desc_esc}\n\n"
        f"{ch}"
        f"🏆 Победителей: {g['winners_count']}\n"
        f"⏳ Принимаем заявки до: {_format_ends_at_user(g['ends_at'])} (по Москве)"
    )


def _is_lottery(g: dict[str, Any]) -> bool:
    return (g.get("giveaway_type") or "giveaway") == "lottery"


def _giveaway_title_html(g: dict[str, Any], gid: int) -> str:
    """Заголовок для постов: название конкурса; если пусто — запасной вариант с номером."""
    t = (g.get("title") or "").strip()
    if not t:
        t = f"Розыгрыш #{gid}"
    # Title is stored as trusted Telegram HTML (from user message html/entities),
    # so keep tg-emoji tags intact instead of escaping them into plain text.
    return _restore_escaped_tg_emoji_html(t)


def build_participant_kb(
    gid: int, referral_enabled: bool, *, join_url: Optional[str] = None
) -> InlineKeyboardMarkup:
    """Кнопка на посте одна и не меняется; реферальная ссылка уходит в личку после участия."""
    _ = referral_enabled  # ссылка в DM, если включён бонус за приглашение
    btn = (
        InlineKeyboardButton(text="🎁 Участвовать", url=join_url)
        if join_url
        else InlineKeyboardButton(text="🎁 Участвовать", callback_data=f"join:{gid}")
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn],
        ]
    )


def build_participant_kb_from_giveaway(g: dict[str, Any]) -> InlineKeyboardMarkup:
    return build_participant_kb(int(g["id"]), bool(g.get("referral_enabled")))


async def _lottery_grid_kb(g: dict[str, Any]) -> InlineKeyboardMarkup:
    gid = int(g["id"])
    total = max(1, min(100, int(g.get("lottery_ticket_count") or 1)))
    token = (g.get("lottery_button_text") or "🎟").strip() or "🎟"
    if len(token) > 6:
        token = token[:6]
    # Keep text token as fallback in case icon_custom_emoji_id is not rendered by client/channel.
    base_style = _lottery_button_style(str(g.get("lottery_cell_color") or "blue"))
    custom_emoji_id = (g.get("lottery_button_custom_emoji_id") or "").strip() or None
    picks: dict[int, int] = {}
    has_winner = False
    async with _pg_conn() as db:
        cur_w = await db.execute(
            "SELECT 1 FROM lottery_picks WHERE giveaway_id = ? AND is_winner = 1 LIMIT 1",
            (gid,),
        )
        has_winner = (await cur_w.fetchone()) is not None
        cur = await db.execute(
            "SELECT ticket_no, is_winner FROM lottery_picks WHERE giveaway_id = ?",
            (gid,),
        )
        for row in await cur.fetchall():
            picks[int(row["ticket_no"])] = int(row["is_winner"])
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for n in range(1, total + 1):
        status = picks.get(n)
        if status is None:
            if has_winner:
                cb = "noop"
                style = ""
            else:
                cb = f"lotpick:{gid}:{n}"
                style = base_style
        elif status == 1:
            cb = "noop"
            style = "success"
        else:
            cb = "noop"
            style = "danger"
        row.append(
            InlineKeyboardButton(
                text=(f"{token} {n}".strip() if token else str(n)),
                callback_data=cb,
                **{**_pe_icon(custom_emoji_id), **({"style": style} if style else {})},
            )
        )
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _build_public_participant_kb(bot: Bot, g: dict[str, Any]) -> InlineKeyboardMarkup:
    if _is_lottery(g):
        return await _lottery_grid_kb(g)
    gid = int(g["id"])
    join_url: Optional[str] = None
    try:
        me = await bot.get_me()
        uname = (me.username or "").strip()
        if uname:
            join_url = f"https://t.me/{uname}?start={JOIN_PREFIX}{gid}"
    except Exception:
        join_url = None
    return build_participant_kb(gid, bool(g.get("referral_enabled")), join_url=join_url)


def build_participant_kb_after_join(gid: int) -> InlineKeyboardMarkup:
    """Личка после успешного участия — кнопка без повторного join."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [_ikb_menu_my()],
        ]
    )


async def _send_join_confirmation_dm(bot: Bot, g: dict[str, Any], user_id: int) -> None:
    gid = int(g["id"])
    title = _restore_escaped_tg_emoji_html((g.get("title") or f"#{gid}").strip() or f"#{gid}")
    lines: list[str] = [
        f"✅ Ты участвуешь в розыгрыше «{title}»!",
    ]
    if g.get("referral_enabled"):
        me = await bot.get_me()
        if me.username:
            link = f"https://t.me/{me.username}?start={REF_PREFIX}{gid}_{user_id}"
            lines.extend(
                [
                    "",
                    "🔗 Твоя персональная ссылка:",
                    f"<code>{link}</code>",
                    "",
                    f"{_tg_pe(_PE_REF_INVITE, '👥')} Приглашай друзей и увеличивай свои шансы на победу:",
                    "",
                    "Если человек перейдёт по твоей ссылке, выполнит все условия (подписки, @username — если требуется) "
                    f"и нажмёт «Участвовать», ты получишь дополнительный вес при выборе победителя {_tg_pe(_PE_REF_WEIGHT, '⭐')}",
                ]
            )
    try:
        await bot.send_message(
            user_id,
            "\n".join(lines),
            parse_mode="HTML",
            disable_web_page_preview=True,
            link_preview_options=_LINK_PREVIEW_OFF,
        )
    except Exception as e:
        log.debug("join confirmation dm: %s", e)


async def _giveaway_dm_status_text_and_kb(
    bot: Bot, db: PgConnAdapter, g: dict[str, Any], uid: int
) -> tuple[str, InlineKeyboardMarkup]:
    gid = int(g["id"])
    cur = await db.execute(
        "SELECT 1 FROM participants WHERE giveaway_id = ? AND user_id = ?",
        (gid, uid),
    )
    already = await cur.fetchone()
    channels = _channels_list(g["channels_json"])
    ok_sub, missing = await check_user_subscribed(bot, uid, channels)
    parts: list[str] = []
    if g.get("require_username"):
        try:
            chm = await bot.get_chat_member(uid, uid)
            has_un = bool(getattr(chm.user, "username", None))
        except Exception:
            has_un = False
        if not has_un:
            parts.append("• Включи @username в профиле Telegram.")
    if not ok_sub:
        miss = await _missing_channels_links_inline_html(bot, missing)
        parts.append("• Подпишись на: " + (miss if miss else ", ".join(f"<code>{m}</code>" for m in missing)))
    status = "✅ Ты уже участвуешь в этом розыгрыше." if already else "Ты пока не участвуешь."
    cond = (
        "✅ Условия выполнены." if not parts else f"{_tg_pe(_PE_DM_NEEDS, '⚠️')} Что нужно выполнить:\n" + "\n".join(parts)
    )
    tickets_block = ""
    if already:
        ref_enabled = bool(g.get("referral_enabled"))
        ref_step = max(1, min(5, int(g.get("referral_ticket_step") or 1)))
        ref_count = 0
        if ref_enabled:
            cur_r = await db.execute(
                "SELECT COUNT(*) AS cnt FROM valid_referrals WHERE giveaway_id = ? AND referrer_id = ?",
                (gid, uid),
            )
            ref_count = int((await cur_r.fetchone())["cnt"])
        bonus = (ref_count // ref_step) if ref_enabled else 0
        total_tickets = 1 + bonus
        if ref_enabled:
            rem = ref_count % ref_step
            need = max(0, ref_step - rem)
            progress = f"{rem}/{ref_step}" if bonus >= 0 else f"0/{ref_step}"
            tickets_block = (
                f"\n{_tg_pe(_PE_REF_TICKET, '🎟')} <b>Твои билеты:</b> {total_tickets}\n"
                f"{_tg_pe(_PE_REF_CONFIRMED, '👥')} Подтверждённых рефералов: {ref_count}\n"
                f"{_tg_pe(_PE_REF_NEXT, '➕')} До следующего билета: {need} (прогресс {progress})\n"
            )
        else:
            tickets_block = f"\n{_tg_pe(_PE_REF_TICKET, '🎟')} <b>Твои билеты:</b> {total_tickets}\n"
    if not already:
        title = _restore_escaped_tg_emoji_html((g.get("title") or f"#{gid}").strip() or f"#{gid}")
        desc = _restore_escaped_tg_emoji_html(g.get("description") or "")
        channels = _channels_list(g.get("channels_json") or "[]")
        sub_lines: list[str] = []
        for tok in channels:
            sub_lines.append(await _channel_subscription_line_clickable_html(bot, tok))
        sub_lines = [x for x in sub_lines if x]
        subscribe_block = ""
        if sub_lines:
            subscribe_block = (
                f"{_tg_pe(_PE_DM_SUBSCRIBE, '📣')} Подпишись:\n" + "\n".join(sub_lines) + "\n\n"
            )
        if parts:
            tail = (
                f"{_tg_pe(_PE_LOT_DOCWARN, 'ℹ️')} {status}\n"
                f"{cond}"
            )
        else:
            tail = "✅ Условия выполнены."
        text = (
            f"<b>{title}</b>\n\n"
            f"{desc}\n\n"
            f"{subscribe_block}"
            f"{_tg_pe(_PE_POST_WINNERS, '🏆')} Победителей: {g['winners_count']}\n"
            f"{_tg_pe(_PE_POST_DEADLINE, '⏳')} Принимаем заявки до: {_format_ends_at_user(g['ends_at'])} (по Москве)\n\n"
            f"{tail}"
        )
    else:
        # Уже участвует: показываем ту же карточку, что и для остальных состояний,
        # но без блока "Что нужно выполнить" — только подтверждение выполнения условий.
        title = _restore_escaped_tg_emoji_html((g.get("title") or f"#{gid}").strip() or f"#{gid}")
        desc = _restore_escaped_tg_emoji_html(g.get("description") or "")
        channels = _channels_list(g.get("channels_json") or "[]")
        sub_lines: list[str] = []
        for tok in channels:
            sub_lines.append(await _channel_subscription_line_clickable_html(bot, tok))
        sub_lines = [x for x in sub_lines if x]
        subscribe_block = ""
        if sub_lines:
            subscribe_block = (
                f"{_tg_pe(_PE_DM_SUBSCRIBE, '📣')} Подпишись:\n" + "\n".join(sub_lines) + "\n\n"
            )
        text = (
            f"<b>{title}</b>\n\n"
            f"{desc}\n\n"
            f"{subscribe_block}"
            f"{_tg_pe(_PE_POST_WINNERS, '🏆')} Победителей: {g['winners_count']}\n"
            f"{_tg_pe(_PE_POST_DEADLINE, '⏳')} Принимаем заявки до: {_format_ends_at_user(g['ends_at'])} (по Москве)\n"
            f"{tickets_block}\n"
            "✅ Условия выполнены."
        )
    kb = build_participant_kb_after_join(gid) if already else build_participant_kb_from_giveaway(g)
    return text, kb


async def _auto_join_if_possible(
    bot: Bot,
    db: PgConnAdapter,
    g: dict[str, Any],
    user: Any,
    *,
    referrer_id: Optional[int] = None,
) -> bool:
    """Пытается сразу вступить в розыгрыш из deep-link, если условия уже выполнены."""
    gid = int(g["id"])
    uid = int(user.id)
    if g.get("status") != "active":
        return False
    if _utc_now() > _parse_stored_ends_at(g["ends_at"]):
        return False
    if g.get("require_username") and not getattr(user, "username", None):
        return False
    channels = _channels_list(g["channels_json"])
    ok_sub, _missing = await check_user_subscribed(bot, uid, channels)
    if not ok_sub:
        return False
    cur = await db.execute(
        "SELECT 1 FROM participants WHERE giveaway_id = ? AND user_id = ?",
        (gid, uid),
    )
    if await cur.fetchone():
        return False
    await db.execute(
        """INSERT INTO participants (giveaway_id, user_id, username, first_name, joined_at)
           VALUES (?, ?, ?, ?, ?)""",
        (gid, uid, getattr(user, "username", None), getattr(user, "first_name", "") or "", _utc_now().isoformat()),
    )
    if g.get("referral_enabled") and referrer_id and int(referrer_id) != uid:
        referrer_id = int(referrer_id)
        cur_r = await db.execute(
            "SELECT 1 FROM participants WHERE giveaway_id = ? AND user_id = ?",
            (gid, referrer_id),
        )
        ref_is_participant = await cur_r.fetchone()
        if ref_is_participant:
            await db.execute(
                """INSERT INTO valid_referrals (giveaway_id, referrer_id, referee_id, created_at)
                   VALUES (?, ?, ?, ?) ON CONFLICT (giveaway_id, referee_id) DO NOTHING""",
                (gid, referrer_id, uid, _utc_now().isoformat()),
            )
        else:
            await db.execute(
                """INSERT INTO referral_queue (giveaway_id, referrer_id, referee_id, created_at)
                   VALUES (?, ?, ?, ?) ON CONFLICT (giveaway_id, referee_id) DO NOTHING""",
                (gid, referrer_id, uid, _utc_now().isoformat()),
            )
    await db.execute("DELETE FROM pending_referral WHERE user_id = ?", (uid,))
    cur_q = await db.execute(
        "SELECT referee_id FROM referral_queue WHERE giveaway_id = ? AND referrer_id = ?",
        (gid, uid),
    )
    queued = await cur_q.fetchall()
    for row_q in queued:
        referee_id_q = int(row_q["referee_id"])
        await db.execute(
            """INSERT INTO valid_referrals (giveaway_id, referrer_id, referee_id, created_at)
               VALUES (?, ?, ?, ?) ON CONFLICT (giveaway_id, referee_id) DO NOTHING""",
            (gid, uid, int(referee_id_q), _utc_now().isoformat()),
        )
        await db.execute(
            "DELETE FROM referral_queue WHERE giveaway_id = ? AND referrer_id = ? AND referee_id = ?",
            (gid, uid, int(referee_id_q)),
        )
    await db.commit()
    await _send_join_confirmation_dm(bot, g, uid)
    return True


def _admin_kb_giveaway(gid: int, *, can_edit_description: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_edit_description:
        rows.extend(
            [
                [
                    InlineKeyboardButton(
                        text="Подвести итоги",
                        callback_data=f"draw:{gid}",
                        **_pe_icon(_PE_ADMIN_DRAW),
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="Выбрать других победителей",
                        callback_data=f"reroll:{gid}",
                        **_pe_icon(_PE_ADMIN_REROLL),
                    )
                ],
            ]
        )
    if can_edit_description:
        rows.append(
            [
                InlineKeyboardButton(
                    text="Изменить описание",
                    callback_data=f"aedesc:{gid}",
                    **_pe_icon(_PE_ADMIN_EDIT_DESC),
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="Кто участвует",
                    callback_data=f"plist:{gid}",
                    **_pe_icon(_PE_ADMIN_PARTICIPANTS),
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="Запостить ещё раз",
                    callback_data=f"repost:{gid}",
                    **_pe_icon(_PE_DRAFT_PUBLISH),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Удалить конкурс",
                    callback_data=f"del_ask:{gid}",
                    **_pe_icon(_PE_DRAFT_DELETE),
                )
            ],
            [_ikb_back("adm_list")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _draft_giveaway_kb(gid: int) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(
            text="Опубликовать",
            callback_data=f"dpub:{gid}",
            **_pe_icon(_PE_DRAFT_PUBLISH),
        )
    ]]
    # Для лотереи в предпросмотре не показываем «Доп. функции» и «Пригласить каналы».
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="Доп. функции",
                    callback_data=f"dextra:{gid}",
                    **_pe_icon(_PE_DRAFT_EXTRA),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Пригласить каналы",
                    callback_data=f"dchinv:{gid}",
                    **_pe_icon(_PE_DRAFT_INVITE_CHANNELS),
                )
            ],
        ]
    )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="Удалить",
                    callback_data=f"del_ask:{gid}",
                    **_pe_icon(_PE_DRAFT_DELETE),
                )
            ],
            [_ikb_back("adm_list")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _draft_lottery_kb(gid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Опубликовать",
                    callback_data=f"dpub:{gid}",
                    **_pe_icon(_PE_DRAFT_PUBLISH),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Удалить",
                    callback_data=f"del_ask:{gid}",
                    **_pe_icon(_PE_DRAFT_DELETE),
                )
            ],
            [_ikb_back("adm_list")],
        ]
    )


async def _send_draft_giveaway_ui_message(bot: Bot, chat_id: int, g: dict[str, Any]) -> Message:
    """Сообщение как в канале (подпись/медиа), но с клавиатурой управления черновиком."""
    card = _giveaway_public_caption(g)
    kb = _draft_lottery_kb(int(g["id"])) if _is_lottery(g) else _draft_giveaway_kb(int(g["id"]))
    card_no_pe = _strip_all_tg_emoji_for_send(card)
    kb_no_pe = _strip_inline_kb_custom_emoji(kb)
    kind = (g.get("post_media_kind") or "").strip()
    fid = g.get("post_media_file_id")

    async def _send(caption: str, markup: InlineKeyboardMarkup, *, parse_mode: Optional[str]) -> Message:
        if kind == "photo" and fid:
            return await bot.send_photo(
                chat_id,
                fid,
                caption=caption,
                reply_markup=markup,
                parse_mode=parse_mode,
            )
        if kind == "animation" and fid:
            return await bot.send_animation(
                chat_id,
                fid,
                caption=caption,
                reply_markup=markup,
                parse_mode=parse_mode,
            )
        if kind == "video" and fid:
            return await bot.send_video(
                chat_id,
                fid,
                caption=caption,
                reply_markup=markup,
                parse_mode=parse_mode,
            )
        return await bot.send_message(
            chat_id,
            caption,
            reply_markup=markup,
            parse_mode=parse_mode,
            disable_web_page_preview=True,
            link_preview_options=_LINK_PREVIEW_OFF,
        )

    try:
        return await _send(card, kb, parse_mode="HTML")
    except TelegramBadRequest as e:
        if not _is_tg_parse_or_custom_emoji_error(e):
            raise
        log.warning(
            "draft UI: HTML/custom emoji rejected (%s); retry without tg-emoji in text and on buttons",
            e,
        )
        try:
            return await _send(card_no_pe, kb_no_pe, parse_mode="HTML")
        except TelegramBadRequest as e2:
            if not _is_tg_parse_or_custom_emoji_error(e2):
                raise
            log.warning("draft UI: retry plain caption + no parse_mode: %s", e2)
            plain = html.escape(_html_to_plain_text_keep_linebreaks(card_no_pe))
            return await _send(plain, kb_no_pe, parse_mode=None)


async def _render_draft_giveaway_screen(
    query: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    g: Optional[dict[str, Any]],
) -> None:
    if not g:
        log.error("render draft screen: giveaway not in DB")
        await query.answer("Черновик не найден в базе.", show_alert=True)
        return
    await query.answer()
    cid, mid = query.message.chat.id, query.message.message_id
    try:
        await bot.delete_message(cid, mid)
    except TelegramBadRequest:
        pass
    except Exception as e:
        log.debug("replace draft screen: %s", e)
    sent = await _send_draft_giveaway_ui_message(bot, cid, g)
    await _remember_ui(state, sent.chat.id, sent.message_id)


async def _present_draft_giveaway_after_wizard(
    bot: Bot,
    state: FSMContext,
    chat_id: int,
    old_message_id: Optional[int],
    g: Optional[dict[str, Any]],
) -> None:
    if not g:
        log.error("present draft after wizard: giveaway not in DB")
        try:
            await bot.send_message(
                chat_id,
                "⚠️ Черновик записался некорректно. Нажми «Создать розыгрыш» ещё раз; если повторится — смотри лог сервера.",
            )
        except Exception as e:
            log.debug("present draft fallback msg: %s", e)
        return
    if old_message_id is not None:
        try:
            await bot.delete_message(chat_id, int(old_message_id))
        except Exception:
            pass
    sent = await _send_draft_giveaway_ui_message(bot, chat_id, g)
    await _remember_ui(state, sent.chat.id, sent.message_id)


async def _insert_draft_giveaway_row(
    uid: int,
    title: str,
    description: str,
    winners_count: int,
    ends_at: str,
    publish_chat_id: int,
    channels_json: str,
    mirror_target_chat_ids: str,
    post_media_kind: Optional[str],
    post_media_file_id: Optional[str],
    giveaway_type: str = "giveaway",
    lottery_ticket_count: int = 0,
    lottery_button_text: str = "🎟",
    lottery_winning_tickets_json: str = "[]",
    lottery_cell_color: str = "blue",
    lottery_button_custom_emoji_id: str = "",
    require_username: int = 0,
    referral_enabled: int = 0,
    referral_ticket_step: int = 1,
) -> int:
    async with _pg_conn() as db:
        cur = await db.execute(
            """INSERT INTO giveaways (
                title, description, giveaway_type, winners_count, ends_at, channels_json,
                require_username, one_entry_only, referral_enabled, referral_ticket_step,
                status, post_chat_id, post_message_id, created_by, created_at,
                mirror_posts_json, mirror_target_chat_ids, mirror_results_json,
                post_media_kind, post_media_file_id, subscribe_only_chat_ids,
                lottery_ticket_count, lottery_button_text, lottery_winning_tickets_json, lottery_cell_color,
                lottery_button_custom_emoji_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 'draft', ?, NULL, ?, ?, '[]', ?, NULL, ?, ?, '[]', ?, ?, ?, ?, ?) RETURNING id""",
            (
                title,
                description,
                giveaway_type,
                winners_count,
                ends_at,
                channels_json,
                require_username,
                referral_enabled,
                max(1, min(5, int(referral_ticket_step or 1))),
                publish_chat_id,
                uid,
                _utc_now().isoformat(),
                mirror_target_chat_ids,
                post_media_kind,
                post_media_file_id,
                max(0, min(100, int(lottery_ticket_count or 0))),
                (lottery_button_text or "🎟")[:16],
                lottery_winning_tickets_json or "[]",
                (lottery_cell_color or "blue")[:20],
                (lottery_button_custom_emoji_id or "")[:64],
            ),
        )
        row_id = await cur.fetchone()
        rid: int | None = None
        if row_id is not None:
            try:
                rid = int(row_id["id"])
            except (KeyError, TypeError, ValueError):
                rid = None
        if rid is None:
            cur2 = await db.execute(
                "SELECT id FROM giveaways WHERE created_by = ? ORDER BY id DESC LIMIT 1",
                (uid,),
            )
            row_fb = await cur2.fetchone()
            if row_fb is None:
                raise RuntimeError("INSERT giveaways did not return id")
            rid = int(row_fb["id"])
        await db.commit()
        return rid


def _draft_extra_kb(g: dict[str, Any]) -> InlineKeyboardMarkup:
    if _is_lottery(g):
        gid = int(g["id"])
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [_ikb_back(f"ddraft:{gid}")]
            ]
        )
    gid = int(g["id"])
    rq = int(g.get("require_username") or 0)
    ref = int(g.get("referral_enabled") or 0)
    step = max(1, min(5, int(g.get("referral_ticket_step") or 1)))
    nav = [[_ikb_back(f"ddraft:{gid}")]]
    step_rows: list[list[InlineKeyboardButton]] = []
    if ref:
        step_rows.append(
            [
                InlineKeyboardButton(
                    text="Рефералов за +1 билет",
                    callback_data="noop",
                    **_pe_icon(_PE_REF_TICKET),
                )
            ]
        )
        step_rows.append(
            [
                InlineKeyboardButton(
                    text=str(n) + (" ✅" if step == n else ""),
                    callback_data=f"dexstep:{gid}:{n}",
                )
                for n in (1, 2, 3, 4, 5)
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Нужен @username" + (" ✅" if rq else ""),
                    callback_data=f"dexrq:{gid}:1",
                ),
                InlineKeyboardButton(
                    text="Без @username" + (" ✅" if not rq else ""),
                    callback_data=f"dexrq:{gid}:0",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="Шанс за приглашение" + (" ✅" if ref else ""),
                    callback_data=f"dexref:{gid}:1",
                ),
                InlineKeyboardButton(
                    text="Без бонуса" + (" ✅" if not ref else ""),
                    callback_data=f"dexref:{gid}:0",
                ),
            ],
            *step_rows,
            *nav,
        ]
    )


_DRAFT_EXTRA_SCREEN_TITLE = f"{_tg_pe(_PE_DRAFT_EXTRA, '⚙️')} <b>Доп.функции</b>\n\n"


def _giveaway_public_caption(g: dict[str, Any]) -> str:
    if _is_lottery(g):
        tickets = max(1, min(100, int(g.get("lottery_ticket_count") or 1)))
        title_html = _restore_escaped_tg_emoji_html((g.get("title") or "").strip())
        desc = _restore_escaped_tg_emoji_html((g.get("description") or "").strip())
        # `description` уже как HTML из чата (`message.html_text`); повторное экранирование ломает <tg-emoji>.
        desc_html = desc if desc else ""
        hint = (
            "<blockquote expandable>Нажми на номер билета ниже - если попадёшь в выигрышный квадрат, "
            "бот опубликует отдельный пост с победителем.</blockquote>"
        )
        body = (
            f"🎰 {title_html}\n\n"
            f"Количество билетов: <b>{tickets}</b>\n"
            f"Победителей: <b>{g['winners_count']}</b>\n\n"
            f"{hint}"
        )
        if desc_html:
            raw = (
                f"🎰 {title_html}\n\n"
                f"{desc_html}\n\n"
                f"Количество билетов: <b>{tickets}</b>\n"
                f"Победителей: <b>{g['winners_count']}</b>\n\n"
                f"{hint}"
            )
        else:
            raw = body
    else:
        raw = (
            f"{_tg_pe(_PE_GW_STEP1, '🎁')} {_restore_escaped_tg_emoji_html((g.get('title') or '').strip())}\n\n"
            f"{_restore_escaped_tg_emoji_html(g.get('description') or '')}\n\n"
            f"{_tg_pe(_PE_POST_WINNERS, '🏆')} Победителей: {g['winners_count']}\n"
            f"{_tg_pe(_PE_POST_DEADLINE, '⏳')} До: {_format_ends_at_user(g['ends_at'])} (МСК)\n\n"
            f"{_tg_pe(_PE_POST_CTA, '🎯')} Жми «Участвовать» под этим постом и выполни все условия, "
            "чтобы попасть в розыгрыш"
        )
    return _sanitize_tg_emoji_html_for_send(raw)


async def _edit_giveaway_public_message(
    bot: Bot, chat_id: int, message_id: int, g: dict[str, Any]
) -> None:
    """Обновляет текст/подпись одного поста розыгрыша в канале (основной или дубль)."""
    card = _giveaway_public_caption(g)
    kb = await _build_public_participant_kb(bot, g)
    kind = (g.get("post_media_kind") or "").strip()
    fid = g.get("post_media_file_id")
    is_channel = False
    try:
        ch = await bot.get_chat(int(chat_id))
        is_channel = getattr(ch, "type", "") == "channel"
    except Exception:
        is_channel = False
    card_text, card_entities = _html_to_text_and_custom_entities(card)
    try:
        if kind == "photo" and fid:
            if is_channel:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=card_text,
                    caption_entities=card_entities or None,
                    reply_markup=kb,
                )
            else:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=card,
                    reply_markup=kb,
                    parse_mode="HTML",
                )
        elif kind == "animation" and fid:
            if is_channel:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=card_text,
                    caption_entities=card_entities or None,
                    reply_markup=kb,
                )
            else:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=card,
                    reply_markup=kb,
                    parse_mode="HTML",
                )
        elif kind == "video" and fid:
            if is_channel:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=card_text,
                    caption_entities=card_entities or None,
                    reply_markup=kb,
                )
            else:
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=card,
                    reply_markup=kb,
                    parse_mode="HTML",
                )
        else:
            if is_channel:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=card_text,
                    entities=card_entities or None,
                    reply_markup=kb,
                    disable_web_page_preview=True,
                    link_preview_options=_LINK_PREVIEW_OFF,
                )
            else:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=card,
                    reply_markup=kb,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    link_preview_options=_LINK_PREVIEW_OFF,
                )
    except TelegramBadRequest as e:
        log.debug("edit giveaway public msg %s/%s: %s", chat_id, message_id, e)


async def _refresh_giveaway_public_posts(bot: Bot, g: dict[str, Any]) -> None:
    """Перерисовывает подпись во всех постах (основной + зеркала)."""
    pc, pm = g.get("post_chat_id"), g.get("post_message_id")
    if pc and pm:
        await _edit_giveaway_public_message(bot, int(pc), int(pm), g)
    for ent in _mirror_posts_list(g):
        try:
            await _edit_giveaway_public_message(
                bot, int(ent["chat_id"]), int(ent["message_id"]), g
            )
        except Exception as e:
            log.debug("refresh mirror: %s", e)


async def _send_giveaway_announcement_to_chat(
    bot: Bot, g: dict[str, Any], publish_cid: int
) -> Message:
    """Один пост (основной чат): текст или медиа с подписью."""
    sent_userbot = await _send_via_userbot_if_possible(bot, g, publish_cid)
    if sent_userbot is not None:
        # Текст от userbot; inline-клавиатуру добавляет бот (как у «Участвовать»).
        try:
            kb_main = await _build_public_participant_kb(bot, g)
            if _is_lottery(g):
                try:
                    await bot.edit_message_reply_markup(
                        chat_id=int(sent_userbot["chat_id"]),
                        message_id=int(sent_userbot["message_id"]),
                        reply_markup=kb_main,
                    )
                except Exception:
                    log.warning(
                        "lottery keyboard edit failed for userbot post %s/%s",
                        sent_userbot["chat_id"],
                        sent_userbot["message_id"],
                    )
            else:
                try:
                    await bot.edit_message_reply_markup(
                        chat_id=int(sent_userbot["chat_id"]),
                        message_id=int(sent_userbot["message_id"]),
                        reply_markup=kb_main,
                    )
                except Exception:
                    await bot.send_message(
                        int(sent_userbot["chat_id"]),
                        "🎯 Участвовать в розыгрыше:",
                        reply_markup=kb_main,
                        disable_web_page_preview=True,
                        link_preview_options=_LINK_PREVIEW_OFF,
                    )
        except Exception as e:
            log.warning("bot fallback button send failed for %s: %s", sent_userbot["chat_id"], e)
        class _M:
            pass
        m = _M()
        m.chat = _M()
        m.chat.id = int(sent_userbot["chat_id"])
        m.message_id = int(sent_userbot["message_id"])
        return m  # type: ignore[return-value]
    card = _giveaway_public_caption(g)
    kb_main = await _build_public_participant_kb(bot, g)
    kind = (g.get("post_media_kind") or "").strip()
    fid = g.get("post_media_file_id")
    is_channel = False
    try:
        ch = await bot.get_chat(int(publish_cid))
        is_channel = getattr(ch, "type", "") == "channel"
    except Exception:
        is_channel = False
    card_text, card_entities = _html_to_text_and_custom_entities(card)
    if kind == "photo" and fid:
        if is_channel:
            return await bot.send_photo(
                publish_cid,
                fid,
                caption=card_text,
                caption_entities=card_entities or None,
                reply_markup=kb_main,
            )
        return await bot.send_photo(
            publish_cid, fid, caption=card, reply_markup=kb_main, parse_mode="HTML"
        )
    if kind == "animation" and fid:
        if is_channel:
            return await bot.send_animation(
                publish_cid,
                fid,
                caption=card_text,
                caption_entities=card_entities or None,
                reply_markup=kb_main,
            )
        return await bot.send_animation(
            publish_cid, fid, caption=card, reply_markup=kb_main, parse_mode="HTML"
        )
    if kind == "video" and fid:
        if is_channel:
            return await bot.send_video(
                publish_cid,
                fid,
                caption=card_text,
                caption_entities=card_entities or None,
                reply_markup=kb_main,
            )
        return await bot.send_video(
            publish_cid, fid, caption=card, reply_markup=kb_main, parse_mode="HTML"
        )
    if is_channel:
        return await bot.send_message(
            publish_cid,
            card_text,
            entities=card_entities or None,
            reply_markup=kb_main,
            disable_web_page_preview=True,
            link_preview_options=_LINK_PREVIEW_OFF,
        )
    return await bot.send_message(
        publish_cid,
        card,
        reply_markup=kb_main,
        parse_mode="HTML",
        disable_web_page_preview=True,
        link_preview_options=_LINK_PREVIEW_OFF,
    )


async def _publish_draft_giveaway_live(
    bot: Bot, query: CallbackQuery, state: FSMContext, gid: int, uid: int
) -> bool:
    """Публикует черновик в канал(ы). True — успех, False — уже показали ошибку."""
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g or not _is_giveaway_owner(g, uid):
        await query.answer("Нет доступа", show_alert=True)
        return False
    if g.get("status") != "draft":
        await query.answer("Этот розыгрыш уже не черновик.", show_alert=True)
        return False
    if _is_lottery(g):
        raw = (g.get("lottery_winning_tickets_json") or "").strip()
        has_winners = False
        if raw:
            try:
                has_winners = bool(json.loads(raw))
            except Exception:
                has_winners = False
        if not has_winners:
            total = max(1, min(100, int(g.get("lottery_ticket_count") or 1)))
            want = max(1, min(int(g.get("winners_count") or 1), total))
            winners = secrets.SystemRandom().sample(list(range(1, total + 1)), want)
            async with _pg_conn() as dbw:
                await dbw.execute(
                    "UPDATE giveaways SET lottery_winning_tickets_json = ? WHERE id = ?",
                    (json.dumps(winners, ensure_ascii=False), gid),
                )
                await dbw.commit()
            async with _pg_conn() as dbr:
                g = await get_giveaway(dbr, gid)
    publish_ids = _publish_chat_ids_from_g(g)
    if not publish_ids:
        await query.answer("Не указаны каналы для публикации.", show_alert=True)
        return False
    for cid in publish_ids:
        perm = await _validate_publish_target_for_send(bot, int(cid))
        if perm:
            await query.answer(perm.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
            return False
    sent: Optional[Message] = None
    mirror_posts: list[dict[str, Any]] = []
    for cid in publish_ids:
        try:
            sm = await _send_giveaway_announcement_to_chat(bot, g, int(cid))
            if sent is None:
                sent = sm
            else:
                mirror_posts.append({"chat_id": sm.chat.id, "message_id": sm.message_id})
        except TelegramBadRequest as e:
            log.warning("publish giveaway post %s: %s", cid, e)
    if sent is None:
        await query.answer("Пост не отправился ни в один канал.", show_alert=True)
        return False

    async with _pg_conn() as db:
        await _crosspost_pending_delete_for_giveaway_db(db, gid)
        await db.execute(
            """UPDATE giveaways SET status = 'active', post_chat_id = ?, post_message_id = ?,
               mirror_posts_json = ? WHERE id = ?""",
            (
                sent.chat.id,
                sent.message_id,
                json.dumps(mirror_posts, ensure_ascii=False),
                gid,
            ),
        )
        await db.commit()

    await state.clear()
    await state.update_data(ui_chat_id=query.message.chat.id, ui_message_id=query.message.message_id)
    await _render_callback_screen(
        query,
        state,
        bot,
        f"✅ Розыгрыш <b>#{gid}</b> опубликован.\n\nУправление:",
        _admin_kb_giveaway(
            gid, can_edit_description=(not _is_lottery(g))
        ),
    )
    return True


def _draft_channels_pick_kb(gid: int, *, show_cancel_pending: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="➕ Добавить / пригласить каналы",
                callback_data=f"dchex:{gid}",
            )
        ],
    ]
    if show_cancel_pending:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🛑 Отменить приглашения",
                    callback_data=f"dchpcan:{gid}",
                )
            ],
        )
    rows.append([_ikb_back(f"ddraft:{gid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _channels_crosswait_kb_draft(pending_id: int, gid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="▶️ Проверить согласия",
                    callback_data=f"xcont:{pending_id}",
                )
            ],
            [_ikb_back(f"ddraft:{gid}")],
        ]
    )


_MEDIA_STEP_INTRO = (
    _giveaway_step6_media_intro_html()
)


def _media_prompt_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да", callback_data="wmedia:yes"),
                InlineKeyboardButton(text="❌ Нет", callback_data="wmedia:no"),
            ],
            *_wizard_nav_kb(show_back=True).inline_keyboard,
        ]
    )


_MEDIA_FILE_PROMPT = (
    "✨ <b>Шаг 6 из 6</b>\n\n"
    "Пришли одно <b>фото</b>, <b>GIF</b> или <b>видео</b> — оно пойдёт в пост.\n"
    "(Документы и альбомы не подойдут.)"
)


def _delete_confirm_kb(gid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить без восстановления", callback_data=f"del_yes:{gid}")],
            [InlineKeyboardButton(text="◀️ Не надо", callback_data=f"ag:{gid}")],
        ]
    )


def _kb_back_admin_giveaway(gid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [_ikb_back(f"ag:{gid}")],
        ]
    )


def _list_giveaways_kb(items: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for g in items[:20]:
        t = _giveaway_title_button_caption(g)
        if len(t) > 36:
            t = t[:35] + "…"
        rows.append([InlineKeyboardButton(text=f"🎁 {t}", callback_data=f"g:{g['id']}")])
    rows.append([_ikb_menu_my()])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _adm_list_kb(items: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for g in items[:20]:
        t = _giveaway_title_button_caption(g)
        if len(t) > 22:
            t = t[:21] + "…"
        st = g.get("status")
        if st == "active":
            badge = "▸"
        elif st == "draft":
            badge = "📝"
        else:
            badge = "✓"
        label = f"{badge} #{g['id']} {t}"
        if len(label) > 40:
            label = label[:39] + "…"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"ag:{g['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _my_joined_list_kb(own_items: list[dict[str, Any]], joined_items: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    own_count = len(own_items)
    joined_count = len(joined_items)
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=f"Созданные тобой ({own_count})",
                callback_data="my:own",
                **_pe_icon(_PE_MY_OWN),
            )
        ],
        [
            InlineKeyboardButton(
                text=f"Ты участвуешь ({joined_count})",
                callback_data="my:joined",
                **_pe_icon(_PE_MY_JOINED),
            )
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


_MY_PAGE_SIZE = 5


def _page_bounds(total: int, page: int, page_size: int = _MY_PAGE_SIZE) -> tuple[int, int, int]:
    total_pages = max(1, (max(0, total) + page_size - 1) // page_size)
    p = min(max(1, int(page or 1)), total_pages)
    start = (p - 1) * page_size
    end = start + page_size
    return p, total_pages, start


def _pager_row(prefix: str, page: int, total_pages: int) -> list[InlineKeyboardButton]:
    left = max(1, page - 1)
    right = min(total_pages, page + 1)
    return [
        InlineKeyboardButton(text="◀️", callback_data=f"{prefix}:p:{left}"),
        InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"),
        InlineKeyboardButton(text="▶️", callback_data=f"{prefix}:p:{right}"),
    ]


def _my_own_items_kb(items: list[dict[str, Any]], active_tab: str, page: int = 1) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    is_d = active_tab == "draft"
    is_a = active_tab == "active"
    is_f = active_tab == "finished"
    rows.append(
        [
            InlineKeyboardButton(
                text="Черновик" + (" ✅" if is_d else ""),
                callback_data="my:own:draft",
                **_pe_icon(_PE_OWN_DRAFT),
            ),
            InlineKeyboardButton(
                text="Идёт" + (" ✅" if is_a else ""),
                callback_data="my:own:active",
                **_pe_icon(_PE_OWN_ACTIVE),
            ),
            InlineKeyboardButton(
                text="Завершён" + (" ✅" if is_f else ""),
                callback_data="my:own:finished",
                **_pe_icon(_PE_OWN_FINISHED),
            ),
        ]
    )
    cur_page, total_pages, start = _page_bounds(len(items), page)
    for g in items[start:start + _MY_PAGE_SIZE]:
        t = _giveaway_title_button_caption(g)
        if len(t) > 30:
            t = t[:29] + "…"
        st = g.get("status")
        badge = "▸" if st == "active" else "📝" if st == "draft" else "✓"
        rows.append([InlineKeyboardButton(text=f"{badge} #{g['id']} {t}", callback_data=f"ag:{g['id']}")])
    if len(items) > _MY_PAGE_SIZE:
        rows.append([* _pager_row(f"my:own:{active_tab}", cur_page, total_pages)])
    rows.append([_ikb_back("menu:my")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _my_joined_items_kb(joined_items: list[dict[str, Any]], page: int = 1) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    cur_page, total_pages, start = _page_bounds(len(joined_items), page)
    for g in joined_items[start:start + _MY_PAGE_SIZE]:
        gid = int(g["id"])
        t = _giveaway_title_button_caption(g)
        if len(t) > 30:
            t = t[:29] + "…"
        st = g.get("status")
        badge = "▸" if st == "active" else "📝" if st == "draft" else "✓"
        rows.append([InlineKeyboardButton(text=f"{badge} #{gid} {t}", callback_data=f"g:{gid}")])
    if len(joined_items) > _MY_PAGE_SIZE:
        rows.append([* _pager_row("my:joined", cur_page, total_pages)])
    rows.append([_ikb_back("menu:my")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _edit_screen(
    bot: Bot,
    chat_id: int,
    message_id: int,
    text: str,
    kb: Optional[InlineKeyboardMarkup] = None,
    parse_mode: str | None = None,
) -> bool:
    try:
        kw: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": kb,
        }
        if parse_mode is not None:
            kw["parse_mode"] = parse_mode
        await bot.edit_message_text(text, **kw)
        return True
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return True
        log.warning("edit_message_text failed: %s", e)
        return False
    except Exception as e:
        log.debug("edit_message_text: %s", e)
        return False


async def _remember_ui(state: FSMContext, chat_id: int, message_id: int) -> None:
    await state.update_data(ui_chat_id=chat_id, ui_message_id=message_id)


async def _wizard_edit(bot: Bot, state: FSMContext, text: str, kb: InlineKeyboardMarkup) -> None:
    data = await state.get_data()
    cid, mid = data.get("ui_chat_id"), data.get("ui_message_id")
    if cid is None or mid is None:
        return
    await _edit_screen(bot, int(cid), int(mid), text, kb, parse_mode="HTML")


async def _wizard_advance_after_valid_input(
    bot: Bot,
    state: FSMContext,
    user_message: Message,
    text: str,
    kb: InlineKeyboardMarkup,
) -> None:
    """Следующий шаг — новое сообщение бота (без reply, чтобы не цитировать удалённый ввод); ввод и старый экран удаляются."""
    data = await state.get_data()
    cid = data.get("ui_chat_id")
    chat_id = int(cid) if cid is not None else user_message.chat.id
    old_mid = data.get("ui_message_id")
    sent = await bot.send_message(
        chat_id,
        text,
        reply_markup=kb,
        parse_mode="HTML",
    )
    await _remember_ui(state, chat_id, sent.message_id)
    try:
        await user_message.delete()
    except Exception as e:
        log.debug("delete user wizard input: %s", e)
    if old_mid is not None and int(old_mid) != sent.message_id:
        try:
            await bot.delete_message(chat_id, int(old_mid))
        except Exception as e:
            log.debug("delete previous wizard screen: %s", e)


async def _render_callback_screen(
    query: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    text: str,
    kb: InlineKeyboardMarkup,
) -> None:
    await query.answer()
    cid, mid = query.message.chat.id, query.message.message_id
    ok = await _edit_screen(bot, cid, mid, text, kb, parse_mode="HTML")
    if ok:
        await _remember_ui(state, cid, mid)
        return
    try:
        await bot.delete_message(cid, mid)
    except Exception as e:
        log.debug("replace screen delete old msg: %s", e)
    sent = await bot.send_message(cid, text, reply_markup=kb, parse_mode="HTML")
    await _remember_ui(state, sent.chat.id, sent.message_id)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, bot: Bot) -> None:
    payload = ""
    if message.text and " " in message.text:
        payload = message.text.split(maxsplit=1)[1].strip()

    uid = message.from_user.id if message.from_user else 0
    deep_giveaway_id: Optional[int] = None
    deep_referrer_id: Optional[int] = None
    data = await state.get_data()
    old_cid, old_mid = data.get("ui_chat_id"), data.get("ui_message_id")
    await state.clear()

    async with _pg_conn() as db:
        cur = await db.execute("SELECT 1 FROM bot_users WHERE user_id = ?", (uid,))
        existed = await cur.fetchone()

        if not existed:
            await db.execute(
                "INSERT INTO bot_users (user_id, first_seen_at, username) VALUES (?, ?, ?)",
                (uid, _utc_now().isoformat(), message.from_user.username if message.from_user else None),
            )
        else:
            await db.execute(
                "UPDATE bot_users SET username = ? WHERE user_id = ?",
                (message.from_user.username if message.from_user else None, uid),
            )

        if payload.startswith(REF_PREFIX):
            rest = payload[len(REF_PREFIX) :]
            parts = rest.split("_", 1)
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                gid, rid = int(parts[0]), int(parts[1])
                if rid != uid:
                    deep_giveaway_id = gid
                    deep_referrer_id = rid
                    g = await get_giveaway(db, gid)
                    if g and g.get("referral_enabled") and g.get("status") == "active":
                        curp = await db.execute(
                            "SELECT 1 FROM participants WHERE giveaway_id = ? AND user_id = ?",
                            (gid, uid),
                        )
                        if not await curp.fetchone():
                            await db.execute(
                                """INSERT INTO pending_referral (user_id, giveaway_id, referrer_id) VALUES (?, ?, ?)
                                ON CONFLICT (user_id) DO UPDATE SET
                                  giveaway_id = EXCLUDED.giveaway_id,
                                  referrer_id = EXCLUDED.referrer_id""",
                                (uid, gid, rid),
                            )

        await db.commit()

    if message.chat.type != ChatType.PRIVATE:
        return
    if old_cid is not None and old_mid is not None:
        try:
            await bot.delete_message(int(old_cid), int(old_mid))
        except Exception:
            pass
    if payload.startswith(JOIN_PREFIX):
        rest = payload[len(JOIN_PREFIX) :]
        if rest.isdigit():
            deep_giveaway_id = int(rest)
    if deep_giveaway_id is not None:
        async with _pg_conn() as db:
            g = await get_giveaway(db, int(deep_giveaway_id))
            if g and g.get("status") == "active":
                if message.from_user:
                    await _auto_join_if_possible(
                        bot,
                        db,
                        g,
                        message.from_user,
                        referrer_id=deep_referrer_id,
                    )
                text, kb = await _giveaway_dm_status_text_and_kb(bot, db, g, uid)
                sent = await bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML")
                await _remember_ui(state, sent.chat.id, sent.message_id)
                return
        sent = await bot.send_message(
            uid,
            "❌ Этот розыгрыш недоступен.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [_ikb_menu_my()],
                ]
            ),
        )
        await _remember_ui(state, sent.chat.id, sent.message_id)
        return
    await state.set_state(default_state)
    await _send_fresh_main_menu_private(bot, message.chat.id, state)


@router.callback_query(F.data == "menu:main")
async def cb_menu_main(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    cid, mid = query.message.chat.id, query.message.message_id
    chat_type = query.message.chat.type
    await state.clear()
    await state.set_state(default_state)
    if chat_type == ChatType.PRIVATE:
        await query.answer()
        try:
            await bot.delete_message(cid, mid)
        except Exception:
            pass
        await _send_fresh_main_menu_private(bot, cid, state)
        return
    await query.answer("Открой бота в личных сообщениях — там меню.", show_alert=True)
    await _strip_reply_keyboard_in_chat(bot, cid)
    try:
        await bot.delete_message(cid, mid)
    except Exception:
        pass
    await state.update_data(ui_chat_id=None, ui_message_id=None)


@router.callback_query(F.data == "menu:saved_chats")
async def cb_menu_saved_chats(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await _open_saved_chats_panel(query, state, bot)


@router.callback_query(F.data == "menu:add_channel")
async def cb_menu_add_channel(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    await query.answer()
    cid, mid = query.message.chat.id, query.message.message_id
    try:
        await bot.delete_message(cid, mid)
    except Exception:
        pass
    await _send_add_channel_invite(bot, uid)


@router.callback_query(F.data == "menu:add_group")
async def cb_menu_add_group(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    await query.answer()
    cid, mid = query.message.chat.id, query.message.message_id
    try:
        await bot.delete_message(cid, mid)
    except Exception:
        pass
    await _send_add_group_invite(bot, uid)


async def _send_saved_chats_panel(bot: Bot, state: FSMContext, user_id: int) -> None:
    async with _pg_conn() as db:
        body, kb = await _saved_chats_list_body_and_keyboard(db, user_id)
    sent_panel = await bot.send_message(user_id, body, reply_markup=kb, parse_mode="HTML")
    await state.update_data(
        saved_chats_msg_chat_id=sent_panel.chat.id,
        saved_chats_msg_message_id=sent_panel.message_id,
    )


async def _open_saved_chats_panel(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    await query.answer()
    cid, mid = query.message.chat.id, query.message.message_id
    try:
        await bot.delete_message(cid, mid)
    except Exception:
        pass
    await _send_saved_chats_panel(bot, state, uid)


@router.callback_query(F.data.startswith("delsc:"))
async def cb_del_saved_chat(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user or not query.message:
        await query.answer()
        return
    uid = query.from_user.id
    try:
        chat_id = int(query.data.split(":", 1)[1])
    except ValueError:
        await query.answer()
        return
    async with _pg_conn() as db:
        await delete_saved_chat(db, uid, chat_id)
    await query.answer("Убрали из списка")
    mchat_id = query.message.chat.id
    mid = query.message.message_id
    await _refresh_saved_chats_panel_message(bot, state, uid, mchat_id, mid)


@router.my_chat_member()
async def on_bot_my_chat_member(
    event: ChatMemberUpdated, bot: Bot, dispatcher: Dispatcher
) -> None:
    """Когда бота добавили в канал/группу (в т.ч. по ссылке 🚀), сохраняем чат для того, кто это сделал."""
    me = await bot.get_me()
    mem_u = event.new_chat_member.user
    if mem_u is None or mem_u.id != me.id:
        return
    actor = event.from_user
    if actor is None:
        return
    chat = event.chat
    if chat.type not in ("channel", "group", "supergroup"):
        return
    new = event.new_chat_member
    if new.status in ("left", "kicked"):
        # Bot removed from chat: remove stale entries from saved chats.
        try:
            async with _pg_conn() as db:
                await db.execute("DELETE FROM saved_chats WHERE chat_id = ?", (chat.id,))
                await db.commit()
        except Exception:
            log.exception("saved_chats cleanup via my_chat_member")
        return
    ctype = "channel" if chat.type == "channel" else "group"
    title = chat.title or (f"@{chat.username}" if getattr(chat, "username", None) else str(chat.id))
    uname = chat.username if getattr(chat, "username", None) else None
    try:
        async with _pg_conn() as db:
            await upsert_saved_chat(db, actor.id, chat.id, ctype, title, uname)
    except Exception:
        log.exception("saved_chats upsert via my_chat_member")
        return
    if ctype == "channel":
        _kickoff_userbot_join(int(chat.id), uname)
    if (
        new.status == "administrator"
        and _userbot_configured()
        and _userbot_auto_promote_enabled()
    ):
        asyncio.create_task(
            _promote_userbot_after_bot_added(
                bot,
                chat,
                chat_public_username=uname,
                chat_kind=ctype,
            )
        )
    key = StorageKey(bot_id=bot.id, chat_id=actor.id, user_id=actor.id)
    ctx = FSMContext(storage=dispatcher.storage, key=key)
    data = await ctx.get_data()
    sc_cid = data.get("saved_chats_msg_chat_id")
    sc_mid = data.get("saved_chats_msg_message_id")
    if sc_cid is not None and sc_mid is not None:
        await _refresh_saved_chats_panel_message(
            bot, ctx, actor.id, int(sc_cid), int(sc_mid)
        )
    await _refresh_publish_step_message_if_open(bot, ctx, actor.id)


@router.callback_query(F.data == "menu:my")
async def cb_menu_my(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    cid, mid = query.message.chat.id, query.message.message_id
    await state.clear()
    await state.update_data(ui_chat_id=cid, ui_message_id=mid)
    async with _pg_conn() as db:
        own_items = await list_my_giveaways(db, uid)
        joined_items = await list_joined_giveaways(db, uid)
        text = (
            f"{_tg_pe(_PE_MENU_MY_GIVEAWAYS, '📁')} <b>Твои розыгрыши</b>\n\n"
            "Открой нужную папку:"
        )
    kb = _my_joined_list_kb(own_items, joined_items)
    await _render_callback_screen(query, state, bot, text, kb)


def _filter_own_items_by_status(own_items: list[dict[str, Any]], tab: str) -> list[dict[str, Any]]:
    if tab == "draft":
        return [g for g in own_items if g.get("status") == "draft"]
    if tab == "active":
        return [g for g in own_items if g.get("status") == "active"]
    if tab == "finished":
        return [g for g in own_items if g.get("status") not in {"draft", "active"}]
    return own_items


@router.callback_query(F.data == "my:own")
async def cb_my_folder_own(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    async with _pg_conn() as db:
        own_items = await list_my_giveaways(db, uid)
    filtered = _filter_own_items_by_status(own_items, "draft")
    text = f"{_tg_pe(_PE_MY_OWN, '🗂')} <b>Созданные тобой — Черновик</b>"
    if not filtered:
        text += "\n\n<i>Пока пусто.</i>"
    await _render_callback_screen(query, state, bot, text, _my_own_items_kb(filtered, "draft", 1))


@router.callback_query(F.data.startswith("my:own:draft"))
async def cb_my_folder_own_draft(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    page = 1
    if query.data and ":p:" in query.data:
        try:
            page = int(query.data.rsplit(":p:", 1)[-1])
        except Exception:
            page = 1
    async with _pg_conn() as db:
        own_items = await list_my_giveaways(db, uid)
    filtered = _filter_own_items_by_status(own_items, "draft")
    text = f"{_tg_pe(_PE_MY_OWN, '🗂')} <b>Созданные тобой — Черновик</b>"
    if not filtered:
        text += "\n\n<i>Пока пусто.</i>"
    await _render_callback_screen(query, state, bot, text, _my_own_items_kb(filtered, "draft", page))


@router.callback_query(F.data.startswith("my:own:active"))
async def cb_my_folder_own_active(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    page = 1
    if query.data and ":p:" in query.data:
        try:
            page = int(query.data.rsplit(":p:", 1)[-1])
        except Exception:
            page = 1
    async with _pg_conn() as db:
        own_items = await list_my_giveaways(db, uid)
    filtered = _filter_own_items_by_status(own_items, "active")
    text = f"{_tg_pe(_PE_MY_OWN, '🗂')} <b>Созданные тобой — Идёт</b>"
    if not filtered:
        text += "\n\n<i>Пока пусто.</i>"
    await _render_callback_screen(query, state, bot, text, _my_own_items_kb(filtered, "active", page))


@router.callback_query(F.data.startswith("my:own:finished"))
async def cb_my_folder_own_finished(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    page = 1
    if query.data and ":p:" in query.data:
        try:
            page = int(query.data.rsplit(":p:", 1)[-1])
        except Exception:
            page = 1
    async with _pg_conn() as db:
        own_items = await list_my_giveaways(db, uid)
    filtered = _filter_own_items_by_status(own_items, "finished")
    text = f"{_tg_pe(_PE_MY_OWN, '🗂')} <b>Созданные тобой — Завершён</b>"
    if not filtered:
        text += "\n\n<i>Пока пусто.</i>"
    await _render_callback_screen(query, state, bot, text, _my_own_items_kb(filtered, "finished", page))


@router.callback_query(F.data.startswith("my:joined"))
async def cb_my_folder_joined(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    page = 1
    if query.data and ":p:" in query.data:
        try:
            page = int(query.data.rsplit(":p:", 1)[-1])
        except Exception:
            page = 1
    async with _pg_conn() as db:
        joined_items = await list_joined_giveaways(db, uid)
    text = f"{_tg_pe(_PE_MY_JOINED, '🎟')} <b>Ты участвуешь</b>\n<code>▸</code> идёт, <code>📝</code> черновик, <code>✓</code> завершён."
    if not joined_items:
        text += "\n\n<i>Пока пусто.</i>"
    await _render_callback_screen(
        query,
        state,
        bot,
        text,
        _my_joined_items_kb(joined_items, page),
    )


@router.callback_query(F.data == "list_giveaways")
async def cb_list_giveaways(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await cb_menu_my(query, state, bot)


@router.callback_query(F.data.startswith("g:"))
async def cb_giveaway_open(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    gid = int(query.data.split(":")[1])
    uid = query.from_user.id if query.from_user else 0
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
        part = None
        if g and g["status"] == "active" and uid:
            cur = await db.execute(
                "SELECT 1 FROM participants WHERE giveaway_id = ? AND user_id = ?",
                (gid, uid),
            )
            part = await cur.fetchone()
    if not g or g["status"] != "active":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [_ikb_menu_my()],
            ]
        )
        await _render_callback_screen(
            query, state, bot, "❌ Этот розыгрыш уже закончили или его сняли.", kb
        )
        return
    if query.message.chat.type == ChatType.PRIVATE:
        async with _pg_conn() as db:
            text, kb = await _giveaway_dm_status_text_and_kb(bot, db, g, uid)
        await _render_callback_screen(query, state, bot, text, kb)
        return
    kb = (
        build_participant_kb_after_join(gid)
        if part
        else build_participant_kb_from_giveaway(g)
    )
    await _render_callback_screen(
        query,
        state,
        bot,
        _giveaway_user_text(g),
        kb,
    )


@router.callback_query(F.data.startswith("ag:"))
async def cb_admin_giveaway_open(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    if await state.get_state() == EditPublishedDescription.text.state:
        await state.clear()
        await state.set_state(default_state)
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g:
        await query.answer("Такого розыгрыша нет", show_alert=True)
        return
    if not _is_giveaway_owner(g, uid):
        await query.answer("Это чужой розыгрыш", show_alert=True)
        return
    if g.get("status") == "draft":
        await _render_draft_giveaway_screen(query, state, bot, g)
        return
    st = g["status"]
    st_ru = "идёт приём участников" if st == "active" else "уже завершён" if st == "finished" else st
    title_show = _user_stored_html_as_safe_plain_escape(g.get("title") or "", max_len=400)
    text = (
        f"🛠 <b>Розыгрыш #{gid}</b>\n"
        f"{title_show}\n\n"
        f"⏳ До: {_format_ends_at_user(g['ends_at'])} (МСК)\n"
        f"Сейчас: {st_ru}\n\n"
        "<i>Это меню видишь только ты — ты автор.</i>"
    )
    can_edit = st == "active" and not _is_lottery(g)
    await _render_callback_screen(
        query, state, bot, text, _admin_kb_giveaway(gid, can_edit_description=can_edit)
    )


@router.callback_query(F.data.startswith("aedesc:"))
async def cb_edit_published_desc_start(
    query: CallbackQuery, state: FSMContext, bot: Bot
) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g or not _is_giveaway_owner(g, uid):
        await query.answer("Нет доступа.", show_alert=True)
        return
    if _is_lottery(g):
        await query.answer("Для лотереи изменение описания отключено.", show_alert=True)
        return
    if g.get("status") != "active":
        await query.answer("Менять описание можно только у идущего розыгрыша.", show_alert=True)
        return
    await state.set_state(EditPublishedDescription.text)
    await state.update_data(edit_published_gid=gid)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"ag:{gid}")],
        ]
    )
    await _render_callback_screen(
        query,
        state,
        bot,
        f"{_tg_pe(_PE_ADMIN_EDIT_DESC, '✏️')} <b>Новое описание в посте</b>\n\n"
        "Пришли <b>одним сообщением</b> текст описания приза - как блок текста под названием в посте "
        "(название и дата окончания в посте не меняются).\n\n"
        "Можно оформление из Telegram (жирный, ссылки).",
        kb,
    )


@router.message(EditPublishedDescription.text, F.chat.type == ChatType.PRIVATE)
async def sg_edit_published_description(message: Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    uid = message.from_user.id
    data = await state.get_data()
    egid = data.get("edit_published_gid")
    if egid is None:
        await state.clear()
        return
    gid = int(egid)
    text_html = _message_html_preserve_custom_emoji(message)
    if not text_html.strip():
        await message.answer("Нужен непустой текст.")
        return
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
        if not g or not _is_giveaway_owner(g, uid) or g.get("status") != "active":
            await state.clear()
            await message.answer("Розыгрыш недоступен или уже не активен.")
            return
        await db.execute("UPDATE giveaways SET description = ? WHERE id = ?", (text_html, gid))
        await db.commit()
        g = await get_giveaway(db, gid)
    await state.clear()
    await _refresh_giveaway_public_posts(bot, g)
    try:
        await message.delete()
    except Exception as e:
        log.debug("delete edit desc input: %s", e)
    sent = await message.answer(
        "✅ Описание в посте (и дублях, если есть) обновлено.",
        reply_markup=_admin_kb_giveaway(gid, can_edit_description=True),
    )
    await _remember_ui(state, sent.chat.id, sent.message_id)


@router.callback_query(F.data.startswith("del_ask:"))
async def cb_delete_ask(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g:
        await query.answer("Такого розыгрыша нет", show_alert=True)
        return
    if not _is_giveaway_owner(g, uid):
        await query.answer("Это чужой розыгрыш", show_alert=True)
        return
    title_show = _user_stored_html_as_safe_plain_escape(g.get("title") or "", max_len=400)
    await _render_callback_screen(
        query,
        state,
        bot,
        f"{_tg_pe(_PE_DRAFT_DELETE, '🗑')} <b>Удалить розыгрыш #{gid}?</b>\n\n"
        f"{title_show}\n\n"
        "Все участники и настройки пропадут из бота безвозвратно. "
        "Пост в канале или группе бот постарается удалить — если у него хватит прав.",
        _delete_confirm_kb(gid),
    )


@router.callback_query(F.data.startswith("del_yes:"))
async def cb_delete_yes(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g:
        await query.answer("Уже удалили или такого не было", show_alert=True)
        return
    if not _is_giveaway_owner(g, uid):
        await query.answer("Это чужой розыгрыш", show_alert=True)
        return

    pc, pm = g.get("post_chat_id"), g.get("post_message_id")
    if pc and pm:
        try:
            await bot.delete_message(int(pc), int(pm))
        except Exception as e:
            log.debug("delete giveaway post: %s", e)
    rc, rm = g.get("results_chat_id"), g.get("results_message_id")
    if rc and rm:
        try:
            await bot.delete_message(int(rc), int(rm))
        except Exception as e:
            log.debug("delete results post: %s", e)
    for ent in _mirror_posts_list(g):
        try:
            await bot.delete_message(int(ent["chat_id"]), int(ent["message_id"]))
        except Exception as e:
            log.debug("delete mirror giveaway post: %s", e)
    for ent in _mirror_results_list(g):
        try:
            await bot.delete_message(int(ent["chat_id"]), int(ent["message_id"]))
        except Exception as e:
            log.debug("delete mirror results post: %s", e)

    try:
        async with _pg_conn() as db:
            await purge_giveaway_from_db(db, gid)
    except Exception as e:
        log.exception("purge giveaway %s from DB failed: %s", gid, e)
        await query.answer(
            "Не удалось удалить розыгрыш в базе. Повторите или проверьте лог сервера.",
            show_alert=True,
        )
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📂 К списку моих розыгрышей", callback_data="adm_list")],
        ]
    )
    ok_text = (
        f"✅ Розыгрыш <b>#{gid}</b> удалён.\n\n"
        "Другие люди и раньше не могли им управлять — только ты."
    )
    try:
        await _render_callback_screen(query, state, bot, ok_text, kb)
    except TelegramBadRequest as e:
        log.warning("post-delete UI edit/send failed (row already purged): %s", e)
        try:
            await bot.send_message(
                query.message.chat.id,
                "✅ Розыгрыш удалён из базы. Нажми «Мои розыгрыши» в меню.",
                reply_markup=kb,
            )
        except Exception as send_ex:
            log.debug("post-delete plain notify: %s", send_ex)


@router.callback_query(F.data.startswith("reflink:"))
async def cb_reflink(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    gid = int(query.data.split(":")[1])
    me = await bot.get_me()
    uid = query.from_user.id
    link = f"https://t.me/{me.username}?start={REF_PREFIX}{gid}_{uid}"
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g or g["status"] != "active":
        await _render_callback_screen(
            query,
            state,
            bot,
            "Такого активного розыгрыша уже нет.",
            _kb_back_to_giveaway(gid),
        )
        return
    if not g.get("referral_enabled"):
        await _render_callback_screen(
            query,
            state,
            bot,
            "Для этого розыгрыша реферальные ссылки выключены.",
            _kb_back_to_giveaway(gid),
        )
        return
    body = (
        "🔗 <b>Поделись ссылкой — получи шанс побольше</b>\n\n"
        f"<code>{link}</code>\n\n"
        "Если человек впервые зайдёт в бота по ней и потом реально примет участие в этом конкурсе, "
        "у тебя будет на один «билет» больше при случайном выборе победителя."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [_ikb_back(f"g:{gid}")],
        ]
    )
    await _render_callback_screen(query, state, bot, body, kb)


@router.callback_query(F.data.startswith("precheck:"))
async def cb_precheck(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    gid = int(query.data.split(":")[1])
    user = query.from_user
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g or g["status"] != "active":
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [_ikb_menu_my()],
            ]
        )
        await _render_callback_screen(
            query, state, bot, "❌ Этот розыгрыш сейчас недоступен.", kb
        )
        return
    channels = _channels_list(g["channels_json"])
    ok_sub, missing = await check_user_subscribed(bot, user.id, channels)
    parts = []
    if g.get("require_username") and not (user.username):
        parts.append("• В настройках Telegram включи имя пользователя (@username).")
    if not ok_sub:
        miss = await _missing_channels_links_inline_html(bot, missing)
        parts.append("• Подпишись на: " + (miss if miss else ", ".join(f"<code>{m}</code>" for m in missing)))
    if parts:
        text = "⚠️ <b>Пока рано жать «Участвовать»</b>\n\n" + "\n".join(parts)
    else:
        text = "✅ <b>Всё ок</b>\n\nМожно нажимать «Участвовать»."
    kb = _kb_back_to_giveaway(gid)
    await _render_callback_screen(query, state, bot, text, kb)


@router.callback_query(F.data.startswith("join:"))
async def cb_join(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    gid = int(query.data.split(":")[1])
    user = query.from_user
    uid = user.id
    back = _kb_back_to_giveaway(gid)
    is_private = query.message.chat.type == ChatType.PRIVATE
    if not is_private:
        await query.answer("Открой участие через кнопку в посте (она ведёт в личку бота).", show_alert=True)
        return

    async def fail(body_html: str, channel_hint: str) -> None:
        if is_private:
            await _render_callback_screen(query, state, bot, body_html, back)
        else:
            plain = re.sub(r"<[^>]+>", "", body_html)
            plain = html.unescape(plain).strip()[:200]
            await query.answer(plain or channel_hint, show_alert=True)

    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
        if not g or g["status"] != "active":
            await fail(
                "❌ Этот розыгрыш уже не принимает участников.",
                "Розыгрыш не принимает участников.",
            )
            return

        if _utc_now() > _parse_stored_ends_at(g["ends_at"]):
            await fail("⏳ Время вышло — приём заявок закрыт.", "Приём заявок закрыт.")
            return

        channels = _channels_list(g["channels_json"])
        ok_sub, missing = await check_user_subscribed(bot, uid, channels)
        if g.get("require_username") and not user.username:
            await fail(
                "⚠️ Нужен видимый @username в Telegram — включи в настройках профиля и попробуй ещё раз.",
                "Нужен видимый @username в профиле.",
            )
            return
        if not ok_sub:
            miss_lines: list[str] = []
            for m in missing:
                miss_lines.append(await _channel_subscription_line_clickable_html(bot, m))
            miss = "\n".join(x for x in miss_lines if x) or "\n".join(f"• <code>{m}</code>" for m in missing)
            await fail(
                f"{_tg_pe(_PE_DM_SUBSCRIBE, '📣')} Сначала подпишись сюда:\n{miss}",
                "Сначала подпишись на каналы из условий.",
            )
            return

        cur = await db.execute(
            "SELECT 1 FROM participants WHERE giveaway_id = ? AND user_id = ?",
            (gid, uid),
        )
        already = await cur.fetchone()
        if already:
            await db.execute("DELETE FROM pending_referral WHERE user_id = ?", (uid,))
            await db.commit()
            if is_private:
                await query.answer("Ты уже участвуешь.")
                await _edit_screen(
                    bot,
                    query.message.chat.id,
                    query.message.message_id,
                    _giveaway_user_text(g),
                    build_participant_kb_after_join(gid),
                    parse_mode="HTML",
                )
            else:
                await query.answer("Ты уже участвуешь в этом розыгрыше.", show_alert=True)
            return

        await db.execute(
            """INSERT INTO participants (giveaway_id, user_id, username, first_name, joined_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                gid,
                uid,
                user.username,
                user.first_name or "",
                _utc_now().isoformat(),
            ),
        )

        cur = await db.execute("SELECT giveaway_id, referrer_id FROM pending_referral WHERE user_id = ?", (uid,))
        pr = await cur.fetchone()
        if pr and int(pr["giveaway_id"]) == gid and g.get("referral_enabled"):
            referrer_id = int(pr["referrer_id"])
            if referrer_id != uid:
                cur_r = await db.execute(
                    "SELECT 1 FROM participants WHERE giveaway_id = ? AND user_id = ?",
                    (gid, referrer_id),
                )
                ref_is_participant = await cur_r.fetchone()
                await db.execute("DELETE FROM pending_referral WHERE user_id = ?", (uid,))
                if ref_is_participant:
                    try:
                        await db.execute(
                            """INSERT INTO valid_referrals (giveaway_id, referrer_id, referee_id, created_at)
                               VALUES (?, ?, ?, ?)""",
                            (gid, referrer_id, uid, _utc_now().isoformat()),
                        )
                    except Exception:
                        pass
                else:
                    try:
                        await db.execute(
                            """INSERT INTO referral_queue (giveaway_id, referrer_id, referee_id, created_at)
                               VALUES (?, ?, ?, ?) ON CONFLICT (giveaway_id, referee_id) DO NOTHING""",
                            (gid, referrer_id, uid, _utc_now().isoformat()),
                        )
                    except Exception:
                        pass

        cur_q = await db.execute(
            "SELECT referee_id FROM referral_queue WHERE giveaway_id = ? AND referrer_id = ?",
            (gid, uid),
        )
        queued = await cur_q.fetchall()
        for row_q in queued:
            referee_id_q = int(row_q["referee_id"])
            try:
                await db.execute(
                    """INSERT INTO valid_referrals (giveaway_id, referrer_id, referee_id, created_at)
                       VALUES (?, ?, ?, ?)""",
                    (gid, uid, int(referee_id_q), _utc_now().isoformat()),
                )
            except Exception:
                pass
            await db.execute(
                "DELETE FROM referral_queue WHERE giveaway_id = ? AND referrer_id = ? AND referee_id = ?",
                (gid, uid, int(referee_id_q)),
            )

        await db.commit()

    await _send_join_confirmation_dm(bot, g, uid)

    if is_private:
        await query.answer("Готово! Подробности в личке у бота.")
        await _edit_screen(
            bot,
            query.message.chat.id,
            query.message.message_id,
            _giveaway_user_text(g),
            build_participant_kb_after_join(gid),
            parse_mode="HTML",
        )
    else:
        tip = (
            "✅ Ты в розыгрыше! Подтверждение и ссылка для друзей — в личке у бота."
            if g.get("referral_enabled")
            else "✅ Ты в розыгрыше! Подтверждение отправлено в личку бота."
        )
        await query.answer(tip, show_alert=True)


async def _weighted_draw(
    bot: Bot,
    db: PgConnAdapter,
    g: dict[str, Any],
    *,
    exclude_user_ids: Optional[set[int]] = None,
) -> list[int]:
    """Перепроверка подписок и выбор победителей по номерам билетов без повторов."""
    exclude_user_ids = exclude_user_ids or set()
    gid = g["id"]
    channels = _channels_list(g["channels_json"])
    ref_enabled = bool(g.get("referral_enabled"))
    ref_step = max(1, min(5, int(g.get("referral_ticket_step") or 1)))
    cur = await db.execute(
        "SELECT user_id FROM participants WHERE giveaway_id = ?",
        (gid,),
    )
    rows = await cur.fetchall()
    tickets: list[tuple[int, int]] = []
    for row in rows:
        uid = int(row["user_id"])
        if uid in exclude_user_ids:
            continue
        ok, _ = await check_user_subscribed(bot, uid, channels)
        if not ok:
            continue
        ref_count = 0
        if ref_enabled:
            cur2 = await db.execute(
                "SELECT COUNT(*) AS cnt FROM valid_referrals WHERE giveaway_id = ? AND referrer_id = ?",
                (gid, uid),
            )
            ref_count = int((await cur2.fetchone())["cnt"])
        bonus = (ref_count // ref_step) if ref_enabled else 0
        tickets.append((uid, 1 + bonus))

    if not tickets:
        return []

    winners_count = min(int(g["winners_count"]), len(tickets))
    rng = secrets.SystemRandom()
    out: list[int] = []
    items = list(tickets)
    for _ in range(winners_count):
        if not items:
            break
        total_w = sum(w for _, w in items)
        r = rng.randrange(1, total_w + 1)
        acc = 0
        chosen_idx = 0
        for i, (uid, w) in enumerate(items):
            acc += w
            if r <= acc:
                chosen_idx = i
                break
        uid, _w = items.pop(chosen_idx)
        out.append(uid)
    return out


async def _format_winners_lines_html(
    bot: Bot,
    giveaway_id: int,
    winner_ids: list[int],
) -> str:
    """Список победителей HTML: кликабельное имя (ссылка tg://user), текст — @username или имя."""
    lines: list[str] = []
    async with _pg_conn() as db:
        for i, wid in enumerate(winner_ids, 1):
            label = ""
            cur = await db.execute(
                "SELECT username, first_name FROM participants WHERE giveaway_id = ? AND user_id = ?",
                (giveaway_id, wid),
            )
            row = await cur.fetchone()
            if row:
                un = (row["username"] or "").strip()
                fn = (row["first_name"] or "").strip()
                if un:
                    label = f"@{un.lstrip('@')}"
                elif fn:
                    label = fn
            if not label:
                try:
                    ch = await bot.get_chat(wid)
                    if ch.username:
                        label = f"@{ch.username}"
                    else:
                        label = (
                            " ".join(
                                x for x in (ch.first_name or "", ch.last_name or "") if x
                            ).strip()
                        )
                except Exception:
                    pass
            if not label:
                label = f"id {wid}"
            safe = html.escape(label, quote=False)
            lines.append(f'{i}. <a href="tg://user?id={wid}">{safe}</a>')
    return "\n".join(lines)


async def _notify_participants_finished(
    bot: Bot, g: dict[str, Any], winners: list[int]
) -> None:
    gid = int(g["id"])
    title = _restore_escaped_tg_emoji_html((g.get("title") or f"#{gid}").strip() or f"#{gid}")
    creator_id = int(g.get("created_by") or 0)
    recipients: set[int] = set()
    if creator_id:
        recipients.add(creator_id)
    for wid in winners:
        recipients.add(int(wid))
    # Владельцы/админы приглашённых каналов: считаем приглашёнными те каналы,
    # где создатель не владелец.
    for cid in _publish_chat_ids_from_g(g):
        try:
            if creator_id and await _user_is_owner_of_chat(bot, int(cid), creator_id):
                continue
            _err, invitees = await _resolve_crosspost_invite_recipients(bot, int(cid))
            for m in invitees:
                if getattr(m, "user", None) and not m.user.is_bot:
                    recipients.add(int(m.user.id))
        except Exception as e:
            log.debug("finish notify invitees %s: %s", cid, e)
    if not recipients:
        return
    if winners:
        body = await _format_winners_lines_html(bot, gid, winners)
        text = (
            f"🏁 <b>{title}</b> — розыгрыш завершён.\n\n"
            f"Победители:\n{body}"
        )
    else:
        text = (
            f"🏁 <b>{title}</b> — розыгрыш завершён.\n\n"
            "Победителей нет: никто не подошёл по условиям или не участвовал."
        )
    for uid in recipients:
        try:
            await bot.send_message(uid, text, parse_mode="HTML")
        except Exception as e:
            log.debug("notify participant %s gid=%s: %s", uid, gid, e)


@router.callback_query(F.data == "adm_new")
async def cb_adm_new(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    cid, mid = query.message.chat.id, query.message.message_id
    await state.clear()
    await state.update_data(ui_chat_id=cid, ui_message_id=mid)
    await state.set_state(CreateGiveaway.kind)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Розыгрыш", callback_data="adm:type:giveaway")],
            [InlineKeyboardButton(text="🎰 Лотерея", callback_data="adm:type:lottery")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="adm:cancel")],
        ]
    )
    await _render_callback_screen(
        query,
        state,
        bot,
        "🎁 <b>Создать розыгрыш</b>\n\nВыбери тип:",
        kb,
    )


@router.callback_query(StateFilter(CreateGiveaway.kind), F.data.startswith("adm:type:"))
async def cb_adm_type_pick(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    mode = query.data.split(":")[-1]
    if mode not in {"giveaway", "lottery"}:
        await query.answer()
        return
    await state.update_data(giveaway_type=mode)
    if mode == "lottery":
        await state.set_state(CreateGiveaway.media_prompt)
        await _render_callback_screen(
            query,
            state,
            bot,
            _lottery_media_prompt_html(),
            _media_prompt_kb(),
        )
        return
    await state.set_state(CreateGiveaway.title)
    await _render_callback_screen(
        query,
        state,
        bot,
        _giveaway_step1_title_html(),
        _wizard_nav_kb(),
    )


@router.callback_query(F.data == "adm:cancel")
async def cb_adm_cancel(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    st = await state.get_state()
    if st is None or st == default_state:
        await query.answer()
        return
    data = await state.get_data()
    pid = data.get("crosspost_pending_id")
    if pid:
        try:
            async with _pg_conn() as db:
                await _crosspost_pending_delete_bundle(db, int(pid))
        except Exception:
            log.debug("cancel crosspost pending %s", pid)
    cid, mid = query.message.chat.id, query.message.message_id
    await query.answer()
    await state.clear()
    await state.set_state(default_state)
    try:
        await bot.delete_message(cid, mid)
    except Exception:
        pass
    if query.message.chat.type == ChatType.PRIVATE:
        await _send_fresh_main_menu_private(bot, cid, state)
        return
    await _strip_reply_keyboard_in_chat(bot, cid)


@router.callback_query(
    StateFilter(
        CreateGiveaway.title,
        CreateGiveaway.kind,
        CreateGiveaway.description,
        CreateGiveaway.winners_count,
        CreateGiveaway.ends_at,
        CreateGiveaway.publish_chat,
        CreateGiveaway.lottery_ticket_count,
        CreateGiveaway.lottery_button_text,
        CreateGiveaway.lottery_cell_color,
        CreateGiveaway.media_prompt,
        CreateGiveaway.media_file,
    ),
    F.data == "adm:back",
)
async def cb_adm_back(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    st = await state.get_state()
    nav = _wizard_nav_kb(show_back=True)

    if st == CreateGiveaway.kind.state:
        await query.answer("Это первый шаг.", show_alert=True)
        return
    if st == CreateGiveaway.title.state:
        await state.set_state(CreateGiveaway.kind)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🎁 Розыгрыш", callback_data="adm:type:giveaway")],
                [InlineKeyboardButton(text="🎰 Лотерея", callback_data="adm:type:lottery")],
                [InlineKeyboardButton(text="❌ Отменить", callback_data="adm:cancel")],
            ]
        )
        await _render_callback_screen(query, state, bot, "🎁 <b>Создать розыгрыш</b>\n\nВыбери тип:", kb)
        return
    if st == CreateGiveaway.lottery_ticket_count.state:
        await state.set_state(CreateGiveaway.description)
        await _render_callback_screen(
            query,
            state,
            bot,
            _lottery_step2_description_html(),
            nav,
        )
        return
    if st == CreateGiveaway.lottery_button_text.state:
        await state.set_state(CreateGiveaway.winners_count)
        data = await state.get_data()
        tc = int(data.get("lottery_ticket_count") or 5)
        await _render_callback_screen(
            query,
            state,
            bot,
            _lottery_step4_winners_html(),
            _lottery_winners_kb(tc),
        )
        return
    if st == CreateGiveaway.lottery_cell_color.state:
        await state.set_state(CreateGiveaway.lottery_button_text)
        await _render_callback_screen(
            query,
            state,
            bot,
            _lottery_step5_emoji_html(),
            _lottery_button_text_kb(),
        )
        return
    if st == CreateGiveaway.title.state:
        await query.answer("Это первый шаг.", show_alert=True)
        return

    if st == CreateGiveaway.description.state:
        data = await state.get_data()
        if (data.get("giveaway_type") or "") == "lottery":
            await state.set_state(CreateGiveaway.media_prompt)
            await _render_callback_screen(
                query,
                state,
                bot,
                _lottery_media_prompt_html(),
                _media_prompt_kb(),
            )
            return
        await state.set_state(CreateGiveaway.title)
        await _render_callback_screen(
            query,
            state,
            bot,
            _giveaway_step1_title_html(),
            _wizard_nav_kb(),
        )
        return

    if st == CreateGiveaway.winners_count.state:
        data = await state.get_data()
        if (data.get("giveaway_type") or "") == "lottery":
            await state.set_state(CreateGiveaway.lottery_ticket_count)
            await _render_callback_screen(
                query,
                state,
                bot,
                _lottery_step3_tickets_html(),
                _lottery_ticket_count_kb(),
            )
            return
        await state.set_state(CreateGiveaway.description)
        await _render_callback_screen(
            query,
            state,
            bot,
            _giveaway_step2_desc_html(),
            nav,
        )
        return

    if st == CreateGiveaway.ends_at.state:
        await state.set_state(CreateGiveaway.winners_count)
        await _render_callback_screen(
            query,
            state,
            bot,
            _giveaway_step3_winners_html(),
            nav,
        )
        return

    if st == CreateGiveaway.publish_chat.state:
        data = await state.get_data()
        if (data.get("giveaway_type") or "") == "lottery":
            await state.set_state(CreateGiveaway.lottery_cell_color)
            await _render_callback_screen(
                query,
                state,
                bot,
                _lottery_step6_color_html(),
                _lottery_color_kb(),
            )
            return
        await state.set_state(CreateGiveaway.ends_at)
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📅 Выбрать дату", callback_data="dt:open")],
                *nav.inline_keyboard,
            ]
        )
        await _render_callback_screen(
            query,
            state,
            bot,
            _giveaway_step4_ends_html(),
            kb,
        )
        return

    if st == CreateGiveaway.media_file.state:
        data = await state.get_data()
        if (data.get("giveaway_type") or "") == "lottery":
            await state.set_state(CreateGiveaway.media_prompt)
            await _render_callback_screen(
                query,
                state,
                bot,
                _lottery_media_prompt_html(),
                _media_prompt_kb(),
            )
            return
        await state.set_state(CreateGiveaway.publish_chat)
        uid = query.from_user.id
        data = await state.get_data()
        selected = [int(x) for x in (data.get("publish_chat_ids") or [])]
        async with _pg_conn() as db:
            items = await list_saved_chats(db, uid)
            kb_inner = await _build_pick_publish_markup(bot, db, uid, selected)
        screen = _publish_pick_screen_html(False, bool(items))
        await _render_callback_screen(
            query, state, bot, screen, _merge_pick_publish_under_nav(kb_inner)
        )
        return

    if st == CreateGiveaway.media_prompt.state:
        data = await state.get_data()
        if (data.get("giveaway_type") or "") == "lottery":
            await state.set_state(CreateGiveaway.kind)
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🎁 Розыгрыш", callback_data="adm:type:giveaway")],
                    [InlineKeyboardButton(text="🎰 Лотерея", callback_data="adm:type:lottery")],
                    [InlineKeyboardButton(text="❌ Отменить", callback_data="adm:cancel")],
                ]
            )
            await _render_callback_screen(
                query, state, bot, "🎁 <b>Создать розыгрыш</b>\n\nВыбери тип:", kb
            )
            return
        await state.set_state(CreateGiveaway.publish_chat)
        uid = query.from_user.id
        data = await state.get_data()
        selected = [int(x) for x in (data.get("publish_chat_ids") or [])]
        async with _pg_conn() as db:
            items = await list_saved_chats(db, uid)
            kb_inner = await _build_pick_publish_markup(bot, db, uid, selected)
        screen = _publish_pick_screen_html(False, bool(items))
        await _render_callback_screen(
            query, state, bot, screen, _merge_pick_publish_under_nav(kb_inner)
        )
        return

    await query.answer()


@router.callback_query(F.data == "adm_list")
async def cb_adm_list(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    async with _pg_conn() as db:
        own_items = await list_my_giveaways(db, uid)
        joined_items = await list_joined_giveaways(db, uid)
    if not own_items and not joined_items:
        await _render_callback_screen(
            query,
            state,
            bot,
            "У тебя пока не было розыгрышей.",
            _my_giveaways_empty_kb(),
        )
        return
    await _render_callback_screen(
        query,
        state,
        bot,
        f"{_tg_pe(_PE_MENU_MY_GIVEAWAYS, '📂')} <b>Твои розыгрыши</b>\n\nОткрой нужную папку:",
        _my_joined_list_kb(own_items, joined_items),
    )


@router.message(CreateGiveaway.title)
async def sg_title(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    await state.update_data(title=_message_html_preserve_custom_emoji(message))
    await state.set_state(CreateGiveaway.description)
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        _giveaway_step2_desc_html(),
        _wizard_nav_kb(show_back=True),
    )


@router.message(CreateGiveaway.description)
async def sg_description(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    await state.update_data(description=_message_html_preserve_custom_emoji(message))
    data = await state.get_data()
    if (data.get("giveaway_type") or "") == "lottery":
        await state.set_state(CreateGiveaway.lottery_ticket_count)
        await _wizard_advance_after_valid_input(
            message.bot,
            state,
            message,
            _lottery_step3_tickets_html(),
            _lottery_ticket_count_kb(),
        )
        return
    await state.set_state(CreateGiveaway.winners_count)
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        _giveaway_step3_winners_html(),
        _wizard_nav_kb(show_back=True),
    )


@router.callback_query(StateFilter(CreateGiveaway.lottery_ticket_count), F.data.startswith("lott:tc:"))
async def cb_lottery_ticket_count(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    try:
        n = int(query.data.split(":")[-1])
    except Exception:
        await query.answer()
        return
    n = max(5, min(100, n))
    await state.update_data(lottery_ticket_count=n, giveaway_type="lottery")
    await state.set_state(CreateGiveaway.winners_count)
    await _render_callback_screen(
        query,
        state,
        bot,
        _lottery_step4_winners_html(),
        _lottery_winners_kb(n),
    )


@router.message(CreateGiveaway.lottery_ticket_count)
async def sg_lottery_ticket_count(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    t = (message.text or "").strip()
    if not t.isdigit():
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Введите число от 5 до 100.",
            _lottery_ticket_count_kb(),
        )
        return
    n = int(t)
    if n < 5 or n > 100:
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Для лотереи нужно от 5 до 100 билетов.",
            _lottery_ticket_count_kb(),
        )
        return
    await state.update_data(lottery_ticket_count=n, giveaway_type="lottery")
    await state.set_state(CreateGiveaway.winners_count)
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        _lottery_step4_winners_html(),
        _lottery_winners_kb(n),
    )


@router.callback_query(StateFilter(CreateGiveaway.winners_count), F.data.startswith("lott:wc:"))
async def cb_lottery_winners_count(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    if (data.get("giveaway_type") or "") != "lottery":
        await query.answer()
        return
    try:
        n = int(query.data.split(":")[-1])
    except Exception:
        await query.answer()
        return
    tc = max(5, min(100, int(data.get("lottery_ticket_count") or 5)))
    max_w = min(15, tc)
    if n < 1 or n > max_w:
        await query.answer(
            f"Победителей не больше числа билетов ({tc}) и не больше 15.",
            show_alert=True,
        )
        return
    await state.update_data(winners_count=n)
    await state.set_state(CreateGiveaway.lottery_button_text)
    await _render_callback_screen(
        query,
        state,
        bot,
        _lottery_step5_emoji_html(),
        _lottery_button_text_kb(),
    )


@router.callback_query(StateFilter(CreateGiveaway.lottery_button_text), F.data.startswith("lott:bt:"))
async def cb_lottery_button_text(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if query.data == "lott:bt:hint":
        await query.answer(
            "Можно отправить обычный emoji или premium emoji.\n"
            "Для premium бот возьмёт custom_emoji_id и подставит в inline.",
            show_alert=True,
        )
        return
    value = query.data.split(":", 2)[-1].strip() or "🎟"
    await state.update_data(
        lottery_button_text=value,
        lottery_button_custom_emoji_id="",
        giveaway_type="lottery",
    )
    await state.set_state(CreateGiveaway.lottery_cell_color)
    await _render_callback_screen(
        query,
        state,
        bot,
        _lottery_step6_color_html(),
        _lottery_color_kb(),
    )


@router.message(CreateGiveaway.lottery_button_text)
async def sg_lottery_button_text(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    txt = (message.text or "").strip()
    custom_emoji_id = _extract_custom_emoji_id_from_message(message)
    if not custom_emoji_id and not txt:
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Отправь emoji одним сообщением.\n"
            "Поддерживаются и <b>обычные</b>, и <b>premium</b> emoji.",
            _lottery_button_text_kb(),
        )
        return
    await state.update_data(
        lottery_button_text=((txt[:16] if txt else "🎟")),
        lottery_button_custom_emoji_id=(custom_emoji_id or ""),
        giveaway_type="lottery",
    )
    await state.set_state(CreateGiveaway.lottery_cell_color)
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        _lottery_step6_color_html(),
        _lottery_color_kb(),
    )


@router.callback_query(StateFilter(CreateGiveaway.lottery_cell_color), F.data.startswith("lott:cc:"))
async def cb_lottery_cell_color(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    color = query.data.split(":")[-1].strip().lower()
    if color not in {"blue", "default"}:
        await query.answer()
        return
    await state.update_data(lottery_cell_color=color, giveaway_type="lottery")
    pub_text, pub_kb = await _advance_to_publish_step_after_end_selected(
        bot, state, query.from_user.id
    )
    await _render_callback_screen(query, state, bot, pub_text, pub_kb)


@router.message(CreateGiveaway.lottery_cell_color)
async def sg_lottery_cell_color(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    raw = (message.text or "").strip().lower()
    color_aliases = {
        "blue": "blue",
        "синий": "blue",
        "default": "default",
        "стандартный": "default",
        "обычный": "default",
    }
    color = color_aliases.get(raw)
    if not color:
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Выберите цвет кнопкой или отправьте базовый цвет текстом.",
            _lottery_color_kb(),
        )
        return
    await state.update_data(lottery_cell_color=color, giveaway_type="lottery")
    pub_text, pub_kb = await _advance_to_publish_step_after_end_selected(
        message.bot, state, message.from_user.id
    )
    await _wizard_advance_after_valid_input(message.bot, state, message, pub_text, pub_kb)


@router.message(CreateGiveaway.winners_count)
async def sg_winners(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    t = (message.text or "").strip()
    if not t.isdigit() or int(t) < 1:
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Нужна цифра больше нуля. Например: <code>3</code>.",
            _wizard_nav_kb(show_back=True),
        )
        return
    data = await state.get_data()
    if (data.get("giveaway_type") or "") == "lottery":
        n = int(t)
        tc = max(5, min(100, int(data.get("lottery_ticket_count") or 5)))
        max_w = min(15, tc)
        if n < 1 or n > max_w:
            await _wizard_edit(
                message.bot,
                state,
                f"⚠️ Победителей: от 1 до <b>{max_w}</b> (не больше числа билетов: {tc}).",
                _lottery_winners_kb(tc),
            )
            return
        await state.update_data(winners_count=n)
        await state.set_state(CreateGiveaway.lottery_button_text)
        await _wizard_advance_after_valid_input(
            message.bot,
            state,
            message,
            _lottery_step5_emoji_html(),
            _lottery_button_text_kb(),
        )
        return
    await state.update_data(winners_count=int(t))
    await state.set_state(CreateGiveaway.ends_at)
    now_local = _utc_now().astimezone(TZ_UTC3)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Выбрать дату", callback_data="dt:open")],
            *(_wizard_nav_kb(show_back=True).inline_keyboard),
        ]
    )
    await state.update_data(
        cal_year=now_local.year,
        cal_month=now_local.month,
        cal_day=None,
    )
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        _giveaway_step4_ends_html(),
        kb,
    )


async def _advance_to_publish_step_after_end_selected(bot: Bot, state: FSMContext, user_id: int) -> tuple[str, InlineKeyboardMarkup]:
    await state.set_state(CreateGiveaway.publish_chat)
    await state.update_data(publish_chat_id=None, publish_chat_ids=[])
    data = await state.get_data()
    async with _pg_conn() as db:
        items = await list_saved_chats(db, user_id)
        kb_inner = await _build_pick_publish_markup(bot, db, user_id, [])
    if (data.get("giveaway_type") or "") == "lottery":
        text = _lottery_publish_pick_html(bool(items))
    else:
        text = _giveaway_step5_publish_html()
    return text, _merge_pick_publish_under_nav(kb_inner)


@router.message(CreateGiveaway.ends_at)
async def sg_ends(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    raw = (message.text or "").strip()
    data = await state.get_data()
    day_raw = data.get("cal_day")
    dt: Optional[datetime] = None
    if day_raw:
        m = re.match(r"^\s*(\d{1,2})[:\s](\d{2})\s*$", raw)
        if m:
            hh, mm = int(m.group(1)), int(m.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                try:
                    day = datetime.strptime(str(day_raw), "%Y-%m-%d")
                    dt_local = day.replace(hour=hh, minute=mm, second=0, microsecond=0, tzinfo=TZ_UTC3)
                    dt = dt_local.astimezone(timezone.utc)
                except Exception:
                    dt = None
    else:
        dt = _parse_dt(raw)
    if not dt:
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Неверный формат.\n"
            "Если выбрал дату в календаре — отправь время как <code>16:00</code> или <code>16 00</code>.\n"
            "Иначе отправь полную дату: <code>31.12.2026 20:00</code>.",
            _wizard_nav_kb(show_back=True),
        )
        return
    await state.update_data(ends_at=dt.isoformat(), cal_day=None)
    pub_text, pub_kb = await _advance_to_publish_step_after_end_selected(
        message.bot, state, message.from_user.id
    )
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        pub_text,
        pub_kb,
    )


@router.callback_query(StateFilter(CreateGiveaway.ends_at), F.data == "dt:open")
async def cb_dt_open(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    now_local = _utc_now().astimezone(TZ_UTC3)
    y = int(data.get("cal_year") or now_local.year)
    m = int(data.get("cal_month") or now_local.month)
    await state.update_data(cal_year=y, cal_month=m)
    await _render_callback_screen(
        query,
        state,
        bot,
        "📅 <b>Выберите дату окончания</b>",
        _ends_calendar_kb(y, m),
    )


@router.callback_query(StateFilter(CreateGiveaway.ends_at), F.data.startswith("dt:m:"))
async def cb_dt_month_nav(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    try:
        _, _, ys, ms, direction = query.data.split(":")
        y, m = int(ys), int(ms)
    except Exception:
        await query.answer()
        return
    if direction == "prev":
        m -= 1
        if m < 1:
            m = 12
            y -= 1
    else:
        m += 1
        if m > 12:
            m = 1
            y += 1
    await state.update_data(cal_year=y, cal_month=m)
    await _render_callback_screen(
        query,
        state,
        bot,
        "📅 <b>Выберите дату окончания</b>",
        _ends_calendar_kb(y, m),
    )


@router.callback_query(StateFilter(CreateGiveaway.ends_at), F.data.startswith("dt:d:"))
async def cb_dt_pick_day(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    try:
        _, _, ys, ms, ds = query.data.split(":")
        day = datetime(int(ys), int(ms), int(ds))
    except Exception:
        await query.answer("Не удалось выбрать эту дату", show_alert=True)
        return
    await state.update_data(cal_day=day.strftime("%Y-%m-%d"))
    await _render_callback_screen(
        query,
        state,
        bot,
        "📅 Дата выбрана: <b>"
        + day.strftime("%d.%m.%Y")
        + "</b>\n\n"
        "Теперь отправь время сообщением:\n"
        "<code>16:00</code> или <code>16 00</code>.",
        _wizard_nav_kb(show_back=True),
    )


@router.message(CreateGiveaway.publish_chat)
async def sg_publish_chat(message: Message, state: FSMContext) -> None:
    if not message.from_user:
        return
    cid, err = await _resolve_publish_chat(message.bot, message)
    if err or cid is None:
        await _wizard_edit(
            message.bot,
            state,
            f"⚠️ {err}",
            _wizard_nav_kb(show_back=True),
        )
        return
    v = await _validate_publish_destination(message.bot, cid, message.from_user.id)
    if v:
        await _wizard_edit(
            message.bot,
            state,
            f"⚠️ {v}",
            _wizard_nav_kb(show_back=True),
        )
        return
    await state.update_data(
        publish_chat_id=cid,
        publish_chat_ids=[cid],
    )
    try:
        await message.answer(f"✅ Выбран чат: <code>{cid}</code>", parse_mode="HTML")
    except Exception:
        pass
    data = await state.get_data()
    if (data.get("giveaway_type") or "giveaway") == "lottery":
        gid = await _finalize_create_giveaway_draft(message.bot, message.from_user.id, data, None, None)
        if gid is None:
            await message.answer("⚠️ Сессия устарела — нажми «Создать розыгрыш» снова.")
            return
        await state.clear()
        async with _pg_conn() as db:
            g = await get_giveaway(db, gid)
        await _present_draft_giveaway_after_wizard(message.bot, state, message.chat.id, None, g)
        return
    await state.set_state(CreateGiveaway.media_prompt)
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        _MEDIA_STEP_INTRO,
        _media_prompt_kb(),
    )


@router.callback_query(StateFilter(CreateGiveaway.publish_chat), F.data.startswith("pubsel:"))
async def cb_pubsel_publish_chat(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    raw = query.data.split(":", 1)[1]
    data = await state.get_data()
    selected = [int(x) for x in (data.get("publish_chat_ids") or [])]
    if raw == "manual":
        async with _pg_conn() as db:
            items = await list_saved_chats(db, uid)
            kb_inner = await _build_pick_publish_markup(bot, db, uid, selected)
        is_lot = (data.get("giveaway_type") or "") == "lottery"
        screen = _publish_pick_screen_html(is_lot, bool(items))
        await _render_callback_screen(
            query,
            state,
            bot,
            screen,
            _merge_pick_publish_under_nav(kb_inner),
        )
        return
    if raw == "invite_ch":
        await query.answer()
        await _send_add_channel_invite(bot, uid)
        return
    if raw == "invite_gr":
        await query.answer()
        await _send_add_group_invite(bot, uid)
        return
    if raw == "done":
        if not selected:
            await query.answer("Выбери хотя бы один канал.", show_alert=True)
            return
        await query.answer()
        await state.update_data(
            publish_chat_id=selected[0],
            publish_chat_ids=selected,
        )
        await query.answer(f"Выбрано chat_id: {selected[0]}")
        data = await state.get_data()
        if (data.get("giveaway_type") or "giveaway") == "lottery":
            gid = await _finalize_create_giveaway_draft(bot, uid, data, None, None)
            if gid is None:
                await query.answer("Сессия устарела — начни заново.", show_alert=True)
                return
            await state.clear()
            async with _pg_conn() as db:
                g = await get_giveaway(db, gid)
            await _render_draft_giveaway_screen(query, state, bot, g)
            return
        await state.set_state(CreateGiveaway.media_prompt)
        await _wizard_edit(
            bot,
            state,
            _MEDIA_STEP_INTRO,
            _media_prompt_kb(),
        )
        return
    try:
        cid = int(raw)
    except ValueError:
        await query.answer("Что-то пошло не так с кнопкой", show_alert=True)
        return
    if cid in selected:
        selected = [x for x in selected if x != cid]
    else:
        v = await _validate_publish_destination(bot, cid, uid)
        if v:
            short = v.replace("<b>", "").replace("</b>", "")
            if len(short) > 190:
                short = short[:187] + "…"
            await query.answer(short, show_alert=True)
            return
        selected.append(cid)
    await state.update_data(
        publish_chat_ids=selected,
        publish_chat_id=(selected[0] if selected else None),
    )
    async with _pg_conn() as db:
        items = await list_saved_chats(db, uid)
        kb_inner = await _build_pick_publish_markup(bot, db, uid, selected)
    is_lot = (data.get("giveaway_type") or "") == "lottery"
    screen = _publish_pick_screen_html(is_lot, bool(items))
    await query.answer("Обновили выбор")
    await _render_callback_screen(
        query,
        state,
        bot,
        screen,
        _merge_pick_publish_under_nav(kb_inner),
    )


async def _finalize_create_giveaway_draft(
    bot: Bot,
    uid: int,
    data: dict[str, Any],
    post_media_kind: Optional[str],
    post_media_file_id: Optional[str],
) -> Optional[int]:
    """Собирает черновик из данных мастера. None — не хватает полей."""
    kind = (data.get("giveaway_type") or "giveaway").strip()
    required = ("winners_count",)
    if kind == "lottery":
        required = required + ("description", "lottery_ticket_count", "lottery_button_custom_emoji_id")
    else:
        required = required + ("title", "description", "ends_at")
    if not all(data.get(k) is not None for k in required):
        return None
    raw_ids = data.get("publish_chat_ids") or []
    if raw_ids:
        selected = [int(x) for x in raw_ids]
    elif data.get("publish_chat_id") is not None:
        selected = [int(data["publish_chat_id"])]
    else:
        return None
    if not selected:
        return None
    pc = int(selected[0])
    mirrors = [int(x) for x in selected[1:] if int(x) != pc]
    all_for_sub = [pc] + mirrors
    tokens = await _subscription_tokens_for_chat_ids(bot, all_for_sub)
    channels_json = json.dumps(tokens, ensure_ascii=False)
    if kind == "lottery":
        title = "Лотерея"
        description = str(data.get("description") or "Лотерейный розыгрыш по билетам.")
        ends_at = (_utc_now() + timedelta(days=3650)).isoformat()
        post_media_kind = post_media_kind or data.get("post_media_kind")
        post_media_file_id = post_media_file_id or data.get("post_media_file_id")
    else:
        title = data["title"]
        description = data["description"]
        ends_at = data["ends_at"]
    return await _insert_draft_giveaway_row(
        uid,
        title,
        description,
        int(data["winners_count"]),
        ends_at,
        pc,
        channels_json,
        json.dumps(mirrors, ensure_ascii=False),
        post_media_kind,
        post_media_file_id,
        kind,
        int(data.get("lottery_ticket_count") or 0),
        str(data.get("lottery_button_text") or "🎟"),
        "[]",
        str(data.get("lottery_cell_color") or "blue"),
        str(data.get("lottery_button_custom_emoji_id") or ""),
        0,
        0,
        1,
    )


async def _complete_wizard_media_and_show_draft(
    message: Message, state: FSMContext, bot: Bot, media_kind: str, file_id: str
) -> None:
    if not message.from_user:
        return
    uid = message.from_user.id
    data = await state.get_data()
    if (data.get("giveaway_type") or "") == "lottery":
        await state.update_data(
            post_media_kind=media_kind,
            post_media_file_id=file_id,
            giveaway_type="lottery",
        )
        await state.set_state(CreateGiveaway.description)
        await _wizard_advance_after_valid_input(
            message.bot,
            state,
            message,
            _lottery_step2_description_html(),
            _wizard_nav_kb(show_back=True),
        )
        return
    ui_cid = data.get("ui_chat_id")
    ui_mid = data.get("ui_message_id")
    gid = await _finalize_create_giveaway_draft(bot, uid, data, media_kind, file_id)
    if gid is None:
        await message.answer("⚠️ Сессия устарела — нажми «Создать розыгрыш» снова.")
        return
    await state.clear()
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    chat_id = int(ui_cid) if ui_cid is not None else message.chat.id
    old_mid = int(ui_mid) if ui_mid is not None else None
    await _present_draft_giveaway_after_wizard(bot, state, chat_id, old_mid, g)
    try:
        await message.delete()
    except Exception:
        pass


@router.callback_query(StateFilter(CreateGiveaway.media_prompt), F.data == "wmedia:no")
async def cb_wmedia_no(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    data = await state.get_data()
    if (data.get("giveaway_type") or "") == "lottery":
        await state.update_data(post_media_kind=None, post_media_file_id=None, giveaway_type="lottery")
        await state.set_state(CreateGiveaway.description)
        await _render_callback_screen(
            query,
            state,
            bot,
            _lottery_step2_description_html(),
            _wizard_nav_kb(show_back=True),
        )
        return
    gid = await _finalize_create_giveaway_draft(bot, uid, data, None, None)
    if gid is None:
        await query.answer("Сессия устарела — начни создание заново.", show_alert=True)
        return
    await state.clear()
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    await _render_draft_giveaway_screen(query, state, bot, g)


@router.callback_query(StateFilter(CreateGiveaway.media_prompt), F.data == "wmedia:yes")
async def cb_wmedia_yes(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    await state.set_state(CreateGiveaway.media_file)
    data = await state.get_data()
    is_lot = (data.get("giveaway_type") or "") == "lottery"
    prompt = _lottery_media_step1_html() if is_lot else _MEDIA_FILE_PROMPT
    kb = _wizard_nav_kb(show_back=True)
    await _render_callback_screen(
        query,
        state,
        bot,
        prompt,
        kb,
    )


@router.message(CreateGiveaway.media_file, F.photo)
async def sg_media_photo(message: Message, state: FSMContext, bot: Bot) -> None:
    if not message.photo:
        return
    fid = message.photo[-1].file_id
    await _complete_wizard_media_and_show_draft(message, state, bot, "photo", fid)


@router.message(CreateGiveaway.media_file, F.video)
async def sg_media_video(message: Message, state: FSMContext, bot: Bot) -> None:
    if not message.video:
        return
    await _complete_wizard_media_and_show_draft(message, state, bot, "video", message.video.file_id)


@router.message(CreateGiveaway.media_file, F.animation)
async def sg_media_animation(message: Message, state: FSMContext, bot: Bot) -> None:
    if not message.animation:
        return
    await _complete_wizard_media_and_show_draft(
        message, state, bot, "animation", message.animation.file_id
    )


@router.message(CreateGiveaway.media_file)
async def sg_media_file_invalid(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    if (data.get("giveaway_type") or "") == "lottery":
        wrong = (
            f"{_tg_pe(_PE_LOT_DOCWARN, '⚠️')} Нужно одно <b>фото</b>, <b>GIF</b> или <b>видео</b>. "
            f"{_tg_pe(_PE_LOT_DOCWARN, '📎')} Документы и альбомы не поддерживаются."
        )
    else:
        wrong = "⚠️ Нужно одно <b>фото</b>, <b>GIF</b> или <b>видео</b> (не документ и не альбом)."
    kb_media = _wizard_nav_kb(show_back=True)
    await _wizard_edit(message.bot, state, wrong, kb_media)


def _draft_edit_only_alert() -> str:
    return "Это доступно только для черновика."


async def _load_owned_draft_giveaway(
    gid: int, uid: int
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g:
        return None, "Такого розыгрыша нет."
    if not _is_giveaway_owner(g, uid):
        return None, "Это чужой розыгрыш."
    if g.get("status") != "draft":
        return None, _draft_edit_only_alert()
    return g, None


@router.callback_query(F.data.startswith("dpub:"))
async def cb_dpub(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        gid = int(query.data.split(":")[1])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    await _publish_draft_giveaway_live(bot, query, state, gid, uid)


@router.callback_query(F.data.startswith("ddraft:"))
async def cb_ddraft(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        gid = int(query.data.split(":")[1])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    await _render_draft_giveaway_screen(query, state, bot, g)


@router.callback_query(F.data.startswith("dextra:"))
async def cb_dextra(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        gid = int(query.data.split(":")[1])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    await _render_callback_screen(
        query,
        state,
        bot,
        _DRAFT_EXTRA_SCREEN_TITLE +
        "Обязательный <b>@username</b> и бонусный шанс за приглашение друзей "
        "(реферальная ссылка в кнопке «Участвовать»).",
        _draft_extra_kb(g),
    )


@router.callback_query(F.data.startswith("dexrq:"))
async def cb_dexrq(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return
    try:
        gid, v = int(parts[1]), int(parts[2])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    async with _pg_conn() as db:
        await db.execute(
            "UPDATE giveaways SET require_username = ? WHERE id = ?",
            (1 if v else 0, gid),
        )
        await db.commit()
        g = await get_giveaway(db, gid)
    await _render_callback_screen(
        query,
        state,
        bot,
        _DRAFT_EXTRA_SCREEN_TITLE +
        "Обязательный <b>@username</b> и бонусный шанс за приглашение друзей.",
        _draft_extra_kb(g),
    )


@router.callback_query(F.data.startswith("dexref:"))
async def cb_dexref(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return
    try:
        gid, v = int(parts[1]), int(parts[2])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    async with _pg_conn() as db:
        await db.execute(
            "UPDATE giveaways SET referral_enabled = ? WHERE id = ?",
            (1 if v else 0, gid),
        )
        await db.commit()
        g = await get_giveaway(db, gid)
    await _render_callback_screen(
        query,
        state,
        bot,
        _DRAFT_EXTRA_SCREEN_TITLE +
        "Обязательный <b>@username</b> и бонусный шанс за приглашение друзей.",
        _draft_extra_kb(g),
    )


@router.callback_query(F.data.startswith("dexstep:"))
async def cb_dexstep(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return
    try:
        gid, step = int(parts[1]), int(parts[2])
    except ValueError:
        await query.answer()
        return
    step = max(1, min(5, step))
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    if not int(g.get("referral_enabled") or 0):
        await query.answer("Сначала включи «Шанс за приглашение».", show_alert=True)
        return
    async with _pg_conn() as db:
        await db.execute(
            "UPDATE giveaways SET referral_ticket_step = ? WHERE id = ?",
            (step, gid),
        )
        await db.commit()
        g = await get_giveaway(db, gid)
    await _render_callback_screen(
        query,
        state,
        bot,
        _DRAFT_EXTRA_SCREEN_TITLE +
        "Обязательный <b>@username</b> и бонусный шанс за приглашение друзей.",
        _draft_extra_kb(g),
    )


@router.callback_query(F.data.startswith("dchinv:"))
async def cb_dchinv(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        gid = int(query.data.split(":")[1])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    body = await _invite_channels_screen_body(bot, g)
    prow = await _fetch_pending_row_for_giveaway(gid)
    pending_id = int(prow["id"]) if prow else None
    kb = await _invite_channels_full_kb(bot, g, pending_id=pending_id)
    await _render_callback_screen(query, state, bot, body, kb)


@router.callback_query(F.data.startswith("dchmgmt:"))
async def cb_dchmgmt(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        gid = int(query.data.split(":")[1])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    body = await _invite_channels_mgmt_body(bot, g)
    kb = await _invite_channels_mgmt_kb(bot, g)
    await _render_callback_screen(query, state, bot, body, kb)


def _parse_gid_cid_cross(callback_data: str) -> tuple[Optional[int], Optional[int]]:
    """dchm:gid:cid с отрицательным cid."""
    parts = callback_data.split(":", 2)
    if len(parts) != 3:
        return None, None
    try:
        return int(parts[1]), int(parts[2])
    except ValueError:
        return None, None


async def _dch_refresh_invite_screen(
    query: CallbackQuery, state: FSMContext, bot: Bot, gid: int, uid: int
) -> None:
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    body = await _invite_channels_screen_body(bot, g)
    prow = await _fetch_pending_row_for_giveaway(gid)
    pending_id = int(prow["id"]) if prow else None
    kb = await _invite_channels_full_kb(bot, g, pending_id=pending_id)
    await _render_callback_screen(query, state, bot, body, kb)


async def _dch_refresh_mgmt_screen(
    query: CallbackQuery,
    state: FSMContext,
    bot: Bot,
    gid: int,
    uid: int,
    *,
    markup_only: bool = False,
) -> None:
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    kb = await _invite_channels_mgmt_kb(bot, g)
    cid, mid = query.message.chat.id, query.message.message_id
    if markup_only:
        # Положение блока на экране Telegram не фиксирует (нет API). Сначала правка reply_markup,
        # потом answer — меньше визуального «двух фаз» (спиннер сняли отдельно от смены кнопок).
        try:
            await bot.edit_message_reply_markup(
                chat_id=cid, message_id=mid, reply_markup=kb
            )
        except TelegramBadRequest as e:
            log.debug("edit_message_reply_markup mgmt: %s", e)
            body = await _invite_channels_mgmt_body(bot, g)
            ok = await _edit_screen(bot, cid, mid, body, kb, parse_mode="HTML")
            if ok:
                await _remember_ui(state, cid, mid)
            else:
                try:
                    await bot.delete_message(cid, mid)
                except Exception:
                    pass
                sent = await bot.send_message(
                    cid, body, reply_markup=kb, parse_mode="HTML"
                )
                await _remember_ui(state, sent.chat.id, sent.message_id)
        else:
            await _remember_ui(state, cid, mid)
        await query.answer()
        return
    body = await _invite_channels_mgmt_body(bot, g)
    await _render_callback_screen(query, state, bot, body, kb)


@router.callback_query(F.data.startswith("dchcap:"))
async def cb_dch_cap_label(query: CallbackQuery, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    gid, cid = _parse_gid_cid_cross(query.data)
    if gid is None or cid is None:
        await query.answer()
        return
    uid = query.from_user.id
    _, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    t = await _chat_title_html(bot, cid)
    plain = t.replace("<b>", "").replace("</b>", "").strip() or str(cid)
    await query.answer(plain[:200], show_alert=True)


@router.callback_query(F.data.startswith("dchtog:"))
async def cb_dch_toggle_mode(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid, cid = _parse_gid_cid_cross(query.data)
    if gid is None or cid is None:
        await query.answer()
        return
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    mir, so = _mirror_subscribe_from_g(g)
    pc = g.get("post_chat_id")
    main_cid = int(pc) if pc is not None else None
    if cid not in mir and cid not in so and (main_cid is None or cid != main_cid):
        await query.answer("Этого чата нет в доп. каналах.", show_alert=True)
        return
    # Если переключают текущий anchor (post_chat_id) в «подписка», назначаем новый anchor
    # из каналов публикации (mir). В UX понятия «основного» нет, это технический якорь.
    if main_cid is not None and cid == main_cid:
        replacement: Optional[int] = None
        for x in mir:
            if int(x) != cid:
                replacement = int(x)
                break
        if replacement is None:
            await query.answer(
                "Нужен хотя бы один канал в режиме публикации (без «подписка ✅»).",
                show_alert=True,
            )
            return
        mir = [int(x) for x in mir if int(x) != replacement and int(x) != cid]
        if cid not in so:
            so.append(cid)
        await _persist_mirror_subscribe(bot, g, mir, so, post_chat_id=replacement)
    else:
        in_sub_only = cid in so and cid not in mir
        if in_sub_only:
            so = [x for x in so if x != cid]
            if cid not in mir:
                mir.append(cid)
        else:
            mir = [x for x in mir if x != cid]
            if cid not in so:
                so.append(cid)
        await _persist_mirror_subscribe(bot, g, mir, so)
    await _dch_refresh_mgmt_screen(query, state, bot, gid, uid, markup_only=True)


@router.callback_query(F.data.startswith("dchm:"))
async def cb_dch_set_mirror(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid, cid = _parse_gid_cid_cross(query.data)
    if gid is None or cid is None:
        await query.answer()
        return
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    mir, so = _mirror_subscribe_from_g(g)
    if cid not in mir and cid not in so:
        await query.answer("Этого чата нет в доп. каналах.", show_alert=True)
        return
    if cid in mir:
        await query.answer("Уже: пост и подписка", show_alert=True)
        return
    so = [x for x in so if x != cid]
    mir.append(cid)
    await _persist_mirror_subscribe(bot, g, mir, so)
    await _dch_refresh_mgmt_screen(query, state, bot, gid, uid, markup_only=True)


@router.callback_query(F.data.startswith("dchs:"))
async def cb_dch_set_subscribe_only(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid, cid = _parse_gid_cid_cross(query.data)
    if gid is None or cid is None:
        await query.answer()
        return
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    mir, so = _mirror_subscribe_from_g(g)
    if cid not in mir and cid not in so:
        await query.answer("Этого чата нет в доп. каналах.", show_alert=True)
        return
    if cid in so and cid not in mir:
        await query.answer("Уже: подписка без поста", show_alert=True)
        return
    mir = [x for x in mir if x != cid]
    if cid not in so:
        so.append(cid)
    await _persist_mirror_subscribe(bot, g, mir, so)
    await _dch_refresh_mgmt_screen(query, state, bot, gid, uid, markup_only=True)


@router.callback_query(F.data.startswith("dchrm:"))
async def cb_dch_remove_extra(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid, cid = _parse_gid_cid_cross(query.data)
    if gid is None or cid is None:
        await query.answer()
        return
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    mir, so = _mirror_subscribe_from_g(g)
    pc = g.get("post_chat_id")
    main_cid = int(pc) if pc is not None else None
    if cid not in mir and cid not in so and (main_cid is None or cid != main_cid):
        await query.answer("Чат уже убран.", show_alert=True)
        return
    post_chat_id_override: Optional[int] = None
    if main_cid is not None and cid == main_cid:
        replacement: Optional[int] = None
        for x in mir:
            if int(x) != cid:
                replacement = int(x)
                break
        if replacement is None:
            await query.answer(
                "Нельзя удалить последний канал публикации. Оставь хотя бы один.",
                show_alert=True,
            )
            return
        post_chat_id_override = replacement
        mir = [int(x) for x in mir if int(x) != replacement and int(x) != cid]
    else:
        mir = [x for x in mir if x != cid]
    so = [x for x in so if x != cid]
    await _persist_mirror_subscribe(bot, g, mir, so, post_chat_id=post_chat_id_override)
    # Важно: удаляем чат и из ожидающего приглашения, иначе он может вернуться
    # после нажатия «Проверить согласия» для старого pending.
    row = await _fetch_pending_row_for_giveaway(gid)
    if row:
        try:
            snap = json.loads(row["snapshot_json"] or "{}")
            extra = [int(x) for x in snap.get("extra_chat_ids", [])]
        except Exception:
            snap, extra = {}, []
        new_extra = [x for x in extra if x != cid]
        async with _pg_conn() as db:
            if new_extra:
                snap["extra_chat_ids"] = new_extra
                await db.execute(
                    "UPDATE crosspost_pending SET snapshot_json = ? WHERE id = ?",
                    (json.dumps(snap, ensure_ascii=False), int(row["id"])),
                )
                await db.execute(
                    "DELETE FROM crosspost_vote WHERE pending_id = ? AND channel_id = ?",
                    (int(row["id"]), cid),
                )
                await db.execute(
                    "DELETE FROM crosspost_notif WHERE pending_id = ? AND channel_id = ?",
                    (int(row["id"]), cid),
                )
                await db.execute(
                    "DELETE FROM crosspost_creator_notified WHERE pending_id = ? AND channel_id = ?",
                    (int(row["id"]), cid),
                )
                await db.commit()
            else:
                await _crosspost_pending_delete_bundle(db, int(row["id"]))
    await _dch_refresh_mgmt_screen(query, state, bot, gid, uid)


@router.callback_query(F.data.startswith("dchpcan:"))
async def cb_dchpcan(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        gid = int(query.data.split(":")[1])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    row = await _fetch_pending_row_for_giveaway(gid)
    if row:
        async with _pg_conn() as db:
            await _crosspost_pending_delete_bundle(db, int(row["id"]))
    cid, mid = query.message.chat.id, query.message.message_id
    await state.clear()
    await state.update_data(ui_chat_id=cid, ui_message_id=mid)
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    body = await _invite_channels_screen_body(bot, g)
    kb = await _invite_channels_full_kb(bot, g, pending_id=None)
    await _render_callback_screen(query, state, bot, body, kb)


@router.callback_query(F.data.startswith("dchex:"))
async def cb_dchex(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        gid = int(query.data.split(":")[1])
    except ValueError:
        await query.answer()
        return
    uid = query.from_user.id
    g, err = await _load_owned_draft_giveaway(gid, uid)
    if err:
        await query.answer(err.replace("<b>", "").replace("</b>", "")[:180], show_alert=True)
        return
    await state.set_state(EditGiveawayInvite.waiting_extra)
    await state.update_data(editing_giveaway_id=gid)
    await _render_callback_screen(
        query,
        state,
        bot,
        "✏️ <b>Дополнительные каналы</b>\n\n"
        "Отправь каналы, на которые тоже нужно подписаться:\n"
        "<code>@канал</code>, ссылка или ID через запятую / с новой строки.\n\n"
        "Бот запросит согласие у владельцев. После «Проверить согласия» согласованные чаты попадут в условия; "
        "Режим «подписка без поста» при необходимости — в «Управление каналами».",
        InlineKeyboardMarkup(
            inline_keyboard=[
                [_ikb_back(f"ddraft:{gid}")],
            ]
        ),
    )


@router.message(EditGiveawayInvite.waiting_extra)
async def sg_edit_channels_extra(message: Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    uid = message.from_user.id
    data = await state.get_data()
    egid = data.get("editing_giveaway_id")
    if egid is None:
        return
    egid = int(egid)
    async with _pg_conn() as db:
        g = await get_giveaway(db, egid)
    if not g or not _is_giveaway_owner(g, uid) or g.get("status") != "draft":
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Черновик не найден или нет доступа.",
            _my_giveaways_panel_kb(),
        )
        return
    pc = g.get("post_chat_id")
    if pc is None:
        return
    pc = int(pc)
    # Не блокируем повторные приглашения: перед новым запуском чистим старый pending.
    prev_pending = await _fetch_pending_row_for_giveaway(egid)
    if prev_pending:
        async with _pg_conn() as db:
            await _crosspost_pending_delete_bundle(db, int(prev_pending["id"]))
    tokens_txt = _parse_multi_channel_tokens(message.text or "")
    fcid = _forwarded_chat_id(message)
    if not tokens_txt and fcid is not None:
        tokens_txt = [str(fcid)]
    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [_ikb_back(f"ddraft:{egid}")],
        ]
    )
    if not tokens_txt:
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Напиши хотя бы один канал или группу.",
            back_kb,
        )
        return
    extra_ids: list[int] = []
    err_parts: list[str] = []
    for tok in tokens_txt:
        if _is_private_invite_link(tok):
            err_parts.append(
                f"{html.escape(tok)}: приватные invite-ссылки не поддерживаются Bot API; "
                "добавь чат через кнопку «Добавить группу» и выбери его из сохранённых."
            )
            continue
        try:
            if tok.lstrip("-").isdigit():
                chat = await _get_chat_by_numeric_id(bot, int(tok))
            else:
                chat = await bot.get_chat(f"@{tok}")
            eid = int(chat.id)
        except Exception:
            err_parts.append(html.escape(tok))
            continue
        if eid == pc:
            continue
        err_inv, _rec = await _resolve_crosspost_invite_recipients(bot, eid)
        if err_inv:
            err_parts.append(f"{html.escape(tok)}: {err_inv}")
            continue
        if eid not in extra_ids:
            extra_ids.append(eid)
    if not extra_ids:
        hint = ""
        if err_parts:
            hint = "\n\n" + "\n".join(err_parts[:5])
        await _wizard_edit(
            message.bot,
            state,
            "⚠️ Ни один канал из списка не подошёл. Проверь id и права бота." + hint,
            back_kb,
        )
        return
    snap = {
        "giveaway_id": egid,
        "title": g["title"],
        "description": g["description"],
        "winners_count": g["winners_count"],
        "ends_at": g["ends_at"],
        "publish_chat_id": pc,
        "extra_chat_ids": extra_ids,
    }
    async with _pg_conn() as db:
        cur = await db.execute(
            """INSERT INTO crosspost_pending (creator_id, snapshot_json, created_at, giveaway_id)
               VALUES (?, ?, ?, ?) RETURNING id""",
            (uid, json.dumps(snap, ensure_ascii=False), _utc_now().isoformat(), egid),
        )
        row_pid = await cur.fetchone()
        await db.commit()
        pid = int(row_pid["id"])
    u = message.from_user
    cl = f"@{u.username}" if u.username else html.escape(u.first_name or str(uid))
    await _dispatch_crosspost_admin_requests(bot, pid, extra_ids, cl, uid)
    await state.update_data(crosspost_pending_id=pid)
    await state.set_state(EditGiveawayInvite.crosswait)
    await _wizard_advance_after_valid_input(
        message.bot,
        state,
        message,
        "✅ Мы написали владельцам в личку.\n\n"
        "Когда будешь готов, нажми «Проверить согласия» — добавим в условия согласованные чаты "
        "(по умолчанию пост + подписка). Режим «подписка без поста» можно сменить в "
        "«⚙️ Управление каналами».",
        _channels_crosswait_kb_draft(pid, egid),
    )


@router.callback_query(F.data.startswith("xcont:"))
async def cb_crosspost_continue(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    try:
        pid = int(query.data.split(":")[1])
    except (IndexError, ValueError):
        await query.answer()
        return
    uid = query.from_user.id
    async with _pg_conn() as db:
        cur = await db.execute(
            "SELECT creator_id, snapshot_json FROM crosspost_pending WHERE id = ?",
            (pid,),
        )
        row = await cur.fetchone()
    if not row:
        await query.answer("Это уже неактуально.", show_alert=True)
        return
    if int(row["creator_id"]) != uid:
        await query.answer("Проверить согласия может только автор.", show_alert=True)
        return
    snap = json.loads(row["snapshot_json"])
    giveaway_id = snap.get("giveaway_id")
    pc = int(snap["publish_chat_id"])
    requested = [int(x) for x in snap["extra_chat_ids"]]
    async with _pg_conn() as db:
        approved = await _crosspost_approved_extra_chat_ids(db, pid, requested)

    if giveaway_id is not None:
        gid = int(giveaway_id)
        async with _pg_conn() as db:
            g0 = await get_giveaway(db, gid)
            if not g0 or not _is_giveaway_owner(g0, uid) or g0.get("status") != "draft":
                await query.answer("Черновик недоступен.", show_alert=True)
                return

        mir_o, so_o = _mirror_subscribe_from_g(g0)
        committed = set(mir_o) | set(so_o)
        approved_new = [int(c) for c in approved if int(c) not in committed]

        if approved_new:
            mir_set = set(mir_o)
            so_set = set(so_o)
            so_set -= mir_set
            for c in approved_new:
                c = int(c)
                so_set.discard(c)
                mir_set.add(c)
            mirrors = list(mir_set)
            subonly = list(so_set)
            all_cids = [int(pc)] + list(mir_set | so_set)
            tokens = await _subscription_tokens_for_chat_ids(bot, all_cids)
            async with _pg_conn() as db:
                await db.execute(
                    """UPDATE giveaways SET channels_json = ?, mirror_target_chat_ids = ?,
                       subscribe_only_chat_ids = ? WHERE id = ?""",
                    (
                        json.dumps(tokens, ensure_ascii=False),
                        json.dumps(mirrors, ensure_ascii=False),
                        json.dumps(subonly, ensure_ascii=False),
                        gid,
                    ),
                )
                await db.commit()
                g = await get_giveaway(db, gid)
            await state.clear()
            await state.update_data(
                ui_chat_id=query.message.chat.id,
                ui_message_id=query.message.message_id,
            )
            base = await _invite_channels_screen_body(bot, g)
            body = (
                base
                + "\n\n✅ <b>Согласованные чаты добавлены</b> (по умолчанию пост + подписка). "
                "Режим «подписка без поста» — в «⚙️ Управление каналами»."
            )
            prow = await _fetch_pending_row_for_giveaway(gid)
            pending_id = int(prow["id"]) if prow else None
            kb = await _invite_channels_full_kb(bot, g, pending_id=pending_id)
            await _render_callback_screen(query, state, bot, body, kb)
            return

        async with _pg_conn() as db:
            all_yes, pending_cids, refused_cids = await _crosspost_pending_vote_groups(
                db, pid, requested
            )

        parts: list[str] = []
        if approved:
            if all_yes:
                parts.append("Все каналы/группы приняли приглашение.")
            else:
                parts.append("Согласованные чаты уже в управлении каналами.")
                await _append_crosspost_alert_vote_lists(bot, parts, pending_cids, refused_cids)
        else:
            await _append_crosspost_alert_vote_lists(bot, parts, pending_cids, refused_cids)
            if not parts:
                parts.append(
                    "Ожидаем ответа от администраторов каналов. "
                    "Когда ответят — нажми «Проверить согласия» снова."
                )

        await query.answer(_callback_answer_alert_text("\n".join(parts)), show_alert=True)
        return

    async with _pg_conn() as db:
        await _crosspost_pending_delete_bundle(db, pid)
    await query.answer("Этот запрос устарел.", show_alert=True)


@router.callback_query(F.data.startswith("xv:"))
async def cb_crosspost_vote(query: CallbackQuery, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    parts = query.data.split(":")
    if len(parts) != 3:
        await query.answer()
        return
    try:
        nid = int(parts[1])
    except ValueError:
        await query.answer()
        return
    vote = "yes" if parts[2] == "y" else "no"
    uid = query.from_user.id
    async with _pg_conn() as db:
        cur = await db.execute(
            "SELECT pending_id, channel_id, admin_id FROM crosspost_notif WHERE id = ?",
            (nid,),
        )
        row = await cur.fetchone()
    if not row or int(row["admin_id"]) != uid:
        await query.answer("Это приглашение не для тебя.", show_alert=True)
        return
    pending_id = int(row["pending_id"])
    channel_id = int(row["channel_id"])
    creator_id: int | None = None
    do_notify = False
    async with _pg_conn() as db:
        await db.execute(
            """INSERT INTO crosspost_vote (pending_id, channel_id, admin_id, vote)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (pending_id, channel_id, admin_id)
               DO UPDATE SET vote = EXCLUDED.vote""",
            (pending_id, channel_id, uid, vote),
        )
        cur = await db.execute(
            """SELECT COUNT(*) AS cnt FROM crosspost_vote
               WHERE pending_id = ? AND channel_id = ? AND vote = 'yes'""",
            (pending_id, channel_id),
        )
        n_yes = int((await cur.fetchone())["cnt"])
        if n_yes == 0:
            await db.execute(
                """DELETE FROM crosspost_creator_notified
                   WHERE pending_id = ? AND channel_id = ?""",
                (pending_id, channel_id),
            )
        elif vote == "yes":
            cur = await db.execute(
                "SELECT 1 FROM crosspost_creator_notified WHERE pending_id = ? AND channel_id = ?",
                (pending_id, channel_id),
            )
            if await cur.fetchone() is None:
                await db.execute(
                    """INSERT INTO crosspost_creator_notified (pending_id, channel_id)
                       VALUES (?, ?)""",
                    (pending_id, channel_id),
                )
                do_notify = True
        await db.commit()
        if do_notify:
            cur = await db.execute(
                "SELECT creator_id FROM crosspost_pending WHERE id = ?", (pending_id,)
            )
            prow = await cur.fetchone()
            creator_id = int(prow["creator_id"]) if prow else None
    await query.answer("Принято, спасибо!")

    if do_notify and creator_id is not None:
        try:
            chat = await bot.get_chat(channel_id)
        except Exception:
            chat = None
        ch_title = html.escape(chat.title or str(channel_id)) if chat else str(channel_id)
        if chat and chat.type == "channel":
            ch_type_ru = "Канал"
        elif chat and chat.type in ("group", "supergroup"):
            ch_type_ru = "Группа"
        else:
            ch_type_ru = "Чат"
        note = (
            f"✅ <b>Согласие по кросспосту</b>\n\n"
            f"{ch_type_ru}: <b>{ch_title}</b> — владелец/админ нажал «Согласен».\n\n"
            "Когда обработаешь остальные приглашения, нажми «Проверить согласия» в боте. "
            "Режим «подписка без поста» — в «⚙️ Управление каналами»."
        )
        try:
            await bot.send_message(
                creator_id,
                note,
                parse_mode="HTML",
                disable_web_page_preview=True,
                link_preview_options=_LINK_PREVIEW_OFF,
            )
        except Exception as e:
            log.debug("notify creator crosspost yes %s: %s", creator_id, e)


@router.message(StateFilter(default_state), F.chat.type == ChatType.PRIVATE, Command("emojiid"))
async def cmd_emoji_id(message: Message) -> None:
    target = message.reply_to_message or message
    ids = _extract_all_custom_emoji_ids_from_message(target)
    if not ids:
        await message.answer(
            "Не найдено premium emoji.\n"
            "Отправь эмодзи сообщением или ответь на сообщение с эмоджи командой /emojiid."
        )
        return
    lines = ["Найденные custom emoji ID:"]
    for cid in ids:
        lines.append(f"<code>{html.escape(cid, quote=False)}</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(
    StateFilter(default_state),
    F.text.startswith("/"),
    ~Command("emojiid"),
)
async def any_slash_show_menu(message: Message, state: FSMContext, bot: Bot) -> None:
    """Команды вида /... кроме /start и /emojiid (их обрабатывают отдельные хендлеры)."""
    if message.chat.type != ChatType.PRIVATE:
        # В группах на прочие /команды тоже молчим.
        return
    kb = _main_menu_reply_kb()
    hint = "Тут всё на кнопках — обновил меню 👇"
    sent = await message.answer(hint, reply_markup=kb)
    await _remember_ui(state, sent.chat.id, sent.message_id)


@router.message(StateFilter(default_state), F.chat.type == ChatType.PRIVATE, F.text == "Мои розыгрыши")
async def menu_btn_my_giveaways(message: Message, state: FSMContext) -> None:
    uid = message.from_user.id if message.from_user else 0
    async with _pg_conn() as db:
        own_items = await list_my_giveaways(db, uid)
        joined_items = await list_joined_giveaways(db, uid)
    if not own_items and not joined_items:
        text = "У тебя пока нет розыгрышей: ни созданных, ни тех, где ты участвуешь."
        kb = _my_giveaways_empty_kb()
    else:
        text = (
            f"{_tg_pe(_PE_MENU_MY_GIVEAWAYS, '📂')} <b>Твои розыгрыши</b>\n"
            "\nОткрой нужную папку:"
        )
        kb = _my_joined_list_kb(own_items, joined_items)
    sent = await message.answer(text, reply_markup=kb, parse_mode="HTML")
    await _remember_ui(state, sent.chat.id, sent.message_id)


@router.message(StateFilter(default_state), F.chat.type == ChatType.PRIVATE, F.text == "Создать розыгрыш")
async def menu_btn_create(message: Message, state: FSMContext, bot: Bot) -> None:
    await state.clear()
    await state.set_state(CreateGiveaway.kind)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Розыгрыш", callback_data="adm:type:giveaway")],
            [InlineKeyboardButton(text="🎰 Лотерея", callback_data="adm:type:lottery")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="adm:cancel")],
        ]
    )
    sent = await message.answer(
        "🎁 <b>Создать розыгрыш</b>\n\nВыбери тип:",
        reply_markup=kb,
    )
    await _remember_ui(state, sent.chat.id, sent.message_id)


@router.message(StateFilter(default_state), F.chat.type == ChatType.PRIVATE, F.chat_shared)
async def menu_reply_chat_shared(message: Message, bot: Bot, dispatcher: Dispatcher) -> None:
    """Выбор чата через request_chat на reply-кнопках «добавить канал/группу»."""
    if not message.from_user or not message.chat_shared:
        return
    rid = message.chat_shared.request_id
    if rid not in (REQ_CHAT_ADD_CHANNEL, REQ_CHAT_ADD_GROUP):
        return
    uid = message.from_user.id
    cs = message.chat_shared
    chat_id = int(cs.chat_id)
    try:
        ch = await bot.get_chat(chat_id)
        ctype = "channel" if ch.type == "channel" else "group"
        title = ch.title or (f"@{ch.username}" if ch.username else str(ch.id))
        uname = ch.username if getattr(ch, "username", None) else None
    except Exception:
        ctype = "channel" if rid == REQ_CHAT_ADD_CHANNEL else "group"
        title = (cs.title or (f"@{cs.username}" if cs.username else None) or str(chat_id)).strip()
        uname = cs.username if getattr(cs, "username", None) else None
    try:
        async with _pg_conn() as db:
            await upsert_saved_chat(db, uid, chat_id, ctype, title, uname)
    except Exception:
        log.exception("saved_chats upsert via chat_shared")
        return
    if ctype == "channel":
        _kickoff_userbot_join(int(chat_id), uname)
    key = StorageKey(bot_id=bot.id, chat_id=uid, user_id=uid)
    ctx = FSMContext(storage=dispatcher.storage, key=key)
    data = await ctx.get_data()
    sc_cid = data.get("saved_chats_msg_chat_id")
    sc_mid = data.get("saved_chats_msg_message_id")
    if sc_cid is not None and sc_mid is not None:
        await _refresh_saved_chats_panel_message(bot, ctx, uid, int(sc_cid), int(sc_mid))
    await _refresh_publish_step_message_if_open(bot, ctx, uid)
    try:
        await message.delete()
    except Exception:
        pass


@router.message(StateFilter(default_state), F.chat.type == ChatType.PRIVATE, F.text == "Сохранёные каналы")
async def menu_btn_saved_chats_reply(message: Message, state: FSMContext, bot: Bot) -> None:
    if not message.from_user:
        return
    await _send_saved_chats_panel(bot, state, message.from_user.id)


@router.message(StateFilter(default_state), F.chat.type == ChatType.PRIVATE, F.text)
async def plain_text_show_menu(message: Message, state: FSMContext) -> None:
    if message.text and message.text.startswith("/"):
        return
    kb = _main_menu_reply_kb()
    hint = "Нажми любую кнопку ниже 👇"
    sent = await message.answer(hint, reply_markup=kb)
    await _remember_ui(state, sent.chat.id, sent.message_id)


@router.callback_query(F.data.startswith("lotpick:"))
async def cb_lottery_pick(query: CallbackQuery, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer()
        return
    try:
        gid = int(parts[1])
        ticket_no = int(parts[2])
    except Exception:
        await query.answer("Некорректный билет.", show_alert=True)
        return
    uid = int(query.from_user.id)
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
        if not g or g.get("status") != "active" or not _is_lottery(g):
            await query.answer("Эта лотерея недоступна.", show_alert=True)
            return
        cur_win = await db.execute(
            "SELECT 1 FROM lottery_picks WHERE giveaway_id = ? AND is_winner = 1 LIMIT 1",
            (gid,),
        )
        if await cur_win.fetchone():
            await query.answer("Победитель уже определён, билеты больше не выбираются.", show_alert=True)
            return
        total = max(1, min(100, int(g.get("lottery_ticket_count") or 1)))
        if ticket_no < 1 or ticket_no > total:
            await query.answer("Такого билета нет.", show_alert=True)
            return
        cur_u = await db.execute(
            "SELECT ticket_no FROM lottery_picks WHERE giveaway_id = ? AND user_id = ?",
            (gid, uid),
        )
        row_u = await cur_u.fetchone()
        if row_u:
            await query.answer(f"Вы уже выбрали билет №{int(row_u['ticket_no'])}.", show_alert=True)
            return
        cur_t = await db.execute(
            "SELECT user_id FROM lottery_picks WHERE giveaway_id = ? AND ticket_no = ?",
            (gid, ticket_no),
        )
        if await cur_t.fetchone():
            await query.answer("Этот билет уже занят, выберите другой.", show_alert=True)
            return
        raw = (g.get("lottery_winning_tickets_json") or "[]").strip()
        try:
            winning_set = {int(x) for x in json.loads(raw)}
        except Exception:
            winning_set = set()
        is_win = 1 if ticket_no in winning_set else 0
        await db.execute(
            "INSERT INTO lottery_picks (giveaway_id, user_id, ticket_no, is_winner, won_at) VALUES (?, ?, ?, ?, ?)",
            (gid, uid, ticket_no, is_win, (_utc_now().isoformat() if is_win else None)),
        )
        await db.commit()
    try:
        fresh_kb = await _lottery_grid_kb(g)
        await bot.edit_message_reply_markup(
            chat_id=query.message.chat.id,
            message_id=query.message.message_id,
            reply_markup=fresh_kb,
        )
    except Exception as e:
        log.debug("lottery repaint %s: %s", gid, e)
    if not is_win:
        await query.answer("Не повезло, это не выигрышный билет.", show_alert=True)
        return
    winner_name = html.escape(
        (
            ("@" + query.from_user.username)
            if query.from_user.username
            else (query.from_user.full_name or f"id {uid}")
        ),
        quote=False,
    )
    text = (
        f"🎉 <b>Есть победитель в лотерее #{gid}!</b>\n\n"
        f"Победитель: <a href=\"tg://user?id={uid}\">{winner_name}</a>\n"
        f"Билет: <b>#{ticket_no}</b>"
    )
    target = int(g.get("post_chat_id") or query.message.chat.id)
    try:
        await bot.send_message(target, text, parse_mode="HTML")
        await _broadcast_results_to_mirrors(bot, g, text)
    except Exception as e:
        log.debug("lottery winner publish %s: %s", gid, e)
    await query.answer("Поздравляем! Вы выиграли 🎉", show_alert=True)


@router.callback_query(F.data.startswith("draw:"))
async def cb_draw(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    back = _kb_back_admin_giveaway(gid)
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
        if not g:
            await _render_callback_screen(query, state, bot, "❌ Такого розыгрыша нет.", back)
            return
        if not _is_giveaway_owner(g, uid):
            await query.answer("Итоги может подвести только автор розыгрыша", show_alert=True)
            return
        if g.get("status") != "active":
            await query.answer(
                "Итоги можно подвести только после публикации розыгрыша.", show_alert=True
            )
            return
        if _is_lottery(g):
            await query.answer("Для лотереи победители определяются сразу по выигрышным билетам.", show_alert=True)
            return
        winners = await _weighted_draw(bot, db, g)
    if not winners:
        await _render_callback_screen(
            query,
            state,
            bot,
            "⚠️ Победителя выбрать не из кого: либо никто не участвовал, либо никто не подходит по подпискам.",
            back,
        )
        return

    body = await _format_winners_lines_html(bot, gid, winners)
    gt = _giveaway_title_html(g, gid)
    publ = f"🎉 <b>{gt} — итоги</b>\n\nПоздравляем победителей:\n{body}"

    pub_chat = g.get("post_chat_id")
    target = int(pub_chat) if pub_chat else query.message.chat.id
    sent = await bot.send_message(target, publ)
    mirror_res = await _broadcast_results_to_mirrors(bot, g, publ)
    wjson = json.dumps(winners)
    async with _pg_conn() as db:
        await db.execute(
            "UPDATE giveaways SET results_chat_id = ?, results_message_id = ?, status = 'finished', last_winners_json = ?, mirror_results_json = ? WHERE id = ?",
            (sent.chat.id, sent.message_id, wjson, json.dumps(mirror_res, ensure_ascii=False), gid),
        )
        await db.commit()
    await _notify_participants_finished(bot, g, winners)
    await _render_callback_screen(
        query,
        state,
        bot,
        "✅ Победители выбраны.\nИтог ушёл туда, где висел пост, и в связанные каналы, если они были.",
        back,
    )


@router.callback_query(F.data == "noop")
async def cb_noop(query: CallbackQuery) -> None:
    await query.answer()


@router.callback_query(F.data.startswith("reroll:"))
async def cb_reroll(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    back = _kb_back_admin_giveaway(gid)
    prev_winners: set[int] = set()
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
        if not g:
            await _render_callback_screen(query, state, bot, "❌ Розыгрыш не найден.", back)
            return
        if not _is_giveaway_owner(g, uid):
            await query.answer("Перевыбрать победителей может только автор", show_alert=True)
            return
        if g.get("status") != "active":
            await query.answer(
                "Перевыбор только для активного опубликованного розыгрыша.", show_alert=True
            )
            return
        if _is_lottery(g):
            await query.answer("Для лотереи ручной перевыбор не используется.", show_alert=True)
            return
        raw = g.get("last_winners_json")
        if raw:
            try:
                prev_winners = {int(x) for x in json.loads(raw)}
            except Exception:
                prev_winners = set()
        winners = await _weighted_draw(bot, db, g, exclude_user_ids=prev_winners)
    if not winners:
        await _render_callback_screen(
            query,
            state,
            bot,
            f"{_tg_pe(_PE_LOT_DOCWARN, '⚠️')} Сейчас некого выбрать: возможно, все уже выигрывали в переролле или не проходят по условиям подписки.",
            back,
        )
        return
    body = await _format_winners_lines_html(bot, gid, winners)
    gt = _giveaway_title_html(g, gid)
    publ = f"🔁 <b>{gt} — новые победители</b>\n\nВот обновлённый список:\n{body}"
    pub_chat = g.get("post_chat_id")
    target = int(pub_chat) if pub_chat else query.message.chat.id
    sent = await bot.send_message(target, publ)
    mirror_res = await _broadcast_results_to_mirrors(bot, g, publ)
    wjson = json.dumps(winners)
    async with _pg_conn() as db:
        await db.execute(
            "UPDATE giveaways SET results_chat_id = ?, results_message_id = ?, last_winners_json = ?, mirror_results_json = ? WHERE id = ?",
            (sent.chat.id, sent.message_id, wjson, json.dumps(mirror_res, ensure_ascii=False), gid),
        )
        await db.commit()
    await _render_callback_screen(
        query,
        state,
        bot,
        "✅ Перевыбрали.\nНовый итог там же, где пост, и в связанных каналах, если настраивали.",
        back,
    )


@router.callback_query(F.data.startswith("plist:"))
async def cb_plist(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    back = _kb_back_admin_giveaway(gid)
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
        cur = await db.execute(
            "SELECT user_id, username, first_name FROM participants WHERE giveaway_id = ? ORDER BY joined_at",
            (gid,),
        )
        rows = await cur.fetchall()
    if not g:
        await _render_callback_screen(query, state, bot, "❌ Розыгрыш не найден.", back)
        return
    if not _is_giveaway_owner(g, uid):
        await query.answer("Список видит только автор розыгрыша", show_alert=True)
        return
    channels = _channels_list(g["channels_json"])
    lines_out: list[str] = []
    for row in rows:
        uid = int(row["user_id"])
        ok, _ = await check_user_subscribed(bot, uid, channels)
        un = row["username"] or ""
        mark = "✅" if ok else "⚠️ подписки нет"
        line = f"{mark} <code>{uid}</code> @{un} {row['first_name'] or ''}"
        lines_out.append(line)
    if not lines_out:
        await _render_callback_screen(
            query, state, bot, f"📋 <b>Участники #{gid}</b>\n\nПока тихо — никто не нажал «Участвовать».", back
        )
        return
    body = "\n".join(lines_out)
    header = f"📋 <b>Участники #{gid}</b>\n\n"
    max_body = 4096 - len(header) - 40
    if len(body) > max_body:
        body = body[:max_body] + "\n\n<i>дальше не влезло</i>"
    await _render_callback_screen(query, state, bot, header + body, back)


@router.callback_query(F.data.startswith("repost:"))
async def cb_repost(query: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if not query.from_user:
        await query.answer()
        return
    uid = query.from_user.id
    gid = int(query.data.split(":")[1])
    back = _kb_back_admin_giveaway(gid)
    async with _pg_conn() as db:
        g = await get_giveaway(db, gid)
    if not g:
        await _render_callback_screen(query, state, bot, "❌ Розыгрыш не найден.", back)
        return
    if not _is_giveaway_owner(g, uid):
        await query.answer("Запостить снова может только автор", show_alert=True)
        return
    if g.get("status") != "active":
        await query.answer("Повторный пост только для активного розыгрыша.", show_alert=True)
        return
    publish_ids = _publish_chat_ids_from_g(g)
    if not publish_ids:
        await _render_callback_screen(
            query,
            state,
            bot,
            "❌ Не знаю, куда постить — нет выбранных каналов для этого розыгрыша.",
            back,
        )
        return
    for cid in publish_ids:
        v = await _validate_publish_target_for_send(bot, int(cid))
        if v:
            await _render_callback_screen(
                query,
                state,
                bot,
                f"❌ Не получилось отправить: {v}",
                back,
            )
            return
    first_sent: Optional[Message] = None
    new_mirrors: list[dict[str, Any]] = []
    for cid in publish_ids:
        try:
            sm = await _send_giveaway_announcement_to_chat(bot, g, int(cid))
            if first_sent is None:
                first_sent = sm
            else:
                new_mirrors.append({"chat_id": int(sm.chat.id), "message_id": int(sm.message_id)})
        except Exception as e:
            log.warning("repost %s: %s", cid, e)
    if first_sent is None:
        await _render_callback_screen(
            query,
            state,
            bot,
            "❌ Не получилось отправить ни в один выбранный канал.",
            back,
        )
        return
    if new_mirrors:
        by_cid: dict[int, dict[str, Any]] = {}
        for e in _mirror_posts_list(g):
            try:
                by_cid[int(e["chat_id"])] = {
                    "chat_id": int(e["chat_id"]),
                    "message_id": int(e["message_id"]),
                }
            except Exception:
                pass
        for ent in new_mirrors:
            try:
                by_cid[int(ent["chat_id"])] = {
                    "chat_id": int(ent["chat_id"]),
                    "message_id": int(ent["message_id"]),
                }
            except Exception:
                pass
        merged = list(by_cid.values())
        async with _pg_conn() as db:
            await db.execute(
                "UPDATE giveaways SET mirror_posts_json = ? WHERE id = ?",
                (json.dumps(merged, ensure_ascii=False), gid),
            )
            await db.commit()
    await _render_callback_screen(
        query,
        state,
        bot,
        "📣 Пост отправлен во все выбранные каналы публикации.",
        back,
    )


_auto_task: Optional[asyncio.Task] = None
_userbot_client: Any = None
_userbot_started = False
_userbot_peer_id_cache: Optional[int] = None


def _userbot_auto_promote_enabled() -> bool:
    """Автоматом выдавать userbot права админа после добавления бота (USERBOT_AUTO_PROMOTE=0 — выкл)."""
    v = (os.environ.get("USERBOT_AUTO_PROMOTE") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _userbot_configured() -> bool:
    return bool(USERBOT_API_ID and USERBOT_API_HASH and USERBOT_SESSION and TelegramClient is not None)


async def _get_userbot_client() -> Any:
    global _userbot_client, _userbot_started
    if not _userbot_configured():
        return None
    if _userbot_client is None:
        try:
            api_id = int(USERBOT_API_ID)
        except Exception:
            log.warning("USERBOT_API_ID is invalid")
            return None
        _userbot_client = TelegramClient(USERBOT_SESSION, api_id, USERBOT_API_HASH)
    if not _userbot_started:
        try:
            await _userbot_client.start()
            _userbot_started = True
            log.info("userbot started")
        except Exception as e:
            log.warning("userbot start failed: %s", e)
            return None
    return _userbot_client


async def _userbot_peer_telegram_user_id() -> Optional[int]:
    """Telegram user_id аккаунта userbot (для promoteChatMember)."""
    global _userbot_peer_id_cache
    if USERBOT_USER_ID_ENV.isdigit():
        return int(USERBOT_USER_ID_ENV)
    if _userbot_peer_id_cache is not None:
        return _userbot_peer_id_cache
    ub = await _get_userbot_client()
    if ub is None:
        return None
    try:
        me = await ub.get_me()
        if me is None:
            return None
        _userbot_peer_id_cache = int(me.id)
        return _userbot_peer_id_cache
    except Exception:
        return None


async def _promote_userbot_after_bot_added(
    bot: Bot,
    chat: Chat,
    *,
    chat_public_username: Optional[str],
    chat_kind: str,
) -> None:
    """После добавления бота в канал/группу: вступить userbot'ом и выдать ему те же рабочие права админа.

    Требует, чтобы у самого бота в этом чате было can_promote_members (см. admin= в ссылке «Добавить …»).
    """
    uid = await _userbot_peer_telegram_user_id()
    if uid is None:
        log.warning("userbot: не удалось узнать user_id — задайте USERBOT_USER_ID или проверьте сессию")
        return
    cid = int(chat.id)
    try:
        await asyncio.sleep(1.0)
        if chat_kind == "channel" and chat_public_username:
            await _userbot_try_join_channel(cid, chat_public_username)
        elif chat_kind != "channel":
            # В группе/супергруппе JoinChannelRequest не используем — ждём, пока userbot уже в чате.
            pass

        for _ in range(36):
            try:
                m = await bot.get_chat_member(cid, uid)
                st = getattr(m, "status", "")
                if st not in ("left", "kicked"):
                    break
            except TelegramBadRequest:
                pass
            await asyncio.sleep(0.5)
        else:
            log.warning(
                "userbot %s не появился в чате %s как участник — не удалось назначить админом "
                "(для приватных каналов добавьте аккаунт userbot вручную или дайте инвайт)",
                uid,
                cid,
            )
            return

        me_bot = await bot.get_me()
        bm = await bot.get_chat_member(cid, me_bot.id)
        if getattr(bm, "status", "") != "administrator":
            return
        if getattr(bm, "can_promote_members", None) is not True:
            log.warning(
                "У бота нет «добавлять администраторов» в чате %s — откройте ссылку «Добавить в канал/группу» заново "
                "(в admin= должно быть promote_members)",
                cid,
            )
            return

        if chat_kind == "channel":
            await bot.promote_chat_member(
                chat_id=cid,
                user_id=uid,
                is_anonymous=False,
                can_manage_chat=False,
                can_change_info=False,
                can_post_messages=True,
                can_edit_messages=True,
                can_delete_messages=True,
                can_invite_users=True,
                can_pin_messages=True,
                can_promote_members=False,
                can_manage_video_chats=False,
                can_restrict_members=False,
                can_post_stories=False,
                can_edit_stories=False,
                can_delete_stories=False,
            )
        else:
            await bot.promote_chat_member(
                chat_id=cid,
                user_id=uid,
                is_anonymous=False,
                can_manage_chat=True,
                can_change_info=False,
                can_post_messages=True,
                can_edit_messages=True,
                can_delete_messages=True,
                can_invite_users=True,
                can_pin_messages=True,
                can_promote_members=False,
                can_manage_video_chats=False,
                can_restrict_members=True,
                can_post_stories=False,
                can_edit_stories=False,
                can_delete_stories=False,
            )
        log.info("userbot %s назначен администратором в чате %s", uid, cid)
    except TelegramBadRequest as e:
        log.warning("promote_chat_member(userbot): %s", e)
    except Exception:
        log.exception("promote userbot in chat %s", cid)


async def _userbot_try_join_channel(chat_id: int, username: Optional[str]) -> bool:
    """
    Attempt to join a channel with userbot (public channels by @username).
    Returns True if join succeeded (or user already participant), False otherwise.
    """
    ub = await _get_userbot_client()
    if ub is None:
        return False
    un = (username or "").strip().lstrip("@")
    if not un:
        return False
    try:
        from telethon.tl.functions.channels import JoinChannelRequest  # type: ignore
    except Exception:
        return False
    try:
        await ub(JoinChannelRequest(un))
        log.info("userbot joined channel @%s (%s)", un, chat_id)
        return True
    except Exception as e:
        s = str(e).lower()
        if "already participant" in s or "user_already_participant" in s:
            return True
        log.warning("userbot join failed for @%s (%s): %s", un, chat_id, e)
        return False


def _kickoff_userbot_join(chat_id: int, username: Optional[str]) -> None:
    """Fire-and-forget join attempt; never blocks chat add flow."""
    async def _run() -> None:
        try:
            await asyncio.wait_for(_userbot_try_join_channel(chat_id, username), timeout=12)
        except Exception:
            pass
    try:
        asyncio.create_task(_run())
    except Exception:
        pass


async def _send_via_userbot_if_possible(
    bot: Bot, g: dict[str, Any], publish_cid: int
) -> Optional[dict[str, int]]:
    # Только каналы: для групп оставляем обычный bot API.
    ub = await _get_userbot_client()
    if ub is None:
        return None
    try:
        ch = await bot.get_chat(int(publish_cid))
    except Exception:
        return None
    if getattr(ch, "type", "") != "channel":
        return None
    card = _giveaway_public_caption(g)
    kind = (g.get("post_media_kind") or "").strip()
    fid = (g.get("post_media_file_id") or "").strip()
    join_url: Optional[str] = None
    try:
        me = await bot.get_me()
        if getattr(me, "username", None):
            join_url = f"https://t.me/{me.username}?start={JOIN_PREFIX}{int(g['id'])}"
    except Exception:
        join_url = None
    tl_buttons: Any = None
    # Лотерея: кнопки билетов прикручивает бот через edit_message_reply_markup.
    if not _is_lottery(g) and join_url and TlButton is not None:
        tl_buttons = [[TlButton.url("🎁 Участвовать", join_url)]]
    try:
        target: Any = None
        chat_username: Optional[str] = None
        try:
            bchat = await bot.get_chat(int(publish_cid))
            chat_username = getattr(bchat, "username", None) or None
        except Exception:
            chat_username = None

        # 1) Fast path by numeric chat id.
        try:
            target = await ub.get_input_entity(int(publish_cid))
        except Exception:
            target = None

        # 2) By public username (if channel is public).
        if target is None and chat_username:
            try:
                target = await ub.get_input_entity(f"@{chat_username.lstrip('@')}")
            except Exception:
                target = None
            if target is None:
                # Try joining public channel first, then resolve again.
                try:
                    joined = await _userbot_try_join_channel(int(publish_cid), chat_username)
                except Exception:
                    joined = False
                if joined:
                    try:
                        target = await ub.get_input_entity(f"@{chat_username.lstrip('@')}")
                    except Exception:
                        target = None

        # 3) Warm entity cache from dialogs and retry by id.
        if target is None:
            try:
                await ub.get_dialogs(limit=300)
            except Exception:
                pass
            try:
                target = await ub.get_input_entity(int(publish_cid))
            except Exception:
                target = None

        if target is None:
            log.warning(
                "userbot cannot resolve channel entity %s; "
                "make sure this user account is in that channel (or channel has @username / invite link)",
                publish_cid,
            )
            return None

        if kind in {"photo", "animation", "video"} and fid:
            # Re-upload media via userbot (bot file_id isn't usable directly in Telethon).
            f = await bot.get_file(fid)
            bio = io.BytesIO()
            await bot.download_file(f.file_path, destination=bio)
            bio.seek(0)
            ext = ".jpg" if kind == "photo" else ".mp4" if kind == "video" else ".gif"
            bio.name = f"giveaway{ext}"
            sm = await ub.send_file(
                target,
                bio,
                caption=card,
                parse_mode="html",
                link_preview=False,
                buttons=tl_buttons,
            )
        else:
            try:
                sm = await ub.send_message(
                    target,
                    card,
                    parse_mode="html",
                    link_preview=False,
                    buttons=tl_buttons,
                )
            except Exception as e_btn:
                # Some channels/accounts may reject user buttons; retry without buttons.
                log.warning("userbot send with inline button failed for %s: %s", publish_cid, e_btn)
                sm = await ub.send_message(target, card, parse_mode="html", link_preview=False)
        return {"chat_id": int(publish_cid), "message_id": int(sm.id)}
    except Exception as e:
        log.warning("userbot send failed for %s: %s", publish_cid, e)
        return None


async def auto_finish_loop(bot: Bot) -> None:
    while True:
        try:
            now = _utc_now()
            async with _pg_conn() as db:
                cur = await db.execute("SELECT * FROM giveaways WHERE status = 'active'")
                rows = await cur.fetchall()
            due = [
                r
                for r in rows
                if (dict(r).get("giveaway_type") or "giveaway") != "lottery"
                and _parse_stored_ends_at(dict(r)["ends_at"]) < now
            ]
            for row in due:
                g = dict(row)
                gid = int(g["id"])
                post_cid = g.get("post_chat_id")

                async with _pg_conn() as db_draw:
                    winners = await _weighted_draw(bot, db_draw, g)

                if not winners:
                    gt = _giveaway_title_html(g, gid)
                    no_win_text = (
                        f"🏁 <b>{gt} — конкурс закончился</b>\n\n"
                        "Победителей нет — либо никто не подошёл по правилам, либо никто не участвовал."
                    )
                    mirror_res: list[dict[str, Any]] = []
                    async with _pg_conn() as db_up:
                        if post_cid:
                            try:
                                await bot.send_message(int(post_cid), no_win_text)
                            except Exception as e:
                                log.exception("auto_finish no winners publish %s: %s", gid, e)
                        mirror_res = await _broadcast_results_to_mirrors(bot, g, no_win_text)
                        await db_up.execute(
                            "UPDATE giveaways SET status = 'finished', last_winners_json = '[]', mirror_results_json = ? WHERE id = ?",
                            (json.dumps(mirror_res, ensure_ascii=False), gid),
                        )
                        await db_up.commit()
                    await _notify_participants_finished(bot, g, [])
                    continue

                if not post_cid:
                    log.warning("auto_finish giveaway %s: winners drawn but no post_chat_id", gid)
                    wjson = json.dumps(winners)
                    async with _pg_conn() as db_up:
                        await db_up.execute(
                            "UPDATE giveaways SET status = 'finished', last_winners_json = ? WHERE id = ?",
                            (wjson, gid),
                        )
                        await db_up.commit()
                    await _notify_participants_finished(bot, g, winners)
                    continue

                body = await _format_winners_lines_html(bot, gid, winners)
                gt = _giveaway_title_html(g, gid)
                text = f"🎉 <b>{gt} — итоги</b>\n\nПоздравляем победителей:\n{body}"
                try:
                    sent = await bot.send_message(int(post_cid), text)
                    mirror_res = await _broadcast_results_to_mirrors(bot, g, text)
                    wjson = json.dumps(winners)
                    async with _pg_conn() as db3:
                        await db3.execute(
                            "UPDATE giveaways SET results_chat_id = ?, results_message_id = ?, status = 'finished', last_winners_json = ?, mirror_results_json = ? WHERE id = ?",
                            (sent.chat.id, sent.message_id, wjson, json.dumps(mirror_res, ensure_ascii=False), gid),
                        )
                        await db3.commit()
                    await _notify_participants_finished(bot, g, winners)
                except Exception as e:
                    log.exception("auto_finish publish winners %s: %s", gid, e)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("auto_finish_loop")
        await asyncio.sleep(60)


async def main() -> None:
    global _POOL
    await init_db()
    if USERBOT_API_ID or USERBOT_API_HASH or USERBOT_SESSION:
        if not _userbot_configured():
            log.warning("userbot env vars set, but config is incomplete or Telethon is unavailable")
        else:
            await _get_userbot_client()
    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()
    dp.include_router(router)

    global _auto_task
    _auto_task = asyncio.create_task(auto_finish_loop(bot))

    try:
        await dp.start_polling(bot)
    finally:
        if _POOL is not None:
            await _POOL.close()
            _POOL = None


if __name__ == "__main__":
    asyncio.run(main())
