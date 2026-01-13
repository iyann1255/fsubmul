import os
import re
import sqlite3
import secrets
import asyncio
import signal
from pathlib import Path
from datetime import datetime, UTC
from typing import Optional, List, Dict, Tuple

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)

# =========================
# LOAD ENV (FIXED PATH)
# =========================
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# =========================
# ENV (ONLY MANAGER BOT REQUIRED)
# =========================
MANAGER_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MANAGER_USERNAME_ENV = os.getenv("BOT_USERNAME", "").strip().lstrip("@")  # optional

DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID", "0"))
DB_PATH = os.getenv("SQLITE_PATH", "data.db")

# superadmin global (boleh bypass semua)
ADMIN_IDS = {
    int(x.strip())
    for x in (os.getenv("ADMIN_IDS", "").split(","))
    if x.strip().isdigit()
}

FSUB_SHOW_N_FALLBACK = int(os.getenv("FSUB_SHOW_N", "4") or "4")

CAPTION_TEMPLATE = os.getenv(
    "CAPTION_TEMPLATE",
    "🎬 <b>Video baru</b>\n"
    "📅 {date}\n\n"
    "Klik tombol di bawah untuk ambil videonya."
)


# =========================
# PERMISSIONS
# =========================
def is_superadmin(user_id: int) -> bool:
    return user_id in ADMIN_IDS if ADMIN_IDS else False


def get_bot_key(context: ContextTypes.DEFAULT_TYPE) -> str:
    return (context.application.bot_data.get("BOT_KEY") or "").strip() or "unknown"


def get_bot_username(context: ContextTypes.DEFAULT_TYPE) -> str:
    return (context.application.bot_data.get("BOT_USERNAME") or "").strip().lstrip("@") or "unknown"


# =========================
# DB CORE
# =========================
def _db_execute(q: str, args: tuple = ()):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(q, args)
    conn.commit()
    conn.close()


def _db_fetchone(q: str, args: tuple = ()) -> Optional[tuple]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(q, args)
    row = cur.fetchone()
    conn.close()
    return row


def _db_fetchall(q: str, args: tuple = ()) -> List[tuple]:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(q, args)
    rows = cur.fetchall()
    conn.close()
    return rows


def _table_has_column(table: str, column: str) -> bool:
    rows = _db_fetchall(f"PRAGMA table_info({table})")
    cols = [r[1] for r in rows]
    return column in cols


def db_init():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # bots registry (owner_id still stored)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bots (
            bot_key TEXT PRIMARY KEY,
            token TEXT NOT NULL,
            username TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            owner_id INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    # access control list per bot
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_access (
            bot_key TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'admin',
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, user_id)
        )
    """)

    # files per bot
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            bot_key TEXT NOT NULL,
            token TEXT NOT NULL,
            db_message_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, token)
        )
    """)

    # uploads per bot
    cur.execute("""
        CREATE TABLE IF NOT EXISTS uploads (
            bot_key TEXT NOT NULL,
            token TEXT NOT NULL,
            uploader_id INTEGER NOT NULL,
            thumb_file_id TEXT,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, token)
        )
    """)

    # fsub rotate state per (bot, token, user)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fsub_state (
            bot_key TEXT NOT NULL,
            token TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            offset INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, token, user_id)
        )
    """)

    # join link cache per bot
    cur.execute("""
        CREATE TABLE IF NOT EXISTS join_links (
            bot_key TEXT NOT NULL,
            channel_key TEXT NOT NULL,
            invite_link TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, channel_key)
        )
    """)

    # per-bot config
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_config (
            bot_key TEXT NOT NULL,
            cfg_key TEXT NOT NULL,
            cfg_val TEXT NOT NULL,
            PRIMARY KEY (bot_key, cfg_key)
        )
    """)

    # per-bot fsub channels
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_fsub_channels (
            bot_key TEXT NOT NULL,
            channel TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, channel)
        )
    """)

    # per-bot post channels
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_post_channels (
            bot_key TEXT NOT NULL,
            channel_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, channel_id)
        )
    """)

    # pending input per bot
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_actions (
            bot_key TEXT NOT NULL,
            admin_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (bot_key, admin_id)
        )
    """)

    # global thumb
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()

    # migrations
    if not _table_has_column("bots", "owner_id"):
        _db_execute("ALTER TABLE bots ADD COLUMN owner_id INTEGER NOT NULL DEFAULT 0")


# =========================
# DB HELPERS
# =========================
def db_bots_list() -> List[Tuple[str, str, int, int]]:
    rows = _db_fetchall("SELECT bot_key, username, enabled, owner_id FROM bots ORDER BY created_at ASC")
    return [(r[0], r[1], int(r[2]), int(r[3])) for r in rows]


def db_bots_get(bot_key: str) -> Optional[Tuple[str, str, str, int, int]]:
    row = _db_fetchone("SELECT bot_key, token, username, enabled, owner_id FROM bots WHERE bot_key=?", (bot_key,))
    if not row:
        return None
    return (row[0], row[1], row[2], int(row[3]), int(row[4]))


def db_bots_upsert(bot_key: str, token: str, username: str, enabled: int, owner_id: int):
    now = datetime.now(UTC).isoformat()
    _db_execute("""
        INSERT INTO bots(bot_key, token, username, enabled, owner_id, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(bot_key) DO UPDATE SET
            token=excluded.token,
            username=excluded.username,
            enabled=excluded.enabled,
            owner_id=excluded.owner_id,
            updated_at=excluded.updated_at
    """, (bot_key, token, username, int(enabled), int(owner_id), now, now))


def db_bots_set_enabled(bot_key: str, enabled: int):
    _db_execute("UPDATE bots SET enabled=?, updated_at=? WHERE bot_key=?",
                (int(enabled), datetime.now(UTC).isoformat(), bot_key))


def db_bots_delete(bot_key: str):
    _db_execute("DELETE FROM bots WHERE bot_key=?", (bot_key,))
    _db_execute("DELETE FROM bot_access WHERE bot_key=?", (bot_key,))
    _db_execute("DELETE FROM bot_config WHERE bot_key=?", (bot_key,))
    _db_execute("DELETE FROM bot_fsub_channels WHERE bot_key=?", (bot_key,))
    _db_execute("DELETE FROM bot_post_channels WHERE bot_key=?", (bot_key,))


# ACL
def db_access_add(bot_key: str, user_id: int, role: str = "admin"):
    _db_execute(
        "INSERT OR REPLACE INTO bot_access(bot_key, user_id, role, created_at) VALUES (?, ?, ?, ?)",
        (bot_key, int(user_id), role, datetime.now(UTC).isoformat()),
    )


def db_access_del(bot_key: str, user_id: int):
    _db_execute("DELETE FROM bot_access WHERE bot_key=? AND user_id=?", (bot_key, int(user_id)))


def db_access_clear(bot_key: str):
    _db_execute("DELETE FROM bot_access WHERE bot_key=?", (bot_key,))


def db_access_list(bot_key: str) -> List[Tuple[int, str]]:
    rows = _db_fetchall("SELECT user_id, role FROM bot_access WHERE bot_key=? ORDER BY created_at ASC", (bot_key,))
    return [(int(r[0]), str(r[1])) for r in rows]


def db_access_has(bot_key: str, user_id: int) -> bool:
    row = _db_fetchone("SELECT 1 FROM bot_access WHERE bot_key=? AND user_id=? LIMIT 1", (bot_key, int(user_id)))
    return bool(row)


# files/uploads
def db_put_file(bot_key: str, token: str, db_message_id: int):
    _db_execute(
        "INSERT OR REPLACE INTO files(bot_key, token, db_message_id, created_at) VALUES (?, ?, ?, ?)",
        (bot_key, token, db_message_id, datetime.now(UTC).isoformat()),
    )


def db_get_file(bot_key: str, token: str) -> Optional[int]:
    row = _db_fetchone("SELECT db_message_id FROM files WHERE bot_key=? AND token=?", (bot_key, token))
    return int(row[0]) if row else None


def db_put_upload(bot_key: str, token: str, uploader_id: int, thumb_file_id: Optional[str]):
    _db_execute(
        "INSERT OR REPLACE INTO uploads(bot_key, token, uploader_id, thumb_file_id, created_at) VALUES (?, ?, ?, ?, ?)",
        (bot_key, token, int(uploader_id), thumb_file_id or "", datetime.now(UTC).isoformat()),
    )


def db_get_upload(bot_key: str, token: str) -> Optional[Tuple[int, str]]:
    row = _db_fetchone("SELECT uploader_id, thumb_file_id FROM uploads WHERE bot_key=? AND token=?", (bot_key, token))
    if not row:
        return None
    return int(row[0]), (row[1] or "")


def db_del_upload(bot_key: str, token: str):
    _db_execute("DELETE FROM uploads WHERE bot_key=? AND token=?", (bot_key, token))


# fsub rotate
def db_get_fsub_offset(bot_key: str, token: str, user_id: int) -> int:
    row = _db_fetchone("SELECT offset FROM fsub_state WHERE bot_key=? AND token=? AND user_id=?",
                       (bot_key, token, int(user_id)))
    return int(row[0]) if row else 0


def db_set_fsub_offset(bot_key: str, token: str, user_id: int, offset: int):
    _db_execute(
        "INSERT OR REPLACE INTO fsub_state(bot_key, token, user_id, offset, updated_at) VALUES (?, ?, ?, ?, ?)",
        (bot_key, token, int(user_id), int(offset), datetime.now(UTC).isoformat()),
    )


def db_step_fsub_offset(bot_key: str, token: str, user_id: int, step: int, total: int) -> int:
    n = max(total, 1)
    cur_off = db_get_fsub_offset(bot_key, token, user_id)
    new_off = (cur_off + step) % n
    db_set_fsub_offset(bot_key, token, user_id, new_off)
    return new_off


# join link cache
def db_get_join_link(bot_key: str, channel_key: str) -> Optional[str]:
    row = _db_fetchone("SELECT invite_link FROM join_links WHERE bot_key=? AND channel_key=?",
                       (bot_key, channel_key))
    return row[0] if row else None


def db_set_join_link(bot_key: str, channel_key: str, invite_link: str):
    _db_execute(
        "INSERT OR REPLACE INTO join_links(bot_key, channel_key, invite_link, updated_at) VALUES (?, ?, ?, ?)",
        (bot_key, channel_key, invite_link, datetime.now(UTC).isoformat()),
    )


# bot config + lists
def db_botcfg_set(bot_key: str, cfg_key: str, cfg_val: str):
    _db_execute("INSERT OR REPLACE INTO bot_config(bot_key, cfg_key, cfg_val) VALUES (?, ?, ?)",
                (bot_key, cfg_key, cfg_val))


def db_botcfg_get(bot_key: str, cfg_key: str) -> Optional[str]:
    row = _db_fetchone("SELECT cfg_val FROM bot_config WHERE bot_key=? AND cfg_key=?",
                       (bot_key, cfg_key))
    return row[0] if row else None


def db_fsub_add(bot_key: str, channel: str):
    _db_execute("INSERT OR IGNORE INTO bot_fsub_channels(bot_key, channel, created_at) VALUES (?, ?, ?)",
                (bot_key, channel, datetime.now(UTC).isoformat()))


def db_fsub_del(bot_key: str, channel: str):
    _db_execute("DELETE FROM bot_fsub_channels WHERE bot_key=? AND channel=?", (bot_key, channel))


def db_fsub_clear(bot_key: str):
    _db_execute("DELETE FROM bot_fsub_channels WHERE bot_key=?", (bot_key,))


def db_fsub_list(bot_key: str) -> List[str]:
    rows = _db_fetchall("SELECT channel FROM bot_fsub_channels WHERE bot_key=? ORDER BY created_at ASC", (bot_key,))
    return [r[0] for r in rows]


def db_post_add(bot_key: str, channel_id: int, title: str):
    _db_execute(
        "INSERT OR REPLACE INTO bot_post_channels(bot_key, channel_id, title, created_at) VALUES (?, ?, ?, ?)",
        (bot_key, int(channel_id), title, datetime.now(UTC).isoformat()),
    )


def db_post_del(bot_key: str, channel_id: int):
    _db_execute("DELETE FROM bot_post_channels WHERE bot_key=? AND channel_id=?", (bot_key, int(channel_id)))


def db_post_clear(bot_key: str):
    _db_execute("DELETE FROM bot_post_channels WHERE bot_key=?", (bot_key,))


def db_post_list(bot_key: str) -> List[Tuple[int, str]]:
    rows = _db_fetchall("SELECT channel_id, title FROM bot_post_channels WHERE bot_key=? ORDER BY created_at ASC",
                        (bot_key,))
    return [(int(r[0]), str(r[1])) for r in rows]


# pending
def db_pending_set(bot_key: str, admin_id: int, action: str, payload: str = ""):
    _db_execute(
        "INSERT OR REPLACE INTO pending_actions(bot_key, admin_id, action, payload, updated_at) VALUES (?, ?, ?, ?, ?)",
        (bot_key, int(admin_id), action, payload or "", datetime.now(UTC).isoformat()),
    )


def db_pending_get(bot_key: str, admin_id: int) -> Optional[Tuple[str, str]]:
    row = _db_fetchone("SELECT action, payload FROM pending_actions WHERE bot_key=? AND admin_id=?",
                       (bot_key, int(admin_id)))
    if not row:
        return None
    return str(row[0]), str(row[1] or "")


def db_pending_clear(bot_key: str, admin_id: int):
    _db_execute("DELETE FROM pending_actions WHERE bot_key=? AND admin_id=?", (bot_key, int(admin_id)))


# global thumb
def db_set(key: str, value: str):
    _db_execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, value))


def db_get(key: str) -> Optional[str]:
    row = _db_fetchone("SELECT value FROM settings WHERE key=?", (key,))
    return row[0] if row else None


def db_del(key: str):
    _db_execute("DELETE FROM settings WHERE key=?", (key,))


# =========================
# OWNER/ACL CHECKS
# =========================
def can_manage_bot(bot_key: str, user_id: int, is_manager: bool) -> bool:
    # manager: only superadmin
    if is_manager:
        return is_superadmin(user_id)

    # client: superadmin bypass
    if is_superadmin(user_id):
        return True

    # client: ACL list
    return db_access_has(bot_key, user_id)


# =========================
# CONFIG GETTERS / TOKENS
# =========================
def get_fsub_show_n(bot_key: str) -> int:
    v = db_botcfg_get(bot_key, "fsub_show_n")
    if v and v.isdigit():
        return max(1, min(int(v), 20))
    return max(1, min(FSUB_SHOW_N_FALLBACK, 20))


def make_token(bot_key: str) -> str:
    return f"{bot_key}.{secrets.token_urlsafe(12)}"


def parse_token(token: str) -> Tuple[str, str]:
    if "." in token:
        bk, rest = token.split(".", 1)
        if bk and rest:
            return bk, rest
    return "", token


def deep_link(bot_username: str, token: str) -> str:
    return f"https://t.me/{bot_username.lstrip('@')}?start={token}"


def normalize_channel_input(s: str) -> Optional[str]:
    s = s.strip()
    if not s:
        return None
    if s.startswith("@"):
        return s
    if s.startswith("-") and s[1:].isdigit():
        return s
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", s):
        return s
    return None


def parse_id_list(text: str) -> List[int]:
    """
    Accept:
      123,456,789
      123 456 789
      123|456|789
    """
    raw = re.split(r"[\s,|]+", (text or "").strip())
    out: List[int] = []
    for x in raw:
        if x and x.isdigit():
            out.append(int(x))
    # unique preserve order
    seen = set()
    uniq = []
    for i in out:
        if i not in seen:
            seen.add(i)
            uniq.append(i)
    return uniq


# =========================
# FSUB / JOIN LINKS
# =========================
async def is_user_joined_all(context: ContextTypes.DEFAULT_TYPE, bot_key: str, user_id: int) -> bool:
    fsubs = db_fsub_list(bot_key)
    if not fsubs:
        return True

    for ch in fsubs:
        try:
            member = await context.bot.get_chat_member(chat_id=ch, user_id=user_id)
            status = str(getattr(member, "status", ""))
            if status in ("left", "kicked"):
                return False
        except Exception:
            return False
    return True


async def ensure_invite_link(context: ContextTypes.DEFAULT_TYPE, bot_key: str, ch: str) -> str:
    if ch.startswith("@"):
        return f"https://t.me/{ch.lstrip('@')}"
    if ch and not ch.startswith("-") and re.fullmatch(r"[A-Za-z0-9_]{5,}", ch):
        return f"https://t.me/{ch}"

    cached = db_get_join_link(bot_key, ch)
    if cached:
        return cached

    try:
        inv = await context.bot.create_chat_invite_link(chat_id=ch, name="FSUB auto link")
        link = getattr(inv, "invite_link", "") or ""
        if link:
            db_set_join_link(bot_key, ch, link)
            return link
    except Exception:
        pass

    return ""


async def build_fsub_keyboard(context: ContextTypes.DEFAULT_TYPE, bot_key: str, token: str, user_id: int) -> InlineKeyboardMarkup:
    fsubs = db_fsub_list(bot_key)
    show_n = get_fsub_show_n(bot_key)

    if not fsubs:
        return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sudah Join", callback_data=f"chk:{token}")]])

    offset = db_get_fsub_offset(bot_key, token, user_id)
    n = len(fsubs)
    k = max(1, min(show_n, n))

    rotated = fsubs[offset:] + fsubs[:offset]
    subset = rotated[:k]

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []

    idx = 1
    for ch in subset:
        url = await ensure_invite_link(context, bot_key, ch)
        if not url:
            continue
        row.append(InlineKeyboardButton(text=f"➡️ Join {idx}", url=url))
        idx += 1
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton("🔄 Ganti List", callback_data=f"rot:{token}"),
        InlineKeyboardButton("✅ Sudah Join", callback_data=f"chk:{token}")
    ])
    return InlineKeyboardMarkup(rows)


# =========================
# POSTING UI
# =========================
def build_post_select_keyboard(token: str, post_channels: List[Tuple[int, str]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []

    for idx, (_cid, title) in enumerate(post_channels, start=1):
        row.append(InlineKeyboardButton(text=f"📤 {title}", callback_data=f"post:{token}:{idx}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton("📤 Semua Channel", callback_data=f"postall:{token}"),
        InlineKeyboardButton("✖️ Batal", callback_data=f"cancel:{token}")
    ])
    return InlineKeyboardMarkup(rows)


async def _post_to_channel(context: ContextTypes.DEFAULT_TYPE, channel_id: int, caption: str, link: str, thumb_file_id: Optional[str]):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬇️ Ambil Video", url=link)]])

    custom_thumb_fid = db_get("custom_thumb_file_id")
    if custom_thumb_fid:
        await context.bot.send_photo(
            chat_id=channel_id,
            photo=custom_thumb_fid,
            caption=caption,
            reply_markup=kb,
            parse_mode=ParseMode.HTML,
        )
        return

    if thumb_file_id:
        await context.bot.send_photo(
            chat_id=channel_id,
            photo=thumb_file_id,
            caption=caption,
            reply_markup=kb,
            parse_mode=ParseMode.HTML,
        )
        return

    await context.bot.send_message(
        chat_id=channel_id,
        text=caption,
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


# =========================
# HELP (USER VS ADMIN)
# =========================
def help_user_text(bot_username: str) -> str:
    return (
        "<b>Help User</b>\n\n"
        "• Kirim <b>video</b> ke bot via PM.\n"
        "• Bot akan kasih pilihan mau posting ke channel mana.\n"
        "• Kalau kamu ambil video via link, kamu harus join FSUB dulu (kalau aktif).\n\n"
        f"Link format:\n<code>https://t.me/{bot_username}?start=TOKEN</code>\n"
    )


def help_admin_text(is_manager: bool) -> str:
    if is_manager:
        return (
            "<b>Help Admin (Manager)</b>\n\n"
            "• /admin → panel tombol\n"
            "• Menu <b>BOTS</b> untuk add/stop/remove bot client\n"
            "• FSUB/POST di-set per bot client (chat ke bot client-nya untuk setting)\n"
        )
    return (
        "<b>Help Admin (Bot Client)</b>\n\n"
        "• /admin → panel tombol\n"
        "• FSUB: add/list/clear + set Show N\n"
        "• POST: add/list/clear\n"
        "• AKSES: atur siapa yang bisa buka /admin di bot ini\n"
    )


def help_admin_buttons(is_manager: bool) -> InlineKeyboardMarkup:
    if is_manager:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("🤖 BOTS", callback_data="adm:bots"),
             InlineKeyboardButton("🖼️ Thumb", callback_data="adm:thumb")],
            [InlineKeyboardButton("✖️ Close", callback_data="adm:close")]
        ])
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ FSUB", callback_data="adm:fsub"),
         InlineKeyboardButton("📤 POST", callback_data="adm:post")],
        [InlineKeyboardButton("🔑 AKSES", callback_data="adm:access"),
         InlineKeyboardButton("🖼️ Thumb", callback_data="adm:thumb")],
        [InlineKeyboardButton("✖️ Close", callback_data="adm:close")]
    ])


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user:
        return

    bot_key = get_bot_key(context)
    bot_u = get_bot_username(context)
    is_manager = bool(context.application.bot_data.get("IS_MANAGER", False))

    if can_manage_bot(bot_key, user.id, is_manager=is_manager):
        return await msg.reply_text(
            help_admin_text(is_manager=is_manager),
            reply_markup=help_admin_buttons(is_manager=is_manager),
            parse_mode=ParseMode.HTML,
        )
    return await msg.reply_text(help_user_text(bot_u), parse_mode=ParseMode.HTML)


# =========================
# ADMIN UI (BUTTONS)
# =========================
def admin_panel_kb(is_manager: bool) -> InlineKeyboardMarkup:
    if is_manager:
        rows = [
            [InlineKeyboardButton("🤖 BOTS", callback_data="adm:bots"),
             InlineKeyboardButton("🖼️ Thumb", callback_data="adm:thumb")],
            [InlineKeyboardButton("✖️ Close", callback_data="adm:close")]
        ]
    else:
        rows = [
            [InlineKeyboardButton("⚙️ FSUB", callback_data="adm:fsub"),
             InlineKeyboardButton("📤 POST", callback_data="adm:post")],
            [InlineKeyboardButton("🔑 AKSES", callback_data="adm:access"),
             InlineKeyboardButton("🖼️ Thumb", callback_data="adm:thumb")],
            [InlineKeyboardButton("✖️ Close", callback_data="adm:close")]
        ]
    return InlineKeyboardMarkup(rows)


def bots_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Bot", callback_data="adm:bots:add"),
         InlineKeyboardButton("📋 List", callback_data="adm:bots:list")],
        [InlineKeyboardButton("⏹ Stop Bot", callback_data="adm:bots:stop"),
         InlineKeyboardButton("🗑 Remove Bot", callback_data="adm:bots:remove")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm:back"),
         InlineKeyboardButton("✖️ Cancel Input", callback_data="adm:cancel")]
    ])


def fsub_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add", callback_data="adm:fsub:add"),
         InlineKeyboardButton("📋 List", callback_data="adm:fsub:list")],
        [InlineKeyboardButton("🧹 Clear", callback_data="adm:fsub:clear"),
         InlineKeyboardButton("🔢 Set Show N", callback_data="adm:fsub:shown")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm:back"),
         InlineKeyboardButton("✖️ Cancel Input", callback_data="adm:cancel")]
    ])


def post_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add", callback_data="adm:post:add"),
         InlineKeyboardButton("📋 List", callback_data="adm:post:list")],
        [InlineKeyboardButton("🧹 Clear", callback_data="adm:post:clear")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm:back"),
         InlineKeyboardButton("✖️ Cancel Input", callback_data="adm:cancel")]
    ])


def access_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add IDs", callback_data="adm:access:add"),
         InlineKeyboardButton("📋 List", callback_data="adm:access:list")],
        [InlineKeyboardButton("🗑 Remove ID", callback_data="adm:access:del"),
         InlineKeyboardButton("🧹 Clear", callback_data="adm:access:clear")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm:back"),
         InlineKeyboardButton("✖️ Cancel Input", callback_data="adm:cancel")]
    ])


def fsub_list_kb(bot_key: str) -> InlineKeyboardMarkup:
    chans = db_fsub_list(bot_key)
    rows: List[List[InlineKeyboardButton]] = []
    for ch in chans[:60]:
        rows.append([InlineKeyboardButton(f"🗑️ {ch}", callback_data=f"adm:fsub:del:{ch}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="adm:fsub")])
    return InlineKeyboardMarkup(rows)


def post_list_kb(bot_key: str) -> InlineKeyboardMarkup:
    chans = db_post_list(bot_key)
    rows: List[List[InlineKeyboardButton]] = []
    for cid, title in chans[:60]:
        rows.append([InlineKeyboardButton(f"🗑️ {title} ({cid})", callback_data=f"adm:post:del:{cid}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="adm:post")])
    return InlineKeyboardMarkup(rows)


# =========================
# BOT MANAGER (multi-client runtime)
# =========================
class BotManager:
    def __init__(self):
        self.apps: Dict[str, Application] = {}
        self.lock = asyncio.Lock()

    def is_running(self, bot_key: str) -> bool:
        app = self.apps.get(bot_key)
        return bool(app and getattr(app, "running", False))

    async def _wire_handlers(self, app: Application, bot_key: str, bot_username: str, is_manager: bool):
        app.bot_data["BOT_KEY"] = bot_key
        app.bot_data["BOT_USERNAME"] = bot_username
        app.bot_data["IS_MANAGER"] = bool(is_manager)

        # Core
        app.add_handler(CommandHandler("start", start_cmd))
        app.add_handler(CallbackQueryHandler(fsub_check_cb, pattern=r"^chk:"))
        app.add_handler(CallbackQueryHandler(fsub_rotate_cb, pattern=r"^rot:"))

        # Help
        app.add_handler(CommandHandler("help", help_cmd))

        # Admin panels
        app.add_handler(CommandHandler("admin", admin_cmd))
        app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^adm:"))
        app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, admin_input_handler))

        # Thumb commands (global)
        app.add_handler(CommandHandler("setthumb", setthumb_cmd))
        app.add_handler(CommandHandler("showthumb", showthumb_cmd))
        app.add_handler(CommandHandler("delthumb", delthumb_cmd))

        # Posting
        app.add_handler(CallbackQueryHandler(post_select_cb, pattern=r"^(post:|postall:|cancel:)"))
        app.add_handler(MessageHandler(filters.VIDEO, handle_video))

    async def start_client(self, token: str) -> Tuple[str, str]:
        tmp = Application.builder().token(token).build()
        await tmp.initialize()
        me = await tmp.bot.get_me()
        await tmp.shutdown()

        username = (me.username or "").lstrip("@")
        if not username:
            raise ValueError("Bot username kosong dari getMe()")
        bot_key = username

        async with self.lock:
            if bot_key in self.apps and getattr(self.apps[bot_key], "running", False):
                return bot_key, username

            app = Application.builder().token(token).build()
            await self._wire_handlers(app, bot_key, username, is_manager=False)

            await app.initialize()
            await app.start()
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

            self.apps[bot_key] = app
            return bot_key, username

    async def stop_client(self, bot_key: str):
        async with self.lock:
            app = self.apps.get(bot_key)
            if not app:
                return
            try:
                await app.updater.stop()
            except Exception:
                pass
            try:
                await app.stop()
            except Exception:
                pass
            try:
                await app.shutdown()
            except Exception:
                pass
            self.apps.pop(bot_key, None)

    async def load_and_start_all(self):
        rows = _db_fetchall("SELECT token FROM bots WHERE enabled=1")
        for (token,) in rows:
            try:
                await self.start_client(str(token))
            except Exception:
                pass


BOT_MANAGER = BotManager()


# =========================
# START + FSUB FLOW
# =========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    bot_key = get_bot_key(context)
    bot_u = get_bot_username(context)

    if not context.args:
        fsubs = db_fsub_list(bot_key)
        posts = db_post_list(bot_key)
        show_n = get_fsub_show_n(bot_key)

        info = (
            "Kirim video ke bot ini via PM.\n"
            "Bot simpan ke DB channel, lalu kamu pilih mau posting ke channel mana.\n\n"
            "• /help untuk panduan\n"
        )
        info += f"\nBot: @{bot_u}"
        info += f"\nFSUB: {len(fsubs)} | tombol tampil: {min(show_n, len(fsubs)) if fsubs else 0}"
        info += f"\nPOST targets: {len(posts)}"
        return await msg.reply_text(info)

    token = context.args[0].strip()
    bk, bare = parse_token(token)
    use_key = bk or bot_key
    real_token = f"{use_key}.{bare}" if bk else token

    db_msg_id = db_get_file(use_key, real_token)
    if not db_msg_id:
        return await msg.reply_text("Token/link tidak valid atau sudah dihapus.")

    user_id = update.effective_user.id if update.effective_user else 0
    joined = await is_user_joined_all(context, use_key, user_id)
    if not joined:
        kb = await build_fsub_keyboard(context, use_key, real_token, user_id)
        return await msg.reply_text(
            "🔒 Kamu harus join semua channel wajib dulu sebelum ambil video.\n\n"
            "Tip: Klik 🔄 Ganti List buat munculin channel lain.",
            reply_markup=kb,
        )

    try:
        await context.bot.copy_message(
            chat_id=update.effective_chat.id,
            from_chat_id=DB_CHANNEL_ID,
            message_id=db_msg_id,
        )
    except Exception as e:
        await msg.reply_text(f"Gagal ngirim video: {e}")


async def fsub_check_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    bot_key = get_bot_key(context)
    data = q.data or ""
    if not data.startswith("chk:"):
        return

    token = data.split("chk:", 1)[1].strip()
    bk, bare = parse_token(token)
    use_key = bk or bot_key
    real_token = f"{use_key}.{bare}" if bk else token

    db_msg_id = db_get_file(use_key, real_token)
    if not db_msg_id:
        return await q.edit_message_text("Token/link tidak valid atau sudah dihapus.")

    user_id = q.from_user.id
    joined = await is_user_joined_all(context, use_key, user_id)
    fsubs = db_fsub_list(use_key)

    if not joined:
        show_n = get_fsub_show_n(use_key)
        db_step_fsub_offset(use_key, real_token, user_id, show_n, total=len(fsubs))
        kb = await build_fsub_keyboard(context, use_key, real_token, user_id)
        return await q.edit_message_text(
            "🔒 Masih belum join semua channel wajib.\n"
            "Join dulu, lalu klik ✅ Sudah Join lagi.\n\n"
            "List join bakal ganti otomatis biar nggak monoton.",
            reply_markup=kb,
        )

    try:
        await context.bot.copy_message(
            chat_id=user_id,
            from_chat_id=DB_CHANNEL_ID,
            message_id=db_msg_id,
        )
        await q.edit_message_text("✅ Videonya sudah aku kirim ke chat kamu.")
    except Exception as e:
        await q.edit_message_text(f"Gagal ngirim video: {e}")


async def fsub_rotate_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    bot_key = get_bot_key(context)
    data = q.data or ""
    if not data.startswith("rot:"):
        return

    token = data.split("rot:", 1)[1].strip()
    bk, bare = parse_token(token)
    use_key = bk or bot_key
    real_token = f"{use_key}.{bare}" if bk else token

    user_id = q.from_user.id
    fsubs = db_fsub_list(use_key)
    show_n = get_fsub_show_n(use_key)

    db_step_fsub_offset(use_key, real_token, user_id, show_n, total=len(fsubs))
    kb = await build_fsub_keyboard(context, use_key, real_token, user_id)
    return await q.edit_message_reply_markup(reply_markup=kb)


# =========================
# VIDEO -> DB -> SELECT POST
# =========================
async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if update.effective_chat.type != "private":
        return await msg.reply_text("Kirim videonya via PM ke bot ya.")
    if not msg.video:
        return

    bot_key = get_bot_key(context)
    bot_u = get_bot_username(context)
    post_channels = db_post_list(bot_key)
    if not post_channels:
        return await msg.reply_text("POST target belum di-set. Admin: /admin → POST → Add.")

    token = make_token(bot_key)

    try:
        copied = await context.bot.copy_message(
            chat_id=DB_CHANNEL_ID,
            from_chat_id=msg.chat_id,
            message_id=msg.message_id,
        )
        db_put_file(bot_key, token, copied.message_id)
    except Exception as e:
        return await msg.reply_text(f"Gagal simpan ke DB channel. Pastikan bot admin.\nError: {e}")

    thumb_file_id = ""
    try:
        if msg.video.thumbnail:
            thumb_file_id = msg.video.thumbnail.file_id
    except Exception:
        thumb_file_id = ""

    uploader_id = update.effective_user.id if update.effective_user else 0
    db_put_upload(bot_key, token, uploader_id, thumb_file_id)

    kb = build_post_select_keyboard(token, post_channels)
    await msg.reply_text("✅ Videonya sudah masuk DB.\nSekarang pilih mau posting ke channel mana:", reply_markup=kb)


async def post_select_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    bot_key = get_bot_key(context)
    bot_u = get_bot_username(context)
    post_channels = db_post_list(bot_key)
    data = q.data or ""

    if data.startswith("cancel:"):
        token = data.split("cancel:", 1)[1].strip()
        up = db_get_upload(bot_key, token)
        if not up:
            return await q.edit_message_text("Session udah nggak ada / token invalid.")
        uploader_id, _ = up

        # uploader atau admin bot boleh cancel
        is_manager = bool(context.application.bot_data.get("IS_MANAGER", False))
        if q.from_user.id != uploader_id and not can_manage_bot(bot_key, q.from_user.id, is_manager=is_manager):
            return await q.answer("Bukan upload kamu.", show_alert=True)

        db_del_upload(bot_key, token)
        return await q.edit_message_text("✖️ Dibatalkan. Videonya tetap aman di DB.")

    if data.startswith("post:"):
        try:
            _, token, idx_s = data.split(":", 2)
            idx = int(idx_s)
        except Exception:
            return

        up = db_get_upload(bot_key, token)
        if not up:
            return await q.edit_message_text("Session udah nggak ada / token invalid.")
        uploader_id, thumb_file_id = up

        is_manager = bool(context.application.bot_data.get("IS_MANAGER", False))
        if q.from_user.id != uploader_id and not can_manage_bot(bot_key, q.from_user.id, is_manager=is_manager):
            return await q.answer("Bukan upload kamu.", show_alert=True)

        if idx < 1 or idx > len(post_channels):
            return await q.edit_message_text("Channel index invalid.")

        channel_id, title = post_channels[idx - 1]
        caption = (
            CAPTION_TEMPLATE.format(date=datetime.now().strftime("%Y-%m-%d %H:%M"))
            + "\n\n"
            + "🔗 <b>Link:</b>\n"
            + deep_link(bot_u, token)
        )

        try:
            await _post_to_channel(context, channel_id, caption, deep_link(bot_u, token), thumb_file_id)
        except Exception as e:
            return await q.edit_message_text(f"Gagal posting ke {title}: {e}")

        db_del_upload(bot_key, token)
        return await q.edit_message_text(f"✅ Posted ke {title}.")

    if data.startswith("postall:"):
        token = data.split("postall:", 1)[1].strip()
        up = db_get_upload(bot_key, token)
        if not up:
            return await q.edit_message_text("Session udah nggak ada / token invalid.")
        uploader_id, thumb_file_id = up

        is_manager = bool(context.application.bot_data.get("IS_MANAGER", False))
        if q.from_user.id != uploader_id and not can_manage_bot(bot_key, q.from_user.id, is_manager=is_manager):
            return await q.answer("Bukan upload kamu.", show_alert=True)

        caption = (
            CAPTION_TEMPLATE.format(date=datetime.now().strftime("%Y-%m-%d %H:%M"))
            + "\n\n"
            + "🔗 <b>Link:</b>\n"
            + deep_link(bot_u, token)
        )

        ok = 0
        fail: List[str] = []
        for cid, title in post_channels:
            try:
                await _post_to_channel(context, cid, caption, deep_link(bot_u, token), thumb_file_id)
                ok += 1
            except Exception as e:
                fail.append(f"{title}: {e}")

        db_del_upload(bot_key, token)
        if fail:
            return await q.edit_message_text(
                f"✅ Posted ke {ok}/{len(post_channels)} channel.\n\nYang gagal:\n" + "\n".join(fail)
            )
        return await q.edit_message_text(f"✅ Posted ke semua channel ({ok}).")


# =========================
# THUMB COMMANDS (GLOBAL SUPERADMIN)
# =========================
async def setthumb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_superadmin(user.id):
        return await msg.reply_text("Khusus superadmin.")
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        return await msg.reply_text("Cara pakai:\n1) Kirim FOTO\n2) Reply foto itu\n3) /setthumb")
    photo = msg.reply_to_message.photo[-1]
    db_set("custom_thumb_file_id", photo.file_id)
    await msg.reply_text("✅ Thumbnail custom diset.")


async def showthumb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_superadmin(user.id):
        return await msg.reply_text("Khusus superadmin.")
    fid = db_get("custom_thumb_file_id")
    if not fid:
        return await msg.reply_text("Belum ada thumbnail custom.")
    await context.bot.send_photo(chat_id=msg.chat_id, photo=fid, caption="Thumbnail custom aktif.")


async def delthumb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_superadmin(user.id):
        return await msg.reply_text("Khusus superadmin.")
    db_del("custom_thumb_file_id")
    await msg.reply_text("✅ Thumbnail custom dihapus.")


# =========================
# ADMIN HANDLERS
# =========================
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user:
        return

    bot_key = get_bot_key(context)
    is_manager = bool(context.application.bot_data.get("IS_MANAGER", False))

    if not can_manage_bot(bot_key, user.id, is_manager=is_manager):
        return await msg.reply_text("Akses ditolak. Kamu bukan admin/owner bot ini.")

    await msg.reply_text("Admin Panel:", reply_markup=admin_panel_kb(is_manager=is_manager))


async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    user = q.from_user
    if not user:
        return

    bot_key = get_bot_key(context)
    is_manager = bool(context.application.bot_data.get("IS_MANAGER", False))

    if not can_manage_bot(bot_key, user.id, is_manager=is_manager):
        return await q.answer("Akses ditolak.", show_alert=True)

    data = q.data or ""
    await q.answer()

    if data == "adm:close":
        try:
            return await q.edit_message_text("Closed.")
        except Exception:
            return

    if data == "adm:back":
        return await q.edit_message_text("Admin Panel:", reply_markup=admin_panel_kb(is_manager=is_manager))

    if data == "adm:cancel":
        db_pending_clear(bot_key, user.id)
        return await q.edit_message_text("Input mode dibatalkan.", reply_markup=admin_panel_kb(is_manager=is_manager))

    # panels
    if data == "adm:fsub":
        return await q.edit_message_text("FSUB Panel:", reply_markup=fsub_panel_kb())

    if data == "adm:post":
        return await q.edit_message_text("POST Panel:", reply_markup=post_panel_kb())

    if data == "adm:access":
        if is_manager:
            return await q.edit_message_text("AKSES hanya untuk bot client (bukan manager).", reply_markup=admin_panel_kb(is_manager=True))
        return await q.edit_message_text("AKSES Panel:", reply_markup=access_panel_kb())

    if data == "adm:thumb":
        return await q.edit_message_text(
            "Thumb (global superadmin):\n"
            "• Reply foto → /setthumb\n"
            "• /showthumb\n"
            "• /delthumb",
            reply_markup=admin_panel_kb(is_manager=is_manager),
        )

    # manager-only: bots registry
    if data == "adm:bots":
        if not is_manager:
            return await q.answer("Menu ini cuma ada di manager.", show_alert=True)
        return await q.edit_message_text("BOTS Panel:", reply_markup=bots_panel_kb())

    if data == "adm:bots:add":
        db_pending_set(bot_key, user.id, "bot_add_token")
        return await q.edit_message_text(
            "Kirim BOT TOKEN dari BotFather.\n"
            "Setelah token valid, aku akan minta ID yang boleh akses bot itu.\n\n"
            "Ketik /help kalau kamu butuh format.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:bots:list":
        bots = db_bots_list()
        if not bots:
            return await q.edit_message_text("Belum ada bot client.", reply_markup=bots_panel_kb())
        lines = []
        for bk, u, en, oid in bots:
            status = "ON" if en == 1 else "OFF"
            running = "RUN" if BOT_MANAGER.is_running(bk) else "STOP"
            lines.append(f"• @{u} | {status} | {running} | owner:{oid}")
        return await q.edit_message_text("Bots:\n" + "\n".join(lines), reply_markup=bots_panel_kb())

    if data == "adm:bots:stop":
        db_pending_set(bot_key, user.id, "bot_stop")
        return await q.edit_message_text(
            "Kirim username bot yang mau di-STOP (tanpa @).",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:bots:remove":
        db_pending_set(bot_key, user.id, "bot_remove")
        return await q.edit_message_text(
            "Kirim username bot yang mau dihapus (tanpa @).",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    # FSUB actions
    if data == "adm:fsub:add":
        db_pending_set(bot_key, user.id, "fsub_add")
        return await q.edit_message_text(
            "Kirim channel FSUB: @username atau -100id atau username.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:fsub:shown":
        db_pending_set(bot_key, user.id, "fsub_shown")
        cur = get_fsub_show_n(bot_key)
        return await q.edit_message_text(
            f"Kirim angka jumlah tombol join tampil (1-20). Saat ini: {cur}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:fsub:list":
        chans = db_fsub_list(bot_key)
        text = "FSUB List:\n" + ("\n".join([f"• {c}" for c in chans]) if chans else "— kosong —")
        return await q.edit_message_text(text, reply_markup=fsub_list_kb(bot_key))

    if data == "adm:fsub:clear":
        db_fsub_clear(bot_key)
        return await q.edit_message_text("✅ FSUB cleared.", reply_markup=fsub_panel_kb())

    if data.startswith("adm:fsub:del:"):
        ch = data.split("adm:fsub:del:", 1)[1].strip()
        db_fsub_del(bot_key, ch)
        return await q.edit_message_text("✅ Deleted.", reply_markup=fsub_list_kb(bot_key))

    # POST actions
    if data == "adm:post:add":
        db_pending_set(bot_key, user.id, "post_add")
        return await q.edit_message_text(
            "Kirim target POST:\nFormat: -100id Judul\nContoh: -1001234567890 CH1",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:post:list":
        chans = db_post_list(bot_key)
        text = "POST Targets:\n" + ("\n".join([f"• {t} ({cid})" for cid, t in chans]) if chans else "— kosong —")
        return await q.edit_message_text(text, reply_markup=post_list_kb(bot_key))

    if data == "adm:post:clear":
        db_post_clear(bot_key)
        return await q.edit_message_text("✅ POST targets cleared.", reply_markup=post_panel_kb())

    if data.startswith("adm:post:del:"):
        cid_s = data.split("adm:post:del:", 1)[1].strip()
        try:
            cid = int(cid_s)
        except ValueError:
            return
        db_post_del(bot_key, cid)
        return await q.edit_message_text("✅ Deleted.", reply_markup=post_list_kb(bot_key))

    # ACCESS actions (client only)
    if data == "adm:access:add":
        if is_manager:
            return
        db_pending_set(bot_key, user.id, "access_add")
        return await q.edit_message_text(
            "Kirim ID yang boleh akses (pisah koma/spasi). Contoh:\n<code>13312413, 13124211</code>\n\nKetik <code>skip</code> untuk batal.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:access:del":
        if is_manager:
            return
        db_pending_set(bot_key, user.id, "access_del")
        return await q.edit_message_text(
            "Kirim 1 user_id yang mau dihapus aksesnya.\nContoh: <code>13312413</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:access:list":
        if is_manager:
            return await q.edit_message_text("AKSES hanya untuk bot client.", reply_markup=admin_panel_kb(is_manager=True))
        lst = db_access_list(bot_key)
        if not lst:
            return await q.edit_message_text("Belum ada akses terset.", reply_markup=access_panel_kb())
        lines = [f"• {uid} ({role})" for uid, role in lst]
        return await q.edit_message_text("Akses list:\n" + "\n".join(lines), reply_markup=access_panel_kb())

    if data == "adm:access:clear":
        if is_manager:
            return
        db_access_clear(bot_key)
        return await q.edit_message_text("✅ Semua akses dihapus. (Pastikan owner masih kamu tambahin lagi!)", reply_markup=access_panel_kb())


async def admin_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or update.effective_chat.type != "private":
        return

    bot_key = get_bot_key(context)
    is_manager = bool(context.application.bot_data.get("IS_MANAGER", False))

    pending = db_pending_get(bot_key, user.id)
    if not pending:
        return

    # gate based on bot (manager/client)
    if not can_manage_bot(bot_key, user.id, is_manager=is_manager):
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text("Akses ditolak.")

    action, payload = pending
    text = (msg.text or "").strip()

    # ----- MANAGER: ADD BOT 2-STEP
    if action == "bot_add_token":
        token = text
        try:
            bot_key_new, username = await BOT_MANAGER.start_client(token)
        except Exception as e:
            return await msg.reply_text(f"Token invalid / gagal start bot: {e}")

        # owner = user who added (superadmin)
        db_bots_upsert(bot_key_new, token, username, enabled=1, owner_id=user.id)

        # ACL: always include owner as 'owner'
        db_access_add(bot_key_new, user.id, role="owner")

        # next step: ask access IDs
        db_pending_set(bot_key, user.id, "bot_add_acl", payload=bot_key_new)
        return await msg.reply_text(
            f"✅ Bot @{username} sudah ON.\n\n"
            "Sekarang kirim ID yang boleh akses bot ini (pisah koma/spasi).\n"
            "Contoh: 13312413, 13124211\n"
            "Atau ketik: skip",
        )

    if action == "bot_add_acl":
        new_bot_key = (payload or "").strip()
        if not new_bot_key:
            db_pending_clear(bot_key, user.id)
            return await msg.reply_text("Payload error. Coba add ulang.")

        if text.lower() == "skip":
            db_pending_clear(bot_key, user.id)
            return await msg.reply_text(f"✅ Selesai. Akses bot @{new_bot_key} cuma owner dulu.", reply_markup=admin_panel_kb(is_manager=True))

        ids = parse_id_list(text)
        if not ids:
            return await msg.reply_text("Tidak ada ID valid. Kirim angka-angka user_id saja, atau ketik skip.")

        # add as admin
        for uid in ids:
            db_access_add(new_bot_key, uid, role="admin")

        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(
            f"✅ Akses ditambahkan ke @{new_bot_key}:\n" + "\n".join([f"• {x}" for x in ids]),
            reply_markup=admin_panel_kb(is_manager=True)
        )

    # ----- MANAGER: STOP/REMOVE BOT
    if action == "bot_stop":
        uname = text.lstrip("@").strip()
        row = db_bots_get(uname)
        if not row:
            return await msg.reply_text("Bot tidak ditemukan di DB.")
        db_bots_set_enabled(uname, 0)
        await BOT_MANAGER.stop_client(uname)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"⏹ @{uname} sudah STOP.", reply_markup=admin_panel_kb(is_manager=True))

    if action == "bot_remove":
        uname = text.lstrip("@").strip()
        row = db_bots_get(uname)
        if not row:
            return await msg.reply_text("Bot tidak ditemukan di DB.")
        await BOT_MANAGER.stop_client(uname)
        db_bots_delete(uname)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"🗑 @{uname} sudah dihapus dari DB.", reply_markup=admin_panel_kb(is_manager=True))

    # ----- CLIENT SETTINGS
    if action == "fsub_add":
        ch = normalize_channel_input(text)
        if not ch:
            return await msg.reply_text("Format tidak valid. Kirim @username atau -100id.")
        db_fsub_add(bot_key, ch)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"✅ FSUB ditambah: {ch}", reply_markup=admin_panel_kb(is_manager=False))

    if action == "fsub_shown":
        if not text.isdigit():
            return await msg.reply_text("Kirim angka saja (1-20).")
        n = max(1, min(int(text), 20))
        db_botcfg_set(bot_key, "fsub_show_n", str(n))
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"✅ Show N diset jadi {n}", reply_markup=admin_panel_kb(is_manager=False))

    if action == "post_add":
        parts = text.split(None, 1)
        if not parts:
            return await msg.reply_text("Format: -100id Judul")
        try:
            cid = int(parts[0])
        except ValueError:
            return await msg.reply_text("Channel ID harus angka -100xxxx.")
        title = parts[1].strip() if len(parts) > 1 and parts[1].strip() else "CH"
        db_post_add(bot_key, cid, title)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"✅ POST target ditambah: {title} ({cid})", reply_markup=admin_panel_kb(is_manager=False))

    if action == "access_add":
        if text.lower() == "skip":
            db_pending_clear(bot_key, user.id)
            return await msg.reply_text("Dibatalkan.", reply_markup=admin_panel_kb(is_manager=False))

        ids = parse_id_list(text)
        if not ids:
            return await msg.reply_text("Tidak ada ID valid. Kirim angka user_id (pisah koma/spasi).")

        for uid in ids:
            # jangan turunin owner jadi admin; kalau uid sama owner, keep owner
            role = "admin"
            # kalau dia owner, set owner tetap
            current = _db_fetchone("SELECT role FROM bot_access WHERE bot_key=? AND user_id=?", (bot_key, int(uid)))
            if current and current[0] == "owner":
                role = "owner"
            db_access_add(bot_key, uid, role=role)

        db_pending_clear(bot_key, user.id)
        return await msg.reply_text("✅ Akses ditambahkan:\n" + "\n".join([f"• {x}" for x in ids]),
                                    reply_markup=admin_panel_kb(is_manager=False))

    if action == "access_del":
        if not text.isdigit():
            return await msg.reply_text("Kirim 1 user_id angka.")
        uid = int(text)

        # prevent removing owner by mistake
        row = _db_fetchone("SELECT role FROM bot_access WHERE bot_key=? AND user_id=?", (bot_key, uid))
        if row and row[0] == "owner":
            return await msg.reply_text("Owner tidak bisa dihapus dari akses lewat menu ini. (Safety)")

        db_access_del(bot_key, uid)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"✅ Akses {uid} dihapus.", reply_markup=admin_panel_kb(is_manager=False))


# =========================
# BUILD MANAGER APP
# =========================
async def build_manager_app() -> Application:
    app = Application.builder().token(MANAGER_TOKEN).build()
    await app.initialize()

    me = await app.bot.get_me()
    manager_username = MANAGER_USERNAME_ENV or (me.username or "").lstrip("@") or "manager"

    app.bot_data["BOT_KEY"] = manager_username
    app.bot_data["BOT_USERNAME"] = manager_username
    app.bot_data["IS_MANAGER"] = True

    # wire handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(fsub_check_cb, pattern=r"^chk:"))
    app.add_handler(CallbackQueryHandler(fsub_rotate_cb, pattern=r"^rot:"))

    app.add_handler(CommandHandler("help", help_cmd))

    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^adm:"))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, admin_input_handler))

    app.add_handler(CommandHandler("setthumb", setthumb_cmd))
    app.add_handler(CommandHandler("showthumb", showthumb_cmd))
    app.add_handler(CommandHandler("delthumb", delthumb_cmd))

    app.add_handler(CallbackQueryHandler(post_select_cb, pattern=r"^(post:|postall:|cancel:)"))
    app.add_handler(MessageHandler(filters.VIDEO, handle_video))

    return app


# =========================
# RUN LOOP (HOLDS PROCESS)
# =========================
async def run_all():
    if not MANAGER_TOKEN or DB_CHANNEL_ID == 0 or not ADMIN_IDS:
        print("ENV CHECK FAILED:")
        print("BOT_TOKEN:", bool(MANAGER_TOKEN))
        print("DB_CHANNEL_ID:", DB_CHANNEL_ID)
        print("ADMIN_IDS:", ADMIN_IDS)
        raise SystemExit("Env wajib: BOT_TOKEN, DB_CHANNEL_ID, ADMIN_IDS")

    db_init()

    manager_app = await build_manager_app()
    await manager_app.start()

    # start enabled clients from DB
    await BOT_MANAGER.load_and_start_all()

    # start polling manager
    await manager_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    # HOLD until SIGINT/SIGTERM
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    print("RUNNING: manager polling active. Press Ctrl+C to stop.")
    await stop_event.wait()

    # shutdown clients
    for bk in list(BOT_MANAGER.apps.keys()):
        await BOT_MANAGER.stop_client(bk)

    # shutdown manager
    try:
        await manager_app.updater.stop()
    except Exception:
        pass
    try:
        await manager_app.stop()
    except Exception:
        pass
    try:
        await manager_app.shutdown()
    except Exception:
        pass


def main():
    asyncio.run(run_all())


if __name__ == "__main__":
    main()
