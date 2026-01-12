import os
import re
import sqlite3
import secrets
import asyncio
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Optional, List, Dict, Tuple

from pathlib import Path

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

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# =========================
# ENV (ONLY MANAGER BOT)
# =========================
MANAGER_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MANAGER_USERNAME = os.getenv("BOT_USERNAME", "").strip().lstrip("@")

DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID", "0"))
DB_PATH = os.getenv("SQLITE_PATH", "data.db")

CAPTION_TEMPLATE = os.getenv(
    "CAPTION_TEMPLATE",
    "🎬 <b>Video baru</b>\n"
    "📅 {date}\n\n"
    "Klik tombol di bawah untuk ambil videonya."
)

ADMIN_IDS = {
    int(x.strip())
    for x in (os.getenv("ADMIN_IDS", "").split(","))
    if x.strip().isdigit()
}

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS if ADMIN_IDS else False

# Optional fallback config (kalau bot belum di-setting lewat panel)
FSUB_SHOW_N_FALLBACK = int(os.getenv("FSUB_SHOW_N", "4") or "4")

# =========================
# DB HELPERS
# =========================
def db_init():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # bots registry (clients)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bots (
            bot_key TEXT PRIMARY KEY,
            token TEXT NOT NULL,
            username TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
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

    # settings global (thumb)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # upload session per bot
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

    # per-bot config (key/value)
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

    # pending input mode (admin panel)
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

    conn.commit()
    conn.close()

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

# bots registry
def db_bots_list() -> List[Tuple[str, str, int]]:
    rows = _db_fetchall("SELECT bot_key, username, enabled FROM bots ORDER BY created_at ASC")
    return [(r[0], r[1], int(r[2])) for r in rows]

def db_bots_get(bot_key: str) -> Optional[Tuple[str, str, str, int]]:
    row = _db_fetchone("SELECT bot_key, token, username, enabled FROM bots WHERE bot_key=?", (bot_key,))
    if not row:
        return None
    return (row[0], row[1], row[2], int(row[3]))

def db_bots_upsert(bot_key: str, token: str, username: str, enabled: int = 1):
    now = datetime.now(UTC).isoformat()
    _db_execute("""
        INSERT INTO bots(bot_key, token, username, enabled, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(bot_key) DO UPDATE SET
            token=excluded.token,
            username=excluded.username,
            enabled=excluded.enabled,
            updated_at=excluded.updated_at
    """, (bot_key, token, username, int(enabled), now, now))

def db_bots_set_enabled(bot_key: str, enabled: int):
    _db_execute("UPDATE bots SET enabled=?, updated_at=? WHERE bot_key=?",
                (int(enabled), datetime.now(UTC).isoformat(), bot_key))

def db_bots_delete(bot_key: str):
    _db_execute("DELETE FROM bots WHERE bot_key=?", (bot_key,))

# files/uploads
def db_put_file(bot_key: str, token: str, db_message_id: int):
    _db_execute(
        "INSERT OR REPLACE INTO files(bot_key, token, db_message_id, created_at) VALUES (?, ?, ?, ?)",
        (bot_key, token, db_message_id, datetime.now(UTC).isoformat()),
    )

def db_get_file(bot_key: str, token: str) -> Optional[int]:
    row = _db_fetchone(
        "SELECT db_message_id FROM files WHERE bot_key = ? AND token = ?",
        (bot_key, token),
    )
    return int(row[0]) if row else None

def db_put_upload(bot_key: str, token: str, uploader_id: int, thumb_file_id: Optional[str]):
    _db_execute(
        "INSERT OR REPLACE INTO uploads(bot_key, token, uploader_id, thumb_file_id, created_at) VALUES (?, ?, ?, ?, ?)",
        (bot_key, token, uploader_id, thumb_file_id or "", datetime.now(UTC).isoformat()),
    )

def db_get_upload(bot_key: str, token: str) -> Optional[Tuple[int, str]]:
    row = _db_fetchone(
        "SELECT uploader_id, thumb_file_id FROM uploads WHERE bot_key = ? AND token = ?",
        (bot_key, token),
    )
    if not row:
        return None
    return int(row[0]), (row[1] or "")

def db_del_upload(bot_key: str, token: str):
    _db_execute("DELETE FROM uploads WHERE bot_key = ? AND token = ?", (bot_key, token))

# fsub state
def db_get_fsub_offset(bot_key: str, token: str, user_id: int) -> int:
    row = _db_fetchone(
        "SELECT offset FROM fsub_state WHERE bot_key=? AND token=? AND user_id=?",
        (bot_key, token, user_id),
    )
    return int(row[0]) if row else 0

def db_set_fsub_offset(bot_key: str, token: str, user_id: int, offset: int):
    _db_execute(
        "INSERT OR REPLACE INTO fsub_state(bot_key, token, user_id, offset, updated_at) VALUES (?, ?, ?, ?, ?)",
        (bot_key, token, user_id, int(offset), datetime.now(UTC).isoformat()),
    )

def db_step_fsub_offset(bot_key: str, token: str, user_id: int, step: int, total: int) -> int:
    n = max(total, 1)
    cur_off = db_get_fsub_offset(bot_key, token, user_id)
    new_off = (cur_off + step) % n
    db_set_fsub_offset(bot_key, token, user_id, new_off)
    return new_off

# join links cache
def db_get_join_link(bot_key: str, channel_key: str) -> Optional[str]:
    row = _db_fetchone(
        "SELECT invite_link FROM join_links WHERE bot_key = ? AND channel_key = ?",
        (bot_key, channel_key),
    )
    return row[0] if row else None

def db_set_join_link(bot_key: str, channel_key: str, invite_link: str):
    _db_execute(
        "INSERT OR REPLACE INTO join_links(bot_key, channel_key, invite_link, updated_at) VALUES (?, ?, ?, ?)",
        (bot_key, channel_key, invite_link, datetime.now(UTC).isoformat()),
    )

# bot config / fsub / post
def db_botcfg_set(bot_key: str, cfg_key: str, cfg_val: str):
    _db_execute(
        "INSERT OR REPLACE INTO bot_config(bot_key, cfg_key, cfg_val) VALUES (?, ?, ?)",
        (bot_key, cfg_key, cfg_val),
    )

def db_botcfg_get(bot_key: str, cfg_key: str) -> Optional[str]:
    row = _db_fetchone(
        "SELECT cfg_val FROM bot_config WHERE bot_key = ? AND cfg_key = ?",
        (bot_key, cfg_key),
    )
    return row[0] if row else None

def db_fsub_add(bot_key: str, channel: str):
    _db_execute(
        "INSERT OR IGNORE INTO bot_fsub_channels(bot_key, channel, created_at) VALUES (?, ?, ?)",
        (bot_key, channel, datetime.now(UTC).isoformat()),
    )

def db_fsub_del(bot_key: str, channel: str):
    _db_execute("DELETE FROM bot_fsub_channels WHERE bot_key = ? AND channel = ?", (bot_key, channel))

def db_fsub_clear(bot_key: str):
    _db_execute("DELETE FROM bot_fsub_channels WHERE bot_key = ?", (bot_key,))

def db_fsub_list(bot_key: str) -> List[str]:
    rows = _db_fetchall(
        "SELECT channel FROM bot_fsub_channels WHERE bot_key = ? ORDER BY created_at ASC",
        (bot_key,),
    )
    return [r[0] for r in rows]

def db_post_add(bot_key: str, channel_id: int, title: str):
    _db_execute(
        "INSERT OR REPLACE INTO bot_post_channels(bot_key, channel_id, title, created_at) VALUES (?, ?, ?, ?)",
        (bot_key, int(channel_id), title, datetime.now(UTC).isoformat()),
    )

def db_post_del(bot_key: str, channel_id: int):
    _db_execute("DELETE FROM bot_post_channels WHERE bot_key=? AND channel_id=?", (bot_key, int(channel_id)))

def db_post_clear(bot_key: str):
    _db_execute("DELETE FROM bot_post_channels WHERE bot_key = ?", (bot_key,))

def db_post_list(bot_key: str) -> List[Tuple[int, str]]:
    rows = _db_fetchall(
        "SELECT channel_id, title FROM bot_post_channels WHERE bot_key=? ORDER BY created_at ASC",
        (bot_key,),
    )
    return [(int(r[0]), str(r[1])) for r in rows]

# pending actions
def db_pending_set(bot_key: str, admin_id: int, action: str, payload: str = ""):
    _db_execute(
        "INSERT OR REPLACE INTO pending_actions(bot_key, admin_id, action, payload, updated_at) VALUES (?, ?, ?, ?, ?)",
        (bot_key, int(admin_id), action, payload or "", datetime.now(UTC).isoformat()),
    )

def db_pending_get(bot_key: str, admin_id: int) -> Optional[Tuple[str, str]]:
    row = _db_fetchone(
        "SELECT action, payload FROM pending_actions WHERE bot_key=? AND admin_id=?",
        (bot_key, int(admin_id)),
    )
    if not row:
        return None
    return str(row[0]), str(row[1] or "")

def db_pending_clear(bot_key: str, admin_id: int):
    _db_execute("DELETE FROM pending_actions WHERE bot_key=? AND admin_id=?", (bot_key, int(admin_id)))

# settings (thumb)
def db_set(key: str, value: str):
    _db_execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, value))

def db_get(key: str) -> Optional[str]:
    row = _db_fetchone("SELECT value FROM settings WHERE key = ?", (key,))
    return row[0] if row else None

def db_del(key: str):
    _db_execute("DELETE FROM settings WHERE key = ?", (key,))

# =========================
# UTIL / CONFIG GETTERS
# =========================
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

def get_bot_key(context: ContextTypes.DEFAULT_TYPE) -> str:
    return (context.application.bot_data.get("BOT_KEY") or "").strip() or "unknown"

def get_bot_username(context: ContextTypes.DEFAULT_TYPE) -> str:
    return (context.application.bot_data.get("BOT_USERNAME") or "").lstrip("@") or "unknown"

def get_fsub_channels_for_bot(bot_key: str) -> List[str]:
    return db_fsub_list(bot_key)

def get_fsub_show_n_for_bot(bot_key: str) -> int:
    v = db_botcfg_get(bot_key, "fsub_show_n")
    if v and v.isdigit():
        return max(1, min(int(v), 20))
    return max(1, min(FSUB_SHOW_N_FALLBACK, 20))

def get_post_channels_for_bot(bot_key: str) -> List[Tuple[int, str]]:
    return db_post_list(bot_key)

def make_token(bot_key: str) -> str:
    return f"{bot_key}.{secrets.token_urlsafe(12)}"

def parse_token(token: str) -> Tuple[str, str]:
    if "." in token:
        bk, rest = token.split(".", 1)
        if bk and rest:
            return bk, rest
    return "", token

def deep_link(token: str, bot_username: str) -> str:
    return f"https://t.me/{bot_username.lstrip('@')}?start={token}"

async def is_user_joined_all(context: ContextTypes.DEFAULT_TYPE, bot_key: str, user_id: int) -> bool:
    fsubs = get_fsub_channels_for_bot(bot_key)
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

# =========================
# UI KEYBOARDS
# =========================
async def build_fsub_keyboard(context: ContextTypes.DEFAULT_TYPE, bot_key: str, token: str, user_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []

    fsubs = get_fsub_channels_for_bot(bot_key)
    show_n = get_fsub_show_n_for_bot(bot_key)

    if not fsubs:
        return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Sudah Join", callback_data=f"chk:{token}")]])

    offset = db_get_fsub_offset(bot_key, token, user_id)
    n = len(fsubs)
    k = max(1, min(show_n, n))

    rotated = fsubs[offset:] + fsubs[:offset]
    subset = rotated[:k]

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

def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 BOTS", callback_data="adm:bots"),
         InlineKeyboardButton("⚙️ FSUB", callback_data="adm:fsub")],
        [InlineKeyboardButton("📤 POST", callback_data="adm:post"),
         InlineKeyboardButton("🖼️ Thumb", callback_data="adm:thumb")],
        [InlineKeyboardButton("✖️ Close", callback_data="adm:close")]
    ])

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

def thumb_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Set (reply foto + /setthumb)", callback_data="adm:noop")],
        [InlineKeyboardButton("👀 Show (/showthumb)", callback_data="adm:noop"),
         InlineKeyboardButton("🗑 Delete (/delthumb)", callback_data="adm:noop")],
        [InlineKeyboardButton("⬅️ Back", callback_data="adm:back")]
    ])

def fsub_list_kb(bot_key: str) -> InlineKeyboardMarkup:
    chans = db_fsub_list(bot_key)
    rows: List[List[InlineKeyboardButton]] = []
    for ch in chans[:40]:
        rows.append([InlineKeyboardButton(f"🗑️ {ch}", callback_data=f"adm:fsub:del:{ch}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="adm:fsub")])
    return InlineKeyboardMarkup(rows)

def post_list_kb(bot_key: str) -> InlineKeyboardMarkup:
    chans = db_post_list(bot_key)
    rows: List[List[InlineKeyboardButton]] = []
    for cid, title in chans[:40]:
        rows.append([InlineKeyboardButton(f"🗑️ {title} ({cid})", callback_data=f"adm:post:del:{cid}")])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data="adm:post")])
    return InlineKeyboardMarkup(rows)

# =========================
# BOT MANAGER (runtime spawn/stop)
# =========================
class BotManager:
    def __init__(self):
        self.apps: Dict[str, Application] = {}
        self.tasks: Dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()

    def is_running(self, bot_key: str) -> bool:
        return bot_key in self.apps and bot_key in self.tasks and not self.tasks[bot_key].done()

    async def build_client_app(self, token: str, username: str) -> Application:
        app = Application.builder().token(token).build()
        app.bot_data["BOT_USERNAME"] = username
        app.bot_data["BOT_KEY"] = username  # bot_key = username biar gampang

        # feature handlers (same as client)
        app.add_handler(CommandHandler("start", start_cmd))
        app.add_handler(CallbackQueryHandler(fsub_check_cb, pattern=r"^chk:"))
        app.add_handler(CallbackQueryHandler(fsub_rotate_cb, pattern=r"^rot:"))

        app.add_handler(CommandHandler("admin", admin_cmd))
        app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^adm:"))

        app.add_handler(CommandHandler("setthumb", setthumb_cmd))
        app.add_handler(CommandHandler("showthumb", showthumb_cmd))
        app.add_handler(CommandHandler("delthumb", delthumb_cmd))

        app.add_handler(CallbackQueryHandler(post_select_cb, pattern=r"^(post:|postall:|cancel:)"))
        app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, admin_input_handler))
        app.add_handler(MessageHandler(filters.VIDEO, handle_video))
        return app

    async def start_client(self, token: str, username: str) -> str:
        async with self.lock:
            bot_key = username
            if self.is_running(bot_key):
                return bot_key

            app = await self.build_client_app(token, username)
            await app.initialize()
            await app.start()

            task = asyncio.create_task(app.updater.start_polling(allowed_updates=Update.ALL_TYPES))
            self.apps[bot_key] = app
            self.tasks[bot_key] = task
            return bot_key

    async def stop_client(self, bot_key: str):
        async with self.lock:
            app = self.apps.get(bot_key)
            task = self.tasks.get(bot_key)
            if not app or not task:
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
            self.tasks.pop(bot_key, None)

    async def load_and_start_all(self):
        # start all enabled clients from DB
        rows = _db_fetchall("SELECT token, username FROM bots WHERE enabled=1")
        for token, username in rows:
            try:
                await self.start_client(str(token), str(username))
            except Exception:
                # jangan crash server kalau 1 token invalid
                pass

BOT_MANAGER = BotManager()

# =========================
# CORE FEATURES (start/fsub/post)
# =========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    bot_key = get_bot_key(context)
    bot_u = get_bot_username(context)

    if not context.args:
        fsubs = get_fsub_channels_for_bot(bot_key)
        show_n = get_fsub_show_n_for_bot(bot_key)
        posts = get_post_channels_for_bot(bot_key)

        info = (
            "Kirim video ke bot ini via PM.\n"
            "Bot simpan ke DB channel, lalu kamu pilih mau posting ke channel mana.\n\n"
            "Admin:\n"
            "• /admin (panel tombol)\n"
            "• /setthumb (reply foto)\n"
            "• /showthumb\n"
            "• /delthumb\n"
        )
        info += f"\n\nBot: @{bot_u}"
        info += f"\nFSUB: {len(fsubs)} channel | tombol tampil: {min(show_n, len(fsubs)) if fsubs else 0}"
        info += f"\nPOST targets: {len(posts)} channel"
        if not posts:
            info += "\nWarning: POST target kosong. Set via /admin → POST."
        return await msg.reply_text(info)

    token = context.args[0].strip()
    bk_from_token, bare = parse_token(token)
    use_bot_key = bot_key if not bk_from_token else bk_from_token
    real_token = f"{use_bot_key}.{bare}" if bk_from_token else token

    db_msg_id = db_get_file(use_bot_key, real_token)
    if not db_msg_id:
        return await msg.reply_text("Token/link tidak valid atau sudah dihapus.")

    user_id = update.effective_user.id if update.effective_user else 0
    joined = await is_user_joined_all(context, use_bot_key, user_id)
    if not joined:
        kb = await build_fsub_keyboard(context, use_bot_key, real_token, user_id)
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
    bk_from_token, bare = parse_token(token)
    use_bot_key = bot_key if not bk_from_token else bk_from_token
    real_token = f"{use_bot_key}.{bare}" if bk_from_token else token

    db_msg_id = db_get_file(use_bot_key, real_token)
    if not db_msg_id:
        return await q.edit_message_text("Token/link tidak valid atau sudah dihapus.")

    user_id = q.from_user.id
    joined = await is_user_joined_all(context, use_bot_key, user_id)

    if not joined:
        fsubs = get_fsub_channels_for_bot(use_bot_key)
        show_n = get_fsub_show_n_for_bot(use_bot_key)
        db_step_fsub_offset(use_bot_key, real_token, user_id, show_n, total=len(fsubs))

        kb = await build_fsub_keyboard(context, use_bot_key, real_token, user_id)
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
        await q.edit_message_text("✅ Mantap. Videonya sudah aku kirim ke chat kamu.")
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
    bk_from_token, bare = parse_token(token)
    use_bot_key = bot_key if not bk_from_token else bk_from_token
    real_token = f"{use_bot_key}.{bare}" if bk_from_token else token

    user_id = q.from_user.id
    fsubs = get_fsub_channels_for_bot(use_bot_key)
    show_n = get_fsub_show_n_for_bot(use_bot_key)

    db_step_fsub_offset(use_bot_key, real_token, user_id, show_n, total=len(fsubs))
    kb = await build_fsub_keyboard(context, use_bot_key, real_token, user_id)
    return await q.edit_message_reply_markup(reply_markup=kb)

async def _post_to_channel(context: ContextTypes.DEFAULT_TYPE, channel_id: int, caption: str, link: str, thumb_file_id: Optional[str]):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬇️ Ambil Video", url=link)]])
    custom_thumb_fid = db_get("custom_thumb_file_id")
    if custom_thumb_fid:
        await context.bot.send_photo(chat_id=channel_id, photo=custom_thumb_fid, caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML)
        return
    if thumb_file_id:
        await context.bot.send_photo(chat_id=channel_id, photo=thumb_file_id, caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML)
        return
    await context.bot.send_message(chat_id=channel_id, text=caption, reply_markup=kb, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if update.effective_chat.type != "private":
        return await msg.reply_text("Kirim videonya via PM ke bot ya.")
    if not msg.video:
        return

    bot_key = get_bot_key(context)
    bot_u = get_bot_username(context)
    post_channels = get_post_channels_for_bot(bot_key)
    if not post_channels:
        return await msg.reply_text("POST target belum di-set. Admin: /admin → POST → Add.")

    token = make_token(bot_key)
    link = deep_link(token, bot_u)

    try:
        copied = await context.bot.copy_message(chat_id=DB_CHANNEL_ID, from_chat_id=msg.chat_id, message_id=msg.message_id)
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
    post_channels = get_post_channels_for_bot(bot_key)

    data = q.data or ""

    if data.startswith("cancel:"):
        token = data.split("cancel:", 1)[1].strip()
        up = db_get_upload(bot_key, token)
        if not up:
            return await q.edit_message_text("Session udah nggak ada / token invalid.")
        uploader_id, _thumb = up
        if q.from_user.id != uploader_id and not is_admin(q.from_user.id):
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
        if q.from_user.id != uploader_id and not is_admin(q.from_user.id):
            return await q.answer("Bukan upload kamu.", show_alert=True)

        if idx < 1 or idx > len(post_channels):
            return await q.edit_message_text("Channel index invalid.")

        channel_id, title = post_channels[idx - 1]
        caption = CAPTION_TEMPLATE.format(date=datetime.now().strftime("%Y-%m-%d %H:%M")) + "\n\n🔗 <b>Link:</b>\n" + deep_link(token, bot_u)

        try:
            await _post_to_channel(context, channel_id, caption, deep_link(token, bot_u), thumb_file_id)
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
        if q.from_user.id != uploader_id and not is_admin(q.from_user.id):
            return await q.answer("Bukan upload kamu.", show_alert=True)

        caption = CAPTION_TEMPLATE.format(date=datetime.now().strftime("%Y-%m-%d %H:%M")) + "\n\n🔗 <b>Link:</b>\n" + deep_link(token, bot_u)

        ok = 0
        fail: List[str] = []
        for cid, title in post_channels:
            try:
                await _post_to_channel(context, cid, caption, deep_link(token, bot_u), thumb_file_id)
                ok += 1
            except Exception as e:
                fail.append(f"{title}: {e}")

        db_del_upload(bot_key, token)
        if fail:
            return await q.edit_message_text(f"✅ Posted ke {ok}/{len(post_channels)} channel.\n\nYang gagal:\n" + "\n".join(fail))
        return await q.edit_message_text(f"✅ Posted ke semua channel ({ok}).")

# =========================
# ADMIN (buttons) + INPUT HANDLER
# =========================
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_admin(user.id):
        return await msg.reply_text("Khusus admin.")
    await msg.reply_text("Admin Panel:", reply_markup=admin_panel_kb())

async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    user = q.from_user
    if not user or not is_admin(user.id):
        return await q.answer("Khusus admin.", show_alert=True)

    bot_key = get_bot_key(context)
    data = q.data or ""
    await q.answer()

    if data == "adm:close":
        try:
            return await q.edit_message_text("Closed.")
        except Exception:
            return

    if data == "adm:back":
        return await q.edit_message_text("Admin Panel:", reply_markup=admin_panel_kb())

    if data == "adm:cancel":
        db_pending_clear(bot_key, user.id)
        return await q.edit_message_text("Input mode dibatalkan.", reply_markup=admin_panel_kb())

    if data == "adm:bots":
        return await q.edit_message_text("BOTS Panel:", reply_markup=bots_panel_kb())

    if data == "adm:fsub":
        return await q.edit_message_text("FSUB Panel:", reply_markup=fsub_panel_kb())

    if data == "adm:post":
        return await q.edit_message_text("POST Panel:", reply_markup=post_panel_kb())

    if data == "adm:thumb":
        return await q.edit_message_text(
            "Thumb masih pakai command lama (biar aman).\n"
            "• Reply foto → /setthumb\n"
            "• /showthumb\n"
            "• /delthumb",
            reply_markup=thumb_panel_kb(),
        )

    # ---- BOTS actions
    if data == "adm:bots:add":
        db_pending_set(bot_key, user.id, "bot_add")
        return await q.edit_message_text(
            "Kirim BOT TOKEN dari BotFather.\n"
            "Nanti aku validasi dan langsung nyalain botnya (tanpa edit env).",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:bots:list":
        bots = db_bots_list()
        if not bots:
            return await q.edit_message_text("Belum ada bot client.", reply_markup=bots_panel_kb())
        lines = []
        for bk, u, en in bots:
            status = "ON" if en == 1 else "OFF"
            running = "RUN" if BOT_MANAGER.is_running(bk) else "STOP"
            lines.append(f"• {u} | {status} | {running}")
        return await q.edit_message_text("Bots:\n" + "\n".join(lines), reply_markup=bots_panel_kb())

    if data == "adm:bots:stop":
        db_pending_set(bot_key, user.id, "bot_stop")
        return await q.edit_message_text(
            "Kirim username bot yang mau di-STOP.\nContoh: botA (tanpa @)",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:bots:remove":
        db_pending_set(bot_key, user.id, "bot_remove")
        return await q.edit_message_text(
            "Kirim username bot yang mau dihapus (REMOVE).\nContoh: botA (tanpa @)",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    # ---- FSUB actions
    if data == "adm:fsub:add":
        db_pending_set(bot_key, user.id, "fsub_add")
        return await q.edit_message_text(
            "Kirim channel FSUB: @username atau -100id atau username.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:fsub:shown":
        db_pending_set(bot_key, user.id, "fsub_shown")
        cur = get_fsub_show_n_for_bot(bot_key)
        return await q.edit_message_text(
            f"Kirim angka jumlah tombol join tampil (1-20).\nSaat ini: {cur}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✖️ Cancel", callback_data="adm:cancel")]]),
        )

    if data == "adm:fsub:list":
        chans = db_fsub_list(bot_key)
        text = "FSUB List:\n" + ("\n".join([f"• {c}" for c in chans]) if chans else "— kosong —")
        return await q.edit_message_text(text, reply_markup=fsub_list_kb(bot_key))

    if data == "adm:fsub:clear":
        db_fsub_clear(bot_key)
        return await q.edit_message_text("✅ FSUB cleared untuk bot ini.", reply_markup=fsub_panel_kb())

    if data.startswith("adm:fsub:del:"):
        ch = data.split("adm:fsub:del:", 1)[1].strip()
        db_fsub_del(bot_key, ch)
        return await q.edit_message_text("✅ Deleted.", reply_markup=fsub_list_kb(bot_key))

    # ---- POST actions
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

async def admin_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_admin(user.id):
        return
    if update.effective_chat and update.effective_chat.type != "private":
        return

    bot_key = get_bot_key(context)
    pending = db_pending_get(bot_key, user.id)
    if not pending:
        return

    action, _payload = pending
    text = (msg.text or "").strip()

    # ---- BOTS: add
    if action == "bot_add":
        token = text
        # validate token by calling getMe using a temporary app
        try:
            tmp = Application.builder().token(token).build()
            await tmp.initialize()
            me = await tmp.bot.get_me()
            await tmp.shutdown()
            username = (me.username or "").lstrip("@")
            if not username:
                raise ValueError("Username kosong")
        except Exception as e:
            return await msg.reply_text(f"Token invalid / gagal validasi: {e}")

        # persist and start
        db_bots_upsert(username, token, username, enabled=1)
        try:
            await BOT_MANAGER.start_client(token, username)
        except Exception as e:
            return await msg.reply_text(f"Bot tersimpan, tapi gagal start runtime: {e}")

        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"✅ Bot client @{username} berhasil ditambah & langsung ON.", reply_markup=admin_panel_kb())

    # ---- BOTS: stop
    if action == "bot_stop":
        uname = text.lstrip("@").strip()
        row = db_bots_get(uname)
        if not row:
            return await msg.reply_text("Bot tidak ditemukan di DB.")
        db_bots_set_enabled(uname, 0)
        await BOT_MANAGER.stop_client(uname)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"⏹ @{uname} sudah STOP.", reply_markup=admin_panel_kb())

    # ---- BOTS: remove
    if action == "bot_remove":
        uname = text.lstrip("@").strip()
        row = db_bots_get(uname)
        if not row:
            return await msg.reply_text("Bot tidak ditemukan di DB.")
        await BOT_MANAGER.stop_client(uname)
        db_bots_delete(uname)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"🗑 @{uname} sudah dihapus dari DB.", reply_markup=admin_panel_kb())

    # ---- FSUB: add
    if action == "fsub_add":
        ch = normalize_channel_input(text)
        if not ch:
            return await msg.reply_text("Format tidak valid. Kirim @username atau -100id.")
        db_fsub_add(bot_key, ch)
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"✅ FSUB ditambah: {ch}", reply_markup=admin_panel_kb())

    # ---- FSUB: show n
    if action == "fsub_shown":
        if not text.isdigit():
            return await msg.reply_text("Kirim angka saja (1-20).")
        n = max(1, min(int(text), 20))
        db_botcfg_set(bot_key, "fsub_show_n", str(n))
        db_pending_clear(bot_key, user.id)
        return await msg.reply_text(f"✅ Show N diset jadi {n}", reply_markup=admin_panel_kb())

    # ---- POST: add
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
        return await msg.reply_text(f"✅ POST target ditambah: {title} ({cid})", reply_markup=admin_panel_kb())

# =========================
# ADMIN THUMB (unchanged commands)
# =========================
async def setthumb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_admin(user.id):
        return await msg.reply_text("Khusus admin.")
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        return await msg.reply_text("Cara pakai:\n1) Kirim FOTO\n2) Reply foto itu\n3) /setthumb")
    photo = msg.reply_to_message.photo[-1]
    db_set("custom_thumb_file_id", photo.file_id)
    await msg.reply_text("✅ Thumbnail custom diset.")

async def showthumb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_admin(user.id):
        return await msg.reply_text("Khusus admin.")
    fid = db_get("custom_thumb_file_id")
    if not fid:
        return await msg.reply_text("Belum ada thumbnail custom.")
    await context.bot.send_photo(chat_id=msg.chat_id, photo=fid, caption="Thumbnail custom aktif.")

async def delthumb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    user = update.effective_user
    if not user or not is_admin(user.id):
        return await msg.reply_text("Khusus admin.")
    db_del("custom_thumb_file_id")
    await msg.reply_text("✅ Thumbnail custom dihapus.")

# =========================
# MAIN (manager bot + start clients from DB)
# =========================
def build_manager_app() -> Application:
    app = Application.builder().token(MANAGER_TOKEN).build()
    app.bot_data["BOT_USERNAME"] = MANAGER_USERNAME or "manager"
    app.bot_data["BOT_KEY"] = MANAGER_USERNAME or "manager"

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(fsub_check_cb, pattern=r"^chk:"))
    app.add_handler(CallbackQueryHandler(fsub_rotate_cb, pattern=r"^rot:"))

    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^adm:"))

    app.add_handler(CommandHandler("setthumb", setthumb_cmd))
    app.add_handler(CommandHandler("showthumb", showthumb_cmd))
    app.add_handler(CommandHandler("delthumb", delthumb_cmd))

    app.add_handler(CallbackQueryHandler(post_select_cb, pattern=r"^(post:|postall:|cancel:)"))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, admin_input_handler))
    app.add_handler(MessageHandler(filters.VIDEO, handle_video))
    return app

async def run_all():
    if not MANAGER_TOKEN or not MANAGER_USERNAME or DB_CHANNEL_ID == 0:
        raise SystemExit("Env wajib: BOT_TOKEN, BOT_USERNAME, DB_CHANNEL_ID")

    db_init()

    # start manager bot
    manager_app = build_manager_app()
    await manager_app.initialize()
    await manager_app.start()

    # start all clients from DB
    await BOT_MANAGER.load_and_start_all()

    # run polling manager (clients polling sudah berjalan sebagai tasks terpisah)
    manager_task = asyncio.create_task(manager_app.updater.start_polling(allowed_updates=Update.ALL_TYPES))

    try:
        await manager_task
    finally:
        # stop clients
        bots = list(BOT_MANAGER.apps.keys())
        for bk in bots:
            await BOT_MANAGER.stop_client(bk)

        # stop manager
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
