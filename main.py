import os
import re
import hmac
import json
import logging
import calendar
import asyncio
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row
from aiohttp import web
from telegram.error import BadRequest, RetryAfter

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ConversationHandler,
    MessageHandler,
    TypeHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Neon (or any Postgres) connection string, e.g.
# postgresql://user:password@host/dbname?sslmode=require
DATABASE_URL = os.environ.get("DATABASE_URL")
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
# Restricts /broadcast to just you — get your numeric ID from @userinfobot
# on Telegram, then set it as this env var. If unset, /broadcast is
# disabled entirely (no one can use it, not even accidentally).
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")
if ADMIN_USER_ID is not None:
    try:
        ADMIN_USER_ID = int(ADMIN_USER_ID.strip())
    except ValueError:
        # A typo here must not take the whole bot down — just leave
        # /broadcast disabled for everyone, same as if it were unset.
        logger.warning("ADMIN_USER_ID is not a number — /broadcast stays disabled.")
        ADMIN_USER_ID = None

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_pg_conn = None


class _SharedConnProxy:
    """Every call site in this file does `conn = get_conn(); ...; conn.close()`
    as if it owned a private connection. Under the hood, get_conn() now hands
    out the same long-lived connection each time instead of opening a fresh
    TCP/TLS connection to Neon per command (which otherwise adds real latency
    to *every* interaction, including every keystroke of an /add wizard).
    This proxy is what makes that transparent to callers: close() is a
    deliberate no-op here — the shared connection stays open and gets handed
    back out on the next get_conn() call. Everything else passes straight
    through to the real connection."""

    def __init__(self, real_conn):
        object.__setattr__(self, "_real", real_conn)

    def close(self):
        pass  # intentional no-op — see class docstring

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_real"), name)


def get_conn():
    global _pg_conn
    # Reconnect lazily: on first use, after a clean close, or after Neon
    # (or the network) has dropped a connection out from under us — a fatal
    # error on a psycopg connection leaves it marked closed, so this check
    # catches that case on the next call rather than needing every one of
    # the ~25 call sites to handle a broken-connection retry individually.
    #
    # autocommit=True matters *specifically because* this connection is
    # shared and reused rather than closed after each use: without it, any
    # single failed statement (e.g. a UNIQUE-constraint violation, which
    # several call sites deliberately catch and turn into a friendly
    # message) leaves the connection's transaction "aborted" — and every
    # later query on that same connection then fails too, silently, until
    # the whole process restarts. With autocommit, each statement is its
    # own transaction, so a failure can't poison anything after it. Every
    # call site's conn.commit() becomes a harmless no-op under autocommit
    # (this is documented, safe psycopg behavior) — left in place rather
    # than stripped out, since it still documents intent at each call site.
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)
    return _SharedConnProxy(_pg_conn)


def init_db():
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            name TEXT NOT NULL,
            interval_seconds INTEGER NOT NULL,
            due_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            notified_at TEXT,
            task_type TEXT NOT NULL DEFAULT 'no_reminder',
            reminder_hour_utc INTEGER,
            reminder_minute_utc INTEGER,
            interval_unit TEXT,
            interval_amount INTEGER,
            lead_time_seconds INTEGER,
            nag_interval_seconds INTEGER,
            next_nag_at TEXT
        )
        """
    )
    # Case-insensitive per-chat uniqueness on name, since Postgres has no
    # direct equivalent of SQLite's "COLLATE NOCASE" column modifier.
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS tasks_chat_id_name_lower_idx "
        "ON tasks (chat_id, LOWER(name))"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id BIGINT PRIMARY KEY,
            utc_offset_minutes INTEGER NOT NULL,
            tz_name TEXT
        )
        """
    )
    # IANA zone name (e.g. 'Europe/Madrid'), when we know it. With it, local
    # times follow daylight saving; without it (a typed '+3'), the fixed
    # offset in utc_offset_minutes is used as-is.
    conn.execute("ALTER TABLE chat_settings ADD COLUMN IF NOT EXISTS tz_name TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS known_chats (
            chat_id BIGINT PRIMARY KEY,
            first_seen TEXT NOT NULL
        )
        """
    )
    # Migrations for databases created before these columns existed (e.g. an
    # existing deployment from before task types / notifications). Safe to
    # run every startup: each is a no-op once the column is present.
    # Existing tasks default to task_type='no_reminder', preserving their exact
    # current behavior.
    existing_cols = {
        row["column_name"]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'tasks'"
        ).fetchall()
    }
    migrations = {
        "notified_at": "ALTER TABLE tasks ADD COLUMN notified_at TEXT",
        "task_type": "ALTER TABLE tasks ADD COLUMN task_type TEXT NOT NULL DEFAULT 'no_reminder'",
        "reminder_hour_utc": "ALTER TABLE tasks ADD COLUMN reminder_hour_utc INTEGER",
        "reminder_minute_utc": "ALTER TABLE tasks ADD COLUMN reminder_minute_utc INTEGER",
        "interval_unit": "ALTER TABLE tasks ADD COLUMN interval_unit TEXT",
        "interval_amount": "ALTER TABLE tasks ADD COLUMN interval_amount INTEGER",
        "lead_time_seconds": "ALTER TABLE tasks ADD COLUMN lead_time_seconds INTEGER",
        "nag_interval_seconds": "ALTER TABLE tasks ADD COLUMN nag_interval_seconds INTEGER",
        "next_nag_at": "ALTER TABLE tasks ADD COLUMN next_nag_at TEXT",
        # Old-school tasks store their reminder as a LOCAL wall-clock time.
        # reminder_hour_utc / reminder_minute_utc are LEGACY: new rows leave
        # them NULL and only the one-time backfill in init_db() reads them
        # (from rows created before this upgrade). Once every deployment has
        # upgraded, they and that backfill can be dropped.
        "reminder_hour_local": "ALTER TABLE tasks ADD COLUMN reminder_hour_local INTEGER",
        "reminder_minute_local": "ALTER TABLE tasks ADD COLUMN reminder_minute_local INTEGER",
    }
    for col, ddl in migrations.items():
        if col not in existing_cols:
            conn.execute(ddl)
    # Data migration: the "no-reminder" task type used to be stored as
    # 'normal' internally (before that name was changed for clarity). Safe
    # to run every startup: a no-op once no rows use the old value.
    conn.execute("UPDATE tasks SET task_type = 'no_reminder' WHERE task_type = 'normal'")
    # Backfill: old-school tasks created before wall-clock storage only have
    # a UTC hour:minute. Convert it once, using the chat's saved offset
    # (that's what their local time was defined against). Idempotent: only
    # rows still missing a local time are touched.
    legacy = conn.execute(
        "SELECT t.id, t.reminder_hour_utc, t.reminder_minute_utc, "
        "COALESCE(cs.utc_offset_minutes, 0) AS off "
        "FROM tasks t LEFT JOIN chat_settings cs ON cs.chat_id = t.chat_id "
        "WHERE t.task_type = 'old_school' AND t.reminder_hour_local IS NULL "
        "AND t.reminder_hour_utc IS NOT NULL"
    ).fetchall()
    for row in legacy:
        lh, lm = utc_to_local_hm(row["reminder_hour_utc"], row["reminder_minute_utc"] or 0, row["off"])
        conn.execute(
            "UPDATE tasks SET reminder_hour_local = %s, reminder_minute_local = %s WHERE id = %s",
            (lh, lm, row["id"]),
        )
    if legacy:
        logger.info("Backfilled local reminder time for %d old-school task(s)", len(legacy))
    conn.commit()
    conn.close()


async def record_known_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registered as an early, unconditional handler (group=-1, see run())
    so every chat that has ever sent the bot anything gets recorded here —
    regardless of whether they ever created a task or set a timezone,
    which is otherwise the only way a chat_id shows up anywhere in the
    database. This is what lets /broadcast reach everyone who has used
    the bot, not just people who happened to add a task."""
    chat = update.effective_chat
    if chat is None:
        return
    conn = get_conn()
    conn.execute(
        "INSERT INTO known_chats (chat_id, first_seen) VALUES (%s, %s) "
        "ON CONFLICT (chat_id) DO NOTHING",
        (chat.id, to_iso(now_utc())),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Telegram legacy-Markdown safety. Task names (and place/timezone names from
# the geocoder) are user-controlled text interpolated into Markdown messages;
# a lone _ * ` or [ in them makes Telegram reject the whole message. Worse,
# for a proactive notification that means it never sends and retries forever.
# Legacy Markdown can't escape characters *inside* an entity, so bold()
# closes the bold run, emits the escaped character, and reopens it.
# ---------------------------------------------------------------------------

_MD_SPECIAL = "_*`["


def esc(text) -> str:
    """Escape for use OUTSIDE any *bold* / _italic_ entity."""
    return "".join("\\" + c if c in _MD_SPECIAL else c for c in str(text))


def bold(text) -> str:
    """Render text as bold, safely, even if it contains Markdown characters."""
    out, buf = [], []

    def flush():
        if buf:
            chunk = "".join(buf)
            # A whitespace-only bold entity is rejected by Telegram.
            out.append(f"*{chunk}*" if chunk.strip() else chunk)
            buf.clear()

    for c in str(text):
        if c in _MD_SPECIAL:
            flush()
            out.append("\\" + c)
        else:
            buf.append(c)
    flush()
    return "".join(out)


# ---------------------------------------------------------------------------
# Interval parsing / formatting  (no timezones needed — everything is
# stored in UTC internally and only ever shown to the user as a duration)
# ---------------------------------------------------------------------------

INTERVAL_RE = re.compile(
    r"(?:(?P<weeks>\d+)\s*w)?"
    r"(?:(?P<days>\d+)\s*d)?"
    r"(?:(?P<hours>\d+)\s*h)?"
    r"(?:(?P<minutes>\d+)\s*m)?",
    re.IGNORECASE,
)


def parse_interval(text: str):
    """Parse things like '3h', '2d', '1d12h', '90m' into seconds."""
    text = text.strip().lower().replace(" ", "")
    if not text:
        return None
    match = INTERVAL_RE.fullmatch(text)
    if not match or not any(match.groupdict().values()):
        return None
    parts = {k: int(v) for k, v in match.groupdict().items() if v}
    delta = timedelta(
        weeks=parts.get("weeks", 0),
        days=parts.get("days", 0),
        hours=parts.get("hours", 0),
        minutes=parts.get("minutes", 0),
    )
    seconds = int(delta.total_seconds())
    return seconds if seconds > 0 else None


def format_duration(seconds) -> str:
    seconds = max(0, int(seconds))
    weeks, seconds = divmod(seconds, 7 * 86400)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, _ = divmod(seconds, 60)
    parts = []
    if weeks:
        parts.append(f"{weeks}w")
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes or not parts:
        parts.append(f"{minutes}m")
    return " ".join(parts)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


# ---------------------------------------------------------------------------
# Old-school interval parsing (day/week/month only — a fixed-hour reminder
# doesn't make sense at sub-day granularity) and calendar-aware date math.
# ---------------------------------------------------------------------------

OLD_SCHOOL_INTERVAL_RE = re.compile(r"(\d+)\s*(d|w|mo)", re.IGNORECASE)


def parse_old_school_interval(text: str):
    """'10d', '2w', '3mo' -> ('d', 10) / ('w', 2) / ('mo', 3). None if invalid."""
    text = text.strip().lower().replace(" ", "")
    match = OLD_SCHOOL_INTERVAL_RE.fullmatch(text)
    if not match:
        return None
    amount = int(match.group(1))
    if amount <= 0:
        return None
    return match.group(2), amount


def format_old_school_interval(unit: str, amount: int) -> str:
    names = {"d": "day", "w": "week", "mo": "month"}
    name = names.get(unit, unit)
    return f"{amount} {name}{'s' if amount != 1 else ''}"  # md-safe: a unit word, not user text


def add_calendar_interval(dt: datetime, unit: str, amount: int) -> datetime:
    """Advances dt by a day/week/month amount. Month arithmetic clamps the
    day to the target month's actual length (e.g. Jan 31 + 1 month -> Feb 28
    or 29, not an invalid Feb 31)."""
    if unit == "d":
        return dt + timedelta(days=amount)
    if unit == "w":
        return dt + timedelta(weeks=amount)
    if unit == "mo":
        month_index = dt.month - 1 + amount  # zero-based for easy modulo math
        year = dt.year + month_index // 12
        month = month_index % 12 + 1
        last_day_of_month = calendar.monthrange(year, month)[1]
        day = min(dt.day, last_day_of_month)
        return dt.replace(year=year, month=month, day=day)
    raise ValueError(f"unknown old-school interval unit: {unit!r}")


def next_old_school_due(from_dt: datetime, local_hour: int, local_minute: int,
                        unit: str, amount: int, tz=timezone.utc) -> datetime:
    """One full interval after from_dt, at the same LOCAL wall-clock time.
    The calendar math happens on the local date, so a 9:00 reminder stays at
    9:00 on the clock across daylight-saving changes (its UTC time shifts)."""
    local = from_dt.astimezone(tz).replace(tzinfo=None)
    advanced = add_calendar_interval(local, unit, amount)
    return local_to_utc_datetime(advanced.date(), local_hour, local_minute, tz)


def old_school_local_time(t, tz):
    """(hour, minute) wall-clock time of an old-school task row."""
    if t["reminder_hour_local"] is not None:
        return t["reminder_hour_local"], t["reminder_minute_local"] or 0
    local_due = from_iso(t["due_at"]).astimezone(tz)  # defensive: un-backfilled row
    return local_due.hour, local_due.minute


# ---------------------------------------------------------------------------
# Per-chat UTC offset (used only for old-school tasks, so hours can be
# asked/shown in the user's local time instead of raw UTC) and date parsing.
# ---------------------------------------------------------------------------

UTC_OFFSET_RE = re.compile(r"(?:UTC)?([+-])(\d{1,2})(?::?(\d{2}))?", re.IGNORECASE)


def parse_utc_offset(text: str):
    """'+3', '-5', '+5:30', '0', 'UTC' -> signed minutes from UTC, or None."""
    text = text.strip().upper().replace(" ", "")
    if text in ("0", "UTC", "UTC+0", "UTC-0", "GMT", "+0", "-0"):
        return 0
    match = UTC_OFFSET_RE.fullmatch(text)
    if not match:
        return None
    sign, hours, minutes = match.group(1), int(match.group(2)), int(match.group(3) or 0)
    if hours > 14 or minutes >= 60:
        return None
    total = hours * 60 + minutes
    return total if sign == "+" else -total


def format_utc_offset(offset_minutes: int) -> str:
    sign = "+" if offset_minutes >= 0 else "-"
    h, m = divmod(abs(offset_minutes), 60)
    return f"UTC{sign}{h}" + (f":{m:02d}" if m else "")


class GeocodingUnavailable(Exception):
    """Raised when the city-lookup service itself couldn't be reached or
    didn't return anything usable (network error, timeout, bad response,
    or a timezone name it returned that couldn't be resolved). Distinct
    from a clean "no matches" response, which just means the place name
    wasn't found — callers should tell the person to enter a UTC offset
    manually instead of implying they mistyped a place name."""


def lookup_city_utc_offset(query: str):
    """Resolves a city/capital name (e.g. "Madrid") to a UTC offset via
    Open-Meteo's free geocoding API (no key required) — it returns an IANA
    timezone name directly, which stdlib zoneinfo then converts to a plain
    offset in minutes, correctly accounting for whatever DST is in effect
    right now. That single fixed number is what actually gets stored (see
    set_chat_timezone). The zone NAME is saved too, so later local-time
    math follows daylight saving instead of freezing today's offset. Returns
    (offset_minutes, display_name, tz_name) for the best match, or None if
    the place name genuinely wasn't found (the service responded fine, it
    just had no match). Raises GeocodingUnavailable if the service itself
    couldn't be reached or reached but returned something unusable.
    """
    try:
        url = "https://geocoding-api.open-meteo.com/v1/search?" + urllib.parse.urlencode(
            {"name": query, "count": 1, "language": "en", "format": "json"}
        )
        with urllib.request.urlopen(url, timeout=6) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.warning("City timezone lookup failed for %r", query, exc_info=True)
        raise GeocodingUnavailable(str(exc)) from exc

    results = data.get("results") or []
    if not results:
        return None
    place = results[0]
    tz_name = place.get("timezone")
    if not tz_name:
        return None

    try:
        local_now = datetime.now(ZoneInfo(tz_name))
        offset_minutes = int(local_now.utcoffset().total_seconds() // 60)
    except Exception as exc:
        logger.warning("Could not resolve timezone %r to an offset", tz_name, exc_info=True)
        raise GeocodingUnavailable(str(exc)) from exc

    name_bits = [place.get("name")]
    admin1 = place.get("admin1")
    if admin1 and admin1 != place.get("name"):
        name_bits.append(admin1)
    country = place.get("country")
    if country:
        name_bits.append(country)
    display_name = ", ".join(bit for bit in name_bits if bit)
    return offset_minutes, display_name, tz_name


def lookup_coords_utc_offset(lat: float, lon: float):
    """Resolves GPS coordinates — from Telegram's native "Share location"
    button — to a UTC offset via the same free Open-Meteo service used
    for city-name lookups (lookup_city_utc_offset), just its regular
    weather-forecast endpoint instead of the geocoding one: passing
    timezone=auto returns the IANA timezone name for any coordinate on
    Earth. Returns (offset_minutes, tz_name) on success. Raises
    GeocodingUnavailable if the service itself couldn't be reached or
    returned something unusable — every coordinate on Earth has *some*
    timezone, so unlike a city name there's no legitimate "not found"
    case here, only reachability failures."""
    try:
        url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(
            {"latitude": lat, "longitude": lon, "timezone": "auto", "current": "temperature_2m"}
        )
        with urllib.request.urlopen(url, timeout=6) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.warning("Coordinate timezone lookup failed for (%s, %s)", lat, lon, exc_info=True)
        raise GeocodingUnavailable(str(exc)) from exc

    tz_name = data.get("timezone")
    if not tz_name:
        raise GeocodingUnavailable("no timezone field in response")

    try:
        local_now = datetime.now(ZoneInfo(tz_name))
        offset_minutes = int(local_now.utcoffset().total_seconds() // 60)
    except Exception as exc:
        logger.warning("Could not resolve timezone %r to an offset", tz_name, exc_info=True)
        raise GeocodingUnavailable(str(exc)) from exc

    return offset_minutes, tz_name


def parse_local_hour_minute(text: str):
    """'14', '14:30', '9:05' -> (hour, minute), or None if invalid. Minute
    defaults to 0 when only an hour is given, so plain-hour answers people
    are already used to (e.g. "9") keep working exactly as before."""
    text = text.strip()
    if ":" in text:
        hour_part, _, minute_part = text.partition(":")
        if not (hour_part.isdigit() and minute_part.isdigit()):
            return None
        hour, minute = int(hour_part), int(minute_part)
    elif text.isdigit():
        hour, minute = int(text), 0
    else:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return hour, minute


def resolve_timezone_input(text: str):
    """Shared by the /add flow's timezone question and the standalone
    /timezone command: tries a raw UTC offset first (e.g. "+3"), then falls
    back to resolving a city/capital name (e.g. "Madrid") via
    lookup_city_utc_offset. Returns (offset_minutes, display_name_or_None,
    tz_name_or_None) on success — display_name/tz_name are only set for a
    city-name match, so callers can tell which path was used — or None if
    neither interpretation worked. Can raise GeocodingUnavailable (left to
    propagate) if a city name was given but the lookup service itself is
    unreachable — callers should catch that separately from a plain None,
    since it calls for a different message (use a manual offset for now,
    not "check your spelling")."""
    offset = parse_utc_offset(text)
    if offset is not None:
        return offset, None, None
    return lookup_city_utc_offset(text)


def _fixed_tz(offset_minutes: int):
    return timezone(timedelta(minutes=offset_minutes))


def _get_chat_tz_row(chat_id: int):
    conn = get_conn()
    row = conn.execute(
        "SELECT utc_offset_minutes, tz_name FROM chat_settings WHERE chat_id = %s", (chat_id,)
    ).fetchone()
    conn.close()
    return row


def _tz_from_settings(offset_minutes: int, tz_name):
    """tzinfo for saved settings: the named IANA zone if we have a usable
    one (follows daylight saving), otherwise the fixed offset."""
    if tz_name:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            logger.warning("Unknown saved timezone %r — using its fixed offset", tz_name)
    return _fixed_tz(offset_minutes)


def get_chat_timezone(chat_id: int):
    """The chat's saved timezone as a tzinfo, or None if it never set one —
    callers use None to mean 'ask first'. Use get_chat_tzinfo when a
    default is fine."""
    row = _get_chat_tz_row(chat_id)
    if row is None:
        return None
    return _tz_from_settings(row["utc_offset_minutes"], row["tz_name"])


def get_chat_tzinfo(chat_id: int):
    """The chat's timezone as a tzinfo, defaulting to UTC when unset."""
    return get_chat_timezone(chat_id) or timezone.utc


def tz_label(tz, at=None) -> str:
    """Human label, e.g. 'Europe/Madrid, UTC+2', 'UTC+3', or 'UTC'. The
    offset shown is the one in effect at `at` (default: now)."""
    at = at or now_utc()
    off = int(at.astimezone(tz).utcoffset().total_seconds() // 60)
    key = getattr(tz, "key", None)
    if key and key != "UTC":
        return f"{key}, {format_utc_offset(off)}"
    return format_utc_offset(off) if off else "UTC"


def set_chat_timezone(chat_id: int, offset_minutes: int, tz_name=None):
    """Saves the chat's timezone, then re-anchors its old-school tasks so
    they keep the same LOCAL reminder time in the new zone. Returns
    (number_of_old_school_tasks_re_anchored, new_tzinfo) — callers that
    need the new zone right away use it instead of querying again. Note the
    side effect on tasks: this is not just a settings write."""
    old_tz = get_chat_tzinfo(chat_id)
    new_tz = _tz_from_settings(offset_minutes, tz_name)
    conn = get_conn()
    conn.execute(
        "INSERT INTO chat_settings (chat_id, utc_offset_minutes, tz_name) VALUES (%s, %s, %s) "
        "ON CONFLICT(chat_id) DO UPDATE SET utc_offset_minutes = excluded.utc_offset_minutes, "
        "tz_name = excluded.tz_name",
        (chat_id, offset_minutes, tz_name),
    )
    conn.commit()
    conn.close()
    return reanchor_old_school_tasks(chat_id, old_tz, new_tz), new_tz


def reanchor_old_school_tasks(chat_id: int, old_tz, new_tz) -> int:
    """After a timezone change, keep each old-school task's local wall-clock
    time (09:00 stays 09:00) by recomputing its next due moment in the new
    zone, on the same local calendar date it was scheduled for before."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, due_at, reminder_hour_local, reminder_minute_local FROM tasks "
        "WHERE chat_id = %s AND task_type = 'old_school' AND reminder_hour_local IS NOT NULL",
        (chat_id,),
    ).fetchall()
    for r in rows:
        old_local_date = from_iso(r["due_at"]).astimezone(old_tz).date()
        new_due = local_to_utc_datetime(
            old_local_date, r["reminder_hour_local"], r["reminder_minute_local"] or 0, new_tz
        )
        conn.execute("UPDATE tasks SET due_at = %s WHERE id = %s", (to_iso(new_due), r["id"]))
    conn.commit()
    conn.close()
    return len(rows)


def tz_saved_notes(n_old_school: int, tz_name, offset_minutes: int) -> str:
    """Extra lines for a timezone-saved confirmation (Markdown)."""
    notes = ""
    if n_old_school:
        notes += (
            f"\n\n🔁 Your {n_old_school} old-school task{'s' if n_old_school != 1 else ''} "
            f"keep their local reminder time{'s' if n_old_school != 1 else ''} in the new zone."
        )
    if tz_name is None and offset_minutes != 0:
        notes += (
            "\n\nTip: a fixed offset doesn't follow daylight saving. Send a city "
            "name instead (like Madrid) and I'll adjust automatically."
        )
    return notes


def utc_to_local_hm(utc_hour: int, utc_minute: int, offset_minutes: int):
    total = (utc_hour * 60 + utc_minute + offset_minutes) % (24 * 60)
    return total // 60, total % 60


def local_today(tz):
    """Today's calendar date in the person's own time zone (not UTC's)."""
    return now_utc().astimezone(tz).date()


def local_to_utc_datetime(local_date, local_hour: int, local_minute: int, tz) -> datetime:
    """A local calendar date + wall-clock time -> the true UTC instant, using
    whatever UTC offset `tz` has ON THAT DATE (so daylight saving is right).
    A wall time that doesn't exist (clocks springing forward) lands an hour
    later; an ambiguous one (clocks falling back) takes the first occurrence."""
    return datetime(
        local_date.year, local_date.month, local_date.day, local_hour, local_minute, tzinfo=tz
    ).astimezone(timezone.utc)


def parse_target_date(text: str, tz=timezone.utc):
    """'today' / 'tomorrow' / 'YYYY-MM-DD' -> a LOCAL date, or None if invalid."""
    text = text.strip().lower()
    if text == "today":
        return local_today(tz)
    if text == "tomorrow":
        return local_today(tz) + timedelta(days=1)
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def date_and_time_keyboard(prefix: str):
    """Shared 'Today / Tomorrow / Skip' quick-pick row used by the old-school
    date question and (with different button labels below) the timezone and
    hour questions."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 Today", callback_data=f"{prefix}:today"),
                InlineKeyboardButton("📅 Tomorrow", callback_data=f"{prefix}:tomorrow"),
            ]
        ]
    )


CAL_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
CAL_WEEKDAYS = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
CAL_MAX_MONTHS_AHEAD = 24  # further dates can still be typed


def clamp_calendar_month(today, year: int, month: int):
    """Keeps a requested month inside the range the calendar offers: from the
    current month up to CAL_MAX_MONTHS_AHEAD months ahead (stale or tampered
    buttons can ask for anything)."""
    offset = (year - today.year) * 12 + (month - today.month)
    offset = max(0, min(CAL_MAX_MONTHS_AHEAD, offset))
    index = today.year * 12 + (today.month - 1) + offset
    return index // 12, index % 12 + 1


def calendar_keyboard(today, year=None, month=None, prefix: str = "osdate"):
    """Inline month calendar (Monday-first). Days before `today` (the person's
    LOCAL today) are shown as · and don't respond; today is [bracketed].
    Callbacks: {prefix}:cal:YYYY-MM to change month, {prefix}:pick:YYYY-MM-DD
    to choose a day, {prefix}:noop for everything that's only decoration."""
    year, month = clamp_calendar_month(today, year or today.year, month or today.month)
    noop = f"{prefix}:noop"

    prev_year, prev_month = (year, month - 1) if month > 1 else (year - 1, 12)
    next_year, next_month = (year, month + 1) if month < 12 else (year + 1, 1)
    can_go_back = clamp_calendar_month(today, prev_year, prev_month) == (prev_year, prev_month) \
        and (year, month) != (today.year, today.month)
    can_go_forward = clamp_calendar_month(today, next_year, next_month) == (next_year, next_month) \
        and (year * 12 + month) - (today.year * 12 + today.month) < CAL_MAX_MONTHS_AHEAD

    rows = [[
        InlineKeyboardButton(
            "◀" if can_go_back else "·",
            callback_data=f"{prefix}:cal:{prev_year}-{prev_month:02d}" if can_go_back else noop,
        ),
        InlineKeyboardButton(f"{CAL_MONTH_NAMES[month - 1]} {year}", callback_data=noop),
        InlineKeyboardButton(
            "▶" if can_go_forward else "·",
            callback_data=f"{prefix}:cal:{next_year}-{next_month:02d}" if can_go_forward else noop,
        ),
    ]]
    rows.append([InlineKeyboardButton(day, callback_data=noop) for day in CAL_WEEKDAYS])
    for week in calendar.Calendar(firstweekday=0).monthdayscalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data=noop))
                continue
            this_day = datetime(year, month, day).date()
            if this_day < today:
                row.append(InlineKeyboardButton("·", callback_data=noop))
            else:
                label = f"[{day}]" if this_day == today else str(day)
                row.append(InlineKeyboardButton(label, callback_data=f"{prefix}:pick:{this_day.isoformat()}"))
        rows.append(row)
    rows.append([
        InlineKeyboardButton("📅 Today", callback_data=f"{prefix}:today"),
        InlineKeyboardButton("📅 Tomorrow", callback_data=f"{prefix}:tomorrow"),
    ])
    return InlineKeyboardMarkup(rows)


def skip_keyboard(prefix: str, label: str = "⏭️ Skip"):
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=f"skip:{prefix}")]])


HELP_TEXT = (
    "I track recurring tasks and tell you when they're due again.\n\n"
    "Tip: use the ✍️ Add / 📊 Status / ✅ Done buttons below the text box "
    "for one-tap access to the basics.\n\n"
    "*Task types* (chosen when you /add)\n"
    "🔔 *Simple reminder* — single push when due, no follow-up\n"
    "🔕 *No reminder* — shown in /status only, never pushes a notification\n"
    "📅 *Old-school* — fires at a fixed local time and date on a "
    "day/week/month cadence, regardless of /done\n"
    "📣 *Advanced - Nagging* — one early reminder a set lead time before "
    "due; then, once it's actually due, keeps nagging on its own cadence "
    "until you respond\n"
    "⏰ *One-time reminder* — fires once, then removes itself\n\n"
    "*Commands*\n"
    "✍️ /add — add a task. Asks what type, then the name, then whatever "
    "that type needs — including an optional first-due date if it's "
    "different from the regular interval (e.g. due in 20h, then every 2d "
    "after that). The one-line shortcut `/add Task A 3d` only covers "
    "Simple reminder tasks — see Interval format section below for more "
    "info\n"
    "✅ /done — mark a task done. Send it alone for a tappable list of "
    "tasks, or directly: `/done Task A`\n"
    "📊 /status — show all tasks (with a type icon) and time remaining\n"
    "📋 /details — show the full details of every task: exact due "
    "date/time, old-school's local fire time and repeat cadence, "
    "nagging's lead time and current nag status, and more\n"
    "✏️ /edit — change a task without deleting and recreating it. Send it "
    "alone for a tappable list, or directly: `/edit Task A`. Asks which "
    "field to change (name, interval, and whatever else applies to that "
    "task's type), then loops back so you can change more than one\n"
    "🗑️ /delete — remove a task (asks you to confirm first). Send it "
    "alone for a tappable list, or directly: `/delete Task A`\n"
    "🔄 /reset — mark ALL tasks done right now, after one quick "
    "confirmation (one-time reminders get removed, old-school tasks are "
    "untouched)\n"
    "🧹 /wipe — delete ALL tasks (asks you to confirm twice)\n"
    "🌍 /timezone — view or change the time zone used for old-school "
    "tasks' local time, at any point (not just the first time you add "
    "one). Send a city (`/timezone Madrid`) to follow daylight saving "
    "automatically, or a fixed offset (`/timezone +3`) that never changes. "
    "Send it alone to be asked\n"
    "❌ /cancel — stop a step-by-step /add in progress\n"
    "❓ /help — show this message\n\n"
    "*Interval format*: combine w(eeks) d(ays) h(ours) m(inutes) — "
    "e.g. `3h`, `2d`, `1d12h`, `3w2d1h`. Old-school intervals are "
    "days/weeks/months only, e.g. `10d`, `2w`, `3mo`.\n"
    "⚠️ *No spaces inside it* — write `3d4h`, not `3d 4h`. In the one-line "
    "`/add Task A 3d`, the interval must be the *last* word; a space in the "
    "middle of it splits it in two, and the first half gets read as part of "
    "the task name instead. If in doubt, just send `/add` alone and I'll "
    "ask for the interval separately."
)

# Persistent on-screen menu (shown below the text box, distinct from the
# native "/" command menu). Tapping these sends their label as plain text,
# which the handlers below match on.
MENU_ADD = "✍️ Add"
MENU_STATUS = "📊 Status"
MENU_DONE = "✅ Done"

MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    [[MENU_ADD, MENU_STATUS, MENU_DONE]],
    resize_keyboard=True,
)

# Only ever shown temporarily, in place of MAIN_MENU_KEYBOARD, for the
# timezone question — a location-share button can only be a reply-keyboard
# button (Telegram has no inline-keyboard equivalent), so Skip lives here
# too, as a second button on the same keyboard, rather than as the usual
# inline button used everywhere else in the bot. Every path that resolves
# this question (typed offset/city, tapping Skip, or an actual location
# share) must explicitly restore MAIN_MENU_KEYBOARD afterward, or this
# would linger near the text box indefinitely.
TIMEZONE_SKIP_TEXT = "⏭️ Skip (use UTC)"
TIMEZONE_QUESTION_KEYBOARD = ReplyKeyboardMarkup(
    [[KeyboardButton("📍 Share location", request_location=True), TIMEZONE_SKIP_TEXT]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

MENU_ADD_PATTERN = f"^{re.escape(MENU_ADD)}$"
MENU_STATUS_PATTERN = f"^{re.escape(MENU_STATUS)}$"
MENU_DONE_PATTERN = f"^{re.escape(MENU_DONE)}$"
MENU_STATUS_OR_DONE_PATTERN = f"^({re.escape(MENU_STATUS)}|{re.escape(MENU_DONE)})$"

# Shown after actions complete (or get cancelled) to point the user to what's next.
TASK_TIP = (
    "_Send ✍️ /add to add another task,\n"
    "📊 /status to see all your tasks,\n"
    "🗑️ /delete to remove a task,\n"
    "or ❓ /help for more instructions._"
)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hey! I'll track your recurring tasks.\n\n" + HELP_TEXT,
        parse_mode="Markdown",
        reply_markup=MAIN_MENU_KEYBOARD,
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


(
    ADD_TYPE,
    ADD_NAME,
    ADD_INTERVAL,
    ADD_FIRST_DUE,
    ADD_ONE_TIME_DELAY,
    ADD_TIMEZONE,
    ADD_OLD_SCHOOL_HOUR,
    ADD_OLD_SCHOOL_DATE,
    ADD_OLD_SCHOOL_INTERVAL,
    ADD_NAGGING_INTERVAL,
    ADD_NAGGING_LEAD,
    ADD_NAGGING_CADENCE,
) = range(12)

(
    EDIT_PICK_TASK,
    EDIT_PICK_FIELD,
    EDIT_NAME,
    EDIT_INTERVAL,
    EDIT_LEAD_TIME,
    EDIT_NAG_CADENCE,
    EDIT_OS_TIME,
    EDIT_OS_CADENCE,
    EDIT_ONE_TIME_DELAY,
) = range(12, 21)

TASK_TYPE_LABELS = {
    "simple": "🔔 Simple reminder",
    "no_reminder": "🔕 No reminder",
    "old_school": "📅 Old-school",
    "nagging": "📣 Advanced - Nagging",
    "one_time": "⏰ One-time reminder",
}
TASK_TYPE_EMOJI = {k: v.split()[0] for k, v in TASK_TYPE_LABELS.items()}


def type_choice_keyboard():
    buttons = [
        [InlineKeyboardButton(label, callback_data=f"addtype:{key}")]
        for key, label in TASK_TYPE_LABELS.items()
    ]
    return InlineKeyboardMarkup(buttons)


def _clear_pending_add_state(context: ContextTypes.DEFAULT_TYPE):
    for key in (
        "pending_name",
        "pending_type",
        "pending_interval_seconds",
        "pending_lead_time_seconds",
        "pending_local_hour",
        "pending_local_minute",
        "pending_date",
        "pending_first_due_seconds",
    ):
        context.user_data.pop(key, None)


# Every one of these flags means "the next plain-text message this chat
# sends should be treated as the answer to a specific pending question,"
# for a lightweight flow that doesn't need a full ConversationHandler (see
# maybe_handle_nag_snooze_reply, which is where they're all consumed). At
# most one of them should ever be genuinely active at a time — call
# _clear_lightweight_reply_flags() before setting any of them, or a stale,
# never-answered one (most commonly an abandoned /timezone question,
# which deliberately keeps waiting on invalid input) can silently swallow
# a later, unrelated reply meant for a completely different flow, like a
# nag's snooze duration — since maybe_handle_nag_snooze_reply checks them
# in a fixed order and always favors whichever was set first, not most
# recently.
_LIGHTWEIGHT_REPLY_FLAGS = (
    "add_session_timed_out",
    "awaiting_timezone_reply",
    "awaiting_nag_snooze_for",
)


def _clear_lightweight_reply_flags(context: ContextTypes.DEFAULT_TYPE):
    for key in _LIGHTWEIGHT_REPLY_FLAGS:
        context.user_data.pop(key, None)


async def _reply_or_edit(update_or_query, text, markdown=False):
    """Edits the originating message if this is a CallbackQuery (button
    tap), or sends a fresh reply if this is a genuine Update (typed
    command). CallbackQuery and Update both have a `.message` attribute, so
    that alone can't tell them apart — `edit_message_text` only exists on
    CallbackQuery, which makes it the reliable check."""
    kwargs = {"parse_mode": "Markdown"} if markdown else {}
    if hasattr(update_or_query, "edit_message_text"):
        await update_or_query.edit_message_text(text, **kwargs)
    else:
        await update_or_query.message.reply_text(text, **kwargs)


def build_task_added_message(
    name,
    task_type,
    interval_seconds,
    due,
    reminder_hour_local=None,
    interval_unit=None,
    interval_amount=None,
    lead_time_seconds=None,
    nag_interval_seconds=None,
    reminder_minute_local=0,
    tz=None,
    due_in_past=False,
):
    lines = [
        "✅ Task added",
        "",
        f"*Task:* {esc(name)}",
        f"*Type:* {TASK_TYPE_LABELS.get(task_type, task_type)}",
    ]
    if task_type == "old_school":
        lines.append(f"*Repeats:* every {format_old_school_interval(interval_unit, interval_amount)}")
        tz = tz or timezone.utc
        lines.append(
            f"*Reminder time:* {reminder_hour_local:02d}:{reminder_minute_local:02d} "
            f"({esc(tz_label(tz, due))})"
        )
        lines.append(f"*First reminder date:* {due.astimezone(tz).date().isoformat()}")
    elif task_type == "nagging":
        lines.append(f"*Interval:* {format_duration(interval_seconds)}")
        lines.append(f"*Early reminder:* once, {format_duration(lead_time_seconds)} before due")
        lines.append(
            f"*Nagging:* starts when it's due, then every {format_duration(nag_interval_seconds)} until done"
        )
    elif task_type in ("no_reminder", "simple"):
        lines.append(f"*Interval:* {format_duration(interval_seconds)}")
    # one_time: interval isn't meaningful, skip it

    lines.append("")
    if due_in_past:
        lines.append(
            "⚠️ That time has already passed for today, so this will remind "
            "you almost immediately, then follow the repeat schedule from there."
        )
    else:
        remaining = int((due - now_utc()).total_seconds())
        lines.append(f"First due in {format_duration(remaining)}.")
    lines.append("")
    lines.append(TASK_TIP)
    return "\n".join(lines)


async def create_task_and_reply(
    update: Update,
    chat_id: int,
    name: str,
    interval_seconds: int,
    task_type: str = "no_reminder",
    reminder_hour_local=None,
    reminder_minute_local=None,
    interval_unit=None,
    interval_amount=None,
    lead_time_seconds=None,
    nag_interval_seconds=None,
    first_due_seconds=None,
    first_due_dt=None,
):
    """Shared insert logic for every /add path and every task type.

    first_due_seconds, if given, is used only to compute the *first* due
    date — e.g. the recurring interval is every 2 days but the very next
    occurrence is in 20 hours. interval_seconds (stored separately) is what
    every future reschedule uses once the first cycle completes via
    apply_done_to_task, so the custom first delay naturally applies once.

    first_due_dt (old-school only) is the exact UTC moment of the very first
    occurrence, already built from the chosen LOCAL date + wall-clock time
    (see add_old_school_interval_received) — rather than adding a full
    interval to "now", which would skip a later-today time to
    interval-away. reminder_hour_local / reminder_minute_local are the
    wall-clock time every later occurrence is pinned to."""
    now = now_utc()
    reminder_minute_local = reminder_minute_local or 0
    tz = None

    if task_type == "old_school":
        tz = get_chat_tzinfo(chat_id)
        due = first_due_dt
    else:
        first_delay = first_due_seconds if first_due_seconds is not None else interval_seconds
        due = now + timedelta(seconds=first_delay)

    next_nag_at = None
    if task_type == "nagging":
        next_nag_at = first_nag_moment(due, lead_time_seconds, now)

    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO tasks (chat_id, name, interval_seconds, due_at, created_at, "
            "task_type, interval_unit, interval_amount, "
            "lead_time_seconds, nag_interval_seconds, next_nag_at, "
            "reminder_hour_local, reminder_minute_local) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                chat_id,
                name,
                interval_seconds,
                to_iso(due),
                to_iso(now),
                task_type,
                interval_unit,
                interval_amount,
                lead_time_seconds,
                nag_interval_seconds,
                to_iso(next_nag_at) if next_nag_at else None,
                reminder_hour_local,
                reminder_minute_local if task_type == "old_school" else None,
            ),
        )
        conn.commit()
        await update.message.reply_text(
            build_task_added_message(
                name,
                task_type,
                interval_seconds,
                due,
                reminder_hour_local,
                interval_unit,
                interval_amount,
                lead_time_seconds,
                nag_interval_seconds,
                reminder_minute_local,
                tz,
                due_in_past=(task_type == "old_school" and due < now),
            ),
            parse_mode="Markdown",
        )
    except psycopg.errors.UniqueViolation:
        await update.message.reply_text(
            f"You already have a task called {bold(name)}. "
            f"Delete it first with /delete, or pick another name.",
            parse_mode="Markdown",
        )
    finally:
        conn.close()


async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _clear_all_conversation_states(context)
    args = context.args

    if not args:
        await update.message.reply_text(
            "*Type?*\n\n"
            "What kind of task is this?\n"
            "Send ❓ /help to see details about each type.",
            parse_mode="Markdown",
            reply_markup=type_choice_keyboard(),
        )
        return ADD_TYPE

    # One-liner shortcut only supports Simple reminder tasks: "/add Task A 3d".
    # The other types need more info than fits on one line.
    interval_text = args[-1]
    seconds = parse_interval(interval_text)

    if seconds is not None and len(args) >= 2:
        name = " ".join(args[:-1]).strip()
        chat_id = update.effective_chat.id
        await create_task_and_reply(update, chat_id, name, seconds, task_type="simple")
        return ConversationHandler.END

    # Otherwise treat everything typed so far as the name, default to
    # Simple reminder, and ask for the interval (preserves the old partial-liner flow).
    name = " ".join(args).strip()
    context.user_data["pending_name"] = name
    context.user_data["pending_type"] = "simple"
    await update.message.reply_text(
        f"Got it — {bold(name)}. How often does it reset? (e.g. 3h, 2d, 1d12h)\n\n"
        f"_Or /cancel to stop._",
        parse_mode="Markdown",
    )
    return ADD_INTERVAL


async def add_type_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    task_type = query.data.split(":", 1)[1]
    context.user_data["pending_type"] = task_type
    await query.edit_message_text(
        "*Name?*\n\nWhat do you want to call your task?\n\n_Or /cancel to stop._",
        parse_mode="Markdown",
    )
    return ADD_NAME


async def add_flow_stale_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Catches a tap on any /add-flow button whose conversation has since
    ended — timed out, cancelled, or superseded by a newer /add or another
    command — registered outside the ConversationHandler, for every
    button pattern used anywhere in the flow, so it only ever fires when
    the in-state handler above didn't claim the tap first. This is the
    only place a stale/expired session is ever mentioned reactively —
    there's no proactive notice when the 5-minute timeout itself elapses,
    since that would be a strange, unprompted ping for something this
    mundane. See add_conversation_timeout and maybe_handle_nag_snooze_reply
    for the equivalent reactive handling of a stale typed reply."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("This menu has expired — try /add again.")


async def add_name_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text(
            "That can't be empty — what should I call the task?\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return ADD_NAME

    chat_id = update.effective_chat.id
    conn = get_conn()
    existing = find_task(conn, chat_id, name)
    conn.close()
    if existing:
        await update.message.reply_text(
            f"*Name taken*\n\n"
            f"You already have a task called {bold(existing['name'])}. Send a "
            f"different name, or /delete that one first.",
            parse_mode="Markdown",
        )
        return ADD_NAME

    context.user_data["pending_name"] = name
    task_type = context.user_data.get("pending_type", "no_reminder")

    if task_type == "one_time":
        await update.message.reply_text(
            f"How long from now should I remind you about {bold(name)}? "
            f"(e.g. 3h, 2d, 1d12h)\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return ADD_ONE_TIME_DELAY

    if task_type == "old_school":
        chat_id = update.effective_chat.id
        saved_tz = get_chat_timezone(chat_id)
        if saved_tz is None:
            await update.message.reply_text(
                "*Your time zone?*\n\n"
                "This is your first time adding an Old-school task.\n"
                "First, what's your UTC offset (time zone)? So old-school reminders can use "
                "your local time. e.g. `+3` for Istanbul, `-5` for US "
                "Eastern, `+5:30` for India — or just send a city/capital name "
                "like `Madrid` (a city also follows daylight saving automatically). "
                "You can change this anytime with /timezone.\n\n"
                "_Or /cancel to stop._",
                parse_mode="Markdown",
                reply_markup=TIMEZONE_QUESTION_KEYBOARD,
            )
            return ADD_TIMEZONE
        await ask_old_school_hour(update, name, saved_tz)
        return ADD_OLD_SCHOOL_HOUR

    if task_type == "nagging":
        await update.message.reply_text(
            f"How often should {bold(name)} reset once it's done? "
            f"(e.g. 3h, 2d, 1d12h)\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return ADD_NAGGING_INTERVAL

    # no_reminder
    await update.message.reply_text(
        f"*Interval?*\n\nHow often does {bold(name)} repeat itself? (e.g. 3h, 2d, 1d12h)\n\n_Or /cancel to stop._",
        parse_mode="Markdown",
    )
    return ADD_INTERVAL


async def add_interval_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 3h, 2d, 1d12h, or 90m — "
            "or /cancel to stop."
        )
        return ADD_INTERVAL

    context.user_data["pending_interval_seconds"] = seconds
    await update.message.reply_text(
        f"*First due in?*\n\n"
        f"How long from now should this task first be due? Send a duration if it's different "
        f"from the {format_duration(seconds)} interval above, "
        f"or tap Skip to use that same interval for the first due date "
        f"too.\n\n_Or /cancel to stop._",
        parse_mode="Markdown",
        reply_markup=skip_keyboard("firstdue"),
    )
    return ADD_FIRST_DUE


async def _first_due_decided(update_or_query, context, first_due_seconds, edit=False):
    """Shared continuation after the first-due question is answered, either
    by typed duration or by tapping Skip."""
    context.user_data["pending_first_due_seconds"] = first_due_seconds
    task_type = context.user_data.get("pending_type", "no_reminder")

    if task_type == "nagging":
        text = (
            "*Early reminder?*\n\n"
            "How long before it's due should I give you a one-time heads-up? "
            "(e.g. 2h, 30m, 1d) The repeated nagging only starts once it's "
            "actually due.\n\n_Or /cancel to stop._"
        )
        if edit:
            await update_or_query.edit_message_text(text, parse_mode="Markdown")
        else:
            await update_or_query.message.reply_text(text, parse_mode="Markdown")
        return ADD_NAGGING_LEAD

    # simple or no_reminder: everything is collected, create it now.
    # create_task_and_reply calls update.message.reply_text(...) — both a
    # genuine Update and a CallbackQuery expose a working .message.reply_text
    # (CallbackQuery.message is a real Message), so update_or_query works
    # either way without special-casing.
    name = context.user_data.pop("pending_name", None)
    interval_seconds = context.user_data.pop("pending_interval_seconds", None)
    context.user_data.pop("pending_first_due_seconds", None)
    context.user_data.pop("pending_type", None)
    chat_id = update_or_query.message.chat_id
    await create_task_and_reply(
        update_or_query,
        chat_id,
        name,
        interval_seconds,
        task_type=task_type,
        first_due_seconds=first_due_seconds,
    )
    return ConversationHandler.END


async def add_first_due_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().lower()
    if text in ("skip", "same", ""):
        first_due_seconds = None
    else:
        first_due_seconds = parse_interval(text)
        if first_due_seconds is None:
            await update.message.reply_text(
                "I couldn't understand that. Try something like 20h, 2d — or "
                "tap Skip to use the same interval as the first due date too.\n\n"
                "_Or /cancel to stop._",
                parse_mode="Markdown",
                reply_markup=skip_keyboard("firstdue"),
            )
            return ADD_FIRST_DUE
    return await _first_due_decided(update, context, first_due_seconds)


async def first_due_skip_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    return await _first_due_decided(query, context, None, edit=True)


async def add_one_time_delay_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 3h, 2d, or 1d12h — "
            "or /cancel to stop."
        )
        return ADD_ONE_TIME_DELAY

    name = context.user_data.pop("pending_name", None)
    context.user_data.pop("pending_type", None)
    chat_id = update.effective_chat.id
    await create_task_and_reply(update, chat_id, name, seconds, task_type="one_time")
    return ConversationHandler.END


async def ask_old_school_hour(update_or_query, name, tz, edit=False):
    local_now = now_utc().astimezone(tz)
    local_now_h, local_now_m = local_now.hour, local_now.minute
    text = (
        f"*What time?*\n\n"
        f"What LOCAL time ({esc(tz_label(tz))}) should {bold(name)} "
        f"remind you at? Send an hour like `14` or a time like `14:30` "
        f"(24-hour), or tap the button to use the current time "
        f"({local_now_h:02d}:{local_now_m:02d}).\n\n_Or /cancel to stop._"
    )
    kb = skip_keyboard("oshour", "⏭️ Use current time")
    if edit:
        await update_or_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update_or_query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def add_timezone_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if text == TIMEZONE_SKIP_TEXT:
        chat_id = update.effective_chat.id
        set_chat_timezone(chat_id, 0)
        name = context.user_data.get("pending_name")
        await update.message.reply_text("Using UTC.", reply_markup=MAIN_MENU_KEYBOARD)
        await ask_old_school_hour(update, name, timezone.utc)
        return ADD_OLD_SCHOOL_HOUR

    try:
        resolved = await asyncio.to_thread(resolve_timezone_input, text)
    except GeocodingUnavailable:
        await update.message.reply_text(
            "I couldn't reach the city-lookup service just now, so place "
            "names aren't working at the moment — please send a UTC offset "
            "instead, like `+3`, `-5`, or `+5:30`, or tap Skip to use "
            "UTC.\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return ADD_TIMEZONE
    if resolved is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like `+3`, `-5`, "
            "`+5:30`, or a city/capital name like `Madrid` — or tap Skip "
            "to use UTC.\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return ADD_TIMEZONE
    offset, place_name, tz_name = resolved
    chat_id = update.effective_chat.id
    n_moved, new_tz = set_chat_timezone(chat_id, offset, tz_name)
    name = context.user_data.get("pending_name")
    if place_name:
        confirmation = (
            f"📍 {bold(place_name)} is *{format_utc_offset(offset)}* right now "
            f"({esc(tz_name)}). Saved."
        )
    else:
        confirmation = f"Saved — using *{format_utc_offset(offset)}*."
    # reply_markup restores the normal persistent menu, in case the
    # location-share/skip keyboard (see TIMEZONE_QUESTION_KEYBOARD) got
    # shown for this question — harmless to include even when it wasn't.
    await update.message.reply_text(
        confirmation + tz_saved_notes(n_moved, tz_name, offset),
        parse_mode="Markdown",
        reply_markup=MAIN_MENU_KEYBOARD,
    )
    await ask_old_school_hour(update, name, new_tz)
    return ADD_OLD_SCHOOL_HOUR


async def add_timezone_location_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a tap on TIMEZONE_QUESTION_KEYBOARD's "Share location"
    button mid-/add — the standalone /timezone command's equivalent
    bare-form case is handled by maybe_handle_location_reply instead,
    since that flow isn't part of this ConversationHandler."""
    loc = update.message.location
    try:
        offset, tz_name = await asyncio.to_thread(
            lookup_coords_utc_offset, loc.latitude, loc.longitude
        )
    except GeocodingUnavailable:
        await update.message.reply_text(
            "I couldn't reach the timezone-lookup service just now — try "
            "sharing again, or send a UTC offset instead, like `+3`, "
            "`-5`, or `+5:30`.\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return ADD_TIMEZONE

    chat_id = update.effective_chat.id
    n_moved, new_tz = set_chat_timezone(chat_id, offset, tz_name)
    name = context.user_data.get("pending_name")
    await update.message.reply_text(
        f"📍 That's *{format_utc_offset(offset)}* right now ({esc(tz_name)}). Saved.",
        parse_mode="Markdown",
        reply_markup=MAIN_MENU_KEYBOARD,
    )
    await ask_old_school_hour(update, name, new_tz)
    return ADD_OLD_SCHOOL_HOUR


async def ask_old_school_date(update_or_query, tz, edit=False):
    text = (
        "*Date?*\n\n"
        "Tap a day on the calendar (· = already passed), or send a date "
        "like `2026-09-01` (year-month-day).\n\n_Or /cancel to stop._"
    )
    kb = calendar_keyboard(local_today(tz))
    if edit:
        await update_or_query.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        await update_or_query.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def add_old_school_hour_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    parsed = parse_local_hour_minute(text)
    if parsed is None:
        await update.message.reply_text(
            "Please send an hour like `14` or a time like `14:30` "
            "(24-hour, local), or tap the button to use the current "
            "time.\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
            reply_markup=skip_keyboard("oshour", "⏭️ Use current time"),
        )
        return ADD_OLD_SCHOOL_HOUR

    local_hour, local_minute = parsed
    # pending_local_hour / pending_local_minute hold the LOCAL wall-clock time; they are
    # only converted to UTC once the date is known (DST depends on the date).
    context.user_data["pending_local_hour"] = local_hour
    context.user_data["pending_local_minute"] = local_minute
    await ask_old_school_date(update, get_chat_tzinfo(update.effective_chat.id))
    return ADD_OLD_SCHOOL_DATE


async def old_school_hour_skip_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tz = get_chat_tzinfo(query.message.chat_id)
    now_local = now_utc().astimezone(tz)
    context.user_data["pending_local_hour"] = now_local.hour
    context.user_data["pending_local_minute"] = now_local.minute
    await ask_old_school_date(query, tz, edit=True)
    return ADD_OLD_SCHOOL_DATE


# ---------------------------------------------------------------------------
# /timezone — standalone command to view or change the saved UTC offset at
# any time, not just the first time an old-school task is created. This is
# a lightweight flag-based flow (like a nag's "Custom delay", see
# awaiting_nag_snooze_for below) rather than its own ConversationHandler,
# since it's just one question with no follow-ups.
# ---------------------------------------------------------------------------

async def _resolve_and_save_timezone(message, chat_id, text):
    """Shared by /timezone's direct one-liner (/timezone Madrid) and its
    bare-form free-text reply: resolves an offset or city name and saves
    it. Returns True on success (a confirmation was sent), or False on
    failure (an explanation was sent) so the caller can decide whether to
    keep waiting for another try."""
    try:
        resolved = await asyncio.to_thread(resolve_timezone_input, text)
    except GeocodingUnavailable:
        await message.reply_text(
            "I couldn't reach the city-lookup service just now, so place "
            "names aren't working at the moment — please send a UTC offset "
            "instead, like `+3`, `-5`, or `+5:30`.",
            parse_mode="Markdown",
        )
        return False
    if resolved is None:
        await message.reply_text(
            "I couldn't understand that. Try a UTC offset like `+3`, `-5`, "
            "`+5:30`, or a city/capital name like `Madrid`.",
            parse_mode="Markdown",
        )
        return False

    offset, place_name, tz_name = resolved
    n_moved, _ = set_chat_timezone(chat_id, offset, tz_name)
    if place_name:
        confirmation = (
            f"📍 {bold(place_name)} is *{format_utc_offset(offset)}* right now "
            f"({esc(tz_name)}). Saved."
        )
    else:
        confirmation = f"Saved — using *{format_utc_offset(offset)}*."
    # reply_markup restores the normal persistent menu, in case the
    # location-share/skip keyboard (see TIMEZONE_QUESTION_KEYBOARD) got
    # shown for this question — harmless to include even when it wasn't.
    await message.reply_text(
        f"{confirmation}{tz_saved_notes(n_moved, tz_name, offset)}\n\n{TASK_TIP}",
        parse_mode="Markdown",
        reply_markup=MAIN_MENU_KEYBOARD,
    )
    return True


async def maybe_handle_location_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Catches a shared location answering /timezone's bare (no-argument)
    form. The in-/add-flow case is handled inside the ConversationHandler
    by add_timezone_location_received instead. A no-op otherwise, so this
    is safe to register as a global catch-all for location messages."""
    if not context.user_data.get("awaiting_timezone_reply"):
        return

    loc = update.message.location
    try:
        offset, tz_name = await asyncio.to_thread(
            lookup_coords_utc_offset, loc.latitude, loc.longitude
        )
    except GeocodingUnavailable:
        await update.message.reply_text(
            "I couldn't reach the timezone-lookup service just now — try "
            "sharing again, or send a UTC offset instead, like `+3`, "
            "`-5`, or `+5:30`."
        )
        return  # keep waiting for a valid reply

    context.user_data.pop("awaiting_timezone_reply", None)
    chat_id = update.effective_chat.id
    n_moved, _ = set_chat_timezone(chat_id, offset, tz_name)
    await update.message.reply_text(
        f"📍 That's *{format_utc_offset(offset)}* right now ({esc(tz_name)}). "
        f"Saved.{tz_saved_notes(n_moved, tz_name, offset)}\n\n{TASK_TIP}",
        parse_mode="Markdown",
        reply_markup=MAIN_MENU_KEYBOARD,
    )


async def timezone_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/timezone — view or change your saved UTC offset (used for
    old-school tasks' local-time scheduling) at any time, not just the
    first time an old-school task is created. Works two ways:
      /timezone           bare — shows the current setting, then asks for
                           a new one (offset or city name).
      /timezone Madrid    direct — resolves and saves immediately, same as
      /timezone +5:30     replying to the bare form.
    Once set (here or from that first old-school question), old-school
    task creation stops asking and just uses it."""
    chat_id = update.effective_chat.id
    args_text = " ".join(context.args).strip() if context.args else ""

    if args_text:
        await _resolve_and_save_timezone(update.message, chat_id, args_text)
        return

    current_tz = get_chat_timezone(chat_id)
    current_line = (
        f"Currently set to *{esc(tz_label(current_tz))}*.\n\n"
        if current_tz is not None
        else "Not set yet — old-school tasks currently use plain UTC.\n\n"
    )
    _clear_lightweight_reply_flags(context)
    context.user_data["awaiting_timezone_reply"] = True
    await update.message.reply_text(
        f"*Your time zone?*\n\n{current_line}"
        f"Send a UTC offset (e.g. `+3`, `-5`, `+5:30`) or a city/capital "
        f"name (e.g. `Madrid` — a city follows daylight saving automatically), "
        f"or tap Skip to use UTC.\n\n_Or /cancel to stop._",
        parse_mode="Markdown",
        reply_markup=TIMEZONE_QUESTION_KEYBOARD,
    )


def old_school_interval_prompt(target_date) -> str:
    """The 'how often?' question, echoing the date that was just chosen."""
    return (
        f"📅 First reminder: *{target_date.strftime('%a %Y-%m-%d')}*\n\n"
        "*How often?*\n\nHow often should it repeat?\n"
        "Days, weeks, or months only — "
        "e.g. `10d`, `2w`, `3mo`.\n\n_Or /cancel to stop._"
    )


async def add_old_school_date_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tz = get_chat_tzinfo(update.effective_chat.id)
    target_date = parse_target_date(update.message.text.strip(), tz)
    if target_date is None:
        await update.message.reply_text(
            "I couldn't understand that. Try `today`, `tomorrow`, or a date "
            "like `2026-09-01` — or pick one below.\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
            reply_markup=calendar_keyboard(local_today(tz)),
        )
        return ADD_OLD_SCHOOL_DATE
    context.user_data["pending_date"] = target_date.isoformat()
    await update.message.reply_text(old_school_interval_prompt(target_date), parse_mode="Markdown")
    return ADD_OLD_SCHOOL_INTERVAL


async def old_school_date_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Every tap on the date question: calendar navigation (osdate:cal:YYYY-MM),
    a picked day (osdate:pick:YYYY-MM-DD), the Today/Tomorrow quick buttons,
    and osdate:noop for the decorative cells (headers, blanks, past days)."""
    query = update.callback_query
    parts = query.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    tz = get_chat_tzinfo(update.effective_chat.id)
    today = local_today(tz)

    if action == "noop":
        await query.answer()
        return ADD_OLD_SCHOOL_DATE

    if action == "cal":
        await query.answer()
        try:
            shown = datetime.strptime(parts[2], "%Y-%m")
        except (IndexError, ValueError):
            return ADD_OLD_SCHOOL_DATE
        try:
            await query.edit_message_reply_markup(
                reply_markup=calendar_keyboard(today, shown.year, shown.month)
            )
        except BadRequest as exc:
            if "not modified" not in str(exc).lower():
                raise
        return ADD_OLD_SCHOOL_DATE

    if action == "pick":
        try:
            target_date = datetime.strptime(parts[2], "%Y-%m-%d").date()
        except (IndexError, ValueError):
            await query.answer()
            return ADD_OLD_SCHOOL_DATE
        if target_date < today:
            # e.g. the calendar was opened yesterday and midnight has passed
            await query.answer("That date has already passed — pick another.", show_alert=True)
            return ADD_OLD_SCHOOL_DATE
    elif action in ("today", "tomorrow"):
        target_date = today + timedelta(days=0 if action == "today" else 1)
    else:  # unknown or tampered button: ignore it rather than guess
        await query.answer()
        return ADD_OLD_SCHOOL_DATE

    await query.answer()
    context.user_data["pending_date"] = target_date.isoformat()
    await query.edit_message_text(old_school_interval_prompt(target_date), parse_mode="Markdown")
    return ADD_OLD_SCHOOL_INTERVAL


async def add_old_school_interval_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = parse_old_school_interval(update.message.text.strip())
    if parsed is None:
        await update.message.reply_text(
            "I couldn't understand that. Use days, weeks, or months only — "
            "e.g. 10d, 2w, 3mo — or /cancel to stop."
        )
        return ADD_OLD_SCHOOL_INTERVAL

    unit, amount = parsed
    name = context.user_data.pop("pending_name", None)
    chat_id = update.effective_chat.id
    tz = get_chat_tzinfo(chat_id)
    now_local = now_utc().astimezone(tz)
    local_hour = context.user_data.pop("pending_local_hour", now_local.hour)
    local_minute = context.user_data.pop("pending_local_minute", now_local.minute)
    date_str = context.user_data.pop("pending_date", local_today(tz).isoformat())
    context.user_data.pop("pending_type", None)
    first_due = local_to_utc_datetime(
        datetime.strptime(date_str, "%Y-%m-%d").date(), local_hour, local_minute, tz
    )
    await create_task_and_reply(
        update,
        chat_id,
        name,
        0,
        task_type="old_school",
        reminder_hour_local=local_hour,
        reminder_minute_local=local_minute,
        interval_unit=unit,
        interval_amount=amount,
        first_due_dt=first_due,
    )
    return ConversationHandler.END


async def add_nagging_interval_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 3h, 2d, 1d12h, or 90m — "
            "or /cancel to stop."
        )
        return ADD_NAGGING_INTERVAL

    context.user_data["pending_interval_seconds"] = seconds
    await update.message.reply_text(
        f"*First due in?*\n\n"
        f"How long from now should this task first be due? Send a duration if it's different "
        f"from the {format_duration(seconds)} interval above, "
        f"or tap Skip to use that same interval for the first due date "
        f"too.\n\n_Or /cancel to stop._",
        parse_mode="Markdown",
        reply_markup=skip_keyboard("firstdue"),
    )
    return ADD_FIRST_DUE


async def add_nagging_lead_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 2h, 30m, or 1d — "
            "or /cancel to stop."
        )
        return ADD_NAGGING_LEAD

    context.user_data["pending_lead_time_seconds"] = seconds
    await update.message.reply_text(
        "*Nag cadence?*\n\n"
        "Once it's actually due, how often should I keep nagging you until "
        "you mark it done? (e.g. 30m, 1h)\n\n_Or /cancel to stop._",
        parse_mode="Markdown",
    )
    return ADD_NAGGING_CADENCE


async def add_nagging_cadence_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 30m, 1h, or 2h — "
            "or /cancel to stop."
        )
        return ADD_NAGGING_CADENCE

    name = context.user_data.pop("pending_name", None)
    interval_seconds = context.user_data.pop("pending_interval_seconds", None)
    lead_time_seconds = context.user_data.pop("pending_lead_time_seconds", None)
    first_due_seconds = context.user_data.pop("pending_first_due_seconds", None)
    context.user_data.pop("pending_type", None)
    chat_id = update.effective_chat.id
    await create_task_and_reply(
        update,
        chat_id,
        name,
        interval_seconds,
        task_type="nagging",
        lead_time_seconds=lead_time_seconds,
        nag_interval_seconds=seconds,
        first_due_seconds=first_due_seconds,
    )
    return ConversationHandler.END


async def add_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _clear_pending_add_state(context)
    await update.message.reply_text(
        f"Cancelled — no task added.\n\n{TASK_TIP}",
        parse_mode="Markdown",
    )
    return ConversationHandler.END


async def add_conversation_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fires automatically once conversation_timeout (below) elapses with no
    reply mid-/add. No proactive message here — a spontaneous notification
    for something as mundane as an idle timeout is more annoying than
    useful. Instead, this quietly clears the stale state and remembers
    that the session expired, so if the person's next plain-text message
    looks like an attempt to continue it, maybe_handle_nag_snooze_reply
    can explain what happened right then instead of just swallowing it
    silently. A tap on any leftover button from the dead conversation is
    handled the same reactive way, by add_flow_stale_button."""
    _clear_pending_add_state(context)
    _clear_lightweight_reply_flags(context)
    context.user_data["add_session_timed_out"] = True


# Commands that should properly end a stuck /add if typed mid-flow, mapped
# to their real handler. Without this, typing e.g. "/status" mid-add still
# *appears* to work (it has its own top-level handler outside the
# conversation), but the add conversation itself never gets told to end —
# it silently lingers, and the next plain-text message you send can get
# swallowed as if it were answering that stale /add prompt.
def _other_commands_while_adding():
    # Built lazily (not as a module-level dict) because several of these
    # handlers are defined further down in this file.
    return {
        "status": status,
        "list": status,
        "details": details_cmd,
        "edit": edit_start,
        "done": done_task,
        "delete": delete_task,
        "reset": reset_start,
        "wipe": wipe_start,
        "help": help_cmd,
        "start": start,
        "timezone": timezone_cmd,
    }


async def add_other_command_interrupt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback: any other top-level command typed while a step-by-step
    /add is in progress cleanly ends the add and runs that command."""
    _clear_pending_add_state(context)
    command = update.message.text.split()[0].lstrip("/").split("@")[0].lower()
    handler = _other_commands_while_adding().get(command)
    if handler:
        await handler(update, context)
    return ConversationHandler.END


async def add_start_from_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for the persistent '✍️ Add' button. Also doubles as
    the handler when that button is tapped mid-flow: it cleanly restarts
    the add instead of letting the button's label be read as an answer."""
    _clear_all_conversation_states(context)
    await update.message.reply_text(
        "*Type?*\n\n"
        "What kind of task is this?\n"
        "Send ❓ /help to see details about each type.",
        parse_mode="Markdown",
        reply_markup=type_choice_keyboard(),
    )
    return ADD_TYPE


async def menu_interrupt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Status/Done menu buttons tapped while a step-by-step /add is in
    progress: abandon the add and run the tapped action instead of
    swallowing the button's label as text input for the current question."""
    _clear_pending_add_state(context)
    text = update.message.text
    if text == MENU_STATUS:
        await status(update, context)
    elif text == MENU_DONE:
        await show_done_list(update, update.effective_chat.id)
    return ConversationHandler.END


async def menu_done_standalone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the '✅ Done' menu button when no /add conversation is active."""
    await show_done_list(update, update.effective_chat.id)


def find_task(conn, chat_id, name):
    return conn.execute(
        "SELECT * FROM tasks WHERE chat_id = %s AND LOWER(name) = LOWER(%s)",
        (chat_id, name),
    ).fetchone()


async def show_done_list(update: Update, chat_id: int):
    """Sends a tappable list of tasks to mark done. Shared by bare /done and
    the persistent '✅ Done' menu button. Old-school tasks are excluded —
    they reschedule on their own and don't respond to /done."""
    conn = get_conn()
    all_tasks = conn.execute(
        "SELECT * FROM tasks WHERE chat_id = %s ORDER BY due_at ASC", (chat_id,)
    ).fetchall()
    conn.close()

    if not all_tasks:
        await update.message.reply_text(
            "No tasks yet. Add one with /add or send /help for more instructions."
        )
        return

    tasks = [t for t in all_tasks if (t["task_type"] or "no_reminder") != "old_school"]
    if not tasks:
        await update.message.reply_text(
            "All your tasks are old-school, which don't need /done — they "
            "reschedule automatically."
        )
        return

    buttons = [
        [
            InlineKeyboardButton(
                f"{TASK_TYPE_EMOJI.get(t['task_type'] or 'no_reminder', '🔁')} {t['name']}",  # md-safe: button label
                callback_data=f"done:{t['id']}",
            )
        ]
        for t in tasks
    ]
    await update.message.reply_text(
        "Which task did you just finish?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def done_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        await show_done_list(update, chat_id)
        return

    name = " ".join(args).strip()
    await mark_done(update, chat_id, name)


def apply_done_to_task(conn, task):
    """Core, type-aware 'what happens when this task is marked done' logic.
    Mutates the DB via the given connection (caller commits/closes). Used by
    both the single /done flow and the bulk /reset flow so they can never
    drift out of sync with each other."""
    task_type = task["task_type"] or "no_reminder"

    if task_type == "old_school":
        # Old-school tasks reschedule on their own fixed cadence regardless
        # of /done — see check_due_tasks. Nothing to do here.
        return {"skipped": True}

    if task_type == "one_time":
        conn.execute("DELETE FROM tasks WHERE id = %s", (task["id"],))
        return {"deleted": True}

    new_due = now_utc() + timedelta(seconds=task["interval_seconds"])
    if task_type == "nagging":
        next_nag = first_nag_moment(new_due, task["lead_time_seconds"], now_utc())
        conn.execute(
            "UPDATE tasks SET due_at = %s, notified_at = NULL, next_nag_at = %s WHERE id = %s",
            (to_iso(new_due), to_iso(next_nag), task["id"]),
        )
    else:
        conn.execute(
            "UPDATE tasks SET due_at = %s, notified_at = NULL WHERE id = %s",
            (to_iso(new_due), task["id"]),
        )
    return {"rescheduled": True, "new_due": new_due}


async def mark_done(update_or_query, chat_id, name):
    """Shared logic for /done and any 'Done' button."""
    conn = get_conn()
    task = find_task(conn, chat_id, name)
    if not task:
        conn.close()
        await _reply_or_edit(update_or_query, f'No task called "{name}". Check /status for the exact name.')  # md-safe: sent without parse_mode
        return

    outcome = apply_done_to_task(conn, task)
    conn.commit()
    conn.close()

    if outcome.get("skipped"):
        text = (
            f"📅 {bold(task['name'])} is old-school — it doesn't need /done. It "
            f"reminds you automatically every "
            f"{format_old_school_interval(task['interval_unit'], task['interval_amount'])}, "
            f"whether you respond or not.\n\n{TASK_TIP}"
        )
    elif outcome.get("deleted"):
        text = (
            f"✅ {bold(task['name'])} done — that was a one-time reminder, so "
            f"it's been removed.\n\n{TASK_TIP}"
        )
    else:
        text = (
            f"✅ {bold(task['name'])} done. Next due in "
            f"{format_duration(task['interval_seconds'])}.\n\n{TASK_TIP}"
        )
    await _reply_or_edit(update_or_query, text, markdown=True)


def delete_confirm_keyboard(task_id):
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⚠️ Yes, delete", callback_data=f"delyes:{task_id}"),
                InlineKeyboardButton("Cancel", callback_data=f"delno:{task_id}"),
            ]
        ]
    )


async def delete_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    if not args:
        conn = get_conn()
        tasks = conn.execute(
            "SELECT * FROM tasks WHERE chat_id = %s ORDER BY due_at ASC", (chat_id,)
        ).fetchall()
        conn.close()

        if not tasks:
            await update.message.reply_text("No tasks to delete.")
            return

        # Tapping a task here goes to delete_ask_button first, not straight to deletion.
        buttons = [
            [InlineKeyboardButton(f"🗑 {t['name']}", callback_data=f"delask:{t['id']}")]  # md-safe: button label
            for t in tasks
        ]
        await update.message.reply_text(
            "Which task do you want to delete?",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    name = " ".join(args).strip()
    conn = get_conn()
    task = find_task(conn, chat_id, name)
    conn.close()
    if not task:
        await update.message.reply_text(f'No task called "{name}".')  # md-safe: sent without parse_mode
        return

    await update.message.reply_text(
        f"Delete {bold(task['name'])}? This can't be undone.",
        parse_mode="Markdown",
        reply_markup=delete_confirm_keyboard(task["id"]),
    )


async def delete_ask_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """First tap — from the bare /delete list. Shows a Yes/Cancel
    confirmation, doesn't delete yet."""
    query = update.callback_query
    await query.answer()
    task_id = int(query.data.split(":", 1)[1])
    chat_id = query.message.chat_id

    conn = get_conn()
    task = conn.execute(
        "SELECT * FROM tasks WHERE id = %s AND chat_id = %s", (task_id, chat_id)
    ).fetchone()
    conn.close()
    if not task:
        await query.edit_message_text("That task no longer exists.")
        return

    await query.edit_message_text(
        f"Delete {bold(task['name'])}? This can't be undone.",
        parse_mode="Markdown",
        reply_markup=delete_confirm_keyboard(task_id),
    )


async def delete_confirm_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Second tap — actually deletes (delyes) or backs out (delno)."""
    query = update.callback_query
    await query.answer()
    action, task_id_str = query.data.split(":", 1)
    task_id = int(task_id_str)
    chat_id = query.message.chat_id

    if action == "delno":
        await query.edit_message_text(
            f"Cancelled — nothing was deleted.\n\n{TASK_TIP}",
            parse_mode="Markdown",
        )
        return

    conn = get_conn()
    task = conn.execute(
        "SELECT * FROM tasks WHERE id = %s AND chat_id = %s", (task_id, chat_id)
    ).fetchone()
    if not task:
        conn.close()
        await query.edit_message_text("That task no longer exists.")
        return
    conn.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
    conn.commit()
    conn.close()
    await query.edit_message_text(
        f"🗑️ Deleted {bold(task['name'])}.\n\n{TASK_TIP}",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# /edit — change a field on an existing task without deleting and
# recreating it. A dedicated ConversationHandler (edit_conv, see main()),
# structurally similar to add_conv: pick a task, pick a field, answer the
# same style of question /add would ask for that field, then loop back to
# the field picker so several fields can be changed in one /edit session.
#
# Design choice worth stating explicitly: editing interval_seconds,
# lead_time_seconds's cousin nag_interval_seconds, or an old-school
# task's cadence only changes the *pattern* going forward — none of these
# touch the currently scheduled due_at. The two exceptions are lead time
# (which recomputes next_nag_at immediately, since otherwise the edit
# would have no visible effect until the next full cycle) and an
# old-school task's time-of-day (which does move due_at, since leaving it
# on the old time until the *next* occurrence would mean the very next
# fire — the one someone editing this would most expect to change —
# still uses the old time). A one-time reminder's "delay" is really just
# rescheduling due_at directly, so that one always moves it.
# ---------------------------------------------------------------------------

def editable_fields_keyboard(task):
    task_type = task["task_type"] or "no_reminder"
    buttons = [[InlineKeyboardButton("✏️ Name", callback_data="editfield:name")]]
    if task_type in ("simple", "no_reminder", "nagging"):
        buttons.append([InlineKeyboardButton("⏱ Interval", callback_data="editfield:interval")])
    if task_type == "nagging":
        buttons.append([InlineKeyboardButton("⏳ Lead time", callback_data="editfield:lead")])
        buttons.append(
            [InlineKeyboardButton("🔁 Nag cadence", callback_data="editfield:nagcadence")]
        )
    if task_type == "old_school":
        buttons.append([InlineKeyboardButton("🕐 Time", callback_data="editfield:ostime")])
        buttons.append([InlineKeyboardButton("📅 Cadence", callback_data="editfield:oscadence")])
    if task_type == "one_time":
        buttons.append(
            [InlineKeyboardButton("⏳ Delay (from now)", callback_data="editfield:delay")]
        )
    buttons.append([InlineKeyboardButton("✅ Done editing", callback_data="editfield:done")])
    return InlineKeyboardMarkup(buttons)


def _edit_field_menu_text(task):
    emoji = TASK_TYPE_EMOJI.get(task["task_type"] or "no_reminder", "🔁")
    return f"{bold('Editing ' + emoji + ' ' + task['name'])}\n\nWhat do you want to change?"


async def edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _clear_all_conversation_states(context)
    chat_id = update.effective_chat.id
    args = context.args

    if args:
        name = " ".join(args).strip()
        conn = get_conn()
        task = find_task(conn, chat_id, name)
        conn.close()
        if not task:
            await update.message.reply_text(
                f"No task called {bold(name)}.", parse_mode="Markdown"
            )
            return ConversationHandler.END
        context.user_data["editing_task_id"] = task["id"]
        await update.message.reply_text(
            _edit_field_menu_text(task),
            parse_mode="Markdown",
            reply_markup=editable_fields_keyboard(task),
        )
        return EDIT_PICK_FIELD

    conn = get_conn()
    tasks = conn.execute(
        "SELECT * FROM tasks WHERE chat_id = %s ORDER BY name ASC", (chat_id,)
    ).fetchall()
    conn.close()
    if not tasks:
        await update.message.reply_text(
            "No tasks yet. Add one with /add or send /help for more instructions."
        )
        return ConversationHandler.END

    buttons = [
        [
            InlineKeyboardButton(
                f"{TASK_TYPE_EMOJI.get(t['task_type'] or 'no_reminder', '🔁')} {t['name']}",  # md-safe: button label
                callback_data=f"editpick:{t['id']}",
            )
        ]
        for t in tasks
    ]
    await update.message.reply_text(
        "*Edit which task?*",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return EDIT_PICK_TASK


async def edit_pick_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    task_id = int(query.data.split(":", 1)[1])
    chat_id = query.message.chat_id
    conn = get_conn()
    task = conn.execute(
        "SELECT * FROM tasks WHERE id = %s AND chat_id = %s", (task_id, chat_id)
    ).fetchone()
    conn.close()
    if not task:
        await query.edit_message_text("That task no longer exists.")
        return ConversationHandler.END

    context.user_data["editing_task_id"] = task["id"]
    await query.edit_message_text(
        _edit_field_menu_text(task),
        parse_mode="Markdown",
        reply_markup=editable_fields_keyboard(task),
    )
    return EDIT_PICK_FIELD


EDIT_FIELD_PROMPTS = {
    "name": ("*New name?*\n\nWhat should I call it instead?", EDIT_NAME),
    "interval": (
        "*New interval?*\n\nE.g. `3h`, `2d`, `1w`. This changes the pattern "
        "going forward — it won't move the currently scheduled due date.",
        EDIT_INTERVAL,
    ),
    "lead": (
        "*New lead time?*\n\nHow long before due should the one-time early "
        "reminder come? E.g. `2h`, `30m`. (Nagging itself starts when it's due.)",
        EDIT_LEAD_TIME,
    ),
    "nagcadence": (
        "*New nag cadence?*\n\nHow often should it re-nag? E.g. `30m`, `1h`.",
        EDIT_NAG_CADENCE,
    ),
    "ostime": (
        "*New time?*\n\nSend an hour like `14` or a time like `14:30` "
        "(24-hour, local).",
        EDIT_OS_TIME,
    ),
    "oscadence": (
        "*New cadence?*\n\nDays, weeks, or months only — e.g. `10d`, `2w`, `3mo`.",
        EDIT_OS_CADENCE,
    ),
    "delay": (
        "*New delay?*\n\nHow long from now should it be due? E.g. `3h`, `1d`.",
        EDIT_ONE_TIME_DELAY,
    ),
}


async def edit_field_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    field = query.data.split(":", 1)[1]
    task_id = context.user_data.get("editing_task_id")
    if not task_id:
        await query.edit_message_text("That edit session has expired — send /edit again.")
        return ConversationHandler.END

    conn = get_conn()
    task = conn.execute("SELECT * FROM tasks WHERE id = %s", (task_id,)).fetchone()
    conn.close()
    if not task:
        await query.edit_message_text("That task no longer exists.")
        return ConversationHandler.END

    if field == "done":
        context.user_data.pop("editing_task_id", None)
        await query.edit_message_text(
            f"Done editing {bold(task['name'])}.\n\n{TASK_TIP}", parse_mode="Markdown"
        )
        return ConversationHandler.END

    text, next_state = EDIT_FIELD_PROMPTS[field]
    await query.edit_message_text(f"{text}\n\n_Or /cancel to stop._", parse_mode="Markdown")
    return next_state


async def _reshow_edit_field_menu(message, task_id):
    """Loops back to the field picker after a successful edit, so several
    fields can be changed in one /edit session without re-running the
    command. Always a fresh message (never an edit), since the caller is
    a text reply, not a callback query."""
    conn = get_conn()
    task = conn.execute("SELECT * FROM tasks WHERE id = %s", (task_id,)).fetchone()
    conn.close()
    await message.reply_text(
        _edit_field_menu_text(task).replace("What do you want", "What else do you want"),
        parse_mode="Markdown",
        reply_markup=editable_fields_keyboard(task),
    )
    return EDIT_PICK_FIELD


async def edit_name_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    task_id = context.user_data.get("editing_task_id")
    chat_id = update.effective_chat.id
    if not text:
        await update.message.reply_text(
            "That can't be empty — what should I call it?\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_NAME

    conn = get_conn()
    existing = find_task(conn, chat_id, text)
    if existing and existing["id"] != task_id:
        conn.close()
        await update.message.reply_text(
            f"*Name taken*\n\nYou already have a task called {bold(existing['name'])}. "
            f"Send a different name.\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_NAME
    conn.execute("UPDATE tasks SET name = %s WHERE id = %s", (text, task_id))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"✅ Renamed to *{text}*.", parse_mode="Markdown")
    return await _reshow_edit_field_menu(update.message, task_id)


async def edit_interval_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 3h, 2d, 1w.\n\n"
            "_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_INTERVAL
    task_id = context.user_data.get("editing_task_id")
    conn = get_conn()
    conn.execute("UPDATE tasks SET interval_seconds = %s WHERE id = %s", (seconds, task_id))
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ Interval updated to {format_duration(seconds)}. The currently "
        f"scheduled due date is unchanged.",
        parse_mode="Markdown",
    )
    return await _reshow_edit_field_menu(update.message, task_id)


async def edit_lead_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 2h, 30m.\n\n"
            "_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_LEAD_TIME
    task_id = context.user_data.get("editing_task_id")
    conn = get_conn()
    task = conn.execute("SELECT * FROM tasks WHERE id = %s", (task_id,)).fetchone()
    due = from_iso(task["due_at"])
    now = now_utc()
    if now >= due:
        # Already due or overdue: nagging is under way and the early reminder
        # no longer applies, so don't disturb the running nag schedule.
        conn.execute("UPDATE tasks SET lead_time_seconds = %s WHERE id = %s", (seconds, task_id))
    else:
        conn.execute(
            "UPDATE tasks SET lead_time_seconds = %s, next_nag_at = %s WHERE id = %s",
            (seconds, to_iso(first_nag_moment(due, seconds, now)), task_id),
        )
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ Lead time updated: one early reminder {format_duration(seconds)} before due. "
        f"Nagging itself still starts when it's due.",
        parse_mode="Markdown",
    )
    return await _reshow_edit_field_menu(update.message, task_id)


async def edit_nag_cadence_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 30m, 1h.\n\n"
            "_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_NAG_CADENCE
    task_id = context.user_data.get("editing_task_id")
    conn = get_conn()
    conn.execute("UPDATE tasks SET nag_interval_seconds = %s WHERE id = %s", (seconds, task_id))
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ Nag cadence updated to every {format_duration(seconds)}.",
        parse_mode="Markdown",
    )
    return await _reshow_edit_field_menu(update.message, task_id)


async def edit_os_time_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = parse_local_hour_minute(update.message.text.strip())
    if parsed is None:
        await update.message.reply_text(
            "Please send an hour like `14` or a time like `14:30` "
            "(24-hour, local).\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_OS_TIME
    local_hour, local_minute = parsed
    task_id = context.user_data.get("editing_task_id")
    chat_id = update.effective_chat.id
    tz = get_chat_tzinfo(chat_id)

    conn = get_conn()
    task = conn.execute("SELECT * FROM tasks WHERE id = %s", (task_id,)).fetchone()
    due = from_iso(task["due_at"])
    # Put the new wall-clock time on the *local* date of the current due
    # moment, then convert with the offset in effect on that date.
    new_due = local_to_utc_datetime(due.astimezone(tz).date(), local_hour, local_minute, tz)
    if new_due <= now_utc():
        new_due = next_old_school_due(
            new_due, local_hour, local_minute, task["interval_unit"], task["interval_amount"], tz
        )
    conn.execute(
        "UPDATE tasks SET reminder_hour_local = %s, reminder_minute_local = %s, due_at = %s "
        "WHERE id = %s",
        (local_hour, local_minute, to_iso(new_due), task_id),
    )
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ Time updated to {local_hour:02d}:{local_minute:02d} local.",
        parse_mode="Markdown",
    )
    return await _reshow_edit_field_menu(update.message, task_id)


async def edit_os_cadence_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parsed = parse_old_school_interval(update.message.text.strip())
    if parsed is None:
        await update.message.reply_text(
            "I couldn't understand that. Use days, weeks, or months only — "
            "e.g. 10d, 2w, 3mo.\n\n_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_OS_CADENCE
    unit, amount = parsed
    task_id = context.user_data.get("editing_task_id")
    conn = get_conn()
    conn.execute(
        "UPDATE tasks SET interval_unit = %s, interval_amount = %s WHERE id = %s",
        (unit, amount, task_id),
    )
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ Cadence updated to every {format_old_school_interval(unit, amount)}.",
        parse_mode="Markdown",
    )
    return await _reshow_edit_field_menu(update.message, task_id)


async def edit_delay_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 3h, 1d.\n\n"
            "_Or /cancel to stop._",
            parse_mode="Markdown",
        )
        return EDIT_ONE_TIME_DELAY
    task_id = context.user_data.get("editing_task_id")
    new_due = now_utc() + timedelta(seconds=seconds)
    conn = get_conn()
    conn.execute("UPDATE tasks SET due_at = %s WHERE id = %s", (to_iso(new_due), task_id))
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ Now due in {format_duration(seconds)}.", parse_mode="Markdown"
    )
    return await _reshow_edit_field_menu(update.message, task_id)


def _clear_pending_edit_state(context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("editing_task_id", None)


# Every ConversationHandler's own "clear my pending state" function gets
# registered here. Add a new one whenever you add a new ConversationHandler
# — every TRUE ENTRY POINT (add_start, add_start_from_menu, edit_start;
# NOT a flow's own /cancel or timeout, which only need to clear their own
# state) calls _clear_all_conversation_states() once, instead of needing
# a bespoke, easy-to-forget pairwise call to every other flow's clearer.
# This exists because /add and /edit are two independent
# ConversationHandlers, each only tracking its own active state: starting
# one doesn't tell the other (if it has a silently abandoned session of
# its own) that its state is now stale. Without this, a leftover button
# or stray reply from the abandoned flow could still be actionable later,
# referencing data a completely different flow has since moved past. A
# third conversational flow added later only needs one line here — not a
# hunt through every existing entry point to add a manual call.
_CONVERSATION_STATE_CLEARERS = [
    _clear_pending_add_state,
    _clear_pending_edit_state,
]


def _clear_all_conversation_states(context: ContextTypes.DEFAULT_TYPE):
    for clear in _CONVERSATION_STATE_CLEARERS:
        clear(context)


async def edit_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    _clear_pending_edit_state(context)
    await update.message.reply_text(
        f"Cancelled — no changes made.\n\n{TASK_TIP}", parse_mode="Markdown"
    )
    return ConversationHandler.END


async def edit_conversation_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fires automatically once conversation_timeout (below) elapses with
    no reply mid-/edit. No proactive message — same reasoning as
    add_conversation_timeout: a spontaneous notification for an idle
    timeout is more annoying than useful. A tap on any leftover button
    from the dead session is still handled reactively, by
    edit_flow_stale_button."""
    _clear_pending_edit_state(context)


def _other_commands_while_editing():
    # Built lazily, same reasoning as _other_commands_while_adding.
    return {
        "add": add_start,
        "status": status,
        "list": status,
        "details": details_cmd,
        "done": done_task,
        "delete": delete_task,
        "reset": reset_start,
        "wipe": wipe_start,
        "help": help_cmd,
        "start": start,
        "timezone": timezone_cmd,
    }


async def edit_other_command_interrupt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fallback: any other top-level command typed while a step-by-step
    /edit is in progress cleanly ends the edit and runs that command —
    mirrors add_other_command_interrupt."""
    _clear_pending_edit_state(context)
    command = update.message.text.split()[0].lstrip("/").split("@")[0].lower()
    handler = _other_commands_while_editing().get(command)
    if handler:
        await handler(update, context)
    return ConversationHandler.END


async def edit_flow_stale_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Catches a tap on an /edit-flow button whose conversation has since
    ended — mirrors add_flow_stale_button."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("This menu has expired — try /edit again.")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conn = get_conn()
    tasks = conn.execute(
        "SELECT * FROM tasks WHERE chat_id = %s ORDER BY due_at ASC", (chat_id,)
    ).fetchall()
    conn.close()

    if not tasks:
        await update.message.reply_text(
            "No tasks yet. Add one with /add or send /help for more instructions."
        )
        return

    now = now_utc()
    lines = []
    for t in tasks:
        emoji = TASK_TYPE_EMOJI.get(t["task_type"] or "no_reminder", "🔁")
        due = from_iso(t["due_at"])
        delta = (due - now).total_seconds()
        if delta <= 0:
            status_text = f"🔴 DUE — overdue by {format_duration(-delta)}"
        else:
            status_text = f"🟢 in {format_duration(delta)}"
        lines.append(f"{emoji} {bold(t['name'])} — {status_text}")

    lines.append("")
    lines.append("_Send ✅ /done to reset, or 🗑️ /delete to remove._")
    lines.append("_Want more? 📋 /details shows the full picture._")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
    )


def format_task_details(t, tz, now):
    """One task's full detail block for /details — the fuller, per-type
    reference view that /status's compact one-line-per-task summary
    doesn't have room for (exact due date/time, old-school's local fire
    time and cadence, nagging's lead time and current nag status, etc.)."""
    task_type = t["task_type"] or "no_reminder"
    emoji = TASK_TYPE_EMOJI.get(task_type, "🔁")
    label = TASK_TYPE_LABELS.get(task_type, task_type)
    lines = [f"{emoji} {bold(t['name'])} — {label}"]

    due = from_iso(t["due_at"])

    def fmt_when(dt):
        local_dt = dt.astimezone(tz)
        tz_text = esc(tz_label(tz, dt))
        stamp = local_dt.strftime('%Y-%m-%d %H:%M')
        return f"{stamp} {tz_text}" if tz_text == "UTC" else f"{stamp} local ({tz_text})"

    def fmt_relative(dt):
        delta = (dt - now).total_seconds()
        return f"in {format_duration(delta)}" if delta > 0 else f"overdue by {format_duration(-delta)}"

    if task_type == "old_school":
        lines.append(
            f"Repeats: every {format_old_school_interval(t['interval_unit'], t['interval_amount'])}"
        )
        lh, lm = old_school_local_time(t, tz)
        lines.append(f"Fires at: {lh:02d}:{lm:02d} ({esc(getattr(tz, 'key', None) or tz_label(tz))})")
        lines.append(f"Next occurrence: {fmt_when(due)}")
    elif task_type == "nagging":
        lines.append(f"Interval: {format_duration(t['interval_seconds'])}")
        lines.append(f"Early reminder: once, {format_duration(t['lead_time_seconds'])} before due")
        lines.append(f"Nag cadence: every {format_duration(t['nag_interval_seconds'])}, from the due time")
        lines.append(f"Due: {fmt_when(due)} ({fmt_relative(due)})")
        if t["next_nag_at"]:
            next_nag = from_iso(t["next_nag_at"])
            if now >= due:
                lines.append(
                    "Nagging: active now" if next_nag <= now
                    else f"Nagging: active, next nag {fmt_relative(next_nag)}"
                )
            elif next_nag < due:
                lines.append(
                    "Early reminder: sending now" if next_nag <= now
                    else f"Early reminder: {fmt_relative(next_nag)}"
                )
            else:
                lines.append(f"Nagging starts: {fmt_relative(next_nag)}")
    elif task_type == "one_time":
        lines.append(f"Due: {fmt_when(due)} ({fmt_relative(due)})")
    else:  # simple, no_reminder
        lines.append(f"Interval: {format_duration(t['interval_seconds'])}")
        lines.append(f"Due: {fmt_when(due)} ({fmt_relative(due)})")
        if task_type == "simple":
            lines.append(
                "Notified: yes, waiting for /done" if t["notified_at"] else "Notified: not yet"
            )

    lines.append(f"Created: {fmt_when(from_iso(t['created_at']))}")
    return "\n".join(lines)


async def details_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conn = get_conn()
    tasks = conn.execute(
        "SELECT * FROM tasks WHERE chat_id = %s ORDER BY due_at ASC", (chat_id,)
    ).fetchall()
    conn.close()

    if not tasks:
        await update.message.reply_text(
            "No tasks yet. Add one with /add or send /help for more instructions."
        )
        return

    tz = get_chat_tzinfo(chat_id)
    now = now_utc()
    blocks = [format_task_details(t, tz, now) for t in tasks]

    # Split at task-block boundaries, never mid-block, so a long list can't
    # get cut off in the middle of one task's details or break Markdown by
    # splitting a *bold* span across two messages.
    chunks = []
    current = ""
    for block in blocks:
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) > 3500:
            chunks.append(current)
            current = block
        else:
            current = candidate
    if current:
        chunks.append(current)
    chunks[-1] += "\n\n_Send 📊 /status for a quick summary, or 🗑️ /delete to remove a task._"

    for chunk in chunks:
        await update.message.reply_text(chunk, parse_mode="Markdown")


async def done_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    task_id = int(query.data.split(":", 1)[1])
    chat_id = query.message.chat_id

    conn = get_conn()
    task = conn.execute(
        "SELECT * FROM tasks WHERE id = %s AND chat_id = %s", (task_id, chat_id)
    ).fetchone()
    conn.close()
    if not task:
        await query.edit_message_text("That task no longer exists.")
        return
    await mark_done(query, chat_id, task["name"])


async def wipe_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    conn = get_conn()
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM tasks WHERE chat_id = %s", (chat_id,)
    ).fetchone()["c"]
    conn.close()

    if count == 0:
        await update.message.reply_text("You don't have any tasks, so there's nothing to wipe.")
        return

    buttons = [
        [
            InlineKeyboardButton("⚠️ Yes, delete everything", callback_data="wipe:1:yes"),
            InlineKeyboardButton("Cancel", callback_data="wipe:1:no"),
        ]
    ]
    await update.message.reply_text(
        f"This will delete *all {count} task(s)* in this chat. Are you sure?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def wipe_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    _, step, choice = query.data.split(":")

    if choice == "no":
        await query.edit_message_text(
            f"Cancelled — nothing was deleted.\n\n{TASK_TIP}",
            parse_mode="Markdown",
        )
        return

    if step == "1":
        # First "yes" — ask again before touching anything.
        buttons = [
            [
                InlineKeyboardButton("⚠️ Yes, I'm sure — delete it all", callback_data="wipe:2:yes"),
                InlineKeyboardButton("Cancel", callback_data="wipe:2:no"),
            ]
        ]
        await query.edit_message_text(
            "Really sure? This *cannot be undone* — every task in this chat will be "
            "gone for good.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    # step == "2" and choice == "yes" -> actually delete now
    conn = get_conn()
    cur = conn.execute("DELETE FROM tasks WHERE chat_id = %s", (chat_id,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    await query.edit_message_text(
        f"🗑 Deleted all {deleted} task(s).\n\n"
        f"_Send ✍️ /add to add a task,\nor ❓ /help for more instructions._",
        parse_mode="Markdown",
    )


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: /broadcast <message> — sends <message> to every chat
    that has ever talked to the bot (see known_chats / record_known_chat).
    Requires ADMIN_USER_ID to be set and match the sender; otherwise this
    behaves as if the command doesn't exist at all, rather than revealing
    an admin-only feature to everyone else. Shows a preview and asks for
    confirmation first, since a broadcast can't be unsent once it's out."""
    if ADMIN_USER_ID is None or update.effective_user.id != ADMIN_USER_ID:
        return

    text = update.message.text.partition(" ")[2].strip()
    if not text:
        await update.message.reply_text(
            "Usage: `/broadcast Your message here`\n\n"
            "Markdown formatting is supported and sent exactly as written.",
            parse_mode="Markdown",
        )
        return

    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) AS c FROM known_chats").fetchone()["c"]
    conn.close()
    if count == 0:
        await update.message.reply_text("No known chats to broadcast to yet.")
        return

    context.user_data["pending_broadcast"] = text
    buttons = [
        [
            InlineKeyboardButton("📣 Yes, send it", callback_data="broadcast:yes"),
            InlineKeyboardButton("Cancel", callback_data="broadcast:no"),
        ]
    ]
    keyboard = InlineKeyboardMarkup(buttons)
    try:
        await update.message.reply_text(
            f"*Preview* — this is exactly what will be sent:\n\n{text}\n\n"
            f"Send this to all {count} known chat(s)?",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        context.user_data["pending_broadcast_plain"] = False
    except BadRequest as exc:
        if "parse" not in str(exc).lower():
            raise
        # Unbalanced * _ ` or [ in the admin's text. Rather than failing
        # silently (or, worse, sending a broadcast that every chat rejects),
        # preview — and later send — it as plain text, and say so.
        context.user_data["pending_broadcast_plain"] = True
        await update.message.reply_text(
            "⚠️ Preview — Telegram couldn't read the formatting in your message "
            "(an unmatched * _ ` or [ ), so it will be sent as PLAIN text with no "
            f"bold/italics. This is exactly what will be sent:\n\n{text}\n\n"
            f"Send this to all {count} known chat(s)?",
            reply_markup=keyboard,
        )


async def _send_broadcast_one(bot, chat_id, text, plain: bool):
    """Sends one broadcast message. Uses Markdown unless the preview already
    found it unparseable (or Telegram rejects it for that reason here), in
    which case it goes out as plain text. Waits out Telegram's flood limit
    (RetryAfter) a couple of times instead of counting it as a failure."""
    for attempt in range(3):
        try:
            if not plain:
                try:
                    await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
                    return
                except BadRequest as exc:
                    if "parse" not in str(exc).lower():
                        raise
            await bot.send_message(chat_id=chat_id, text=text)
            return
        except RetryAfter as exc:
            delay = exc.retry_after
            delay = delay.total_seconds() if hasattr(delay, "total_seconds") else float(delay)
            if attempt == 2:
                raise
            await asyncio.sleep(min(delay, 30) + 0.5)


async def broadcast_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if ADMIN_USER_ID is None or query.from_user.id != ADMIN_USER_ID:
        return

    choice = query.data.split(":")[1]
    if choice == "no":
        context.user_data.pop("pending_broadcast", None)
        context.user_data.pop("pending_broadcast_plain", None)
        await query.edit_message_text("Broadcast cancelled — nothing was sent.")
        return

    text = context.user_data.pop("pending_broadcast", None)
    plain = context.user_data.pop("pending_broadcast_plain", False)
    if not text:
        await query.edit_message_text(
            "That broadcast preview has expired — send /broadcast again."
        )
        return

    conn = get_conn()
    chat_ids = [
        row["chat_id"] for row in conn.execute("SELECT chat_id FROM known_chats").fetchall()
    ]
    conn.close()

    await query.edit_message_text(f"Sending to {len(chat_ids)} chat(s)...")
    sent = 0
    failed = 0
    for chat_id in chat_ids:
        try:
            await _send_broadcast_one(context.bot, chat_id, text, plain)
            sent += 1
        except Exception as exc:
            # Most commonly: they blocked the bot, or deleted the chat.
            # Not worth surfacing individually — just tally it.
            logger.info("Broadcast to chat %s failed: %s", chat_id, exc)
            failed += 1
        await asyncio.sleep(0.05)  # stay comfortably under Telegram's rate limits

    await query.message.reply_text(
        f"Broadcast done — sent to {sent}, failed for {failed}."
    )


async def reset_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Asks for a single confirmation before rescheduling every task from now."""
    chat_id = update.effective_chat.id
    conn = get_conn()
    count = conn.execute(
        "SELECT COUNT(*) AS c FROM tasks WHERE chat_id = %s", (chat_id,)
    ).fetchone()["c"]
    conn.close()

    if count == 0:
        await update.message.reply_text("You don't have any tasks to reset.")
        return

    buttons = [
        [
            InlineKeyboardButton("🔄 Yes, reset all", callback_data="reset:yes"),
            InlineKeyboardButton("Cancel", callback_data="reset:no"),
        ]
    ]
    await update.message.reply_text(
        f"This will mark all {count} task(s) as done right now: Simple, "
        f"No-reminder, and Nagging tasks reschedule, one-time reminders get "
        f"removed, and old-school tasks are left alone (they reschedule on "
        f"their own). Continue?",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def reset_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Applies 'done' to every task in this chat, as if you'd just finished
    each one — type-aware, so it behaves consistently with a single /done."""
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    _, choice = query.data.split(":")

    if choice == "no":
        await query.edit_message_text(
            f"Cancelled — nothing was reset.\n\n{TASK_TIP}",
            parse_mode="Markdown",
        )
        return

    conn = get_conn()
    tasks = conn.execute("SELECT * FROM tasks WHERE chat_id = %s", (chat_id,)).fetchall()

    if not tasks:
        conn.close()
        await query.edit_message_text("You don't have any tasks to reset.")
        return

    reset_lines = []
    skipped_names = []
    for t in tasks:
        outcome = apply_done_to_task(conn, t)
        if outcome.get("skipped"):
            skipped_names.append(t["name"])
        elif outcome.get("deleted"):
            reset_lines.append(f"• {bold(t['name'])} — done, removed (one-time reminder)")
        else:
            reset_lines.append(f"• {bold(t['name'])} — next due in {format_duration(t['interval_seconds'])}")
    conn.commit()
    conn.close()

    parts = [f"🔄 Reset {len(reset_lines)} task(s), counting from right now:"] + reset_lines
    if skipped_names:
        parts.append("")
        parts.append(
            "Old-school tasks aren't affected by /reset (they reschedule "
            "on their own): " + ", ".join(skipped_names)
        )
    parts.append("")
    parts.append(TASK_TIP)

    await query.edit_message_text("\n".join(parts), parse_mode="Markdown")


NOTIFY_CHECK_INTERVAL_SECONDS = 60

# Quick-pick snooze options shown on every nag: (button label, seconds)
NAG_SNOOZE_OPTIONS = [("15m", 15 * 60), ("1h", 60 * 60), ("8h", 8 * 60 * 60)]


# check_due_tasks is triggered from three independent places (the 60-second
# in-process timer, every webhook, and /tick). Each run reads the task list,
# sends messages, and only THEN records that it did. Without serialization,
# two overlapping runs — very likely right after a cold start, when a wake-up
# ping and the timer land together — both see the same task as un-notified
# and both send it. The lock makes the second run wait, then re-read the
# database and see the first run's bookkeeping. (One process = one lock is
# enough here; Render's free tier runs a single instance.)
_check_lock = asyncio.Lock()


async def check_due_tasks(context: ContextTypes.DEFAULT_TYPE):
    async with _check_lock:
        await _check_due_tasks_locked(context)


async def _check_due_tasks_locked(context: ContextTypes.DEFAULT_TYPE):
    """Runs every NOTIFY_CHECK_INTERVAL_SECONDS. Proactively messages chats
    about due tasks — behavior depends on each task's type. 'no_reminder' (No
    reminder) tasks are intentionally excluded: they're tracked in /status
    only and never push a notification — use Simple or Nagging for that."""
    now = now_utc()
    conn = get_conn()
    tasks = conn.execute("SELECT * FROM tasks").fetchall()
    conn.close()

    for t in tasks:
        task_type = t["task_type"] or "no_reminder"
        try:
            if task_type == "simple":
                await _check_simple(context, t, now)
            elif task_type == "one_time":
                await _check_one_time(context, t, now)
            elif task_type == "old_school":
                await _check_old_school(context, t, now)
            elif task_type == "nagging":
                await _check_nagging(context, t, now)
        except Exception:
            logger.exception("Error checking task %s (type=%s)", t["id"], task_type)


async def _check_simple(context, t, now):
    """Single notification when due, no follow-up until /done."""
    if t["notified_at"] or from_iso(t["due_at"]) > now:
        return
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Done", callback_data=f"done:{t['id']}")]]
    )
    try:
        await context.bot.send_message(
            chat_id=t["chat_id"],
            text=f"🔔 {bold(t['name'])} is due.",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
    except Exception:
        logger.exception("Failed to notify chat %s about task %s — retrying next cycle.", t["chat_id"], t["id"])
        return
    conn = get_conn()
    conn.execute("UPDATE tasks SET notified_at = %s WHERE id = %s", (to_iso(now), t["id"]))
    conn.commit()
    conn.close()


async def _check_one_time(context, t, now):
    """Single notification, then the task is gone — it served its purpose."""
    if from_iso(t["due_at"]) > now:
        return
    try:
        await context.bot.send_message(
            chat_id=t["chat_id"],
            text=f"⏰ {bold(t['name'])} — reminder!",
            parse_mode="Markdown",
        )
    except Exception:
        logger.exception("Failed to send one-time reminder for task %s — retrying next cycle.", t["id"])
        return
    conn = get_conn()
    conn.execute("DELETE FROM tasks WHERE id = %s", (t["id"],))
    conn.commit()
    conn.close()


async def _check_old_school(context, t, now):
    """Notifies once, then immediately advances to the next fixed occurrence
    regardless of whether the user ever responds — /done has no effect on
    this schedule (see apply_done_to_task)."""
    if from_iso(t["due_at"]) > now:
        return
    try:
        await context.bot.send_message(
            chat_id=t["chat_id"],
            text=f"📅 {bold(t['name'])} is due.",
            parse_mode="Markdown",
        )
    except Exception:
        logger.exception("Failed to send old-school reminder for task %s — retrying next cycle.", t["id"])
        return
    tz = get_chat_tzinfo(t["chat_id"])
    lh, lm = old_school_local_time(t, tz)
    next_due = next_old_school_due(
        from_iso(t["due_at"]), lh, lm, t["interval_unit"], t["interval_amount"], tz
    )
    # If the service was asleep across several occurrences, don't replay each
    # missed one as its own message, one per minute — the single notification
    # just sent covers them; jump ahead to the first occurrence still in the
    # future. (Bounded so a corrupt row can never spin forever.)
    for _ in range(1000):
        if next_due > now:
            break
        next_due = next_old_school_due(
            next_due, lh, lm, t["interval_unit"], t["interval_amount"], tz
        )
    conn = get_conn()
    conn.execute("UPDATE tasks SET due_at = %s WHERE id = %s", (to_iso(next_due), t["id"]))
    conn.commit()
    conn.close()


def nag_keyboard(task_id):
    snooze_row = [
        InlineKeyboardButton(f"⏰ {label}", callback_data=f"nag_snooze:{task_id}:{seconds}")
        for label, seconds in NAG_SNOOZE_OPTIONS
    ]
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Done", callback_data=f"done:{task_id}")],
            snooze_row,
            [InlineKeyboardButton("✏️ Custom delay", callback_data=f"nag_custom:{task_id}")],
        ]
    )


def first_nag_moment(due: datetime, lead_seconds, now: datetime) -> datetime:
    """When a freshly (re)scheduled nagging task should next speak.

    The lead time is a ONE-OFF early reminder at (due - lead). The repeated
    nagging only starts at the real due time. So the first thing to happen is
    the early reminder — unless that moment has already passed (the lead is
    longer than the time left, e.g. right after Done), in which case there is
    no point sending a heads-up late: the next thing is the due time itself."""
    early = due - timedelta(seconds=lead_seconds or 0)
    return early if early > now else due


async def _check_nagging(context, t, now):
    """Two phases, both driven by next_nag_at alone (no extra state):

      * BEFORE the due time: a single early reminder ("is due in X") when
        next_nag_at (= due - lead time) arrives, after which next_nag_at jumps
        to the due time itself. Nothing more is sent until then.
      * FROM the due time on: "is due!" and then again every nag_interval
        until /done (or until the person snoozes it).

    Which phase we're in is simply whether the due time is still ahead, so a
    service that slept past the due time never sends a stale 'is due in X'."""
    if not t["next_nag_at"] or from_iso(t["next_nag_at"]) > now:
        return
    due = from_iso(t["due_at"])
    remaining = (due - now).total_seconds()
    early_reminder = remaining > 0
    due_text = f"is due in {format_duration(remaining)}" if early_reminder else "is due!"
    try:
        await context.bot.send_message(
            chat_id=t["chat_id"],
            text=f"🔔 {bold(t['name'])} {due_text}",
            parse_mode="Markdown",
            reply_markup=nag_keyboard(t["id"]),
        )
    except Exception:
        logger.exception("Failed to send nag for task %s — retrying next cycle.", t["id"])
        return
    # After the early reminder, stay quiet until the real due time; after a
    # real nag, wait one nag interval.
    next_nag = due if early_reminder else now + timedelta(seconds=t["nag_interval_seconds"])
    conn = get_conn()
    conn.execute("UPDATE tasks SET next_nag_at = %s WHERE id = %s", (to_iso(next_nag), t["id"]))
    conn.commit()
    conn.close()


async def nag_snooze_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """One of the 15m/1h/8h quick-pick buttons on a nag."""
    query = update.callback_query
    await query.answer()
    _, task_id_str, seconds_str = query.data.split(":")
    task_id, seconds = int(task_id_str), int(seconds_str)
    chat_id = query.message.chat_id

    conn = get_conn()
    task = conn.execute(
        "SELECT * FROM tasks WHERE id = %s AND chat_id = %s", (task_id, chat_id)
    ).fetchone()
    if not task:
        conn.close()
        await query.edit_message_text("That task no longer exists.")
        return
    next_nag = now_utc() + timedelta(seconds=seconds)
    conn.execute("UPDATE tasks SET next_nag_at = %s WHERE id = %s", (to_iso(next_nag), task_id))
    conn.commit()
    conn.close()
    await query.edit_message_text(
        f"⏰ Snoozed {bold(task['name'])} — I'll remind you again in {format_duration(seconds)}.",
        parse_mode="Markdown",
    )


async def nag_custom_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """'Custom delay' on a nag — asks for a free-text duration, same parser
    as everywhere else (e.g. 45m, 3h, 2d)."""
    query = update.callback_query
    await query.answer()
    task_id = int(query.data.split(":", 1)[1])
    chat_id = query.message.chat_id

    conn = get_conn()
    task = conn.execute(
        "SELECT * FROM tasks WHERE id = %s AND chat_id = %s", (task_id, chat_id)
    ).fetchone()
    conn.close()
    if not task:
        await query.edit_message_text("That task no longer exists.")
        return

    _clear_lightweight_reply_flags(context)
    context.user_data["awaiting_nag_snooze_for"] = task_id
    await query.edit_message_text(
        f"How much later should I remind you about {bold(task['name'])}? "
        f"(e.g. 45m, 3h, 2d)",
        parse_mode="Markdown",
    )


async def maybe_handle_nag_snooze_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Catches free-text replies for lightweight, single-question flows
    that don't need a full ConversationHandler: a nag's 'Custom delay'
    button, and /timezone's bare (no-argument) form. Also where a stale,
    timed-out /add session gets mentioned — reactively, only if the next
    thing typed looks like an attempt to continue it (see
    add_conversation_timeout). A no-op for every other plain-text message
    (nothing else currently expects arbitrary top-level plain text once
    no /add conversation is active, so this is safe to register as a
    global catch-all)."""
    if context.user_data.pop("add_session_timed_out", None):
        await update.message.reply_text(
            f"Your last /add session timed out after 5 minutes of "
            f"inactivity — no task was added.\n\n"
            f"_Send ✍️ /add to start over,\n"
            f"📊 /status to see all your tasks,\n"
            f"🗑️ /delete to remove a task,\n"
            f"or ❓ /help for more instructions._",
            parse_mode="Markdown",
        )
        return

    if context.user_data.get("awaiting_timezone_reply"):
        text = update.message.text.strip()
        if text == TIMEZONE_SKIP_TEXT:
            context.user_data.pop("awaiting_timezone_reply", None)
            chat_id = update.effective_chat.id
            set_chat_timezone(chat_id, 0)
            await update.message.reply_text(
                f"Saved — using UTC.\n\n{TASK_TIP}",
                parse_mode="Markdown",
                reply_markup=MAIN_MENU_KEYBOARD,
            )
            return
        chat_id = update.effective_chat.id
        saved = await _resolve_and_save_timezone(update.message, chat_id, text)
        if saved:
            context.user_data.pop("awaiting_timezone_reply", None)
        return  # keep waiting for a valid reply if not saved

    task_id = context.user_data.get("awaiting_nag_snooze_for")
    if not task_id:
        return

    seconds = parse_interval(update.message.text.strip())
    if seconds is None:
        await update.message.reply_text(
            "I couldn't understand that. Try something like 45m, 3h, or 2d."
        )
        return  # keep waiting for a valid reply

    context.user_data.pop("awaiting_nag_snooze_for", None)
    conn = get_conn()
    task = conn.execute("SELECT * FROM tasks WHERE id = %s", (task_id,)).fetchone()
    if not task:
        conn.close()
        await update.message.reply_text("That task no longer exists.")
        return
    next_nag = now_utc() + timedelta(seconds=seconds)
    conn.execute("UPDATE tasks SET next_nag_at = %s WHERE id = %s", (to_iso(next_nag), task_id))
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"⏰ Snoozed {bold(task['name'])} — I'll remind you again in {format_duration(seconds)}.",
        parse_mode="Markdown",
    )


async def cancel_pending_text_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Global /cancel — bails out of the lightweight, non-/add free-text
    prompts above (a nag's 'Custom delay' and /timezone's bare form),
    which otherwise have no way to be interrupted since they aren't part
    of a ConversationHandler. A no-op if neither is pending — add_conv has
    its own /cancel (add_cancel) for the actual /add wizard, and that one
    takes priority automatically whenever a conversation is active."""
    had_pending = False
    if context.user_data.pop("awaiting_timezone_reply", None):
        had_pending = True
    if context.user_data.pop("awaiting_nag_snooze_for", None):
        had_pending = True
    if had_pending:
        await update.message.reply_text(f"Cancelled.\n\n{TASK_TIP}", parse_mode="Markdown")


async def post_init(application: Application):
    # This populates the "/" menu button next to the text box in Telegram.
    await application.bot.set_my_commands(
        [
            BotCommand("add", "✍️ Add new task"),
            BotCommand("done", "✅ Mark a task done"),
            BotCommand("status", "📊 Show task status"),
            BotCommand("details", "📋 Show full task details"),
            BotCommand("edit", "✏️ Edit a task"),
            BotCommand("delete", "🗑️ Delete a task"),
            BotCommand("reset", "🔄 Reset all tasks"),
            BotCommand("wipe", "🧹 Delete all tasks"),
            BotCommand("timezone", "🌍 View or change your time zone"),
            BotCommand("cancel", "❌ Cancel add task"),
            BotCommand("help", "❓ Show help"),
        ]
    )


def build_application() -> Application:
    """Builds and registers every handler. Identical to the polling version
    of this bot, including the same in-process JobQueue check_due_tasks
    poller (see run()) — /tick and the webhook's opportunistic check are
    what wake this service back up from asleep, not what drive checks
    while it's already running."""
    application = Application.builder().token(TOKEN).post_init(post_init).build()

    # Runs before every other handler, for every update, regardless of what
    # it is — see record_known_chat for why.
    application.add_handler(TypeHandler(Update, record_known_chat), group=-1)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))

    add_conv = ConversationHandler(
        entry_points=[
            CommandHandler("add", add_start),
            MessageHandler(filters.Regex(MENU_ADD_PATTERN), add_start_from_menu),
        ],
        states={
            ADD_TYPE: [CallbackQueryHandler(add_type_chosen, pattern=r"^addtype:")],
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name_received)],
            ADD_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_interval_received)],
            ADD_FIRST_DUE: [
                CallbackQueryHandler(first_due_skip_button, pattern=r"^skip:firstdue$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_first_due_received),
            ],
            ADD_ONE_TIME_DELAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_one_time_delay_received)
            ],
            ADD_TIMEZONE: [
                MessageHandler(filters.LOCATION, add_timezone_location_received),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_timezone_received),
            ],
            ADD_OLD_SCHOOL_HOUR: [
                CallbackQueryHandler(old_school_hour_skip_button, pattern=r"^skip:oshour$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_old_school_hour_received),
            ],
            ADD_OLD_SCHOOL_DATE: [
                CallbackQueryHandler(old_school_date_button, pattern=r"^osdate:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_old_school_date_received),
            ],
            ADD_OLD_SCHOOL_INTERVAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_old_school_interval_received)
            ],
            ADD_NAGGING_INTERVAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_nagging_interval_received)
            ],
            ADD_NAGGING_LEAD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_nagging_lead_received)
            ],
            ADD_NAGGING_CADENCE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_nagging_cadence_received)
            ],
            ConversationHandler.TIMEOUT: [TypeHandler(Update, add_conversation_timeout)],
        },
        # These apply no matter which state above is active, so a menu tap,
        # another command, or /cancel always cleanly ends a step-by-step
        # /add instead of leaving it stuck (see add_other_command_interrupt).
        fallbacks=[
            CommandHandler("cancel", add_cancel),
            CommandHandler("add", add_start),
            MessageHandler(filters.Regex(MENU_ADD_PATTERN), add_start_from_menu),
            MessageHandler(filters.Regex(MENU_STATUS_OR_DONE_PATTERN), menu_interrupt),
            CommandHandler(
                [
                    "status", "list", "details", "edit", "done", "delete",
                    "reset", "wipe", "help", "start", "timezone",
                ],
                add_other_command_interrupt,
            ),
        ],
        conversation_timeout=300,  # abandon a half-finished /add after 5 min of silence
    )
    application.add_handler(add_conv)
    # Stale/expired-conversation fallbacks for every button pattern used
    # anywhere in the /add flow — registered outside add_conv, so each
    # only ever fires when the in-state handler didn't claim the tap
    # first (i.e. the conversation that button belonged to has already
    # ended, whether from /cancel, being superseded by a newer /add, or
    # the 5-minute timeout). See add_flow_stale_button.
    application.add_handler(CallbackQueryHandler(add_flow_stale_button, pattern=r"^addtype:"))
    application.add_handler(
        CallbackQueryHandler(add_flow_stale_button, pattern=r"^skip:firstdue$")
    )
    application.add_handler(
        CallbackQueryHandler(add_flow_stale_button, pattern=r"^skip:oshour$")
    )
    application.add_handler(CallbackQueryHandler(add_flow_stale_button, pattern=r"^osdate:"))

    edit_conv = ConversationHandler(
        entry_points=[CommandHandler("edit", edit_start)],
        states={
            EDIT_PICK_TASK: [CallbackQueryHandler(edit_pick_button, pattern=r"^editpick:")],
            EDIT_PICK_FIELD: [CallbackQueryHandler(edit_field_button, pattern=r"^editfield:")],
            EDIT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_name_received)],
            EDIT_INTERVAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_interval_received)
            ],
            EDIT_LEAD_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_lead_received)
            ],
            EDIT_NAG_CADENCE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_nag_cadence_received)
            ],
            EDIT_OS_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_os_time_received)
            ],
            EDIT_OS_CADENCE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_os_cadence_received)
            ],
            EDIT_ONE_TIME_DELAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_delay_received)
            ],
            ConversationHandler.TIMEOUT: [TypeHandler(Update, edit_conversation_timeout)],
        },
        # Mirrors add_conv's fallbacks: /cancel, or any other top-level
        # command (including /add itself) cleanly ends a step-by-step
        # /edit instead of leaving it stuck.
        fallbacks=[
            CommandHandler("cancel", edit_cancel),
            CommandHandler(
                [
                    "add", "status", "list", "details", "done", "delete",
                    "reset", "wipe", "help", "start", "timezone",
                ],
                edit_other_command_interrupt,
            ),
        ],
        conversation_timeout=300,
    )
    application.add_handler(edit_conv)
    # Stale/expired-conversation fallbacks for /edit's button patterns —
    # same reasoning as add_flow_stale_button above.
    application.add_handler(CallbackQueryHandler(edit_flow_stale_button, pattern=r"^editpick:"))
    application.add_handler(
        CallbackQueryHandler(edit_flow_stale_button, pattern=r"^editfield:")
    )

    # Persistent menu buttons, active when no /add conversation is in progress
    # (the mid-conversation taps are handled inside add_conv's fallbacks above).
    application.add_handler(MessageHandler(filters.Regex(MENU_STATUS_PATTERN), status))
    application.add_handler(MessageHandler(filters.Regex(MENU_DONE_PATTERN), menu_done_standalone))

    application.add_handler(CommandHandler("done", done_task))
    application.add_handler(CommandHandler("delete", delete_task))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("list", status))
    application.add_handler(CommandHandler("details", details_cmd))
    application.add_handler(CommandHandler("reset", reset_start))
    application.add_handler(CommandHandler("wipe", wipe_start))
    application.add_handler(CommandHandler("broadcast", broadcast_cmd))
    application.add_handler(CallbackQueryHandler(broadcast_button, pattern=r"^broadcast:"))
    application.add_handler(CommandHandler("timezone", timezone_cmd))
    # Global /cancel for the lightweight (non-/add) free-text prompts above
    # — a nag's 'Custom delay' and /timezone's bare form. add_conv's own
    # /cancel (in its fallbacks) always takes priority while a /add is
    # actually active, since that ConversationHandler is registered first.
    application.add_handler(CommandHandler("cancel", cancel_pending_text_reply))
    application.add_handler(CallbackQueryHandler(done_button, pattern=r"^done:"))
    application.add_handler(CallbackQueryHandler(delete_ask_button, pattern=r"^delask:"))
    application.add_handler(CallbackQueryHandler(delete_confirm_button, pattern=r"^del(yes|no):"))
    application.add_handler(CallbackQueryHandler(wipe_button, pattern=r"^wipe:"))
    application.add_handler(CallbackQueryHandler(reset_button, pattern=r"^reset:"))
    application.add_handler(CallbackQueryHandler(nag_snooze_button, pattern=r"^nag_snooze:"))
    application.add_handler(CallbackQueryHandler(nag_custom_button, pattern=r"^nag_custom:"))
    # Catch-all for a shared location answering the bare /timezone form
    # (the in-/add-flow case is handled inside add_conv's own state
    # instead — see ADD_TIMEZONE above). filters.LOCATION never overlaps
    # with the text catch-all below, so registration order between them
    # doesn't matter.
    application.add_handler(MessageHandler(filters.LOCATION, maybe_handle_location_reply))
    # Catch-all for the free-text reply after 'Custom delay' on a nag, or
    # after the bare /timezone form. Must stay last so it never preempts
    # the more specific handlers above.
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, maybe_handle_nag_snooze_reply)
    )

    return application


# ---------------------------------------------------------------------------
# Web server (replaces run_polling): one route receives Telegram's webhook,
# one route is hit by an external cron pinger to trigger a due-task check
# while the service is otherwise asleep, one is a plain health check.
# ---------------------------------------------------------------------------

PORT = int(os.environ.get("PORT", "8080"))
BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
WEBHOOK_SECRET_TOKEN = os.environ.get("WEBHOOK_SECRET_TOKEN", "")
TICK_SECRET = os.environ.get("TICK_SECRET", "")
WEBHOOK_PATH = f"/webhook/{TOKEN}" if TOKEN else "/webhook"


# Typed key for the shared Application (aiohttp warns on bare string keys).
PTB_APP_KEY = web.AppKey("ptb_application", Application)


async def handle_webhook(request):
    supplied = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
    if not WEBHOOK_SECRET_TOKEN or not hmac.compare_digest(supplied.encode(), WEBHOOK_SECRET_TOKEN.encode()):
        return web.Response(status=401, text="unauthorized")

    application = request.app[PTB_APP_KEY]
    try:
        data = await request.json()
        update = Update.de_json(data, application.bot)
        await application.process_update(update)
    except Exception:
        # Always answer 200: a non-2xx makes Telegram redeliver the same
        # update, which would repeat a command that may have half-run.
        logger.exception("Error while processing a webhook update")

    # Opportunistic check: while someone's actively chatting with the bot,
    # it's already awake, so piggyback a due-check here too — but in the
    # background, so Telegram gets its 200 immediately instead of waiting on
    # (possibly many) outgoing notifications.
    _spawn_background(_safe_check(application))

    return web.Response(status=200, text="ok")


_background_tasks = set()


def _spawn_background(coro):
    # Keep a reference: the event loop only holds weak references to tasks,
    # so a fire-and-forget task can otherwise be garbage-collected mid-run.
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _safe_check(application):
    try:
        await check_due_tasks(application)
    except Exception:
        logger.exception("Opportunistic check_due_tasks failed")


async def handle_tick(request):
    supplied = request.query.get("key", "")
    if not TICK_SECRET or not hmac.compare_digest(supplied.encode(), TICK_SECRET.encode()):
        return web.Response(status=401, text="unauthorized")
    application = request.app[PTB_APP_KEY]
    await check_due_tasks(application)
    return web.Response(status=200, text="ok")


async def handle_health(request):
    return web.Response(status=200, text="task-bot is running")


def build_web_app(application: Application) -> web.Application:
    app = web.Application()
    app[PTB_APP_KEY] = application
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/tick", handle_tick)
    app.router.add_post("/tick", handle_tick)
    app.router.add_get("/", handle_health)
    return app


async def run():
    missing = [
        name
        for name, val in [
            ("TELEGRAM_BOT_TOKEN", TOKEN),
            ("DATABASE_URL", DATABASE_URL),
        ]
        if not val
    ]
    if missing:
        raise SystemExit(f"Missing required environment variable(s): {', '.join(missing)}")

    init_db()

    application = build_application()
    await application.initialize()
    await application.start()  # required for JobQueue (conversation_timeout) to run
    await post_init(application)

    # Also check for due tasks on a plain in-process timer, not just when a
    # webhook/tick request happens to arrive — otherwise, if a task falls
    # due while the service is genuinely awake (e.g. mid-conversation) but
    # nothing else triggers a check before the next /tick ping, it silently
    # waits for that ping instead of firing right away. This only runs
    # while the process is already alive; it doesn't keep Render from
    # spinning the service down on its own idle timer, since Render's idle
    # detection is based on incoming HTTP traffic, not in-process activity —
    # /tick and the webhook's opportunistic check remain what wake it back
    # up. Mirrors Railway's polling loop exactly (see NOTIFY_CHECK_INTERVAL_SECONDS).
    application.job_queue.run_repeating(
        check_due_tasks, interval=NOTIFY_CHECK_INTERVAL_SECONDS, first=10
    )

    web_app = build_web_app(application)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info("Listening on port %s", PORT)

    if BASE_URL and WEBHOOK_SECRET_TOKEN and TICK_SECRET:
        webhook_url = f"{BASE_URL}{WEBHOOK_PATH}"
        await application.bot.set_webhook(url=webhook_url, secret_token=WEBHOOK_SECRET_TOKEN)
        logger.info("Webhook registered at %s", webhook_url)
    else:
        logger.warning(
            "BASE_URL, WEBHOOK_SECRET_TOKEN, and/or TICK_SECRET not set yet — "
            "server is running and reachable (so Render will show it as live "
            "and give you a URL), but the Telegram webhook has NOT been "
            "registered. Set those three variables once you know this "
            "service's public URL, then redeploy."
        )

    await asyncio.Event().wait()  # run forever


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
