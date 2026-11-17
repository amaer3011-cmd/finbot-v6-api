"""بوت تيليجرام — حل منصة الثقافة المالية (v6: شريط تقدم محسّن + إحصائيات)."""
import asyncio
import html
import io
import logging
import os
import random
import threading
import time
from collections import deque
from pathlib import Path

from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter, TelegramError
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

import api_engine as core
import db
from verses import VERSES

BASE         = Path(__file__).resolve().parent
BOT_TOKEN    = os.getenv("BOT_TOKEN", "")
DEV_USERNAME = os.getenv("DEV_USERNAME", "A7med19_7").lstrip("@")


def _int_setting(name, default, minimum):
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer.") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be at least {minimum}.")
    return value


MAX_JOBS        = _int_setting("MAX_JOBS",        1,   1)
MAX_QUEUE       = _int_setting("MAX_QUEUE",       10,  0)
JOB_TIMEOUT_MIN = _int_setting("JOB_TIMEOUT_MIN", 240, 1)
OWNER_CHAT_ID   = _int_setting("OWNER_CHAT_ID",   0,   0)
ANSWERS_FILE    = BASE / os.getenv("ANSWERS_FILE", "answers.txt")
ASSETS_DIR      = BASE / "assets"
WELCOME_IMAGES  = sorted(ASSETS_DIR.glob("welcome_*.jpg"))
_FILE_IDS       = {}

logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("finbot")

_AR_DIGITS = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")


# ═══════════════════════════════════════════════════════
#  أدوات تنسيق الأرقام والوقت
# ═══════════════════════════════════════════════════════

def ar_num(n):
    return f"{n:,}".replace(",", "٬").translate(_AR_DIGITS)

def ar_num_two_digits(n):
    return ar_num(n).rjust(2, "٠")

def ar_percent(done, total):
    if total <= 0:
        return "٠٪"
    pct = max(0, min(100, int(done * 100 / total)))
    return ar_num(pct) + "٪"

def progress_bar(done, total, width=10):
    if width <= 0:
        return ""
    if total <= 0:
        return "░" * width
    filled = max(0, min(width, round(width * done / total)))
    return "█" * filled + "░" * (width - filled)

def format_duration(seconds):
    seconds = max(0, int(seconds))
    hours, rem  = divmod(seconds, 3600)
    minutes, s  = divmod(rem, 60)
    if hours:
        return f"{ar_num(hours)}س {ar_num_two_digits(minutes)}د {ar_num_two_digits(s)}ث"
    if minutes:
        return f"{ar_num(minutes)}د {ar_num_two_digits(s)}ث"
    return f"{ar_num(seconds)}ث"

def fmt_ts(ts):
    """Unix timestamp → تاريخ قابل للقراءة."""
    if not ts:
        return "—"
    import datetime
    dt = datetime.datetime.fromtimestamp(ts)
    return dt.strftime("%Y/%m/%d %H:%M")


# ═══════════════════════════════════════════════════════
#  رسائل الملخص
# ═══════════════════════════════════════════════════════

def job_summary(questions, activities, elapsed, platform_progress=None):
    progress_line = ""
    if (
        isinstance(platform_progress, tuple)
        and len(platform_progress) == 2
        and platform_progress[1] > 0
    ):
        done, total = platform_progress
        done = max(0, min(done, total))
        bar  = progress_bar(done, total, 12)
        progress_line = (
            f"📊 آخر تقدم مؤكد: <code>{bar}</code> <b>{ar_percent(done, total)}</b> "
            f"({ar_num(done)}/{ar_num(total)})\n"
        )
    if activities is None:
        activity_line = "📚 العناصر المسجلة: غير مؤكدة\n"
    else:
        activity_line = f"📚 عناصر أُكملت: <b>{ar_num(activities)}</b>\n"
    return (
        progress_line
        + f"📝 أسئلة أُجيبت: <b>{ar_num(questions)}</b>\n"
        + activity_line
        + f"⏱️ إجمالي الوقت: <b>{format_duration(elapsed)}</b>"
    )


def completion_message(questions, activities, elapsed, last, platform_progress=None):
    return (
        "┌─────────────────────┐\n"
        "│  🎉 <b>اكتملت المهمة بنجاح</b>  │\n"
        "└─────────────────────┘\n\n"
        "<i>لم يتبق نشاط مفتوح متاح للمتابعة.</i>\n"
        "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"{job_summary(questions, activities, elapsed, platform_progress)}\n"
        f"📍 آخر نشاط: <b>{html.escape(last or '—')}</b>\n"
        "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        "<i>قد تظل أنشطة مقفولة بانتظار فتحها من المنصة.</i>"
    )


# ═══════════════════════════════════════════════════════
#  منطق الطابور
# ═══════════════════════════════════════════════════════

def queue_full(app):
    return len(app.bot_data["cancels"]) >= MAX_JOBS + MAX_QUEUE


async def acquire_job_slot(semaphore, cancel, poll_interval=0.25):
    while not cancel.is_set():
        try:
            acquired = await asyncio.wait_for(semaphore.acquire(), timeout=poll_interval)
        except asyncio.TimeoutError:
            continue
        if cancel.is_set():
            semaphore.release()
            return False
        return acquired
    return False


QUEUE_FULL_TEXT = "🚦 البوت مشغول دلوقتي والطابور ممتلئ. جرّب تاني بعد شوية 🌿"


# ═══════════════════════════════════════════════════════
#  أزرار
# ═══════════════════════════════════════════════════════

def btn(text, style=None, **kw):
    if style:
        try:
            return InlineKeyboardButton(text, style=style, **kw)
        except TypeError:
            pass
    return InlineKeyboardButton(text, **kw)


def main_menu(total):
    return InlineKeyboardMarkup([
        [btn("🚀 حل منصة الثقافة المالية", "success", callback_data="solve")],
        [
            btn("📖 طريقة الاستخدام",   "primary", callback_data="help"),
            btn("🔒 الأمان والخصوصية", "primary", callback_data="privacy"),
        ],
        [
            btn("📈 إحصائياتي",         "primary", callback_data="mystats"),
            btn(f"🤍 صلِّ على النبي ﷺ  •  {ar_num(total)}", "primary", callback_data="salat"),
        ],
        [btn(f"Dev: @{DEV_USERNAME}", url=f"https://t.me/{DEV_USERNAME}")],
    ])


def cancel_kb():
    return InlineKeyboardMarkup([[btn("❌ إلغاء", "danger", callback_data="cancel_flow")]])

def report_consent_kb():
    return InlineKeyboardMarkup([
        [btn("✅ موافق لهذه المهمة",  "success", callback_data="report_consent_yes")],
        [btn("لا، بدون تقرير",        "primary", callback_data="report_consent_no")],
        [btn("❌ إلغاء",             "danger",  callback_data="cancel_flow")],
    ])

def stop_kb():
    return InlineKeyboardMarkup([[btn("⛔ إيقاف الحل", "danger", callback_data="stop")]])

def after_kb():
    return InlineKeyboardMarkup([
        [btn("🔁 تشغيل تاني",       "success", callback_data="solve")],
        [btn("📈 إحصائياتي",        "primary", callback_data="mystats")],
        [btn("🏠 القائمة الرئيسية", "primary", callback_data="menu")],
    ])


# ═══════════════════════════════════════════════════════
#  رسائل الترحيب
# ═══════════════════════════════════════════════════════

async def send_welcome(chat, name):
    idx     = random.randrange(len(VERSES))
    v       = VERSES[idx]
    caption = (
        f"✦ ━━━━━━━━━━━━ ✦\n"
        f"<b>أهلاً بيك يا {html.escape(name)}</b>\n\n"
        f"<blockquote>{html.escape(v['shatr1'])}\n{html.escape(v['shatr2'])}\n"
        f"<i>{html.escape(v['poet'])}</i></blockquote>\n\n"
        "⏱️ وقتك أغلى من إنك تقعد تدوس «التالي» ساعات.\n"
        "اضغط الزر اللي تحت وسيب الباقي عليّ 👇\n\n"
        "🤍 <b>صلِّ على النبي ﷺ</b>"
    )
    markup = main_menu(db.get_salawat())
    image  = WELCOME_IMAGES[idx] if idx < len(WELCOME_IMAGES) else None
    if image:
        photo = _FILE_IDS.get(image.name) or image.read_bytes()
        try:
            sent = await chat.send_photo(
                photo, caption=caption, parse_mode=ParseMode.HTML, reply_markup=markup
            )
            _FILE_IDS[image.name] = sent.photo[-1].file_id
            return
        except TelegramError as exc:
            log.warning("welcome photo failed (%s) — fallback to text", exc)
            _FILE_IDS.pop(image.name, None)
    await chat.send_message(caption, parse_mode=ParseMode.HTML, reply_markup=markup)


# ═══════════════════════════════════════════════════════
#  معالجات الأوامر الأساسية
# ═══════════════════════════════════════════════════════

async def on_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.touch_user(user.id, user.full_name)
    ctx.user_data.clear()
    await send_welcome(update.effective_chat, user.first_name or "يا غالي")


async def on_cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    ctx.user_data.clear()
    await update.message.reply_text("تمام، اتلغى. اكتب /start عشان ترجع للقائمة.")


HELP_TEXT = (
    "📖 <b>طريقة الاستخدام</b>\n<i>أربع خطوات وخلاص</i>\n\n"
    "١) اضغط «حل منصة الثقافة المالية».\n"
    "٢) ابعت كود الدخول بتاعك (بيتحفظ عشان متكتبوش كل مرة).\n"
    "٣) ابعت الباسورد.\n"
    "٤) البوت يدخل حسابك ويحل الدروس بالترتيب.\n\n"
    "• الدروس اللي كنت مخلصها قبل كده بيعدّي عليها.\n"
    "• تقدر توقف الحل في أي وقت بزر «إيقاف».\n"
    "• ممكن تقفل تيليجرام — الحل بيكمّل على السيرفر.\n\n"
    "📌 <b>الأوامر المتاحة</b>\n"
    "/start — القائمة الرئيسية\n"
    "/mystats — إحصائياتك الشخصية\n"
    "/cancel — إلغاء أي خطوة جارية"
)

PRIVACY_TEXT = (
    "🔒 <b>الأمان والخصوصية</b>\n<i>شفافية كاملة</i>\n\n"
    "• <b>الكود</b> بيتحفظ عشان متكتبوش كل مرة.\n"
    "• <b>الباسورد</b> بيتستخدم مرة واحدة بس، ومبيتكتبش في أي ملف أو قاعدة بيانات، "
    "وبيتشال من الذاكرة لما الحل يخلص.\n"
    "• البوت بيمسح رسالتك اللي فيها الباسورد أول ما يستلمها.\n"
    "• مع ذلك الباسورد بيعدّي على سيرفر البوت أثناء الدخول.\n"
    "• قبل كل مهمة، تقدر توافق اختياريًا على إرسال تقرير عطل."
)


async def on_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.message.reply_html(HELP_TEXT)

async def on_privacy(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.message.reply_html(PRIVACY_TEXT)

async def on_help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(HELP_TEXT)

async def on_error(update, ctx: ContextTypes.DEFAULT_TYPE):
    error = ctx.error
    if isinstance(error, BadRequest) and "query is too old" in str(error).casefold():
        log.info("ignored expired Telegram callback query")
        return
    log.error("unhandled error", exc_info=error)

async def on_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await send_welcome(q.message.chat, q.from_user.first_name or "يا غالي")


# ═══════════════════════════════════════════════════════
#  📈 الإحصائيات
# ═══════════════════════════════════════════════════════

def _build_mystats_text(uid):
    s = db.get_user_stats(uid)
    if s["sessions"] == 0:
        return (
            "📈 <b>إحصائياتك الشخصية</b>\n\n"
            "لسه ما شغّلتش الحل. اضغط «حل الثقافة المالية» وابدأ! 🚀"
        )

    success_rate = (
        int(s["finished"] * 100 / s["sessions"]) if s["sessions"] else 0
    )
    bar_sessions = min(s["sessions"], 10)
    sessions_bar = "🟩" * bar_sessions + "⬜" * (10 - bar_sessions)

    return (
        "┌─────────────────────┐\n"
        "│    📈 <b>إحصائياتك</b>       │\n"
        "└─────────────────────┘\n\n"
        f"👤 <b>{html.escape(s['name'] or 'مجهول')}</b>\n"
        f"📅 أول جلسة: <i>{fmt_ts(s['first_seen'])}</i>\n"
        f"🕐 آخر جلسة: <i>{fmt_ts(s['last_run'])}</i>\n\n"
        "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        f"🔁 إجمالي الجلسات:  <b>{ar_num(s['sessions'])}</b>\n"
        f"   {sessions_bar}\n"
        f"✅ جلسات مكتملة:   <b>{ar_num(s['finished'])}</b>  "
        f"({ar_num(success_rate)}٪)\n\n"
        f"📝 أسئلة أُجيبت:   <b>{ar_num(s['total_q'])}</b>\n"
        f"📚 أنشطة أُكملت:   <b>{ar_num(s['total_act'])}</b>\n\n"
        f"⏱️ وقت الحل الكلي:  <b>{format_duration(s['total_sec'])}</b>\n"
        f"🏆 أطول جلسة:      <b>{format_duration(s['longest_sec'])}</b>\n"
        "▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
        "🤍 <i>صلِّ على النبي ﷺ وابدأ جلسة جديدة</i>"
    )


async def on_mystats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = _build_mystats_text(uid)
    kb   = InlineKeyboardMarkup([
        [btn("🚀 حل المنصة", "success", callback_data="solve")],
        [btn("🏠 القائمة",   "primary", callback_data="menu")],
    ])
    await update.message.reply_html(text, reply_markup=kb)


async def on_mystats_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    await q.answer()
    uid  = q.from_user.id
    text = _build_mystats_text(uid)
    kb   = InlineKeyboardMarkup([
        [btn("🚀 حل المنصة", "success", callback_data="solve")],
        [btn("🏠 القائمة",   "primary", callback_data="menu")],
    ])
    await q.message.reply_html(text, reply_markup=kb)


async def on_stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """أوامر أدمن فقط — /stats."""
    if OWNER_CHAT_ID and update.effective_user.id != OWNER_CHAT_ID:
        return
    g   = db.get_global_stats()
    top = f"🥇 أكثر مستخدم: <b>{html.escape(g['top_user'][0])}</b> ({ar_num(g['top_user'][1])} سؤال)" if g["top_user"] else ""
    text = (
        "┌─────────────────────┐\n"
        "│  📊 <b>إحصائيات البوت</b>    │\n"
        "└─────────────────────┘\n\n"
        f"👥 إجمالي المستخدمين: <b>{ar_num(g['total_users'])}</b>\n"
        f"🔥 نشطون اليوم:       <b>{ar_num(g['active_today'])}</b>\n"
        f"🔁 إجمالي الجلسات:    <b>{ar_num(g['total_jobs'])}</b>\n"
        f"📝 إجمالي الأسئلة:    <b>{ar_num(g['total_questions'])}</b>\n"
        f"🤍 الصلوات على النبي: <b>{ar_num(g['salawat'])}</b>\n\n"
        f"{top}"
    )
    await update.message.reply_html(text)


# ═══════════════════════════════════════════════════════
#  الصلاة على النبي ﷺ
# ═══════════════════════════════════════════════════════

async def on_salat(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    now = time.monotonic()
    if now - ctx.user_data.get("last_salat", 0) < 0.35:
        await q.answer()
        return
    ctx.user_data["last_salat"] = now
    total = db.add_salawat()
    await q.answer("اللهم صلِّ وسلِّم وبارك على سيدنا محمد ﷺ")
    try:
        await q.edit_message_reply_markup(main_menu(total))
    except (BadRequest, RetryAfter):
        pass


# ═══════════════════════════════════════════════════════
#  تدفق الدخول
# ═══════════════════════════════════════════════════════

async def on_solve(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q   = update.callback_query
    uid = q.from_user.id
    await q.answer()
    db.touch_user(uid, q.from_user.full_name)
    if uid in ctx.application.bot_data["cancels"]:
        await q.message.reply_text("⏳ في حل شغّال لحسابك دلوقتي. استنى لما يخلص أو اضغط «إيقاف».")
        return
    if queue_full(ctx.application):
        await q.message.reply_text(QUEUE_FULL_TEXT)
        return
    ctx.user_data["state"] = "await_report_consent"
    ctx.user_data.pop("report_consent", None)
    owner_target = (
        f"(Telegram ID: <code>{OWNER_CHAT_ID}</code>)"
        if OWNER_CHAT_ID
        else "(بعد ضبط OWNER_CHAT_ID في الإعدادات)"
    )
    await q.message.reply_html(
        "🔎 <b>تقارير الأعطال اختيارية</b>\n\n"
        "لو وافقت لهذه المهمة، عند حدوث مشكلة فقط سأرسل لمطوّر البوت "
        f"{owner_target} تقريرًا تقنيًا ولقطة شاشة.\n"
        "لن أرسل كود الدخول أو كلمة المرور. الرفض لا يمنع تشغيل الحل.",
        reply_markup=report_consent_kb(),
    )


async def continue_solve_flow(message, ctx):
    uid   = message.chat.id
    saved = db.get_username(uid)
    if saved:
        kb = InlineKeyboardMarkup([
            [btn("✅ استخدم الكود ده", "success", callback_data="use_saved")],
            [btn("✏️ كود تاني",        "primary", callback_data="new_user")],
        ])
        await message.reply_html(
            f"عندي كود محفوظ لحسابك:\n<code>{html.escape(saved)}</code>\n\nتستخدمه؟",
            reply_markup=kb,
        )
    else:
        await ask_username(message, ctx)


async def on_report_consent(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.message.chat.type != "private" or q.from_user.id != q.message.chat.id:
        return
    ctx.user_data.pop("state", None)
    ctx.user_data["report_consent"] = q.data == "report_consent_yes"
    await q.message.reply_text(
        "✅ هنبعت تقرير منقّح عند حدوث مشكلة فقط."
        if ctx.user_data["report_consent"]
        else "تمام، مش هنبعت أي تقرير."
    )
    await continue_solve_flow(q.message, ctx)


async def ask_username(message, ctx):
    ctx.user_data["state"] = "await_user"
    await message.reply_html(
        "ابعتلي <b>كود الدخول</b> (Login ID) بتاع حسابك على المنصة 👇",
        reply_markup=cancel_kb(),
    )


async def ask_password(message, ctx, username):
    ctx.user_data["state"]    = "await_pass"
    ctx.user_data["username"] = username
    await message.reply_html(
        f"تمام ✅ الكود: <code>{html.escape(username)}</code>\n\n"
        "دلوقتي ابعت <b>الباسورد</b> 🔑\n"
        "<i>هتتمسح رسالتك أول ما أستلمها.</i>",
        reply_markup=cancel_kb(),
    )


async def on_new_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await ask_username(q.message, ctx)


async def on_use_saved(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    await q.answer()
    saved = db.get_username(q.from_user.id)
    if not saved:
        await ask_username(q.message, ctx)
        return
    await ask_password(q.message, ctx, saved)


async def on_cancel_flow(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    ctx.user_data.pop("state",          None)
    ctx.user_data.pop("username",       None)
    ctx.user_data.pop("report_consent", None)
    await q.answer("اتلغى")
    try:
        await q.message.delete()
    except TelegramError:
        pass


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    state = ctx.user_data.get("state")
    text  = (update.message.text or "").strip()
    uid   = update.effective_user.id

    if state == "await_user":
        if not (2 <= len(text) <= 64) or any(c.isspace() for c in text):
            await update.message.reply_text("الكود شكله مش مظبوط (من غير مسافات). ابعته تاني.")
            return
        db.save_username(uid, text)
        await ask_password(update.message, ctx, text)

    elif state == "await_pass":
        password = update.message.text or ""
        try:
            await update.message.delete()
        except TelegramError:
            pass
        username = ctx.user_data.pop("username", None) or db.get_username(uid)
        ctx.user_data.pop("state", None)
        if not username or not password.strip():
            await update.effective_chat.send_message("حصلت مشكلة، ابدأ من جديد بـ /start")
            return
        ctx.application.create_task(
            run_student_job(
                ctx.application,
                update.effective_chat.id,
                uid,
                username,
                password,
                ctx.user_data.pop("report_consent", False),
            ),
            update=update,
        )
        password = None

    elif state == "await_report_consent":
        await update.message.reply_text("اختار من الأزرار قبل المتابعة.", reply_markup=report_consent_kb())

    else:
        await update.message.reply_text("اكتب /start عشان تفتح القائمة 🌿")


# ═══════════════════════════════════════════════════════
#  🎯 شريط التقدم المحسّن
# ═══════════════════════════════════════════════════════

# أيقونات المراحل — تتغير مع تقدم العمل
_PHASE_ICONS = {
    "بيسجّل":    "🔑",
    "يسجّل":     "🔑",
    "login":     "🔑",
    "بيفحص":     "🔍",
    "يفحص":      "🔍",
    "خطة":       "📋",
    "منهج":      "📋",
    "بيحل":      "⚡",
    "اختبار":    "🧪",
    "يحدّث":     "🔄",
    "انتظار":    "⏳",
    "طابور":     "🚦",
}

def _phase_icon(phase: str) -> str:
    for key, icon in _PHASE_ICONS.items():
        if key in phase:
            return icon
    return "⚙️"

# ساعات متحركة
_CLOCKS = "🕛🕐🕑🕒🕓🕔🕕🕖🕗🕘🕙🕚"


class Progress:
    def __init__(self, bot, chat_id, message_id):
        self.bot        = bot
        self.chat_id    = chat_id
        self.message_id = message_id

        self.lines               = deque(maxlen=8)
        self.completed           = set()
        self.transitions         = set()
        self.plan_units          = {}
        self.platform_progress   = None
        self.initial_platform_done = None
        self.questions           = 0
        self.activities          = None
        self.header              = "⚙️ <b>جاري حل المنصة...</b>"
        self.phase               = "بيجهّز الاتصال بالمنصة"
        self.current_activity    = ""
        self.quiz_position       = None
        self.show_stop           = True
        self.dirty               = True
        self.t0                  = time.monotonic()
        self.last_edit_at        = 0.0
        self.last_rendered       = None
        self.retry_not_before    = 0.0
        self.edit_failures       = 0

    # ─── intake ───

    def _record_plan_unit(self, text):
        parts = (text or "").rsplit("|", 2)
        if len(parts) != 3:
            return
        name, done, total = parts
        try:
            done, total = int(done), int(total)
        except ValueError:
            return
        normalized = core.normalize_text(name)
        if normalized and total > 0:
            self.plan_units[normalized] = (name, done, total)

    def handle(self, kind, text):
        raw = text or ""
        e   = html.escape(raw)

        if kind == "q_ok":
            self.questions += 1
            self.dirty = True
            return
        elif kind == "phase":
            self.phase = raw
            self.dirty = True
            return
        elif kind == "login_ok":
            self.phase = "✅ تم تسجيل الدخول — بيفحص خطة المنهج"
            self.dirty = True
            return
        elif kind == "open":
            self.current_activity = raw
            self.phase            = "بيحل النشاط الحالي"
            self.quiz_position    = None
            self.dirty = True
            return
        elif kind == "quiz_solving":
            self.current_activity = raw
            self.phase            = "بيحل الاختبار"
            self.quiz_position    = None
            self.dirty = True
            return
        elif kind == "quiz_progress":
            parts = raw.split("|", 1)
            if len(parts) == 2:
                try:
                    self.quiz_position = (int(parts[0]), int(parts[1]))
                except ValueError:
                    self.quiz_position = None
            self.phase = "بيحل الاختبار"
            self.dirty = True
            return
        elif kind == "log":
            self.phase = raw
            self.dirty = True
            return
        elif kind == "plan_todo":
            self._record_plan_unit(raw)
            self.dirty = True
            return
        elif kind == "plan_progress":
            parts = raw.split("|", 1)
            if len(parts) == 2:
                try:
                    done, total = (int(v) for v in parts)
                except ValueError:
                    return
                if 0 <= done <= total and total > 0:
                    if self.initial_platform_done is None:
                        self.initial_platform_done = done
                    self.platform_progress = (done, total)
                    self.activities = max(0, done - self.initial_platform_done)
                    self.dirty = True
            return
        elif kind in ("done", "moved_on"):
            normalized = core.normalize_text(text or "")
            if not normalized or normalized in self.transitions:
                return
            self.transitions.add(normalized)
            self.lines.append(f"➡️ انتقل من: <b>{e}</b>")
            if core.normalize_text(self.current_activity) == normalized:
                self.current_activity = ""
                self.quiz_position    = None
                self.phase = "بيحدّث حالة المنهج"
            elif not self.current_activity:
                self.phase = "بيحدّث حالة المنهج"
        elif kind == "quiz_done":
            normalized = core.normalize_text(text or "")
            if not normalized or normalized in self.completed:
                return
            self.completed.add(normalized)
            self.lines.append(f"✅ اجتاز الاختبار: <b>{e}</b>")
            if core.normalize_text(self.current_activity) == normalized:
                self.current_activity = ""
                self.quiz_position    = None
                self.phase = "بيحدّث حالة المنهج"
            elif not self.current_activity:
                self.phase = "بيحدّث حالة المنهج"
        elif kind == "group_done":
            normalized = core.normalize_text(text or "")
            if not normalized or normalized in self.completed:
                return
            self.completed.add(normalized)
            self.lines.append(f"✅ اكتملت الوحدة: <b>{e}</b>")
        elif kind in ("plan_done", "group_skipped"):
            if kind == "plan_done":
                self._record_plan_unit(raw)
                name = e.split("|")[0]
            else:
                name = e
            normalized = core.normalize_text(text or "")
            if not normalized or normalized in self.completed:
                return
            self.completed.add(normalized)
            self.lines.append(f"⏭️ مكتملة مسبقًا: <b>{name}</b>")
        else:
            return
        self.dirty = True

    # ─── render ───

    def _eta_line(self, elapsed):
        done, total = self.overall_items()
        if (
            self.initial_platform_done is None
            or total <= 0
            or done <= self.initial_platform_done
            or done >= total
            or elapsed < 30
        ):
            return ""
        rate = (done - self.initial_platform_done) / elapsed
        if rate <= 0:
            return ""
        remaining = (total - done) / rate
        estimate  = round(remaining, -1) or remaining
        return (
            f"⏳ المتوقع للانتهاء: <b>~{format_duration(estimate)}</b> "
            "<i>(تقريبي)</i>\n"
        )

    def overall_items(self):
        return self.platform_progress if self.platform_progress is not None else (0, 0)

    def render(self):
        elapsed = time.monotonic() - self.t0
        clock   = _CLOCKS[int(elapsed // 10) % 12]
        icon    = _phase_icon(self.phase)

        # ── شريط التقدم الرئيسي ──
        if self.platform_progress is None:
            bar_line = "📊 التقدم: <b>في انتظار تأكيد API...</b>\n"
        else:
            done, total = self.platform_progress
            pct         = ar_percent(done, total)
            bar         = progress_bar(done, total, 14)
            bar_line    = (
                f"📊 <code>{bar}</code> <b>{pct}</b>\n"
                f"    ({ar_num(done)} / {ar_num(total)} عنصر)\n"
            )

        # ── النشاط الحالي ──
        activity_line = ""
        if self.current_activity:
            activity_line = (
                f"🎯 <b>{html.escape(self.current_activity[:100])}</b>\n"
            )

        # ── سؤال الاختبار ──
        quiz_line = ""
        if self.quiz_position:
            cur, tot = self.quiz_position
            q_bar    = progress_bar(cur, tot, 8)
            quiz_line = (
                f"🧪 سؤال <b>{ar_num(cur)}/{ar_num(tot)}</b> "
                f"<code>{q_bar}</code>\n"
            )

        # ── الأسئلة والأنشطة ──
        if self.activities is None:
            act_line = "📚 أنشطة أُكملت: <b>—</b>\n"
        else:
            act_line = f"📚 أنشطة أُكملت: <b>{ar_num(self.activities)}</b>\n"

        body = "\n".join(self.lines) or "<i>جاري البدء...</i>"

        return (
            f"{self.header}\n"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"{icon} <b>{html.escape(self.phase[:100])}</b>\n"
            f"{activity_line}"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"{bar_line}"
            f"{quiz_line}"
            f"📝 أسئلة: <b>{ar_num(self.questions)}</b>  "
            f"{act_line}"
            f"{clock} الوقت: <b>{format_duration(elapsed)}</b>\n"
            f"{self._eta_line(elapsed)}"
            "▔▔▔▔▔▔▔▔▔▔▔▔▔▔\n"
            f"<b>📌 آخر التحديثات</b>\n"
            f"{body}"
        )

    async def flush(self, force=False):
        now = time.monotonic()
        if not force and not self.dirty:
            return
        if now < self.retry_not_before:
            return
        if not force and self.last_edit_at and now - self.last_edit_at < 1.5:
            return
        rendered = self.render()
        if rendered == self.last_rendered:
            self.dirty = False
            return
        try:
            await self.bot.edit_message_text(
                rendered,
                chat_id=self.chat_id,
                message_id=self.message_id,
                parse_mode=ParseMode.HTML,
                reply_markup=stop_kb() if self.show_stop else None,
            )
            self.dirty        = False
            self.last_rendered = rendered
            self.last_edit_at  = time.monotonic()
            self.retry_not_before = 0.0
            self.edit_failures = 0
        except RetryAfter as exc:
            self.dirty = True
            self.retry_not_before = time.monotonic() + exc.retry_after
            log.warning("progress edit rate-limited; retrying after %.1fs", exc.retry_after)
        except BadRequest as exc:
            if "message is not modified" in str(exc).casefold():
                self.dirty        = False
                self.last_rendered = rendered
                self.last_edit_at  = time.monotonic()
            else:
                self.dirty = False
                log.warning("progress edit rejected: %s", exc)
        except TelegramError as exc:
            self.dirty = True
            self.edit_failures += 1
            delay = min(30, 2 ** min(self.edit_failures - 1, 5))
            self.retry_not_before = time.monotonic() + delay
            log.warning("progress edit failed; retrying in %ss: %s", delay, exc)


async def ticker(progress, stop):
    last_clock = progress.t0
    while not stop.is_set():
        if time.monotonic() - last_clock >= 10:
            progress.dirty = True
            last_clock = time.monotonic()
        try:
            await asyncio.wait_for(stop.wait(), timeout=1)
        except asyncio.TimeoutError:
            pass
        if progress.dirty:
            await progress.flush()


# ═══════════════════════════════════════════════════════
#  تشخيص للأدمن
# ═══════════════════════════════════════════════════════

async def send_owner_diagnostic(app, report):
    if not OWNER_CHAT_ID:
        return
    details = (
        f"🚨 تقرير عطل #{report['id']}\n"
        f"الحالة:    {report['status']}\n"
        f"الوقت:     {report['timestamp']}\n"
        f"الموقع:    {report['page']}\n"
        f"النشاط:    {report['activity']}\n"
        f"التشخيص:   {report['error']}"
    )[:4000]
    try:
        await app.bot.send_message(chat_id=OWNER_CHAT_ID, text=details)
    except TelegramError as exc:
        log.warning("owner diagnostic message failed (%s)", type(exc).__name__)
    screenshot = report.get("screenshot")
    if screenshot:
        image = io.BytesIO(screenshot)
        image.name = f"diagnostic-{report['id']}.png"
        try:
            await app.bot.send_photo(
                chat_id=OWNER_CHAT_ID,
                photo=image,
                caption="لقطة منقّحة: الشريط العلوي وحقول الإدخال محجوبة.",
            )
        except TelegramError as exc:
            log.warning("owner diagnostic screenshot failed (%s)", type(exc).__name__)


# ═══════════════════════════════════════════════════════
#  تشغيل الحل
# ═══════════════════════════════════════════════════════

async def run_student_job(app, chat_id, uid, username, password, report_consent=False):
    data = app.bot_data
    if uid in data["cancels"]:
        return
    if queue_full(app):
        password = None
        await app.bot.send_message(chat_id, QUEUE_FULL_TEXT, reply_markup=after_kb())
        return

    cancel        = threading.Event()
    timed_out     = threading.Event()
    data["cancels"][uid] = cancel
    queued        = len(data["cancels"]) > MAX_JOBS
    queue_reserved = queued
    if queue_reserved:
        data["waiting"] += 1

    started      = time.time()
    loop         = asyncio.get_running_loop()
    progress     = None
    stop_ticker  = None
    ticker_task  = None
    timer        = None
    sem          = data["sem"]
    acquired     = False
    result       = {"status": "error", "error": "unknown"}

    def hook(kind, text):
        if kind == "diagnostic":
            loop.call_soon_threadsafe(
                lambda: app.create_task(send_owner_diagnostic(app, text))
            )
            return
        if kind == "debug":
            safe = str(text)
            for secret in (password, username):
                if secret:
                    safe = safe.replace(secret, "***")
            log.info("solver uid=%s: %s", uid, safe[:600])
            return
        loop.call_soon_threadsafe(progress.handle, kind, text)

    def on_timeout():
        timed_out.set()
        cancel.set()

    try:
        msg = await app.bot.send_message(
            chat_id, "⏳ جاري التجهيز...", reply_markup=stop_kb()
        )
        progress    = Progress(app.bot, chat_id, msg.message_id)
        stop_ticker = asyncio.Event()
        ticker_task = asyncio.create_task(ticker(progress, stop_ticker))

        if queued:
            progress.handle(
                "phase",
                f"في انتظار مكان في الطابور (ترتيبك {ar_num(data['waiting'])})",
            )
        acquired = await acquire_job_slot(sem, cancel)
        if queue_reserved:
            data["waiting"] -= 1
            queue_reserved   = False

        if not acquired:
            result = {"status": "stopped"}
        else:
            try:
                if cancel.is_set():
                    result = {"status": "stopped"}
                else:
                    progress.handle("phase", "بيسجّل الدخول")
                    timer = loop.call_later(JOB_TIMEOUT_MIN * 60, on_timeout)
                    result = await asyncio.to_thread(
                        core.run_job,
                        username, password,
                        data["entries"],
                        hook, cancel,
                        True, report_consent,
                    )
            finally:
                sem.release()
                acquired = False
    except Exception as exc:  # noqa: BLE001
        log.error("job crashed uid=%s (%s)", uid, type(exc).__name__)
        result = {"status": "error", "error": "حصل خطأ غير متوقع في البوت."}
    finally:
        password = None
        if timer:
            timer.cancel()
        if acquired:
            sem.release()
        if queue_reserved:
            data["waiting"] -= 1
        if stop_ticker:
            stop_ticker.set()
        if ticker_task:
            try:
                await ticker_task
            except Exception as exc:  # noqa: BLE001
                log.warning("progress ticker stopped unexpectedly (%s)", type(exc).__name__)
        if data["cancels"].get(uid) is cancel:
            data["cancels"].pop(uid, None)

    elapsed = time.monotonic() - (progress.t0 if progress else time.monotonic())

    if progress is None:
        db.log_job(uid, started, result.get("status", "error"), 0, 0, result.get("error", ""))
        return

    status           = result.get("status")
    if timed_out.is_set():
        status = "timeout"
    last             = result.get("last") or "—"
    platform_progress = progress.overall_items()
    summary = job_summary(progress.questions, progress.activities, elapsed, platform_progress)

    if status == "finished":
        progress.header = "🎉 <b>اكتملت الأنشطة المتاحة</b>"
        final = completion_message(
            progress.questions, progress.activities, elapsed, last, platform_progress
        )
    elif status == "tests_pending":
        test_name = html.escape((result.get("test") or "")[:120])
        if result.get("reason") == "failed":
            progress.header = "📝 <b>الاختبار لم يُجتز</b>"
            final = (
                f"📝 <b>المنصة أكدت عدم اجتياز الاختبار:</b>\n"
                f"{('• ' + test_name) if test_name else ''}\n\n{summary}\n\n"
                "لم أسجّل الاختبار مكتملًا. راجع المحاولة في المنصة ثم شغّل الحل مجددًا."
            )
        else:
            progress.header = "📝 <b>وقفت عند اختبار — مفيش إجابات مطابقة</b>"
            final = (
                f"📝 <b>وصلت لاختبار مش موجود في بنك الإجابات:</b>\n"
                f"{('• ' + test_name) if test_name else ''}\n\n{summary}\n\n"
                "حلّه بنفسك، وبعدين شغّل الحل تاني والبوت يكمّل اللي بعده."
            )
    elif status == "login_failed":
        progress.header = "❌ <b>فشل تسجيل الدخول</b>"
        final = "❌ الكود أو الباسورد غلط، أو الحساب مش متاح.\nراجع بياناتك وحاول تاني."
    elif status == "stopped":
        progress.header = "⛔ <b>اتوقّف الحل</b>"
        final = f"⛔ وقفت الحل.\n\n{summary}"
    elif status == "stuck":
        progress.header = "🧩 <b>تعذّر إكمال بعض الأنشطة</b>"
        err  = html.escape((result.get("error") or "لم يتوفر سبب إضافي.")[:700])
        final = (
            "🧩 <b>جرّبت الإجابات الاحتياطية ومحاولات الاستعادة، لكن المنصة لم تسمح بإكمال بعض الأنشطة.</b>\n"
            f"<blockquote>{err}</blockquote>\n\n{summary}\n"
            f"📍 آخر نشاط: <b>{html.escape(last)}</b>\n\n"
            "شغّل الحل مجددًا ليعيد المحاولة من التقدم المحفوظ."
        )
    elif status == "timeout":
        progress.header = "⌛ <b>خلصت المدة القصوى</b>"
        final = f"⌛ الحل خد وقت أطول من المسموح.\n\n{summary}\nتقدر تشغّله تاني وهيكمّل من حيث وقف."
    else:
        progress.header = "⚠️ <b>الحل وقف بسبب مشكلة</b>"
        err  = html.escape((result.get("error") or "")[:500])
        final = f"⚠️ الحل وقف:\n<code>{err}</code>\n\n{summary}\nآخر نشاط: {html.escape(last)}"
        log.warning("job error uid=%s: %s", uid, result.get("error"))

    progress.show_stop = False
    progress.dirty     = True
    await progress.flush(force=True)
    await app.bot.send_message(chat_id, final, parse_mode=ParseMode.HTML, reply_markup=after_kb())
    db.log_job(
        uid, started, status,
        progress.questions, progress.activities,
        result.get("error", "") or "",
        duration=int(elapsed),
    )


async def on_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q     = update.callback_query
    event = ctx.application.bot_data["cancels"].get(q.from_user.id)
    if event:
        event.set()
        await q.answer("هوقف بعد الخطوة الحالية…")
    else:
        await q.answer("مفيش حل شغّال.")


# ═══════════════════════════════════════════════════════
#  تهيئة وبناء التطبيق
# ═══════════════════════════════════════════════════════

async def post_init(app: Application):
    db.init(os.getenv("DB_PATH", str(BASE / "bot.db")))
    app.bot_data["sem"]     = asyncio.Semaphore(MAX_JOBS)
    app.bot_data["cancels"] = {}
    app.bot_data["waiting"] = 0
    app.bot_data["entries"] = core.parse_answer_file(ANSWERS_FILE)
    log.info("loaded %d answers", len(app.bot_data["entries"]))


def build_app():
    req = HTTPXRequest(
        connect_timeout=30.0, read_timeout=30.0,
        write_timeout=30.0,   pool_timeout=30.0,
    )
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(req)
        .concurrent_updates(True)
        .post_init(post_init)
        .build()
    )
    private = filters.ChatType.PRIVATE

    app.add_handler(CommandHandler("start",    on_start,    filters=private))
    app.add_handler(CommandHandler("menu",     on_start,    filters=private))
    app.add_handler(CommandHandler("cancel",   on_cancel_cmd, filters=private))
    app.add_handler(CommandHandler("help",     on_help_cmd, filters=private))
    app.add_handler(CommandHandler("mystats",  on_mystats_cmd, filters=private))
    app.add_handler(CommandHandler("stats",    on_stats_cmd, filters=private))  # أدمن
    app.add_error_handler(on_error)

    for pattern, handler in [
        (r"^solve$",                  on_solve),
        (r"^use_saved$",              on_use_saved),
        (r"^new_user$",               on_new_user),
        (r"^cancel_flow$",            on_cancel_flow),
        (r"^report_consent_(yes|no)$", on_report_consent),
        (r"^stop$",                   on_stop),
        (r"^help$",                   on_help),
        (r"^privacy$",                on_privacy),
        (r"^salat$",                  on_salat),
        (r"^menu$",                   on_menu),
        (r"^mystats$",                on_mystats_cb),
    ]:
        app.add_handler(CallbackQueryHandler(handler, pattern=pattern))

    app.add_handler(MessageHandler(private & filters.TEXT & ~filters.COMMAND, on_text))
    return app


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("حط BOT_TOKEN في ملف .env")
    build_app().run_polling()
