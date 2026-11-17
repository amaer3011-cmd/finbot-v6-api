"""
api_engine.py — محرك حل المنصة عبر الـ API فقط (HTTP خالص، بلا متصفح ولا Playwright).

الواجهة مطابقة لـ core.run_job القديم، فـ bot.py بيستخدمه بسطر واحد:
    import api_engine as core

التدفق (مأخوذ من api_capture.json ومن ردّ GET /chapters/{id}/blocks الحقيقي):
    POST /auth/login → GET /subjects/{id}/curriculum → لكل نشاط غير مكتمل ومفتوح:
      chapter : GET /chapters/{id}/blocks → POST /chapters/{id}/answer (لكل سؤال) → POST /chapters/{id}/complete
      review  : GET /reviews/{id}/items → POST /reviews/{id}/complete
      test    : POST /tests/{id}/start → PUT /tests/{id}/questions/{test_question_id}/answer → POST /tests/{id}/submit
    ثم نعيد جلب المنهج (الأنشطة التالية بتتفتح) لحد ما يخلص كل شيء.

الأمان من التعليق: لو نشاط فضل غير مكتمل بعد عدة محاولات بنتخطاه ونكمّل الباقي بدل ما نلف للأبد.
"""

import html as _html
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

import httpx

# منطق المطابقة (خالص) — بنعيد تصديرها لأن bot.py بيستخدم core.parse_answer_file / core.normalize_text
from matching import parse_answer_file  # noqa: F401  (bot.py بيستخدمها عبر core)
from matching import (
    AnswerMatchError,
    answer_option_index,
    normalize_text,
    resolve_answer,
)

_log = logging.getLogger(__name__)

# ─────────────────────── إعدادات ───────────────────────────────
SITE_URL = os.getenv("SITE_URL_BASE", "https://egy.app.spx-learning-square.com").rstrip("/")
BASE_URL = f"{SITE_URL}/api/v1"
SUBJECT_ID = int(os.getenv("SUBJECT_ID", "7"))
ANSWER_DELAY = float(os.getenv("ANSWER_DELAY", "0.05"))        # ثانية بين الإجابات
MAX_ITEM_ATTEMPTS = int(os.getenv("MAX_ITEM_ATTEMPTS", "3"))    # محاولات النشاط الواحد قبل تخطيه
MAX_TEST_ATTEMPTS = int(os.getenv("MAX_TEST_ATTEMPTS", "4"))    # محاولات الاختبار داخل النشاط الواحد
TEST_PARALLEL = max(1, int(os.getenv("TEST_PARALLEL", "1")))    # عدد طلبات حفظ إجابات الاختبار المتزامنة
UNMATCHED_POLICY = os.getenv("UNMATCHED_POLICY", "guess").strip().lower()  # guess | stop
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "4"))
RETRY_STATUSES = {429, 500, 502, 503, 504}


# ─────────────────────── Exceptions ────────────────────────────
class Stopped(RuntimeError):
    pass


class LoginFailed(RuntimeError):
    pass


class PlatformError(RuntimeError):
    def __init__(self, status, message, path=""):
        super().__init__(f"HTTP {status}: {message} ({path})" if path else f"HTTP {status}: {message}")
        self.status = status
        self.message = message
        self.path = path


class ItemIncomplete(RuntimeError):
    """النشاط اتعالج لكن المنصة ما سجّلتوش مكتمل."""

    def __init__(self, message, reason="incomplete"):
        super().__init__(message)
        self.reason = reason


class UnknownStructure(RuntimeError):
    """شكل ردّ المنصة مش زي المتوقع — الرسالة فيها مفاتيح للتشخيص بدون بيانات حساسة."""


# ─────────────────────── أدوات نصوص وأحداث ─────────────────────
_TAGS = re.compile(r"<[^>]+>")


def _clean(text) -> str:
    """HTML entities (&#34;) + أي وسوم → نص عادي."""
    s = _html.unescape(str(text or ""))
    if "<" in s:
        s = _TAGS.sub(" ", s)
    return " ".join(s.split())


def _say(hook, kind: str, text: str = ""):
    if hook:
        try:
            hook(kind, text)
        except Exception:  # noqa: BLE001
            pass


def _check(cancel: Optional[threading.Event]):
    if cancel is not None and cancel.is_set():
        raise Stopped()


def _sleep(cancel, seconds: float):
    if seconds <= 0:
        return
    if cancel is not None:
        if cancel.wait(seconds):
            raise Stopped()
    else:
        time.sleep(seconds)


# ─────────────────────── إجابات متعلَّمة ───────────────────────
class LearnedAnswers:
    """
    إجابات صحيحة اتعلّمناها من ردّ المنصة نفسها (ردّ تقديم الاختبار فيه is_correct لكل خيار،
    وردّ إجابة الدرس فيه correct_option_id). بتتخزّن في ملف JSON وبتاخد أولوية على البنك.
    """

    def __init__(self, path):
        self._path = Path(path)
        self._lock = threading.Lock()
        self._data: dict = {}
        try:
            self._data = json.loads(self._path.read_text(encoding="utf-8"))
            if not isinstance(self._data, dict):
                self._data = {}
        except (OSError, ValueError):
            self._data = {}

    def __len__(self):
        return len(self._data)

    def lookup(self, question: str, option_texts) -> Optional[int]:
        record = self._data.get(normalize_text(question))
        if not record:
            return None
        try:
            return answer_option_index(option_texts, record["a"])
        except (AnswerMatchError, KeyError):
            return None

    def add(self, question: str, answer: str) -> bool:
        key = normalize_text(question)
        if not key or not answer:
            return False
        with self._lock:
            if self._data.get(key, {}).get("a") == answer:
                return False
            self._data[key] = {"q": question[:300], "a": answer}
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                tmp = self._path.with_suffix(".tmp")
                tmp.write_text(json.dumps(self._data, ensure_ascii=False), encoding="utf-8")
                tmp.replace(self._path)
            except OSError as exc:
                _log.warning("learned answers not saved (%s)", exc)
        return True


_learned_singleton: Optional[LearnedAnswers] = None
_learned_lock = threading.Lock()


def get_learned() -> LearnedAnswers:
    global _learned_singleton
    with _learned_lock:
        if _learned_singleton is None:
            path = os.getenv("LEARNED_FILE") or str(
                Path(os.getenv("DB_PATH", "bot.db")).resolve().parent / "learned_answers.json"
            )
            _learned_singleton = LearnedAnswers(path)
        return _learned_singleton


# ─────────────────────── HTTP Client ───────────────────────────
_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
    "Origin": SITE_URL,
    "Referer": f"{SITE_URL}/subjects/{SUBJECT_ID}/curriculum",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}
_NO_CACHE = {"Cache-Control": "no-cache", "Pragma": "no-cache"}


def _error_message(response: httpx.Response) -> str:
    try:
        body = response.json()
        err = body.get("error") or {}
        return str(err.get("message") or err.get("code") or body)[:200]
    except Exception:  # noqa: BLE001
        return (response.text or "")[:200]


class PlatformClient:
    def __init__(self, cancel: Optional[threading.Event] = None):
        self._cancel = cancel
        self._creds = None
        timeout = httpx.Timeout(connect=10, read=30, write=15, pool=10)
        try:
            self._http = httpx.Client(
                base_url=BASE_URL, headers=_HEADERS, timeout=timeout,
                follow_redirects=True, http2=True,
            )
        except ImportError:  # h2 غير مثبتة
            self._http = httpx.Client(
                base_url=BASE_URL, headers=_HEADERS, timeout=timeout, follow_redirects=True,
            )

    def close(self):
        self._http.close()

    # ── طبقة الطلبات: إعادة محاولة + إعادة دخول عند انتهاء الجلسة ──
    def _request(self, method: str, path: str, body=None, *, relogin=True) -> dict:
        last_error = None
        for attempt in range(HTTP_RETRIES + 1):
            _check(self._cancel)
            try:
                if method == "GET":
                    response = self._http.get(path, headers=_NO_CACHE)
                else:
                    response = self._http.request(method, path, json=body if body is not None else {})
            except httpx.TransportError as exc:
                last_error = PlatformError(0, f"{type(exc).__name__}", path)
                _sleep(self._cancel, min(8.0, 0.6 * (2 ** attempt)))
                continue

            status = response.status_code
            if status in RETRY_STATUSES and attempt < HTTP_RETRIES:
                wait = 0.0
                try:
                    wait = float(response.headers.get("retry-after", "0"))
                except ValueError:
                    wait = 0.0
                last_error = PlatformError(status, _error_message(response), path)
                _sleep(self._cancel, min(15.0, max(wait, 0.6 * (2 ** attempt))))
                continue

            if status == 401 and relogin and self._creds and path != "/auth/login":
                self.login(*self._creds)   # الجلسة انتهت: ندخل تاني مرة واحدة
                return self._request(method, path, body, relogin=False)

            if status == 204 or not response.content:
                if status >= 400:
                    raise PlatformError(status, "رد فارغ", path)
                return {}
            try:
                payload = response.json()
            except ValueError:
                raise PlatformError(status, "رد غير JSON", path) from None
            if status >= 400 or (isinstance(payload, dict) and payload.get("success") is False):
                raise PlatformError(status, _error_message(response), path)
            return payload
        raise last_error or PlatformError(0, "فشل الاتصال", path)

    def _get(self, path):
        return self._request("GET", path)

    def _data(self, method, path, body=None):
        return self._request(method, path, body).get("data")

    # ── auth ──
    def login(self, username: str, password: str) -> dict:
        self._creds = (username.strip(), password)
        try:
            payload = self._request(
                "POST", "/auth/login",
                {"login_id": username.strip(), "password": password}, relogin=False,
            )
        except PlatformError as exc:
            if exc.status in (400, 401, 403, 404, 422):
                raise LoginFailed(exc.message) from None
            raise
        return payload.get("data") or {}

    # ── المنهج ──
    def curriculum(self) -> list:
        return self._data("GET", f"/subjects/{SUBJECT_ID}/curriculum") or []

    def subject(self) -> dict:
        return self._data("GET", f"/subjects/{SUBJECT_ID}") or {}

    def unlock_achievements(self):
        try:
            self._data("POST", f"/subjects/{SUBJECT_ID}/achievements/unlock", {})
        except PlatformError:
            pass  # اختياري (المتصفح بينادي عليه بعد كل درس)

    # ── الدروس ──
    def chapter_blocks(self, chapter_id):
        return self._data("GET", f"/chapters/{chapter_id}/blocks") or {}

    def chapter_answer(self, chapter_id, block_id, option_id) -> dict:
        return self._data(
            "POST", f"/chapters/{chapter_id}/answer",
            {"block_id": block_id, "selected_option_id": option_id, "hint_used": 0},
        ) or {}

    def chapter_complete(self, chapter_id) -> dict:
        return self._data("POST", f"/chapters/{chapter_id}/complete", {}) or {}

    # ── المراجعات ──
    def review_items(self, review_id):
        return self._data("GET", f"/reviews/{review_id}/items") or {}

    def review_complete(self, review_id) -> dict:
        return self._data("POST", f"/reviews/{review_id}/complete", {}) or {}

    # ── الاختبارات ──
    def test_start(self, test_id) -> dict:
        return self._data("POST", f"/tests/{test_id}/start", {}) or {}

    def test_answer(self, test_id, test_question_id, option_id) -> bool:
        data = self._data(
            "PUT", f"/tests/{test_id}/questions/{test_question_id}/answer",
            {"selected_option_id": option_id},
        ) or {}
        return data.get("saved") is True

    def test_submit(self, test_id) -> dict:
        return self._data("POST", f"/tests/{test_id}/submit", {}) or {}


# ─────────────────────── اختيار الإجابة ────────────────────────
def _guess_index(option_texts) -> int:
    """لما السؤال مش في البنك: أطول خيار (الخيار الصحيح غالبًا الأكثر تفصيلًا)، ولو متساوية أول واحد."""
    if not option_texts:
        return 0
    return max(range(len(option_texts)), key=lambda i: (len(option_texts[i]), -i))


def _choose(question, option_texts, entries, learned):
    """يرجّع (رقم_الخيار, المصدر) — المصدر: learned | bank | guess."""
    if learned is not None:
        index = learned.lookup(question, option_texts)
        if index is not None:
            return index, "learned"
    try:
        index, _answer, _source = resolve_answer(question, entries, option_texts)
        return index, "bank"
    except AnswerMatchError:
        pass
    if UNMATCHED_POLICY == "stop":
        raise ItemIncomplete(f"سؤال بلا إجابة في البنك: {question[:90]}", reason="unmatched")
    return _guess_index(option_texts), "guess"


# ─────────────────────── قراءة الأسئلة من الردود ───────────────
def _sorted_by(records, key):
    return sorted(records, key=lambda r: (r.get(key) is None, r.get(key) or 0))


def _parse_chapter_question(block):
    """يرجّع (question_text, [(option_id, option_text), ...]) من بلوك سؤال في درس."""
    question = block.get("question") or {}
    q_text = _clean(question.get("question_text") or question.get("text") or block.get("content"))
    options = block.get("options") or question.get("options") or question.get("choices") or []
    parsed = [
        (o["id"], _clean(o.get("text") or o.get("label")))
        for o in _sorted_by([o for o in options if isinstance(o, dict)], "sort_order")
        if o.get("id") is not None
    ]
    if not q_text or len(parsed) < 2:
        raise UnknownStructure(
            f"بلوك سؤال بشكل غير متوقع (id={block.get('id')}, type={block.get('type')}, "
            f"مفاتيح={sorted(block.keys())}, مفاتيح السؤال={sorted(question.keys())})"
        )
    q_type = question.get("type")
    if q_type and q_type != "mcq":
        raise UnknownStructure(f"نوع سؤال غير مدعوم في الدرس: {q_type} (block {block.get('id')})")
    return q_text, parsed


def _records_by_id(value) -> dict:
    """dict {id: record} من dict أو list (المفاتيح ممكن تكون نص)."""
    out = {}
    if isinstance(value, dict):
        for key, record in value.items():
            if isinstance(record, dict):
                rid = record.get("id", key)
                try:
                    out[int(rid)] = record
                except (TypeError, ValueError):
                    continue
    elif isinstance(value, list):
        for record in value:
            if isinstance(record, dict) and record.get("id") is not None:
                out[int(record["id"])] = record
    return out


def _options_by_question(value) -> dict:
    out = {}
    if isinstance(value, dict):
        for key, options in value.items():
            if isinstance(options, list):
                try:
                    out[int(key)] = [o for o in options if isinstance(o, dict)]
                except (TypeError, ValueError):
                    continue
    elif isinstance(value, list):
        for option in value:
            if isinstance(option, dict) and option.get("question_id") is not None:
                out.setdefault(int(option["question_id"]), []).append(option)
    return out


def _map_test_questions(data) -> dict:
    """
    test_question_id (اللي في رابط حفظ الإجابة) → question_id (اللي بيها النص والخيارات).
    المنصة بتولّد الاتنين تصاعديًا بنفس الترتيب في كل اختبار (اتأكدت منه على اختبار 30 في الـ capture:
    491→872 … 500→881 والخيارات المختارة بتتبع نفس السؤال)، فنستخدم الربط الصريح لو موجود وإلا الترتيب.
    """
    responses = data.get("question_responses") or []
    explicit = {}
    for qr in responses:
        if qr.get("question_id") is not None and qr.get("test_question_id") is not None:
            explicit[int(qr["test_question_id"])] = int(qr["question_id"])
    if responses and len(explicit) == len(responses):
        return explicit
    test_questions = data.get("test_questions")
    if isinstance(test_questions, list):
        mapping = {
            int(t["id"]): int(t["question_id"])
            for t in test_questions
            if isinstance(t, dict) and t.get("id") is not None and t.get("question_id") is not None
        }
        if mapping:
            return mapping
    tq_ids = sorted(int(qr["test_question_id"]) for qr in responses)
    q_ids = sorted(_records_by_id(data.get("questions")).keys())
    if not tq_ids or len(tq_ids) != len(q_ids):
        raise UnknownStructure(
            f"تعذّر ربط أسئلة الاختبار (responses={len(tq_ids)}, questions={len(q_ids)}, "
            f"مفاتيح الرد={sorted(data.keys())})"
        )
    return dict(zip(tq_ids, q_ids))


# ─────────────────────── حل الأنشطة ────────────────────────────
class _Ctx:
    """حالة الحل المشتركة بين الأنشطة."""

    def __init__(self, client, entries, learned, hook, cancel):
        self.client = client
        self.entries = entries
        self.learned = learned
        self.hook = hook
        self.cancel = cancel
        self.questions = 0
        self.guessed = 0
        self.learned_new = 0


def _solve_chapter(ctx: _Ctx, item) -> None:
    client = ctx.client
    title = item.get("title") or f"chapter-{item['id']}"
    _say(ctx.hook, "open", title)

    data = client.chapter_blocks(item["id"])
    cleared = set((data.get("chapter") or {}).get("cleared_block_ids") or [])
    blocks = _sorted_by([b for b in data.get("blocks") or [] if isinstance(b, dict)], "sort_order")

    for block in blocks:
        _check(ctx.cancel)
        if block.get("type") != "question" and block.get("question_id") is None:
            continue
        if block.get("id") in cleared:
            continue
        q_text, options = _parse_chapter_question(block)
        option_texts = [text for _id, text in options]
        index, how = _choose(q_text, option_texts, ctx.entries, ctx.learned)
        response = client.chapter_answer(item["id"], block["id"], options[index][0])
        ctx.questions += 1
        _say(ctx.hook, "q_ok", q_text[:100])
        if how == "guess":
            ctx.guessed += 1
        if response.get("is_correct") is False:
            # المنصة بتكشف الإجابة الصحيحة: نتعلمها عشان ما نغلطش فيها تاني
            correct_id = response.get("correct_option_id")
            correct_text = next((t for oid, t in options if oid == correct_id), None)
            if correct_text and ctx.learned is not None and ctx.learned.add(q_text, correct_text):
                ctx.learned_new += 1
        _sleep(ctx.cancel, ANSWER_DELAY)

    result = client.chapter_complete(item["id"])
    if result.get("completed") is not True:
        raise ItemIncomplete(f"المنصة لم تسجّل الدرس مكتملًا: {title}")
    client.unlock_achievements()
    _say(ctx.hook, "done", title)


def _solve_review(ctx: _Ctx, item) -> None:
    title = item.get("title") or f"review-{item['id']}"
    _say(ctx.hook, "open", title)
    ctx.client.review_items(item["id"])           # المتصفح بيفتح النقاط قبل ما يكمّلها
    result = ctx.client.review_complete(item["id"])
    if result.get("completed") is not True:
        raise ItemIncomplete(f"المنصة لم تسجّل المراجعة مكتملة: {title}")
    _say(ctx.hook, "done", title)


def _attempt_test(ctx: _Ctx, test_id, title) -> dict:
    """محاولة واحدة: start → إجابات → submit. يرجّع ردّ التقديم بعد التعلّم منه."""
    client = ctx.client
    started = client.test_start(test_id)
    responses = started.get("question_responses") or []
    if not responses:
        raise UnknownStructure(f"بدأ الاختبار بدون أسئلة (مفاتيح الرد={sorted(started.keys())})")
    mapping = _map_test_questions(started)
    questions = _records_by_id(started.get("questions"))
    options_map = _options_by_question(started.get("options"))

    plan = []   # (idx, test_question_id, option_id, was_guess)
    for position, qr in enumerate(responses, 1):
        tq_id = int(qr["test_question_id"])
        if qr.get("selected_option_id") is not None:
            continue   # محاولة جارية متكمّلة جزئيًا
        q_id = mapping[tq_id]
        question = questions.get(q_id) or {}
        q_text = _clean(question.get("question_text") or question.get("text"))
        options = [
            (o["id"], _clean(o.get("text")))
            for o in _sorted_by(options_map.get(q_id, []), "sort_order")
            if o.get("id") is not None
        ]
        if not q_text or len(options) < 2:
            raise UnknownStructure(f"سؤال اختبار ناقص البيانات (question_id={q_id}, tq={tq_id})")
        index, how = _choose(q_text, [t for _i, t in options], ctx.entries, ctx.learned)
        plan.append((position, tq_id, options[index][0], how == "guess"))

    total = len(responses)

    def _save(step):
        position, tq_id, option_id, _guess = step
        _check(ctx.cancel)
        if not client.test_answer(test_id, tq_id, option_id):
            raise ItemIncomplete(f"المنصة لم تؤكد حفظ إجابة الاختبار ({title})")
        return position

    if TEST_PARALLEL > 1 and len(plan) > 1:
        with ThreadPoolExecutor(max_workers=TEST_PARALLEL) as pool:
            for position in pool.map(_save, plan):
                _say(ctx.hook, "quiz_progress", f"{position}|{total}")
                ctx.questions += 1
                _say(ctx.hook, "q_ok", "")
    else:
        for step in plan:
            position = _save(step)
            _say(ctx.hook, "quiz_progress", f"{position}|{total}")
            ctx.questions += 1
            _say(ctx.hook, "q_ok", "")
            _sleep(ctx.cancel, ANSWER_DELAY)
    ctx.guessed += sum(1 for step in plan if step[3])

    _say(ctx.hook, "log", "📤 بيقدّم الاختبار…")
    submitted = client.test_submit(test_id)
    _learn_from_submission(ctx, submitted, mapping, questions)
    return submitted


def _learn_from_submission(ctx: _Ctx, submitted, mapping, started_questions) -> None:
    """ردّ التقديم فيه is_correct على الخيارات: نحفظ الصحيح لأي سؤال غلطنا فيه."""
    if ctx.learned is None:
        return
    questions = _records_by_id(submitted.get("questions")) or started_questions
    options_map = _options_by_question(submitted.get("options"))
    for qr in submitted.get("question_responses") or []:
        if qr.get("is_correct") is True:
            continue
        q_id = mapping.get(int(qr.get("test_question_id", -1)))
        options = options_map.get(q_id) or []
        correct = next((o for o in options if o.get("is_correct") is True), None)
        text = _clean((questions.get(q_id) or {}).get("question_text"))
        if correct and text and ctx.learned.add(text, _clean(correct.get("text"))):
            ctx.learned_new += 1


def _solve_test(ctx: _Ctx, item) -> None:
    title = item.get("title") or f"test-{item['id']}"
    test_id = item["id"]
    _say(ctx.hook, "quiz_solving", title)
    _say(ctx.hook, "log", "🏁 بيبدأ الاختبار…")
    last_score = None
    for attempt in range(1, MAX_TEST_ATTEMPTS + 1):
        _check(ctx.cancel)
        submitted = _attempt_test(ctx, test_id, title)
        response = submitted.get("response") or {}
        if response.get("passed") is True:
            _say(ctx.hook, "quiz_done", title)
            return
        last_score = response.get("total_score")
        if attempt < MAX_TEST_ATTEMPTS:
            _say(ctx.hook, "log", f"🔁 الدرجة {last_score}% أقل من المطلوب؛ بيعيد المحاولة بالإجابات المتعلَّمة ({attempt}/{MAX_TEST_ATTEMPTS})")
    raise ItemIncomplete(
        f"الاختبار لم يُجتز بعد {MAX_TEST_ATTEMPTS} محاولات (آخر درجة: {last_score}%): {title}",
        reason="failed",
    )


# ─────────────────────── قراءة المنهج ──────────────────────────
def _unit_items(unit):
    """أنشطة الوحدة بالترتيب الحقيقي: الأقسام بترتيبها ثم أي أنشطة على مستوى الوحدة (بدون تكرار)."""
    seen, ordered = set(), []

    def add(item):
        key = (item.get("item_type"), item.get("id"))
        if key not in seen:
            seen.add(key)
            ordered.append(item)

    for section in _sorted_by([s for s in unit.get("sections") or [] if isinstance(s, dict)], "sort_order"):
        for item in _sorted_by([i for i in section.get("items") or [] if isinstance(i, dict)], "item_order"):
            add(item)
    for item in unit.get("items") or []:
        if isinstance(item, dict):
            add(item)
    return ordered


def _item_key(item):
    return (item.get("item_type"), item.get("id"))


def _next_pending(curriculum, skipped):
    """أول نشاط مفتوح وغير مكتمل (وغير متخطّى) بترتيب الوحدات."""
    for unit in _sorted_by([u for u in curriculum if isinstance(u, dict)], "unit_order"):
        if not unit.get("is_accessible") or unit.get("is_hidden"):
            continue
        for item in _unit_items(unit):
            if item.get("is_hidden") or item.get("is_completed") or _item_key(item) in skipped:
                continue
            if item.get("is_unlocked"):
                return item
    return None


def _totals(curriculum):
    done = sum(int(u.get("completed_items") or 0) for u in curriculum if isinstance(u, dict))
    total = sum(int(u.get("total_items") or 0) for u in curriculum if isinstance(u, dict))
    return done, total


def _emit_plan(hook, curriculum, announced):
    """أحداث الخطة اللي البوت بيرسم منها الوحدات والتقدم الكلي."""
    for unit in _sorted_by([u for u in curriculum if isinstance(u, dict)], "unit_order"):
        name = unit.get("title") or ""
        done, total = int(unit.get("completed_items") or 0), int(unit.get("total_items") or 0)
        if total <= 0 or not unit.get("is_accessible"):
            continue
        if done >= total:
            if name not in announced["done"]:
                announced["done"].add(name)
                _say(hook, "plan_done" if not announced["started"] else "group_done",
                     f"{name}|{done}|{total}" if not announced["started"] else name)
        else:
            _say(hook, "plan_todo", f"{name}|{done}|{total}")
    announced["started"] = True
    done, total = _totals(curriculum)
    if total > 0:
        _say(hook, "plan_progress", f"{done}|{total}")


# ─────────────────────── نقطة الدخول ───────────────────────────
def run_job(
    username: str,
    password: str,
    entries,
    hook: Optional[Callable] = None,
    cancel: Optional[threading.Event] = None,
    headless: bool = True,          # للتوافق مع bot.py
    report_consent: bool = False,   # للتوافق مع bot.py
) -> dict:
    """يرجّع dict: status = finished | stopped | login_failed | stuck | tests_pending | error (+ last, error, test, reason)."""
    started_at = time.monotonic()
    client = PlatformClient(cancel)
    ctx = _Ctx(client, entries, get_learned(), hook, cancel)
    last_title = ""
    failures = []                 # (item, error_text, reason)
    attempts: dict = {}
    skipped: set = set()
    announced = {"done": set(), "started": False}
    result: dict = {"status": "error", "error": "unknown"}

    try:
        _say(hook, "phase", "بيسجّل الدخول")
        user = client.login(username, password)
        _say(hook, "login_ok", str(user.get("name") or ""))

        while True:
            _check(cancel)
            curriculum = client.curriculum()
            _emit_plan(hook, curriculum, announced)
            item = _next_pending(curriculum, skipped)
            if item is None:
                break

            key = _item_key(item)
            last_title = item.get("title") or last_title
            attempts[key] = attempts.get(key, 0) + 1
            if attempts[key] > MAX_ITEM_ATTEMPTS:
                skipped.add(key)
                continue

            item_type = str(item.get("item_type") or "").lower()
            try:
                if item_type == "chapter":
                    _solve_chapter(ctx, item)
                elif item_type == "review":
                    _solve_review(ctx, item)
                elif item_type == "test":
                    _solve_test(ctx, item)
                else:
                    raise UnknownStructure(f"نوع نشاط غير معروف: {item_type!r} ({last_title})")
            except (Stopped, LoginFailed):
                raise
            except ItemIncomplete as exc:
                failures.append((item, str(exc), exc.reason))
                _say(hook, "log", f"⚠️ {exc}")
                if exc.reason == "failed":
                    skipped.add(key)        # ما نكرر اختبار فاشل (الإجابات المتعلَّمة اتحفظت للتشغيل الجاي)
            except UnknownStructure as exc:
                failures.append((item, str(exc), "structure"))
                skipped.add(key)
                _say(hook, "log", f"⚠️ {str(exc)[:160]}")
            except PlatformError as exc:
                failures.append((item, str(exc), "platform"))
                _say(hook, "log", f"⚠️ {str(exc)[:160]}")
                if exc.status in (400, 403, 404, 409, 422):
                    skipped.add(key)

        # ── النتيجة النهائية ──
        done, total = _totals(client.curriculum())
        unresolved = [f for f in failures if _item_key(f[0]) in skipped or attempts.get(_item_key(f[0]), 0) >= MAX_ITEM_ATTEMPTS]
        if total and done >= total:
            result = {"status": "finished", "last": last_title}
        elif unresolved:
            item, text, reason = unresolved[-1]
            if str(item.get("item_type")) == "test" and reason == "failed":
                result = {"status": "tests_pending", "reason": "failed", "test": item.get("title", ""), "last": last_title}
            elif reason == "unmatched" and str(item.get("item_type")) == "test":
                result = {"status": "tests_pending", "reason": "unmatched", "test": item.get("title", ""), "last": last_title}
            else:
                result = {"status": "stuck", "error": text, "last": last_title}
        else:
            result = {
                "status": "stuck",
                "error": f"باقي أنشطة لم تُفتح بعد ({done}/{total}) — المنصة لم تفتح الوحدة التالية.",
                "last": last_title,
            }

    except Stopped:
        result = {"status": "stopped", "last": last_title}
    except LoginFailed:
        result = {"status": "login_failed", "last": last_title}
    except PlatformError as exc:
        result = {"status": "error", "error": str(exc)[:300], "last": last_title}
    except Exception as exc:  # noqa: BLE001
        _log.exception("Unexpected error in run_job")
        result = {"status": "error", "error": f"{type(exc).__name__}: {exc}"[:300], "last": last_title}
    finally:
        client.close()

    result.setdefault("last", last_title)
    result["elapsed"] = round(time.monotonic() - started_at, 1)
    result["guessed"] = ctx.guessed
    result["learned"] = ctx.learned_new
    return result
