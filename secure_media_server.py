import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import re
import time
import uuid
from collections import defaultdict, OrderedDict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import yt_dlp
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, field_validator
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from starlette.background import BackgroundTask


# ══════════════════════════════════════════════════════════════════════════════
# [CONFIG]
# ══════════════════════════════════════════════════════════════════════════════

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).parent.resolve()
DOWNLOAD_FOLDER = BASE_DIR / "downloads"
DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

ALLOWED_HOSTS: list[str] = ["*"]
ALLOWED_ORIGINS: list[str] = ["*"]

API_KEY = os.getenv("API_KEY", "")
API_KEY_HEADER = "X-API-Key"

ENABLE_API_KEY = os.getenv("ENABLE_API_KEY", "true").lower() == "true"
ALLOW_DYNAMIC_KEYS = os.getenv("ALLOW_DYNAMIC_KEYS", "true").lower() == "true"

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "500"))
MAX_DURATION_SECONDS = int(os.getenv("MAX_DURATION_SECONDS", "600"))
DOWNLOAD_RATE_LIMIT_PER_IP = int(os.getenv("DOWNLOAD_RATE_LIMIT_PER_IP", "10"))

ALLOWED_DOMAINS: set[str] = {
    "youtube.com",
    "www.youtube.com",
    "youtu.be",
    "m.youtube.com",
    "music.youtube.com",
    "tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
    "tiktokcdn.com",
    "instagram.com",
    "www.instagram.com",
    "facebook.com",
    "www.facebook.com",
    "fb.watch",
    "reddit.com",
    "www.reddit.com",
    "redd.it",
    "twitter.com",
    "www.twitter.com",
    "x.com",
    "www.x.com",
    "t.co",
    "twitch.tv",
    "www.twitch.tv",
    "soundcloud.com",
    "www.soundcloud.com",
    "snapchat.com",
    "www.snapchat.com",
    "linkedin.com",
    "www.linkedin.com",
    "pinterest.com",
    "www.pinterest.com",
    "pin.it",
    "dailymotion.com",
    "www.dailymotion.com",
    "dai.ly",
    "bsky.app",
    "www.bsky.app",
}

YOUTUBE_DOMAINS: set[str] = {
    "youtube.com",
    "www.youtube.com",
    "youtu.be",
    "m.youtube.com",
    "music.youtube.com",
}

TIKTOK_DOMAINS: set[str] = {
    "tiktok.com",
    "www.tiktok.com",
    "vm.tiktok.com",
    "vt.tiktok.com",
}

SNAPCHAT_DOMAINS: set[str] = {
    "snapchat.com",
    "www.snapchat.com",
    "t.snapchat.com",
    "web.snapchat.com",
}


def is_tiktok_url(url: str) -> bool:
    try:
        p = urlparse(url.lower())
        return p.hostname in TIKTOK_DOMAINS
    except:
        return False


def is_snapchat_url(url: str) -> bool:
    try:
        p = urlparse(url.lower())
        return p.hostname in SNAPCHAT_DOMAINS
    except:
        return False


RATE_LIMIT = int(os.getenv("RATE_LIMIT", "30"))
ANALYZE_CACHE_TTL = int(os.getenv("ANALYZE_CACHE_TTL", "300"))
ANALYZE_CACHE_SIZE = int(os.getenv("ANALYZE_CACHE_SIZE", "200"))

MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "6"))
FILE_TTL_SECONDS = int(os.getenv("FILE_TTL_SECONDS", "3600"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# ══════════════════════════════════════════════════════════════════════════════
# [LOGGING]
# ══════════════════════════════════════════════════════════════════════════════


class _JSONFormatter(logging.Formatter):
    _SKIP = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(
            {k: v for k, v in record.__dict__.items() if k not in self._SKIP}
        )
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def _setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()
    h = logging.StreamHandler()
    h.setFormatter(_JSONFormatter())
    root.addHandler(h)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("yt_dlp").setLevel(logging.WARNING)


_setup_logging(LOG_LEVEL)
logger = logging.getLogger("media_server")


# ══════════════════════════════════════════════════════════════════════════════
# [SECURITY]
# ══════════════════════════════════════════════════════════════════════════════

_PRIVATE_NETWORKS = [
    ipaddress.ip_network(c)
    for c in [
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "::1/128",
        "fc00::/7",
        "fe80::/10",
    ]
]


class URLValidationError(ValueError):
    pass


def _is_private_ip(host: str) -> bool:
    try:
        addr = ipaddress.ip_address(host)
        return any(addr in net for net in _PRIVATE_NETWORKS)
    except ValueError:
        return False


def validate_media_url(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        raise URLValidationError("URL must be a non-empty string.")
    raw = raw.strip()
    if len(raw) > 2048:
        raise URLValidationError("URL exceeds maximum length.")
    for bad in ("file://", "ftp://", "gopher://", "data:", "javascript:", "vbscript:"):
        if raw.lower().startswith(bad):
            raise URLValidationError("URL scheme not allowed.")
    try:
        p = urlparse(raw)
    except Exception:
        raise URLValidationError("Malformed URL.")
    if p.scheme.lower() not in ("https", "http"):
        raise URLValidationError("Only HTTPS/HTTP URLs are accepted.")
    host = p.hostname or ""
    if not host:
        raise URLValidationError("URL has no hostname.")
    if _is_private_ip(host):
        raise URLValidationError("Private/loopback addresses are not allowed.")
    if re.match(r"^\d+\.\d+\.\d+\.\d+$", host):
        raise URLValidationError("IP address URLs are not allowed.")
    if host.lower() not in ALLOWED_DOMAINS:
        raise URLValidationError("URL must point to a supported platform.")
    return urlunparse(
        (p.scheme.lower(), p.netloc.lower(), p.path, p.params, p.query, "")
    )


def is_youtube_url(url: str) -> bool:
    try:
        p = urlparse(url.lower())
        return p.hostname in YOUTUBE_DOMAINS
    except:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# [CACHE]
# ══════════════════════════════════════════════════════════════════════════════


@dataclass
class _CacheEntry:
    data: Any
    expires_at: float


class _LRUCache:
    def __init__(self, maxsize: int, ttl: int) -> None:
        self._maxsize = maxsize
        self._ttl = ttl
        self._store: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = Lock()
        self.hits = 0
        self.misses = 0

    def get(self, url: str) -> Optional[Any]:
        k = hashlib.sha256(url.encode()).hexdigest()
        with self._lock:
            entry = self._store.get(k)
            if entry is None or time.time() > entry.expires_at:
                if entry:
                    del self._store[k]
                self.misses += 1
                return None
            self._store.move_to_end(k)
            self.hits += 1
            return entry.data

    def set(self, url: str, data: Any) -> None:
        k = hashlib.sha256(url.encode()).hexdigest()
        with self._lock:
            if k in self._store:
                self._store.move_to_end(k)
            self._store[k] = _CacheEntry(data=data, expires_at=time.time() + self._ttl)
            while len(self._store) > self._maxsize:
                self._store.popitem(last=False)


analyze_cache = _LRUCache(maxsize=ANALYZE_CACHE_SIZE, ttl=ANALYZE_CACHE_TTL)


# ══════════════════════════════════════════════════════════════════════════════
# [QUEUE] YouTube Download Queue
# ══════════════════════════════════════════════════════════════════════════════


class JobStatus(str, Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    DONE = "done"
    FAILED = "failed"


@dataclass
class DownloadJob:
    job_id: str
    url: str
    format_id: str
    is_audio_only: bool = False
    status: JobStatus = JobStatus.QUEUED
    progress: int = 0
    speed: str = ""
    eta: str = ""
    message: str = "Queued"
    file_path: Optional[str] = None
    file_size: int = 0
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3
    ip_address: str = ""


class _DownloadQueue:
    def __init__(self) -> None:
        self._jobs: dict[str, DownloadJob] = {}
        self._queue: Optional[asyncio.Queue] = None
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self._progress_queue: asyncio.Queue = asyncio.Queue()
        self._ip_downloads: dict[str, int] = {}
        self._ip_lock = Lock()

    def _q(self) -> asyncio.Queue:
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=100)
        return self._queue

    def _check_ip_limit(self, ip: str) -> bool:
        with self._ip_lock:
            count = self._ip_downloads.get(ip, 0)
            return count < DOWNLOAD_RATE_LIMIT_PER_IP

    def _increment_ip_download(self, ip: str) -> None:
        with self._ip_lock:
            self._ip_downloads[ip] = self._ip_downloads.get(ip, 0) + 1

    def _decrement_ip_download(self, ip: str) -> None:
        with self._ip_lock:
            if ip in self._ip_downloads:
                self._ip_downloads[ip] = max(0, self._ip_downloads[ip] - 1)

    async def enqueue(
        self,
        url: str,
        format_id: str = "best",
        resolution: str = "best",
        is_audio_only: bool = False,
        ip_address: str = "",
    ) -> DownloadJob:
        if not self._check_ip_limit(ip_address):
            raise RuntimeError(
                f"Download limit exceeded for IP. Max: {DOWNLOAD_RATE_LIMIT_PER_IP} concurrent downloads."
            )

        q = self._q()
        if q.full():
            raise RuntimeError("Download queue is full.")

        job = DownloadJob(
            job_id=str(uuid.uuid4()),
            url=url,
            format_id=format_id,
            is_audio_only=is_audio_only,
            ip_address=ip_address,
        )
        self._jobs[job.job_id] = job
        self._increment_ip_download(ip_address)
        await q.put(job.job_id)
        return job

    def get_job(self, job_id: str) -> Optional[DownloadJob]:
        return self._jobs.get(job_id)

    async def update(self, job_id: str, **kwargs) -> None:
        job = self._jobs.get(job_id)
        if not job:
            return
        for k, v in kwargs.items():
            setattr(job, k, v)

    def update_sync(self, job_id: str, **kwargs) -> None:
        job = self._jobs.get(job_id)
        if not job:
            return
        for k, v in kwargs.items():
            setattr(job, k, v)

    async def _progress_loop(self) -> None:
        while True:
            try:
                job_id, kwargs = await self._progress_queue.get()
                await self.update(job_id, **kwargs)
            except Exception:
                pass

    async def worker_loop(self) -> None:
        q = self._q()
        asyncio.create_task(self._progress_loop())
        logger.info("Download worker started")

        while True:
            job_id = await q.get()
            job = self._jobs.get(job_id)
            if not job:
                q.task_done()
                continue

            async def process(j: DownloadJob) -> None:
                async with self._semaphore:
                    success = await _run_download(j)
                    if not success and j.retry_count < j.max_retries:
                        j.retry_count += 1
                        j.status = JobStatus.QUEUED
                        j.message = f"Retry {j.retry_count}/{j.max_retries}"
                        logger.warning(
                            f"Job {j.job_id} failed, scheduling retry {j.retry_count}"
                        )
                        await asyncio.sleep(j.retry_count * 2)
                        await q.put(j.job_id)
                    else:
                        self._decrement_ip_download(j.ip_address)
                q.task_done()

            asyncio.create_task(process(job))

    @property
    def active_count(self) -> int:
        return sum(1 for j in self._jobs.values() if j.status == JobStatus.DOWNLOADING)


download_queue = _DownloadQueue()


async def _run_download(job: DownloadJob) -> bool:
    """Download using yt-dlp for YouTube. Returns True on success, False on failure."""
    try:
        await download_queue.update(
            job.job_id, status=JobStatus.DOWNLOADING, message="Starting download..."
        )

        safe_title = re.sub(r"[^\w\s-]", "", job.url.split("?")[0].split("/")[-1][:50])
        ext = "m4a" if job.is_audio_only else "mp4"
        output_path = str(DOWNLOAD_FOLDER / f"{job.job_id}_{safe_title}.{ext}")

        format_id = job.format_id
        if job.is_audio_only:
            format_selector = "bestaudio/best"
        elif format_id and format_id != "best":
            format_selector = (
                f"{format_id}+bestaudio/best[ext=m4a]/{format_id}+bestaudio/best"
            )
        else:
            format_selector = "bestvideo+bestaudio/best"

        ydl_opts = {
            "format": format_selector,
            "outtmpl": output_path,
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 60,
            "retries": 10,
            "fragment_retries": 10,
            "concurrent_fragment_downloads": 8,
            "buffersize": 16384,
            "http_chunk_size": 524288,
            "continuedl": True,
            "nopart": False,
            "sleep_interval_requests": 2,
            "max_sleep_interval": 5,
            "nocheckcertificate": True,
            "http_headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            "progress_hooks": [],
        }

        if is_youtube_url(job.url):
            extractor_args = {
                "youtube": {
                    "player_client": ["android", "web"],
                    "player_skip": ["webpage", "configs"],
                }
            }
            ydl_opts["extractor_args"] = extractor_args
            cookie_file = str(BASE_DIR / "cookies.txt")
            if os.path.exists(cookie_file):
                ydl_opts["cookiefile"] = cookie_file

        job_id_local = job.job_id

        def progress_hook(d: dict) -> None:
            if d["status"] == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes") or 0
                speed = d.get("speed") or 0
                eta = d.get("eta") or 0
                progress = int((downloaded / total * 100)) if total > 0 else 0
                speed_str = f"{speed / 1024 / 1024:.1f} MB/s" if speed else ""
                eta_str = f"{eta}s" if eta else ""
                download_queue.update_sync(
                    job_id_local,
                    status=JobStatus.DOWNLOADING,
                    progress=progress,
                    speed=speed_str,
                    eta=eta_str,
                    message=f"Downloading... {progress}%",
                )

        ydl_opts["progress_hooks"] = [progress_hook]

        def _download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([job.url])

        await asyncio.to_thread(_download)

        job = download_queue.get_job(job.job_id)
        files = list(DOWNLOAD_FOLDER.glob(f"{job.job_id}_*"))

        if files:
            actual_path = str(files[0])
            size = os.path.getsize(actual_path)

            max_size_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
            if size > max_size_bytes:
                logger.error(f"File too large: {size} bytes (max: {max_size_bytes})")
                if os.path.exists(actual_path):
                    os.remove(actual_path)
                await download_queue.update(
                    job.job_id,
                    status=JobStatus.FAILED,
                    error=f"File too large. Max size: {MAX_FILE_SIZE_MB}MB",
                )
                return False

            await download_queue.update(
                job.job_id,
                status=JobStatus.DONE,
                progress=100,
                message="Complete",
                file_path=actual_path,
                file_size=size,
            )
            return True
        else:
            logger.error(f"No files found for job {job.job_id}")
            await download_queue.update(
                job.job_id, status=JobStatus.FAILED, error="Download failed"
            )
            return False

    except Exception as e:
        logger.error(f"Download error for job {job.job_id}: {e}")
        await download_queue.update(job.job_id, status=JobStatus.FAILED, error=str(e))
        return False


# ══════════════════════════════════════════════════════════════════════════════
# [DYNAMIC KEY SYSTEM]
# ══════════════════════════════════════════════════════════════════════════════

_session_keys: dict[str, dict] = {}
_session_lock = Lock()
SESSION_KEY_TTL_SECONDS = 3600


def _generate_session_key() -> str:
    import secrets

    return secrets.token_urlsafe(32)


def _validate_api_key(key: str) -> bool:
    if not key:
        return False

    with _session_lock:
        if key in _session_keys:
            session = _session_keys[key]
            if time.time() < session["expires_at"]:
                session["expires_at"] = time.time() + SESSION_KEY_TTL_SECONDS
                return True
            else:
                del _session_keys[key]
                return False

        if not ENABLE_API_KEY and ALLOW_DYNAMIC_KEYS:
            return True

        if API_KEY and key == API_KEY:
            return True

        return False


# ══════════════════════════════════════════════════════════════════════════════
# [MIDDLEWARE]
# ══════════════════════════════════════════════════════════════════════════════

_rate_store: dict[str, deque] = defaultdict(deque)
_rate_lock = Lock()


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("X-Forwarded-For")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(ip: str, limit: int, window: int = 60) -> bool:
    now = time.monotonic()
    cutoff = now - window
    with _rate_lock:
        q = _rate_store[ip]
        while q and q[0] < cutoff:
            q.popleft()
        if len(q) >= limit:
            return False
        q.append(now)
        return True


def _err(status: int, code: str, msg: str) -> JSONResponse:
    return JSONResponse(
        status_code=status, content={"error": {"code": code, "message": msg}}
    )


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        resp = await call_next(request)
        resp.headers.update(
            {
                "X-Content-Type-Options": "nosniff",
                "X-Frame-Options": "DENY",
                "Cache-Control": "no-store",
            }
        )
        return resp


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        ip = _client_ip(request)
        if not _check_rate_limit(ip, RATE_LIMIT):
            return _err(429, "RATE_LIMITED", "Too many requests.")
        return await call_next(request)


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if ENABLE_API_KEY:
            provided_key = request.headers.get(API_KEY_HEADER, "")
            if not _validate_api_key(provided_key):
                return _err(401, "UNAUTHORIZED", "Invalid or missing API key.")
        return await call_next(request)


# ══════════════════════════════════════════════════════════════════════════════
# [APP]
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="Vortex Media Server",
    version="8.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.add_middleware(TrustedHostMiddleware, allowed_hosts=ALLOWED_HOSTS)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(APIKeyAuthMiddleware)


async def _file_cleanup_worker() -> None:
    """Background worker to clean up old files and stale jobs."""
    while True:
        try:
            await asyncio.sleep(300)
            current_time = time.time()
            ttl = FILE_TTL_SECONDS
            cleaned = 0
            total_size = 0

            for file_path in DOWNLOAD_FOLDER.iterdir():
                if file_path.is_file():
                    file_age = current_time - file_path.stat().st_mtime
                    if file_age > ttl:
                        size = file_path.stat().st_size
                        file_path.unlink()
                        cleaned += 1
                        total_size += size

            expired_jobs = []
            for job_id, job in download_queue._jobs.items():
                job_age = current_time - job.created_at
                if job_age > ttl and job.status in (JobStatus.DONE, JobStatus.FAILED):
                    expired_jobs.append(job_id)

            for job_id in expired_jobs:
                download_queue._jobs.pop(job_id, None)

            if cleaned > 0:
                logger.info(
                    f"Cleaned {cleaned} files ({total_size / 1024 / 1024:.1f} MB), "
                    f"removed {len(expired_jobs)} stale jobs"
                )
        except Exception as e:
            logger.error(f"Cleanup error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(download_queue.worker_loop())
    asyncio.create_task(_file_cleanup_worker())
    logger.info(f"Vortex Media Server v8.0 started (Production mode)")
    logger.info(f"API Key auth: {'enabled' if ENABLE_API_KEY else 'disabled'}")
    logger.info(
        f"Max file size: {MAX_FILE_SIZE_MB}MB, Max duration: {MAX_DURATION_SECONDS}s"
    )
    yield
    logger.info("Server shutting down")


app.router.lifespan_context = lifespan


# ── Models ──────────────────────────────────────────────────────────────────


class AnalyzeRequest(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def must_be_valid_media_url(cls, v: str) -> str:
        return validate_media_url(v)


class DownloadRequest(BaseModel):
    url: str
    format_id: str = "best"
    resolution: str = "best"
    is_audio_only: bool = False

    @field_validator("url")
    @classmethod
    def must_be_valid_url(cls, v: str) -> str:
        return validate_media_url(v)


# ── /analyze ────────────────────────────────────────────────────────────────


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(float(value)) if value else None
    return None


class _Counter:
    def __init__(self):
        self._v = 0
        self._l = Lock()

    def inc(self, n=1):
        with self._l:
            self._v += n

    @property
    def value(self):
        return self._v


metrics_requests_total = _Counter()


@app.post("/analyze")
async def analyze(body: AnalyzeRequest, request: Request):
    """Analyze URL and return formats for all platforms."""
    ip = _client_ip(request)
    metrics_requests_total.inc()

    cached = analyze_cache.get(body.url)
    if cached:
        logger.info("Cache hit /analyze", extra={"ip": ip})
        return cached

    try:
        is_tiktok = is_tiktok_url(body.url)
        is_snapchat = is_snapchat_url(body.url)
        is_youtube = is_youtube_url(body.url)

        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "socket_timeout": 60,
            "retries": 10,
            "fragment_retries": 10,
            "ignoreerrors": is_tiktok or is_snapchat,
            "nocheckcertificate": True,
            "sleep_interval_requests": 2,
            "max_sleep_interval": 5,
            "http_headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        }

        if is_youtube:
            ydl_opts["extractor_args"] = {
                "youtube": {
                    "player_client": ["android", "web"],
                    "player_skip": ["webpage", "configs"],
                }
            }
            cookie_file = str(BASE_DIR / "cookies.txt")
            if os.path.exists(cookie_file):
                ydl_opts["cookiefile"] = cookie_file

        def _extract():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(body.url, download=False)

        info = await asyncio.to_thread(_extract)
        if info is None:
            raise RuntimeError("Failed to extract video info")

        video_formats = []
        audio_formats = []
        seen_resolutions: set[str] = set()

        formats_to_process = info.get("formats", []) or info.get(
            "requested_formats", []
        )

        for f in formats_to_process:
            format_id = f.get("format_id", "")
            vcodec = f.get("vcodec") or "none"
            acodec = f.get("acodec") or "none"
            height = f.get("height")
            width = f.get("width")
            ext = f.get("ext") or "mp4"
            url = f.get("url")
            tbr = f.get("tbr") or 0
            abr = f.get("abr") or 0
            fps = f.get("fps") or 0
            format_note = f.get("format_note") or ""
            fsize = _to_int(f.get("filesize")) or _to_int(f.get("filesize_approx")) or 0

            if not url or url.startswith(("rtmp", "mms")):
                continue
            if format_note in ("storyboard", "preview", "thumbnails"):
                continue

            has_video = vcodec not in ("none", "null", "")
            has_audio = acodec not in ("none", "null", "")

            if has_video:
                if height is None:
                    height = 0
                res = f"{height}p" if height > 0 else "Best"
                if res not in seen_resolutions:
                    seen_resolutions.add(res)
                    video_formats.append(
                        {
                            "format_id": format_id,
                            "ext": ext,
                            "resolution": res,
                            "width": width,
                            "height": height if height > 0 else None,
                            "fps": fps,
                            "filesize": fsize,
                            "video_url": url,
                            "audio_url": url if has_audio else None,
                            "is_combined": has_video and has_audio,
                            "has_audio_stream": has_audio,
                        }
                    )

            if has_audio and not has_video:
                bitrate = int(abr or tbr or 0)
                audio_formats.append(
                    {
                        "format_id": format_id,
                        "ext": ext,
                        "resolution": f"{bitrate}kbps" if bitrate else "Best Audio",
                        "bitrate": bitrate,
                        "filesize": fsize,
                        "audio_url": url,
                    }
                )

        video_formats.sort(
            key=lambda x: int(
                x.get("resolution", "0").replace("p", "").replace("Best", "9999")
            )
        )
        audio_formats.sort(key=lambda x: x.get("bitrate") or 0, reverse=True)

        if is_tiktok and not video_formats:
            video_formats.append(
                {
                    "format_id": "best",
                    "ext": "mp4",
                    "resolution": "Best",
                    "width": None,
                    "height": None,
                    "fps": 0,
                    "filesize": 0,
                    "video_url": None,
                    "audio_url": None,
                    "is_combined": True,
                }
            )

        if is_snapchat and not video_formats:
            direct_url = info.get("url") or info.get("video_url")
            if direct_url:
                video_formats.append(
                    {
                        "format_id": "best",
                        "ext": info.get("ext") or "mp4",
                        "resolution": "Best",
                        "width": None,
                        "height": None,
                        "fps": 0,
                        "filesize": _to_int(info.get("filesize"))
                        or _to_int(info.get("filesize_approx"))
                        or 0,
                        "video_url": direct_url,
                        "audio_url": direct_url,
                        "is_combined": True,
                        "has_audio_stream": True,
                    }
                )
            else:
                video_formats.append(
                    {
                        "format_id": "best",
                        "ext": "mp4",
                        "resolution": "Best",
                        "width": None,
                        "height": None,
                        "fps": 0,
                        "filesize": 0,
                        "video_url": None,
                        "audio_url": None,
                        "is_combined": True,
                    }
                )

        result = {
            "title": info.get("title"),
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "description": info.get("description"),
            "uploader": info.get("uploader")
            or info.get("channel")
            or info.get("creator"),
            "upload_date": info.get("upload_date"),
            "view_count": info.get("view_count"),
            "like_count": info.get("like_count"),
            "is_live": info.get("is_live") or False,
            "is_private": info.get("private") or False,
            "is_unlisted": info.get("unlisted") or False,
            "webpage_url": info.get("webpage_url"),
            "extractor": info.get("extractor"),
            "extractor_key": info.get("extractor_key"),
            "is_youtube": is_youtube_url(body.url),
            "is_tiktok": is_tiktok,
            "is_snapchat": is_snapchat_url(body.url),
            "formats": {"video": video_formats, "audio": audio_formats},
            "expires_in": 300,
        }

        analyze_cache.set(body.url, result)
        logger.info(
            "Analyze done", extra={"ip": ip, "platform": info.get("extractor_key")}
        )
        return result

    except URLValidationError as e:
        return _err(400, "INVALID_URL", str(e))
    except yt_dlp.utils.DownloadError as e:
        error_msg = str(e)
        if "age" in error_msg.lower() or "restricted" in error_msg.lower():
            return _err(403, "AGE_RESTRICTED", "This video is age-restricted.")
        if "private" in error_msg.lower():
            return _err(403, "PRIVATE_VIDEO", "This video is private.")
        if "not found" in error_msg.lower() or "404" in error_msg:
            return _err(404, "NOT_FOUND", "Video not found.")
        return _err(422, "EXTRACTION_FAILED", "Could not retrieve video info.")
    except Exception:
        logger.exception("Error in /analyze")
        return _err(500, "INTERNAL_ERROR", "An unexpected error occurred.")


# ── /download (YouTube, TikTok, Snapchat) ────────────────────────────────


@app.post("/download")
async def download(body: DownloadRequest, request: Request):
    """Start YouTube/TikTok/Snapchat download using yt-dlp."""
    if (
        not is_youtube_url(body.url)
        and not is_tiktok_url(body.url)
        and not is_snapchat_url(body.url)
    ):
        return _err(
            400,
            "NOT_SUPPORTED",
            "This endpoint is for YouTube/TikTok/Snapchat downloads only.",
        )

    ip = _client_ip(request)
    try:
        job = await download_queue.enqueue(
            url=body.url,
            format_id=body.format_id,
            resolution=body.resolution,
            is_audio_only=body.is_audio_only,
            ip_address=ip,
        )
        return {"status": "queued", "job_id": job.job_id, "message": "Download queued"}
    except RuntimeError as e:
        return _err(503, "QUEUE_FULL", str(e))
    except Exception:
        logger.exception("Error in /download")
        return _err(500, "INTERNAL_ERROR", "Failed to start download.")


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    """Get download job status."""
    job = download_queue.get_job(job_id)
    if not job:
        return _err(404, "NOT_FOUND", "Job not found.")

    resp = {
        "job_id": job.job_id,
        "status": job.status.value,
        "progress": job.progress,
        "speed": job.speed,
        "eta": job.eta,
        "message": job.message,
    }
    if job.status == JobStatus.DONE and job.file_path:
        resp["file_path"] = job.file_path
        resp["file_size"] = job.file_size
    if job.status == JobStatus.FAILED:
        resp["error"] = job.error
    return resp


@app.get("/file/{job_id}")
async def get_file(job_id: str):
    """Download the completed file and delete it immediately after."""
    job = download_queue.get_job(job_id)
    if not job:
        return _err(404, "NOT_FOUND", "Job not found.")
    if job.status != JobStatus.DONE:
        return _err(400, "NOT_READY", "Download not complete.")
    if not job.file_path or not os.path.exists(job.file_path):
        return _err(404, "FILE_NOT_FOUND", "File not found.")

    file_path = job.file_path
    filename = os.path.basename(file_path)

    def cleanup():
        try:
            import time

            time.sleep(10)
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted file after download: {file_path}")
            download_queue._jobs.pop(job_id, None)
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

    return FileResponse(
        path=file_path,
        filename=filename,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Accept-Ranges": "bytes",
        },
        background=BackgroundTask(cleanup),
    )


@app.head("/file/{job_id}")
async def head_file(job_id: str):
    """Get file info without downloading. Useful for resume support."""
    job = download_queue.get_job(job_id)
    if not job:
        return _err(404, "NOT_FOUND", "Job not found.")
    if job.status != JobStatus.DONE:
        return _err(400, "NOT_READY", "Download not complete.")
    if not job.file_path or not os.path.exists(job.file_path):
        return _err(404, "FILE_NOT_FOUND", "File not found.")

    file_path = job.file_path
    file_size = os.path.getsize(file_path)
    filename = os.path.basename(file_path)

    return Response(
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
            "Content-Type": "application/octet-stream",
        }
    )


# ── /stream (Zero Disk Usage) ────────────────────────────────────────────


async def _stream_generator(process):
    """Generator that yields chunks and ensures process cleanup."""
    try:
        while True:
            chunk = await process.stdout.read(65536)
            if not chunk:
                break
            yield chunk
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()


async def _generate_stream(url: str, format_selector: str, is_audio_only: bool):
    """Stream video/audio directly from yt-dlp without saving to disk."""
    output_path = "-"  # stdout

    process = await asyncio.create_subprocess_exec(
        "yt-dlp",
        "-f",
        format_selector,
        "-o",
        output_path,
        "--no-warnings",
        "--socket-timeout",
        "60",
        "--concurrent-fragments",
        "8",
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return _stream_generator(process)


@app.get("/stream")
async def stream_video(
    request: Request, url: str, format_id: str = "best", is_audio_only: bool = False
):
    """Stream video directly without saving to disk."""
    try:
        url = validate_media_url(url)
    except URLValidationError as e:
        return _err(400, "INVALID_URL", str(e))

    if is_audio_only:
        format_selector = "bestaudio/best"
    elif format_id and format_id != "best":
        format_selector = (
            f"{format_id}+bestaudio/best[ext=m4a]/{format_id}+bestaudio/best"
        )
    else:
        format_selector = "bestvideo+bestaudio/best"

    content_type = "audio/mp4" if is_audio_only else "video/mp4"
    filename = f"video.mp4" if not is_audio_only else "audio.m4a"

    stream = await _generate_stream(url, format_selector, is_audio_only)

    return StreamingResponse(
        stream,
        media_type=content_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


# ── Health ────────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    return {"status": "ok", "version": "8.0.0", "mode": "production"}


# ── Auth ──────────────────────────────────────────────────────────────────


class AuthRequest(BaseModel):
    api_key: str


@app.post("/auth/key")
async def generate_session_key(body: AuthRequest):
    """Generate a session key from an admin API key."""
    if not ENABLE_API_KEY:
        return {"error": {"code": "DISABLED", "message": "Auth is disabled"}}

    if API_KEY and body.api_key != API_KEY:
        return _err(401, "INVALID_KEY", "Invalid admin API key.")

    session_key = _generate_session_key()

    with _session_lock:
        _session_keys[session_key] = {
            "created_at": time.time(),
            "expires_at": time.time() + SESSION_KEY_TTL_SECONDS,
        }

    logger.info(f"Session key generated, TTL: {SESSION_KEY_TTL_SECONDS}s")

    return {
        "session_key": session_key,
        "expires_in": SESSION_KEY_TTL_SECONDS,
        "token_type": "Bearer",
    }


@app.post("/auth/refresh")
async def refresh_session_key(request: Request):
    """Refresh an existing session key."""
    current_key = request.headers.get(API_KEY_HEADER, "")

    with _session_lock:
        if current_key in _session_keys:
            session = _session_keys[current_key]
            session["expires_at"] = time.time() + SESSION_KEY_TTL_SECONDS
            return {
                "session_key": current_key,
                "expires_in": SESSION_KEY_TTL_SECONDS,
            }

    return _err(401, "INVALID_KEY", "Session key not found or expired.")


@app.get("/auth/status")
async def auth_status(request: Request):
    """Check auth status and active sessions."""
    current_key = request.headers.get(API_KEY_HEADER, "")
    is_valid = _validate_api_key(current_key)

    with _session_lock:
        active_count = len(_session_keys)

    return {
        "auth_enabled": ENABLE_API_KEY,
        "dynamic_keys_enabled": ALLOW_DYNAMIC_KEYS,
        "current_key_valid": is_valid,
        "active_sessions": active_count,
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception")
    return _err(500, "INTERNAL_ERROR", "An unexpected error occurred.")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "secure_media_server:app", host=HOST, port=PORT, workers=1, access_log=False
    )
