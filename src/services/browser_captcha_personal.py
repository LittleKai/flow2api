"""
Tự động hóa trình duyệt để lấy reCAPTCHA token
Dùng nodriver (kế thừa undetected-chromedriver) để triển khai trình duyệt chống phát hiện
Hỗ trợ chế độ thường trú: duy trì pool tab thường trú dùng chung toàn cục, sinh token tức thì
"""
import asyncio
import inspect
import time
import os
import sys
import re
import json
import shutil
import tempfile
import subprocess
import types
from typing import Optional, Dict, Any, Iterable

from ..core.logger import debug_logger
from ..core.config import config

# Tái dùng quy ước thư mục cache của chế độ browser, tránh thay đổi vị trí liên tục trong container.
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")


# ==================== Nhận diện môi trường Docker ====================
def _is_running_in_docker() -> bool:
    """Kiểm tra có đang chạy trong Docker container không."""
    # Cách 1: kiểm tra file /.dockerenv
    if os.path.exists('/.dockerenv'):
        return True
    # Cách 2: kiểm tra cgroup
    try:
        with open('/proc/1/cgroup', 'r') as f:
            content = f.read()
            if 'docker' in content or 'kubepods' in content or 'containerd' in content:
                return True
    except:
        pass
    # Cách 3: kiểm tra biến môi trường
    if os.environ.get('DOCKER_CONTAINER') or os.environ.get('KUBERNETES_SERVICE_HOST'):
        return True
    return False


IS_DOCKER = _is_running_in_docker()


def _is_truthy_env(name: str) -> bool:
    """Kiểm tra biến môi trường có bằng true không."""
    value = os.environ.get(name, "")
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_optional_bool_env(name: str) -> Optional[bool]:
    """Đọc biến môi trường bool tùy chọn; chưa set hoặc không nhận ra trả None."""
    value = os.environ.get(name)
    if value is None:
        return None

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


ALLOW_DOCKER_HEADED = (
    _is_truthy_env("ALLOW_DOCKER_HEADED_CAPTCHA")
    or _is_truthy_env("ALLOW_DOCKER_BROWSER_CAPTCHA")
)
DOCKER_HEADED_BLOCKED = IS_DOCKER and not ALLOW_DOCKER_HEADED


# ==================== Tự cài nodriver ====================
def _run_pip_install(package: str, use_mirror: bool = False) -> bool:
    """Chạy lệnh pip install.
    
    Args:
        package: tên package
        use_mirror: có dùng mirror trong nước không
    
    Returns:
        đã cài thành công chưa
    """
    cmd = [sys.executable, '-m', 'pip', 'install', package]
    if use_mirror:
        cmd.extend(['-i', 'https://pypi.tuna.tsinghua.edu.cn/simple'])
    
    try:
        debug_logger.log_info(f"[BrowserCaptcha] Đang cài {package}...")
        print(f"[BrowserCaptcha] Đang cài {package}...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode == 0:
            debug_logger.log_info(f"[BrowserCaptcha] ✅ Cài {package} thành công")
            print(f"[BrowserCaptcha] ✅ Cài {package} thành công")
            return True
        else:
            debug_logger.log_warning(f"[BrowserCaptcha] Cài {package} thất bại: {result.stderr[:200]}")
            return False
    except Exception as e:
        debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi cài {package}: {e}")
        return False


def _ensure_nodriver_installed() -> bool:
    """Đảm bảo nodriver đã được cài.
    
    Returns:
        đã cài thành công/đã sẵn có
    """
    try:
        import nodriver
        debug_logger.log_info("[BrowserCaptcha] nodriver đã được cài")
        return True
    except ImportError:
        pass
    
    debug_logger.log_info("[BrowserCaptcha] Chưa cài nodriver, bắt đầu tự cài...")
    print("[BrowserCaptcha] Chưa cài nodriver, bắt đầu tự cài...")
    
    # Thử nguồn chính thức trước
    if _run_pip_install('nodriver', use_mirror=False):
        return True
    
    # Nguồn chính thất bại, thử mirror trong nước
    debug_logger.log_info("[BrowserCaptcha] Cài từ nguồn chính thất bại, thử mirror trong nước...")
    print("[BrowserCaptcha] Cài từ nguồn chính thất bại, thử mirror trong nước...")
    if _run_pip_install('nodriver', use_mirror=True):
        return True
    
    debug_logger.log_error("[BrowserCaptcha] ❌ Tự cài nodriver thất bại, cài thủ công: pip install nodriver")
    print("[BrowserCaptcha] ❌ Tự cài nodriver thất bại, cài thủ công: pip install nodriver")
    return False


def _run_playwright_install(use_mirror: bool = False) -> bool:
    """Cài trình duyệt chromium cho playwright, tái dùng cách cài của chế độ browser."""
    cmd = [sys.executable, '-m', 'playwright', 'install', 'chromium']
    env = os.environ.copy()

    if use_mirror:
        env['PLAYWRIGHT_DOWNLOAD_HOST'] = 'https://npmmirror.com/mirrors/playwright'

    try:
        debug_logger.log_info("[BrowserCaptcha] Đang cài trình duyệt chromium...")
        print("[BrowserCaptcha] Đang cài trình duyệt chromium...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, env=env)
        if result.returncode == 0:
            debug_logger.log_info("[BrowserCaptcha] ✅ Cài trình duyệt chromium thành công")
            print("[BrowserCaptcha] ✅ Cài trình duyệt chromium thành công")
            return True

        debug_logger.log_warning(f"[BrowserCaptcha] Cài chromium thất bại: {result.stderr[:200]}")
        return False
    except Exception as e:
        debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi cài chromium: {e}")
        return False


def _ensure_playwright_installed() -> bool:
    """Đảm bảo playwright khả dụng để tái dùng binary chromium của nó."""
    try:
        import playwright  # noqa: F401
        debug_logger.log_info("[BrowserCaptcha] playwright đã được cài")
        return True
    except ImportError:
        pass

    debug_logger.log_info("[BrowserCaptcha] Chưa cài playwright, bắt đầu tự cài...")
    print("[BrowserCaptcha] Chưa cài playwright, bắt đầu tự cài...")

    if _run_pip_install('playwright', use_mirror=False):
        return True

    debug_logger.log_info("[BrowserCaptcha] Cài từ nguồn chính thất bại, thử mirror trong nước...")
    print("[BrowserCaptcha] Cài từ nguồn chính thất bại, thử mirror trong nước...")
    if _run_pip_install('playwright', use_mirror=True):
        return True

    debug_logger.log_error("[BrowserCaptcha] ❌ Tự cài playwright thất bại, cài thủ công: pip install playwright")
    print("[BrowserCaptcha] ❌ Tự cài playwright thất bại, cài thủ công: pip install playwright")
    return False


def _detect_playwright_browser_path() -> Optional[str]:
    """Đọc đường dẫn file chromium thực thi do playwright quản lý."""
    detect_script = (
        "from playwright.sync_api import sync_playwright\n"
        "with sync_playwright() as p:\n"
        "    print(p.chromium.executable_path or '')\n"
    )
    env = os.environ.copy()
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "0") or "0")

    try:
        result = subprocess.run(
            [sys.executable, "-c", detect_script],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
        browser_path_lines = (result.stdout or "").strip().splitlines()
        browser_path = browser_path_lines[-1].strip() if browser_path_lines else ""
        if result.returncode == 0 and browser_path and os.path.exists(browser_path):
            debug_logger.log_info(f"[BrowserCaptcha] Phát hiện playwright chromium: {browser_path}")
            return browser_path

        stderr_text = (result.stderr or "").strip()
        if stderr_text:
            debug_logger.log_warning(f"[BrowserCaptcha] Phát hiện playwright chromium thất bại: {stderr_text[:200]}")
    except Exception as e:
        debug_logger.log_info(f"[BrowserCaptcha] Lỗi khi phát hiện playwright chromium: {e}")

    return None


def _ensure_playwright_browser_path() -> Optional[str]:
    """Đảm bảo có binary chromium tái dùng được và trả về đường dẫn."""
    browser_path = _detect_playwright_browser_path()
    if browser_path:
        return browser_path

    if not _ensure_playwright_installed():
        return None

    debug_logger.log_info("[BrowserCaptcha] Chưa cài playwright chromium, bắt đầu tự cài...")
    print("[BrowserCaptcha] Chưa cài playwright chromium, bắt đầu tự cài...")

    if not _run_playwright_install(use_mirror=False):
        debug_logger.log_info("[BrowserCaptcha] Cài từ nguồn chính thất bại, thử mirror trong nước...")
        print("[BrowserCaptcha] Cài từ nguồn chính thất bại, thử mirror trong nước...")
        if not _run_playwright_install(use_mirror=True):
            debug_logger.log_error("[BrowserCaptcha] ❌ Tự cài chromium thất bại, cài thủ công: python -m playwright install chromium")
            print("[BrowserCaptcha] ❌ Tự cài chromium thất bại, cài thủ công: python -m playwright install chromium")
            return None

    return _detect_playwright_browser_path()


# Thử import nodriver
uc = None
NODRIVER_AVAILABLE = False
_NODRIVER_RUNTIME_PATCHED = False

if DOCKER_HEADED_BLOCKED:
    debug_logger.log_warning(
        "[BrowserCaptcha] Phát hiện môi trường Docker, mặc định vô hiệu Captcha trình duyệt tích hợp."
        "Nếu cần bật hãy đặt ALLOW_DOCKER_HEADED_CAPTCHA=true."
        "Chế độ personal mặc định hỗ trợ headless, không bắt buộc DISPLAY/Xvfb."
    )
    print("[BrowserCaptcha] ⚠️ Phát hiện môi trường Docker, mặc định vô hiệu Captcha trình duyệt tích hợp")
    print("[BrowserCaptcha] Nếu cần bật hãy đặt ALLOW_DOCKER_HEADED_CAPTCHA=true")
else:
    if IS_DOCKER and ALLOW_DOCKER_HEADED:
        debug_logger.log_warning(
            "[BrowserCaptcha] Đã bật whitelist Docker cho Captcha trình duyệt tích hợp; chế độ personal sẽ quyết định có cần DISPLAY/Xvfb theo cấu hình headless"
        )
        print("[BrowserCaptcha] ✅ Đã bật whitelist Docker cho Captcha trình duyệt tích hợp")
    if _ensure_nodriver_installed():
        try:
            import nodriver as uc
            NODRIVER_AVAILABLE = True
        except ImportError as e:
            debug_logger.log_error(f"[BrowserCaptcha] Import nodriver thất bại: {e}")
            print(f"[BrowserCaptcha] ❌ Import nodriver thất bại: {e}")


_RUNTIME_ERROR_KEYWORDS = (
    "has been closed",
    "browser has been closed",
    "target closed",
    "connection closed",
    "connection lost",
    "connection refused",
    "connection reset",
    "broken pipe",
    "session closed",
    "not attached to an active page",
    "no session with given id",
    "cannot find context with specified id",
    "websocket is not open",
    "no close frame received or sent",
    "cannot call write to closing transport",
    "cannot write to closing transport",
    "cannot call send once a close message has been sent",
    "connectionclosederror",
    "connectionrefusederror",
    "disconnected",
    "errno 111",
)

_NORMAL_CLOSE_KEYWORDS = (
    "connectionclosedok",
    "normal closure",
    "normal_closure",
    "sent 1000 (ok)",
    "received 1000 (ok)",
    "close(code=1000",
)


def _flatten_exception_text(error: Any) -> str:
    """Nối text chain exception để nhận dạng đồng nhất khi nodriver mất kết nối."""
    visited: set[int] = set()
    pending = [error]
    parts: list[str] = []

    while pending:
        current = pending.pop()
        if current is None:
            continue

        current_id = id(current)
        if current_id in visited:
            continue
        visited.add(current_id)

        parts.append(type(current).__name__)

        message = str(current or "").strip()
        if message:
            parts.append(message)

        args = getattr(current, "args", None)
        if isinstance(args, tuple):
            for arg in args:
                arg_text = str(arg or "").strip()
                if arg_text:
                    parts.append(arg_text)

        pending.append(getattr(current, "__cause__", None))
        pending.append(getattr(current, "__context__", None))

    return " | ".join(parts).lower()


def _is_runtime_disconnect_error(error: Any) -> bool:
    """Nhận dạng mất kết nối trình duyệt / websocket."""
    error_text = _flatten_exception_text(error)
    if not error_text:
        return False
    return any(keyword in error_text for keyword in _RUNTIME_ERROR_KEYWORDS) or any(
        keyword in error_text for keyword in _NORMAL_CLOSE_KEYWORDS
    )


def _is_runtime_normal_close_error(error: Any) -> bool:
    """Nhận dạng websocket đóng bình thường (1000) - rời phiên đúng dự kiến."""
    error_text = _flatten_exception_text(error)
    if not error_text:
        return False
    return any(keyword in error_text for keyword in _NORMAL_CLOSE_KEYWORDS)


def _finalize_nodriver_send_task(connection, transaction, tx_id: int, task: asyncio.Task):
    """Thu hồi exception background của nodriver websocket.send, tránh event loop in lỗi task chưa retrieve."""
    try:
        task.result()
    except asyncio.CancelledError:
        connection.mapper.pop(tx_id, None)
        if not transaction.done():
            transaction.cancel()
    except Exception as e:
        connection.mapper.pop(tx_id, None)
        if not transaction.done():
            try:
                transaction.set_exception(e)
            except Exception:
                pass

        if _is_runtime_normal_close_error(e):
            debug_logger.log_info(
                f"[BrowserCaptcha] nodriver websocket thoát sau khi đóng bình thường: {type(e).__name__}: {e}"
            )
        elif _is_runtime_disconnect_error(e):
            debug_logger.log_warning(
                f"[BrowserCaptcha] nodriver websocket.send thoát sau khi mất kết nối: {type(e).__name__}: {e}"
            )
        else:
            debug_logger.log_warning(
                f"[BrowserCaptcha] Ngoại lệ nodriver websocket.send: {type(e).__name__}: {e}"
            )


def _patch_nodriver_connection_instance(connection_instance):
    """Gom exception background của websocket.send ở cấp instance connection."""
    if not connection_instance or getattr(connection_instance, "_flow2api_send_patched", False):
        return

    try:
        from nodriver.core import connection as nodriver_connection_module
    except Exception as e:
        debug_logger.log_warning(f"[BrowserCaptcha] Load nodriver.connection thất bại, bỏ qua patch connection: {e}")
        return

    async def patched_send(self, cdp_obj, _is_update=False):
        if self.closed:
            await self.connect()
        if not _is_update:
            await self._register_handlers()

        transaction = nodriver_connection_module.Transaction(cdp_obj)
        tx_id = next(self.__count__)
        transaction.id = tx_id
        self.mapper[tx_id] = transaction

        send_task = asyncio.create_task(self.websocket.send(transaction.message))
        send_task.add_done_callback(
            lambda task, connection=self, tx=transaction, current_tx_id=tx_id:
            _finalize_nodriver_send_task(connection, tx, current_tx_id, task)
        )
        return await transaction

    connection_instance.send = types.MethodType(patched_send, connection_instance)
    connection_instance._flow2api_send_patched = True


def _patch_nodriver_browser_instance(browser_instance):
    """Gom update_targets ở cấp instance trình duyệt và bổ sung patch connection cho target mới."""
    if not browser_instance:
        return

    _patch_nodriver_connection_instance(getattr(browser_instance, "connection", None))
    for target in list(getattr(browser_instance, "targets", []) or []):
        _patch_nodriver_connection_instance(target)

    if getattr(browser_instance, "_flow2api_update_targets_patched", False):
        return

    original_update_targets = browser_instance.update_targets

    async def patched_update_targets(self, *args, **kwargs):
        try:
            result = await original_update_targets(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as e:
                if _is_runtime_disconnect_error(e):
                    log_message = (
                        f"[BrowserCaptcha] nodriver.update_targets thoát sau khi trình duyệt mất kết nối: "
                        f"{type(e).__name__}: {e}"
                    )
                    if _is_runtime_normal_close_error(e):
                        debug_logger.log_info(log_message)
                    else:
                        debug_logger.log_warning(log_message)
                    return []
                raise

        _patch_nodriver_connection_instance(getattr(self, "connection", None))
        for target in list(getattr(self, "targets", []) or []):
            _patch_nodriver_connection_instance(target)
        return result

    browser_instance.update_targets = types.MethodType(patched_update_targets, browser_instance)
    browser_instance._flow2api_update_targets_patched = True


def _patch_nodriver_runtime(browser_instance=None):
    """Patch instance trình duyệt nodriver để giảm noise khi mất kết nối và pass-through exception."""
    global _NODRIVER_RUNTIME_PATCHED

    if not NODRIVER_AVAILABLE or uc is None:
        return

    if browser_instance is not None:
        _patch_nodriver_browser_instance(browser_instance)

    if not _NODRIVER_RUNTIME_PATCHED:
        _NODRIVER_RUNTIME_PATCHED = True
        debug_logger.log_info("[BrowserCaptcha] Đã bật patch an toàn cho runtime nodriver")


def _parse_proxy_url(proxy_url: str):
    """Parse a proxy URL into (protocol, host, port, username, password)."""
    if not proxy_url:
        return None, None, None, None, None
    url = proxy_url.strip()
    if not re.match(r'^(http|https|socks5h?|socks5)://', url):
        url = f"http://{url}"
    m = re.match(r'^(socks5h?|socks5|http|https)://(?:([^:]+):([^@]+)@)?([^:]+):(\d+)$', url)
    if not m:
        return None, None, None, None, None
    protocol, username, password, host, port = m.groups()
    if protocol == "socks5h":
        protocol = "socks5"
    return protocol, host, port, username, password


def _create_proxy_auth_extension(protocol: str, host: str, port: str, username: str, password: str) -> str:
    """Create a temporary Chrome extension directory for proxy authentication.
    Returns the path to the extension directory."""
    ext_dir = tempfile.mkdtemp(prefix="nodriver_proxy_auth_")

    scheme_map = {"http": "http", "https": "https", "socks5": "socks5"}
    scheme = scheme_map.get(protocol, "http")

    manifest = {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Proxy Auth Helper",
        "permissions": [
            "proxy", "tabs", "unlimitedStorage", "storage",
            "<all_urls>", "webRequest", "webRequestBlocking"
        ],
        "background": {"scripts": ["background.js"]},
        "minimum_chrome_version": "76.0.0"
    }
    background_js = (
        "var config = {\n"
        '    mode: "fixed_servers",\n'
        "    rules: {\n"
        "        singleProxy: {\n"
        f'            scheme: "{scheme}",\n'
        f'            host: "{host}",\n'
        f"            port: parseInt({port})\n"
        "        },\n"
        '        bypassList: ["localhost"]\n'
        "    }\n"
        "};\n"
        'chrome.proxy.settings.set({value: config, scope: "regular"}, function(){});\n'
        "chrome.webRequest.onAuthRequired.addListener(\n"
        "    function(details) {\n"
        "        return {\n"
        "            authCredentials: {\n"
        f'                username: "{username}",\n'
        f'                password: "{password}"\n'
        "            }\n"
        "        };\n"
        "    },\n"
        '    {urls: ["<all_urls>"]},\n'
        "    ['blocking']\n"
        ");\n"
    )
    with open(os.path.join(ext_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    with open(os.path.join(ext_dir, "background.js"), "w", encoding="utf-8") as f:
        f.write(background_js)
    return ext_dir


class ResidentTabInfo:
    """Cấu trúc thông tin tab thường trú."""
    def __init__(self, tab, slot_id: str, project_id: Optional[str] = None):
        self.tab = tab
        self.slot_id = slot_id
        self.project_id = project_id or slot_id
        self.recaptcha_ready = False
        self.created_at = time.time()
        self.last_used_at = time.time()  # Thời điểm dùng lần cuối
        self.use_count = 0  # Số lần dùng
        self.fingerprint: Optional[Dict[str, Any]] = None
        self.solve_lock = asyncio.Lock()  # Tuần tự hóa thực thi trên cùng một tab, giảm xung đột concurrency


class BrowserCaptchaService:
    """Tự động hóa trình duyệt để lấy reCAPTCHA token (nodriver chế độ có giao diện).
    
    Hỗ trợ hai chế độ:
    1. Chế độ thường trú (Resident Mode): duy trì pool tab thường trú dùng chung toàn cục, ai giành được tab rảnh người đó thực thi
    2. Chế độ truyền thống (Legacy Mode): mỗi request tạo tab mới (fallback)
    """

    _instance: Optional['BrowserCaptchaService'] = None
    _lock = asyncio.Lock()

    def __init__(self, db=None):
        """Khởi tạo dịch vụ."""
        self.headless = self._resolve_headless_mode()  # Mặc định có giao diện, có thể dùng biến môi trường fallback về headless
        self.browser = None
        self._initialized = False
        self.website_key = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
        self.db = db
        # Dùng None để nodriver tự tạo thư mục tạm, tránh vấn đề khóa thư mục
        self.user_data_dir = None

        # Thuộc tính chế độ thường trú: tab Captcha là pool dùng chung toàn cục, không còn bind 1-1 theo project_id
        self._resident_tabs: dict[str, 'ResidentTabInfo'] = {}  # slot_id -> thông tin tab thường trú
        self._project_resident_affinity: dict[str, str] = {}  # project_id -> slot_id (lần dùng gần nhất)
        self._resident_slot_seq = 0
        self._resident_pick_index = 0
        self._resident_lock = asyncio.Lock()  # Bảo vệ thao tác tab thường trú
        self._browser_lock = asyncio.Lock()  # Bảo vệ init/đóng/restart trình duyệt, tránh dựng instance lặp lại
        self._runtime_recover_lock = asyncio.Lock()  # Tuần tự hóa recover cấp trình duyệt, tránh bão restart concurrent
        self._tab_build_lock = asyncio.Lock()  # Tuần tự hóa cold start/rebuild, giảm jitter nodriver
        self._legacy_lock = asyncio.Lock()  # Tránh legacy fallback tạo tab tạm mất kiểm soát khi concurrent
        self._max_resident_tabs = 5  # Số tab thường trú tối đa (hỗ trợ concurrency)
        self._idle_tab_ttl_seconds = 600  # Timeout idle cho tab (giây)
        self._idle_reaper_task: Optional[asyncio.Task] = None  # Tác vụ thu hồi tab idle
        self._command_timeout_seconds = 8.0
        self._navigation_timeout_seconds = 20.0
        self._solve_timeout_seconds = 45.0
        self._session_refresh_timeout_seconds = 45.0
        self._health_probe_ttl_seconds = max(
            0.0,
            float(getattr(config, "browser_personal_health_probe_ttl_seconds", 10.0) or 10.0),
        )
        self._last_health_probe_at = 0.0
        self._last_health_probe_ok = False
        self._fingerprint_cache_ttl_seconds = max(
            0.0,
            float(getattr(config, "browser_personal_fingerprint_ttl_seconds", 300.0) or 300.0),
        )
        self._last_fingerprint_at = 0.0

        # Tương thích API cũ (giữ single resident làm alias)
        self.resident_project_id: Optional[str] = None  # Tương thích ngược
        self.resident_tab = None                         # Tương thích ngược
        self._running = False                            # Tương thích ngược
        self._recaptcha_ready = False                    # Tương thích ngược
        self._last_fingerprint: Optional[Dict[str, Any]] = None
        self._resident_error_streaks: dict[str, int] = {}
        self._last_runtime_restart_at = 0.0
        self._proxy_url: Optional[str] = None
        self._proxy_ext_dir: Optional[str] = None
        # Tab thường trú giải Captcha cho site tùy chỉnh (dùng cho score-test)
        self._custom_tabs: dict[str, Dict[str, Any]] = {}
        self._custom_lock = asyncio.Lock()
        self._refresh_runtime_tunables()

    @classmethod
    async def get_instance(cls, db=None) -> 'BrowserCaptchaService':
        """Lấy instance singleton."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(db)
                    # Khởi động tác vụ thu hồi tab idle
                    cls._instance._idle_reaper_task = asyncio.create_task(
                        cls._instance._idle_tab_reaper_loop()
                    )
        return cls._instance

    async def reload_config(self):
        """Hot-update cấu hình (load lại từ database)."""
        from ..core.config import config
        old_max_tabs = self._max_resident_tabs
        old_idle_ttl = self._idle_tab_ttl_seconds
        old_probe_ttl = self._health_probe_ttl_seconds
        old_fingerprint_ttl = self._fingerprint_cache_ttl_seconds

        self._max_resident_tabs = config.personal_max_resident_tabs
        self._idle_tab_ttl_seconds = config.personal_idle_tab_ttl_seconds
        self._refresh_runtime_tunables()

        debug_logger.log_info(
            f"[BrowserCaptcha] Đã hot-update cấu hình Personal: "
            f"max_tabs {old_max_tabs}->{self._max_resident_tabs}, "
            f"idle_ttl {old_idle_ttl}s->{self._idle_tab_ttl_seconds}s, "
            f"probe_ttl {old_probe_ttl}s->{self._health_probe_ttl_seconds}s, "
            f"fingerprint_ttl {old_fingerprint_ttl}s->{self._fingerprint_cache_ttl_seconds}s"
        )

    def _resolve_headless_mode(self) -> bool:
        """Chế độ personal mặc định có giao diện, chỉ fallback headless khi biến môi trường yêu cầu rõ."""
        for env_name in ("PERSONAL_BROWSER_HEADLESS", "FLOW2API_PERSONAL_HEADLESS"):
            override = _get_optional_bool_env(env_name)
            if override is not None:
                debug_logger.log_info(
                    f"[BrowserCaptcha] Chế độ headless Personal điều khiển bởi biến môi trường {env_name}: {override}"
                )
                return override

        return False

    def _refresh_runtime_tunables(self):
        """Làm mới tham số tuning runtime, mặc định dùng giá trị bảo thủ ít tải."""
        try:
            self._health_probe_ttl_seconds = max(
                0.2,
                float(getattr(config, "browser_personal_health_probe_ttl_seconds", 10.0) or 10.0),
            )
        except Exception:
            self._health_probe_ttl_seconds = 10.0

        try:
            self._fingerprint_cache_ttl_seconds = max(
                0.0,
                float(getattr(config, "browser_personal_fingerprint_cache_ttl_seconds", 3600.0) or 3600.0),
            )
        except Exception:
            self._fingerprint_cache_ttl_seconds = 3600.0

    def _requires_virtual_display(self) -> bool:
        """Chỉ yêu cầu Docker/Linux cung cấp DISPLAY/Xvfb khi ở chế độ có giao diện rõ ràng."""
        return bool(IS_DOCKER and os.name == "posix" and not self.headless)

    def _check_available(self):
        """Kiểm tra dịch vụ có khả dụng không."""
        if DOCKER_HEADED_BLOCKED:
            raise RuntimeError(
                "Phát hiện môi trường Docker, mặc định vô hiệu Captcha trình duyệt tích hợp."
                "Nếu cần bật hãy đặt biến môi trường ALLOW_DOCKER_HEADED_CAPTCHA=true."
            )
        if self._requires_virtual_display() and not os.environ.get("DISPLAY"):
            raise RuntimeError(
                "Đã bật Captcha trình duyệt tích hợp trong Docker nhưng chưa đặt DISPLAY."
                "Vui lòng đặt DISPLAY (ví dụ :99) và khởi động Xvfb."
            )
        if not NODRIVER_AVAILABLE or uc is None:
            raise RuntimeError(
                "nodriver chưa được cài hoặc không khả dụng."
                "Vui lòng cài thủ công: pip install nodriver"
            )

    async def _run_with_timeout(self, awaitable, timeout_seconds: float, label: str):
        """Gom timeout thao tác nodriver để tránh một lần kẹt làm chậm cả luồng request."""
        effective_timeout = max(0.5, float(timeout_seconds or 0))
        try:
            return await asyncio.wait_for(awaitable, timeout=effective_timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError(f"{label} timeout ({effective_timeout:.1f}s)") from e

    async def _wait_for_display_ready(self, display_value: str, timeout_seconds: float = 5.0):
        """Trong Docker chế độ có giao diện, chờ Xvfb socket sẵn sàng để tránh fail khi vừa restart container."""
        if not (IS_DOCKER and display_value and display_value.startswith(":") and os.name == "posix"):
            return

        display_suffix = display_value.split(".", 1)[0].lstrip(":")
        if not display_suffix.isdigit():
            return

        socket_path = f"/tmp/.X11-unix/X{display_suffix}"
        deadline = time.monotonic() + max(0.5, float(timeout_seconds or 0))
        while time.monotonic() < deadline:
            if os.path.exists(socket_path):
                return
            await asyncio.sleep(0.1)

        raise RuntimeError(
            f"Socket Xvfb ứng với DISPLAY={display_value} chưa sẵn sàng: {socket_path}"
        )

    def _mark_browser_health(self, healthy: bool):
        self._last_health_probe_at = time.monotonic()
        self._last_health_probe_ok = bool(healthy)

    def _is_browser_health_fresh(self) -> bool:
        if not (self._initialized and self.browser and self._last_health_probe_ok):
            return False
        try:
            if self.browser.stopped:
                return False
        except Exception:
            return False
        ttl_seconds = max(0.0, float(self._health_probe_ttl_seconds or 0.0))
        if ttl_seconds <= 0:
            return False
        return (time.monotonic() - self._last_health_probe_at) < ttl_seconds

    def _is_fingerprint_cache_fresh(self) -> bool:
        if not self._last_fingerprint:
            return False
        ttl_seconds = max(0.0, float(self._fingerprint_cache_ttl_seconds or 0.0))
        if ttl_seconds <= 0:
            return False
        return (time.monotonic() - self._last_fingerprint_at) < ttl_seconds

    def _invalidate_browser_health(self):
        self._last_health_probe_at = 0.0
        self._last_health_probe_ok = False

    def _mark_runtime_restart(self):
        self._last_runtime_restart_at = time.time()

    def _was_runtime_restarted_recently(self, window_seconds: float = 5.0) -> bool:
        if self._last_runtime_restart_at <= 0.0:
            return False
        return (time.time() - self._last_runtime_restart_at) <= max(0.0, window_seconds)

    def _is_browser_runtime_error(self, error: Any) -> bool:
        """Nhận dạng exception điển hình khi runtime trình duyệt bị hỏng/đã đóng."""
        return _is_runtime_disconnect_error(error)

    def _decode_nodriver_object_entries(self, value: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(value, list):
            return None

        result: Dict[str, Any] = {}
        for entry in value:
            if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                return None
            key, entry_value = entry
            if not isinstance(key, str):
                return None
            result[key] = self._normalize_nodriver_evaluate_result(entry_value)
        return result

    def _normalize_nodriver_evaluate_result(self, value: Any) -> Any:
        if value is None:
            return None

        deep_serialized_value = getattr(value, "deep_serialized_value", None)
        if deep_serialized_value is not None:
            return self._normalize_nodriver_evaluate_result(deep_serialized_value)

        type_name = getattr(value, "type_", None)
        if type_name is not None and hasattr(value, "value"):
            raw_value = getattr(value, "value", None)
            if type_name == "object":
                object_entries = self._decode_nodriver_object_entries(raw_value)
                if object_entries is not None:
                    return object_entries
            if raw_value is not None:
                return self._normalize_nodriver_evaluate_result(raw_value)
            unserializable_value = getattr(value, "unserializable_value", None)
            if unserializable_value is not None:
                return str(unserializable_value)
            return value

        if isinstance(value, dict):
            typed_value_keys = {"type", "value", "objectId", "weakLocalObjectReference"}
            if "type" in value and set(value.keys()).issubset(typed_value_keys):
                raw_value = value.get("value")
                if value.get("type") == "object":
                    object_entries = self._decode_nodriver_object_entries(raw_value)
                    if object_entries is not None:
                        return object_entries
                return self._normalize_nodriver_evaluate_result(raw_value)
            return {
                key: self._normalize_nodriver_evaluate_result(item)
                for key, item in value.items()
            }

        if isinstance(value, list):
            object_entries = self._decode_nodriver_object_entries(value)
            if object_entries is not None:
                return object_entries
            return [self._normalize_nodriver_evaluate_result(item) for item in value]

        return value

    async def _probe_browser_runtime(self) -> bool:
        """Thăm dò nhẹ xem kết nối nodriver hiện tại còn khả dụng không."""
        if not self.browser:
            self._invalidate_browser_health()
            return False
        if self._is_browser_health_fresh():
            return True

        try:
            _ = self.browser.tabs
            await self._run_with_timeout(
                self.browser.connection.send("Browser.getVersion"),
                timeout_seconds=3.0,
                label="browser.health_probe",
            )
            self._mark_browser_health(True)
            return True
        except Exception as e:
            self._mark_browser_health(False)
            debug_logger.log_warning(f"[BrowserCaptcha] Health check trình duyệt thất bại: {e}")
            return False

    async def _recover_browser_runtime(self, project_id: Optional[str] = None, reason: str = "runtime_error") -> bool:
        """Khi runtime trình duyệt bị hỏng, ưu tiên restart toàn trình duyệt và khôi phục pool resident."""
        normalized_project_id = str(project_id or "").strip()
        async with self._runtime_recover_lock:
            if self.browser and self._initialized and not getattr(self.browser, "stopped", False):
                try:
                    if await self._probe_browser_runtime():
                        debug_logger.log_info(
                            f"[BrowserCaptcha] Runtime trình duyệt đã được coroutine khác khôi phục, tái dùng luôn (project_id={normalized_project_id or '<empty>'}, reason={reason})"
                        )
                        return True
                except Exception:
                    pass

            self._invalidate_browser_health()

            if normalized_project_id:
                try:
                    if await self._restart_browser_for_project_unlocked(normalized_project_id):
                        self._mark_runtime_restart()
                        return True
                except Exception as e:
                    debug_logger.log_warning(
                        f"[BrowserCaptcha] Khôi phục bằng restart trình duyệt thất bại (project_id={normalized_project_id}, reason={reason}): {e}"
                    )

            try:
                await self._shutdown_browser_runtime(cancel_idle_reaper=False, reason=f"recover:{reason}")
                await self.initialize()
                self._mark_runtime_restart()
                return True
            except Exception as e:
                debug_logger.log_error(f"[BrowserCaptcha] Khôi phục runtime trình duyệt thất bại ({reason}): {e}")
                return False

    async def _tab_evaluate(
        self,
        tab,
        script: str,
        label: str,
        timeout_seconds: Optional[float] = None,
        *,
        await_promise: bool = False,
        return_by_value: bool = True,
    ):
        result = await self._run_with_timeout(
            tab.evaluate(
                script,
                await_promise=await_promise,
                return_by_value=return_by_value,
            ),
            timeout_seconds or self._command_timeout_seconds,
            label,
        )
        if return_by_value:
            return self._normalize_nodriver_evaluate_result(result)
        return result

    async def _tab_get(self, tab, url: str, label: str, timeout_seconds: Optional[float] = None):
        return await self._run_with_timeout(
            tab.get(url),
            timeout_seconds or self._navigation_timeout_seconds,
            label,
        )

    async def _browser_get(self, url: str, label: str, new_tab: bool = False, timeout_seconds: Optional[float] = None):
        return await self._run_with_timeout(
            self.browser.get(url, new_tab=new_tab),
            timeout_seconds or self._navigation_timeout_seconds,
            label,
        )

    async def _tab_reload(self, tab, label: str, timeout_seconds: Optional[float] = None):
        return await self._run_with_timeout(
            tab.reload(),
            timeout_seconds or self._navigation_timeout_seconds,
            label,
        )

    async def _get_browser_cookies(self, label: str, timeout_seconds: Optional[float] = None):
        return await self._run_with_timeout(
            self.browser.cookies.get_all(),
            timeout_seconds or self._command_timeout_seconds,
            label,
        )

    async def _browser_send_command(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        label: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
    ):
        return await self._run_with_timeout(
            self.browser.connection.send(method, params) if params else self.browser.connection.send(method),
            timeout_seconds or self._command_timeout_seconds,
            label or method,
        )

    async def _idle_tab_reaper_loop(self):
        """Vòng lặp thu hồi tab idle."""
        while True:
            try:
                await asyncio.sleep(30)  # Kiểm tra mỗi 30 giây
                current_time = time.time()
                tabs_to_close = []

                async with self._resident_lock:
                    for slot_id, resident_info in list(self._resident_tabs.items()):
                        if resident_info.solve_lock.locked():
                            continue
                        idle_seconds = current_time - resident_info.last_used_at
                        if idle_seconds >= self._idle_tab_ttl_seconds:
                            tabs_to_close.append(slot_id)
                            debug_logger.log_info(
                                f"[BrowserCaptcha] slot={slot_id} idle {idle_seconds:.0f}s, chuẩn bị thu hồi"
                            )

                for slot_id in tabs_to_close:
                    await self._close_resident_tab(slot_id)

            except asyncio.CancelledError:
                return
            except Exception as e:
                debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi thu hồi tab idle: {e}")

    async def _evict_lru_tab_if_needed(self) -> bool:
        """Nếu đạt giới hạn pool chia sẻ, dùng LRU để loại bỏ tab idle lâu chưa dùng nhất."""
        async with self._resident_lock:
            if len(self._resident_tabs) < self._max_resident_tabs:
                return True

            lru_slot_id = None
            lru_project_hint = None
            lru_last_used = float('inf')

            for slot_id, resident_info in self._resident_tabs.items():
                if resident_info.solve_lock.locked():
                    continue
                if resident_info.last_used_at < lru_last_used:
                    lru_last_used = resident_info.last_used_at
                    lru_slot_id = slot_id
                    lru_project_hint = resident_info.project_id

        if lru_slot_id:
            debug_logger.log_info(
                f"[BrowserCaptcha] Số tab đạt giới hạn ({self._max_resident_tabs}), "
                f"loại bỏ slot cũ nhất slot={lru_slot_id}, project_hint={lru_project_hint}"
            )
            await self._close_resident_tab(lru_slot_id)
            return True

        debug_logger.log_warning(
            f"[BrowserCaptcha] Số tab đạt giới hạn ({self._max_resident_tabs}), "
            "nhưng hiện không có tab idle nào có thể loại bỏ an toàn"
        )
        return False

    async def _get_reserved_tab_ids(self) -> set[int]:
        """Thu thập các tab đang bị pool resident/custom chiếm; legacy không được tái dùng."""
        reserved_tab_ids: set[int] = set()

        async with self._resident_lock:
            for resident_info in self._resident_tabs.values():
                if resident_info and resident_info.tab:
                    reserved_tab_ids.add(id(resident_info.tab))

        async with self._custom_lock:
            for item in self._custom_tabs.values():
                tab = item.get("tab") if isinstance(item, dict) else None
                if tab:
                    reserved_tab_ids.add(id(tab))

        return reserved_tab_ids

    def _next_resident_slot_id(self) -> str:
        self._resident_slot_seq += 1
        return f"slot-{self._resident_slot_seq}"

    def _forget_project_affinity_for_slot_locked(self, slot_id: Optional[str]):
        if not slot_id:
            return
        stale_projects = [
            project_id
            for project_id, mapped_slot_id in self._project_resident_affinity.items()
            if mapped_slot_id == slot_id
        ]
        for project_id in stale_projects:
            self._project_resident_affinity.pop(project_id, None)

    def _resolve_affinity_slot_locked(self, project_id: Optional[str]) -> Optional[str]:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            return None
        slot_id = self._project_resident_affinity.get(normalized_project_id)
        if slot_id and slot_id in self._resident_tabs:
            return slot_id
        if slot_id:
            self._project_resident_affinity.pop(normalized_project_id, None)
        return None

    def _remember_project_affinity(self, project_id: Optional[str], slot_id: Optional[str], resident_info: Optional[ResidentTabInfo]):
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id or not slot_id or resident_info is None:
            return
        self._project_resident_affinity[normalized_project_id] = slot_id
        resident_info.project_id = normalized_project_id

    def _resolve_resident_slot_for_project_locked(
        self,
        project_id: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[ResidentTabInfo]]:
        """Ưu tiên mapping gần nhất; không có mapping thì fallback chọn toàn cục trong pool chung."""
        slot_id = self._resolve_affinity_slot_locked(project_id)
        if slot_id:
            resident_info = self._resident_tabs.get(slot_id)
            if resident_info and resident_info.tab:
                return slot_id, resident_info
        return self._select_resident_slot_locked(project_id)

    def _select_resident_slot_locked(
        self,
        project_id: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[ResidentTabInfo]]:
        candidates = [
            (slot_id, resident_info)
            for slot_id, resident_info in self._resident_tabs.items()
            if resident_info and resident_info.tab
        ]
        if not candidates:
            return None, None

        # Pool Captcha chia sẻ không còn bind theo project_id nữa; ở đây chỉ dựa vào "sẵn sàng / rảnh / lịch sử dùng"
        # để chọn toàn cục, tránh 4 token/4 project bị gán cứng vào tab cố định.
        ready_idle = [
            (slot_id, resident_info)
            for slot_id, resident_info in candidates
            if resident_info.recaptcha_ready and not resident_info.solve_lock.locked()
        ]
        ready_busy = [
            (slot_id, resident_info)
            for slot_id, resident_info in candidates
            if resident_info.recaptcha_ready and resident_info.solve_lock.locked()
        ]
        cold_idle = [
            (slot_id, resident_info)
            for slot_id, resident_info in candidates
            if not resident_info.recaptcha_ready and not resident_info.solve_lock.locked()
        ]

        pool = ready_idle or ready_busy or cold_idle or candidates
        pool.sort(key=lambda item: (item[1].last_used_at, item[1].use_count, item[1].created_at, item[0]))

        pick_index = self._resident_pick_index % len(pool)
        self._resident_pick_index = (self._resident_pick_index + 1) % max(len(candidates), 1)
        return pool[pick_index]

    async def _ensure_resident_tab(
        self,
        project_id: Optional[str] = None,
        *,
        force_create: bool = False,
        return_slot_key: bool = False,
    ):
        """Đảm bảo trong pool tab Captcha chia sẻ có tab khả dụng.

        Logic:
        - Ưu tiên tái dùng tab rảnh
        - Nếu mọi tab đều bận và chưa đạt giới hạn, tiếp tục mở rộng
        - Sau khi đạt giới hạn, cho phép request xếp hàng chờ tab có sẵn
        """
        def wrap(slot_id: Optional[str], resident_info: Optional[ResidentTabInfo]):
            if return_slot_key:
                return slot_id, resident_info
            return resident_info

        async with self._resident_lock:
            slot_id, resident_info = self._select_resident_slot_locked(project_id)
            if self._resident_tabs:
                all_busy = all(info.solve_lock.locked() for info in self._resident_tabs.values())
            else:
                all_busy = True

            should_create = force_create or not resident_info or (all_busy and len(self._resident_tabs) < self._max_resident_tabs)
            if not should_create:
                return wrap(slot_id, resident_info)

            if len(self._resident_tabs) >= self._max_resident_tabs:
                return wrap(slot_id, resident_info)

        async with self._tab_build_lock:
            async with self._resident_lock:
                slot_id, resident_info = self._select_resident_slot_locked(project_id)
                if self._resident_tabs:
                    all_busy = all(info.solve_lock.locked() for info in self._resident_tabs.values())
                else:
                    all_busy = True

                should_create = force_create or not resident_info or (all_busy and len(self._resident_tabs) < self._max_resident_tabs)
                if not should_create:
                    return wrap(slot_id, resident_info)

                if len(self._resident_tabs) >= self._max_resident_tabs:
                    return wrap(slot_id, resident_info)

                new_slot_id = self._next_resident_slot_id()

            resident_info = await self._create_resident_tab(new_slot_id, project_id=project_id)
            if resident_info is None:
                async with self._resident_lock:
                    slot_id, fallback_info = self._select_resident_slot_locked(project_id)
                return wrap(slot_id, fallback_info)

            async with self._resident_lock:
                self._resident_tabs[new_slot_id] = resident_info
                self._sync_compat_resident_state()
                return wrap(new_slot_id, resident_info)

    async def _rebuild_resident_tab(
        self,
        project_id: Optional[str] = None,
        *,
        slot_id: Optional[str] = None,
        return_slot_key: bool = False,
    ):
        """Rebuild một tab trong pool chia sẻ. Ưu tiên rebuild slot gần dùng nhất của project hiện tại."""
        def wrap(actual_slot_id: Optional[str], resident_info: Optional[ResidentTabInfo]):
            if return_slot_key:
                return actual_slot_id, resident_info
            return resident_info

        async with self._tab_build_lock:
            async with self._resident_lock:
                actual_slot_id = slot_id
                if actual_slot_id is None:
                    actual_slot_id, _ = self._resolve_resident_slot_for_project_locked(project_id)

                old_resident = self._resident_tabs.pop(actual_slot_id, None) if actual_slot_id else None
                self._forget_project_affinity_for_slot_locked(actual_slot_id)
                if actual_slot_id:
                    self._resident_error_streaks.pop(actual_slot_id, None)
                self._sync_compat_resident_state()

            if old_resident:
                try:
                    async with old_resident.solve_lock:
                        await self._close_tab_quietly(old_resident.tab)
                except Exception:
                    await self._close_tab_quietly(old_resident.tab)

            actual_slot_id = actual_slot_id or self._next_resident_slot_id()
            resident_info = await self._create_resident_tab(actual_slot_id, project_id=project_id)
            if resident_info is None:
                debug_logger.log_warning(
                    f"[BrowserCaptcha] slot={actual_slot_id}, project_id={project_id} rebuild tab chia sẻ thất bại"
                )
                return wrap(actual_slot_id, None)

            async with self._resident_lock:
                self._resident_tabs[actual_slot_id] = resident_info
                self._remember_project_affinity(project_id, actual_slot_id, resident_info)
                self._sync_compat_resident_state()
                return wrap(actual_slot_id, resident_info)

    def _sync_compat_resident_state(self):
        """Đồng bộ thuộc tính tương thích single resident phiên bản cũ."""
        first_resident = next(iter(self._resident_tabs.values()), None)
        if first_resident:
            self.resident_project_id = first_resident.project_id
            self.resident_tab = first_resident.tab
            self._running = True
            self._recaptcha_ready = bool(first_resident.recaptcha_ready)
        else:
            self.resident_project_id = None
            self.resident_tab = None
            self._running = False
            self._recaptcha_ready = False

    async def _close_tab_quietly(self, tab):
        if not tab:
            return
        try:
            await self._run_with_timeout(
                tab.close(),
                timeout_seconds=5.0,
                label="tab.close",
            )
        except Exception:
            pass

    async def _disconnect_browser_connection_quietly(self, browser_instance, reason: str):
        """Cố gắng đóng DevTools websocket trước để giảm background task nodriver crash khi trình duyệt thoát."""
        if not browser_instance:
            return

        connection = getattr(browser_instance, "connection", None)
        disconnect_method = getattr(connection, "disconnect", None) if connection else None
        if disconnect_method is None:
            return

        try:
            result = disconnect_method()
            if inspect.isawaitable(result):
                await self._run_with_timeout(
                    result,
                    timeout_seconds=5.0,
                    label=f"browser.disconnect:{reason}",
                )
            await asyncio.sleep(0)
        except Exception as e:
            if self._is_browser_runtime_error(e):
                debug_logger.log_warning(
                    f"[BrowserCaptcha] Phát hiện trạng thái đã mất kết nối khi đóng kết nối trình duyệt ({reason}): {e}"
                )
                return
            debug_logger.log_warning(
                f"[BrowserCaptcha] Ngoại lệ khi đóng kết nối trình duyệt ({reason}): {type(e).__name__}: {e}"
            )

    async def _stop_browser_process(self, browser_instance, reason: str = "browser_stop"):
        """Tương thích API stop đồng bộ của nodriver, dừng tiến trình trình duyệt an toàn."""
        if not browser_instance:
            return

        await self._disconnect_browser_connection_quietly(browser_instance, reason=reason)

        stop_method = getattr(browser_instance, "stop", None)
        if stop_method is None:
            return
        result = stop_method()
        if inspect.isawaitable(result):
            await self._run_with_timeout(
                result,
                timeout_seconds=10.0,
                label="browser.stop",
            )

    async def _shutdown_browser_runtime_locked(self, reason: str):
        """Dưới điều kiện đang giữ _browser_lock, dọn sạch runtime trình duyệt hiện tại."""
        browser_instance = self.browser
        self.browser = None
        self._initialized = False
        self._last_fingerprint = None
        self._last_fingerprint_at = 0.0
        self._mark_browser_health(False)
        self._cleanup_proxy_extension()
        self._proxy_url = None

        async with self._resident_lock:
            resident_items = list(self._resident_tabs.values())
            self._resident_tabs.clear()
            self._project_resident_affinity.clear()
            self._resident_error_streaks.clear()
            self._sync_compat_resident_state()

        custom_items = list(self._custom_tabs.values())
        self._custom_tabs.clear()

        closed_tabs = set()

        async def close_once(tab):
            if not tab:
                return
            tab_key = id(tab)
            if tab_key in closed_tabs:
                return
            closed_tabs.add(tab_key)
            await self._close_tab_quietly(tab)

        for resident_info in resident_items:
            await close_once(resident_info.tab)

        for item in custom_items:
            tab = item.get("tab") if isinstance(item, dict) else None
            await close_once(tab)

        if browser_instance:
            try:
                await self._stop_browser_process(browser_instance, reason=reason)
            except Exception as e:
                debug_logger.log_warning(
                    f"[BrowserCaptcha] Dừng instance trình duyệt thất bại ({reason}): {e}"
                )

    async def _resolve_personal_proxy(self):
        """Read proxy config for personal captcha browser.
        Priority: captcha browser_proxy > request proxy."""
        if not self.db:
            return None, None, None, None, None
        try:
            captcha_cfg = await self.db.get_captcha_config()
            if captcha_cfg.browser_proxy_enabled and captcha_cfg.browser_proxy_url:
                url = captcha_cfg.browser_proxy_url.strip()
                if url:
                    debug_logger.log_info(f"[BrowserCaptcha] Personal dùng proxy Captcha: {url}")
                    return _parse_proxy_url(url)
        except Exception as e:
            debug_logger.log_warning(f"[BrowserCaptcha] Đọc cấu hình proxy Captcha thất bại: {e}")
        try:
            proxy_cfg = await self.db.get_proxy_config()
            if proxy_cfg and proxy_cfg.enabled and proxy_cfg.proxy_url:
                url = proxy_cfg.proxy_url.strip()
                if url:
                    debug_logger.log_info(f"[BrowserCaptcha] Personal fallback dùng proxy request: {url}")
                    return _parse_proxy_url(url)
        except Exception as e:
            debug_logger.log_warning(f"[BrowserCaptcha] Đọc cấu hình proxy request thất bại: {e}")
        return None, None, None, None, None

    def _cleanup_proxy_extension(self):
        """Remove temporary proxy auth extension directory."""
        if self._proxy_ext_dir and os.path.isdir(self._proxy_ext_dir):
            try:
                shutil.rmtree(self._proxy_ext_dir, ignore_errors=True)
            except Exception:
                pass
            self._proxy_ext_dir = None

    async def initialize(self):
        """Khởi tạo trình duyệt nodriver."""
        self._check_available()

        if (
            self._initialized
            and self.browser
            and not self.browser.stopped
            and self._is_browser_health_fresh()
        ):
            if self._idle_reaper_task is None or self._idle_reaper_task.done():
                self._idle_reaper_task = asyncio.create_task(self._idle_tab_reaper_loop())
            return

        async with self._browser_lock:
            browser_needs_restart = False
            browser_executable_path = None
            display_value = os.environ.get("DISPLAY", "").strip()
            browser_args = []

            if self._initialized and self.browser:
                try:
                    if self.browser.stopped:
                        debug_logger.log_warning("[BrowserCaptcha] Trình duyệt đã dừng, chuẩn bị khởi tạo lại...")
                        self._mark_browser_health(False)
                        browser_needs_restart = True
                    elif self._is_browser_health_fresh():
                        if self._idle_reaper_task is None or self._idle_reaper_task.done():
                            self._idle_reaper_task = asyncio.create_task(self._idle_tab_reaper_loop())
                        return
                    elif not await self._probe_browser_runtime():
                        debug_logger.log_warning("[BrowserCaptcha] Kết nối trình duyệt đã không hoạt động, chuẩn bị khởi tạo lại...")
                        browser_needs_restart = True
                    else:
                        _patch_nodriver_runtime(self.browser)
                        if self._idle_reaper_task is None or self._idle_reaper_task.done():
                            self._idle_reaper_task = asyncio.create_task(self._idle_tab_reaper_loop())
                        return
                except Exception as e:
                    debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi kiểm tra trạng thái trình duyệt, chuẩn bị khởi tạo lại: {e}")
                    browser_needs_restart = True
            elif self.browser is not None or self._initialized:
                browser_needs_restart = True

            if browser_needs_restart:
                await self._shutdown_browser_runtime_locked(reason="initialize_recovery")

            try:
                if self.user_data_dir:
                    debug_logger.log_info(f"[BrowserCaptcha] Đang khởi động trình duyệt nodriver (thư mục user data: {self.user_data_dir})...")
                    os.makedirs(self.user_data_dir, exist_ok=True)
                else:
                    debug_logger.log_info(f"[BrowserCaptcha] Đang khởi động trình duyệt nodriver (dùng thư mục tạm)...")

                browser_executable_path = os.environ.get("BROWSER_EXECUTABLE_PATH", "").strip() or None
                if browser_executable_path and not os.path.exists(browser_executable_path):
                    debug_logger.log_warning(
                        f"[BrowserCaptcha] Trình duyệt chỉ định không tồn tại, chuyển sang tự phát hiện: {browser_executable_path}"
                    )
                    browser_executable_path = None
                if not browser_executable_path:
                    playwright_browser_path = _ensure_playwright_browser_path()
                    if playwright_browser_path:
                        browser_executable_path = playwright_browser_path
                        debug_logger.log_info(
                            f"[BrowserCaptcha] Tái dùng playwright chromium làm trình duyệt nodriver: {browser_executable_path}"
                        )
                if browser_executable_path:
                    debug_logger.log_info(
                        f"[BrowserCaptcha] Dùng file thực thi trình duyệt chỉ định: {browser_executable_path}"
                    )
                    try:
                        version_result = subprocess.run(
                            [browser_executable_path, "--version"],
                            capture_output=True,
                            text=True,
                            timeout=10,
                        )
                        version_output = (
                            (version_result.stdout or "").strip()
                            or (version_result.stderr or "").strip()
                            or "<empty>"
                        )
                        debug_logger.log_info(
                            "[BrowserCaptcha] Dò phiên bản trình duyệt: "
                            f"rc={version_result.returncode}, output={version_output[:200]}"
                        )
                    except Exception as version_error:
                        debug_logger.log_warning(
                            f"[BrowserCaptcha] Dò phiên bản trình duyệt thất bại: {version_error}"
                        )

                # Parse cấu hình proxy
                self._cleanup_proxy_extension()
                self._proxy_url = None
                protocol, host, port, username, password = await self._resolve_personal_proxy()
                proxy_server_arg = None
                if protocol and host and port:
                    if username and password:
                        self._proxy_ext_dir = _create_proxy_auth_extension(protocol, host, port, username, password)
                        debug_logger.log_info(
                            f"[BrowserCaptcha] Proxy Personal cần xác thực, đã tạo extension: {self._proxy_ext_dir}"
                        )
                    proxy_server_arg = f"--proxy-server={protocol}://{host}:{port}"
                    self._proxy_url = f"{protocol}://{host}:{port}"
                    debug_logger.log_info(f"[BrowserCaptcha] Proxy trình duyệt Personal: {self._proxy_url}")

                launch_in_background = bool(getattr(config, "browser_launch_background", True))
                browser_args = [
                    '--disable-quic',
                    '--disable-features=UseDnsHttpsSvcb',
                    '--disable-dev-shm-usage',
                    '--disable-setuid-sandbox',
                    '--disable-gpu',
                    '--disable-infobars',
                    '--hide-scrollbars',
                    '--window-size=1280,720',
                    '--profile-directory=Default',
                    '--disable-background-networking',
                    '--disable-sync',
                    '--disable-translate',
                    '--disable-default-apps',
                    '--no-first-run',
                    '--no-default-browser-check',
                    '--no-zygote',
                ]
                if launch_in_background and not self.headless:
                    browser_args.extend([
                        '--start-minimized',
                        '--disable-background-timer-throttling',
                        '--disable-renderer-backgrounding',
                        '--disable-backgrounding-occluded-windows',
                    ])
                    if sys.platform.startswith("win"):
                        browser_args.append('--window-position=-32000,-32000')
                    else:
                        browser_args.append('--window-position=3000,3000')
                    debug_logger.log_info("[BrowserCaptcha] Trình duyệt Personal có giao diện sẽ khởi động ở chế độ nền")
                elif not self.headless:
                    debug_logger.log_info("[BrowserCaptcha] Trình duyệt Personal có giao diện sẽ khởi động ở cửa sổ hiển thị")
                if proxy_server_arg:
                    browser_args.append(proxy_server_arg)
                if self._proxy_ext_dir:
                    browser_args.append(f'--load-extension={self._proxy_ext_dir}')
                else:
                    browser_args.append('--disable-extensions')

                effective_launch_args = list(browser_args)
                if self._requires_virtual_display():
                    await self._wait_for_display_ready(display_value)

                effective_uid = "n/a"
                if hasattr(os, "geteuid"):
                    try:
                        effective_uid = str(os.geteuid())
                    except Exception:
                        effective_uid = "unknown"

                launch_kwargs = {
                    "headless": self.headless,
                    "user_data_dir": self.user_data_dir,
                    "browser_executable_path": browser_executable_path,
                    "browser_args": browser_args,
                    "sandbox": False,
                }
                launch_config = uc.Config(**launch_kwargs)
                effective_launch_args = launch_config()
                debug_logger.log_info(
                    "[BrowserCaptcha] Context khởi động nodriver: "
                    f"docker={IS_DOCKER}, display={display_value or '<empty>'}, "
                    f"uid={effective_uid}, headless={self.headless}, background={launch_in_background}, sandbox=False, "
                    f"executable={browser_executable_path or '<auto>'}, "
                    f"args={' '.join(effective_launch_args)}"
                )

                # Khởi động trình duyệt nodriver (chạy nền, không chiếm foreground)
                try:
                    self.browser = await self._run_with_timeout(
                        uc.start(**launch_kwargs),
                        timeout_seconds=30.0,
                        label="nodriver.start",
                    )
                except Exception as start_error:
                    error_text = str(start_error or "").lower()
                    needs_explicit_no_sandbox = "no_sandbox" in error_text or "root" in error_text
                    if not needs_explicit_no_sandbox:
                        raise

                    fallback_browser_args = list(browser_args)
                    if '--no-sandbox' not in fallback_browser_args:
                        fallback_browser_args.append('--no-sandbox')

                    fallback_kwargs = dict(launch_kwargs)
                    fallback_kwargs["browser_args"] = fallback_browser_args
                    fallback_kwargs["sandbox"] = True
                    fallback_config = uc.Config(**fallback_kwargs)
                    effective_launch_args = fallback_config()
                    debug_logger.log_warning(
                        "[BrowserCaptcha] nodriver khởi động lần đầu thất bại, retry với --no-sandbox rõ ràng: "
                        f"{type(start_error).__name__}: {start_error}"
                    )
                    self.browser = await self._run_with_timeout(
                        uc.start(**fallback_kwargs),
                        timeout_seconds=30.0,
                        label="nodriver.start.retry_no_sandbox",
                    )

                _patch_nodriver_runtime(self.browser)
                self._initialized = True
                self._mark_browser_health(True)
                if self._idle_reaper_task is None or self._idle_reaper_task.done():
                    self._idle_reaper_task = asyncio.create_task(self._idle_tab_reaper_loop())
                debug_logger.log_info(f"[BrowserCaptcha] ✅ Trình duyệt nodriver đã khởi động (Profile: {self.user_data_dir})")

            except Exception as e:
                self.browser = None
                self._initialized = False
                self._mark_browser_health(False)
                debug_logger.log_error(
                    "[BrowserCaptcha] ❌ Khởi động trình duyệt thất bại: "
                    f"{type(e).__name__}: {str(e)} | "
                    f"display={display_value or '<empty>'} | "
                    f"executable={browser_executable_path or '<auto>'} | "
                    f"args={' '.join(effective_launch_args) if effective_launch_args else '<none>'}"
                )
                raise

    async def warmup_resident_tabs(self, project_ids: Iterable[str], limit: Optional[int] = None) -> list[str]:
        """Warm-up pool tab Captcha chia sẻ để giảm jitter cold start cho request đầu."""
        normalized_project_ids: list[str] = []
        seen_projects = set()
        for raw_project_id in project_ids:
            project_id = str(raw_project_id or "").strip()
            if not project_id or project_id in seen_projects:
                continue
            seen_projects.add(project_id)
            normalized_project_ids.append(project_id)

        await self.initialize()

        try:
            warm_limit = self._max_resident_tabs if limit is None else max(1, min(self._max_resident_tabs, int(limit)))
        except Exception:
            warm_limit = self._max_resident_tabs

        warmed_slots: list[str] = []
        for index in range(warm_limit):
            warm_project_id = normalized_project_ids[index] if index < len(normalized_project_ids) else f"warmup-{index + 1}"
            slot_id, resident_info = await self._ensure_resident_tab(
                warm_project_id,
                force_create=True,
                return_slot_key=True,
            )
            if resident_info and resident_info.tab and slot_id:
                if slot_id not in warmed_slots:
                    warmed_slots.append(slot_id)
                continue
            debug_logger.log_warning(f"[BrowserCaptcha] Warm-up tab chia sẻ thất bại (seed={warm_project_id})")

        return warmed_slots

    # ========== API chế độ thường trú ==========

    async def start_resident_mode(self, project_id: str):
        """Khởi động chế độ thường trú.
        
        Args:
            project_id: ID project dùng cho thường trú
        """
        if not str(project_id or "").strip():
            debug_logger.log_warning("[BrowserCaptcha] Khởi động chế độ thường trú thất bại: project_id rỗng")
            return

        warmed_slots = await self.warmup_resident_tabs([project_id], limit=1)
        if warmed_slots:
            debug_logger.log_info(f"[BrowserCaptcha] ✅ Đã khởi động pool Captcha thường trú chia sẻ (seed_project: {project_id})")
            return

        debug_logger.log_error(f"[BrowserCaptcha] Khởi động chế độ thường trú thất bại (seed_project: {project_id})")

    async def stop_resident_mode(self, project_id: Optional[str] = None):
        """Dừng chế độ thường trú.
        
        Args:
            project_id: chỉ định project_id hoặc slot_id; nếu None sẽ đóng mọi tab thường trú
        """
        target_slot_id = None
        if project_id:
            async with self._resident_lock:
                target_slot_id = project_id if project_id in self._resident_tabs else self._resolve_affinity_slot_locked(project_id)

        if target_slot_id:
            await self._close_resident_tab(target_slot_id)
            self._resident_error_streaks.pop(target_slot_id, None)
            debug_logger.log_info(f"[BrowserCaptcha] Đã đóng tab chia sẻ slot={target_slot_id} (request={project_id})")
            return

        async with self._resident_lock:
            slot_ids = list(self._resident_tabs.keys())
            resident_items = list(self._resident_tabs.values())
            self._resident_tabs.clear()
            self._project_resident_affinity.clear()
            self._resident_error_streaks.clear()
            self._sync_compat_resident_state()

        for resident_info in resident_items:
            if resident_info and resident_info.tab:
                await self._close_tab_quietly(resident_info.tab)
        debug_logger.log_info(f"[BrowserCaptcha] Đã đóng tất cả tab thường trú chia sẻ (tổng {len(slot_ids)} tab)")

    async def _wait_for_document_ready(self, tab, retries: int = 30, interval_seconds: float = 1.0) -> bool:
        """Chờ document của trang load xong."""
        for _ in range(retries):
            try:
                ready_state = await self._tab_evaluate(
                    tab,
                    "document.readyState",
                    label="document.readyState",
                    timeout_seconds=2.0,
                )
                if ready_state == "complete":
                    return True
            except Exception:
                pass
            await asyncio.sleep(interval_seconds)
        return False

    def _is_server_side_flow_error(self, error_text: str) -> bool:
        error_lower = (error_text or "").lower()
        return any(keyword in error_lower for keyword in [
            "http error 500",
            "public_error",
            "internal error",
            "reason=internal",
            "reason: internal",
            "\"reason\":\"internal\"",
            "server error",
            "upstream error",
        ])

    async def _clear_tab_site_storage(self, tab) -> Dict[str, Any]:
        """Dọn sạch storage cục bộ của site hiện tại nhưng giữ trạng thái đăng nhập trong cookies."""
        result = await self._tab_evaluate(tab, """
            (async () => {
                const summary = {
                    local_storage_cleared: false,
                    session_storage_cleared: false,
                    cache_storage_deleted: [],
                    indexed_db_deleted: [],
                    indexed_db_errors: [],
                    service_worker_unregistered: 0,
                };

                try {
                    window.localStorage.clear();
                    summary.local_storage_cleared = true;
                } catch (e) {
                    summary.local_storage_error = String(e);
                }

                try {
                    window.sessionStorage.clear();
                    summary.session_storage_cleared = true;
                } catch (e) {
                    summary.session_storage_error = String(e);
                }

                try {
                    if (typeof caches !== 'undefined') {
                        const cacheKeys = await caches.keys();
                        for (const key of cacheKeys) {
                            const deleted = await caches.delete(key);
                            if (deleted) {
                                summary.cache_storage_deleted.push(key);
                            }
                        }
                    }
                } catch (e) {
                    summary.cache_storage_error = String(e);
                }

                try {
                    if (navigator.serviceWorker) {
                        const registrations = await navigator.serviceWorker.getRegistrations();
                        for (const registration of registrations) {
                            const ok = await registration.unregister();
                            if (ok) {
                                summary.service_worker_unregistered += 1;
                            }
                        }
                    }
                } catch (e) {
                    summary.service_worker_error = String(e);
                }

                try {
                    if (typeof indexedDB !== 'undefined' && typeof indexedDB.databases === 'function') {
                        const dbs = await indexedDB.databases();
                        const names = Array.from(new Set(
                            dbs
                                .map((item) => item && item.name)
                                .filter((name) => typeof name === 'string' && name)
                        ));
                        for (const name of names) {
                            try {
                                await new Promise((resolve) => {
                                    const request = indexedDB.deleteDatabase(name);
                                    request.onsuccess = () => resolve(true);
                                    request.onerror = () => resolve(false);
                                    request.onblocked = () => resolve(false);
                                });
                                summary.indexed_db_deleted.push(name);
                            } catch (e) {
                                summary.indexed_db_errors.push(`${name}: ${String(e)}`);
                            }
                        }
                    } else {
                        summary.indexed_db_unsupported = true;
                    }
                } catch (e) {
                    summary.indexed_db_errors.push(String(e));
                }

                return summary;
            })()
        """, label="clear_tab_site_storage", timeout_seconds=15.0)
        return result if isinstance(result, dict) else {}

    async def _clear_resident_storage_and_reload(self, project_id: str) -> bool:
        """Dọn dữ liệu site của tab thường trú và refresh, thử self-heal tại chỗ."""
        async with self._resident_lock:
            slot_id, resident_info = self._resolve_resident_slot_for_project_locked(project_id)

        if not resident_info or not resident_info.tab:
            debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id} không có tab chia sẻ nào để dọn")
            return False

        try:
            async with resident_info.solve_lock:
                cleanup_summary = await self._clear_tab_site_storage(resident_info.tab)
                debug_logger.log_warning(
                    f"[BrowserCaptcha] project_id={project_id}, slot={slot_id} đã dọn storage site, chuẩn bị refresh để khôi phục: {cleanup_summary}"
                )

                resident_info.recaptcha_ready = False
                await self._tab_reload(
                    resident_info.tab,
                    label=f"clear_resident_reload:{slot_id or project_id}",
                )

                if not await self._wait_for_document_ready(resident_info.tab, retries=30, interval_seconds=1.0):
                    debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id}, slot={slot_id} page load timeout sau khi dọn")
                    return False

                resident_info.recaptcha_ready = await self._wait_for_recaptcha(resident_info.tab)
                if resident_info.recaptcha_ready:
                    resident_info.last_used_at = time.time()
                    self._remember_project_affinity(project_id, slot_id, resident_info)
                    self._resident_error_streaks.pop(slot_id, None)
                    debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id}, slot={slot_id} đã khôi phục reCAPTCHA sau khi dọn")
                    return True

                debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id}, slot={slot_id} vẫn không khôi phục được reCAPTCHA sau khi dọn")
                return False
        except Exception as e:
            debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id}, slot={slot_id} dọn hoặc refresh thất bại: {e}")
            return False

    async def _recreate_resident_tab(self, project_id: str) -> bool:
        """Đóng và rebuild tab thường trú."""
        slot_id, resident_info = await self._rebuild_resident_tab(project_id, return_slot_key=True)
        if resident_info is None:
            debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id} rebuild tab chia sẻ thất bại")
            return False
        debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id} đã rebuild tab chia sẻ slot={slot_id}")
        return True

    async def _restart_browser_for_project(self, project_id: str) -> bool:
        async with self._runtime_recover_lock:
            if self._was_runtime_restarted_recently():
                try:
                    if await self._probe_browser_runtime():
                        slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)
                        if resident_info is not None and slot_id:
                            self._remember_project_affinity(project_id, slot_id, resident_info)
                            self._resident_error_streaks.pop(slot_id, None)
                            debug_logger.log_warning(
                                f"[BrowserCaptcha] project_id={project_id} phát hiện trình duyệt vừa khôi phục xong, tái dùng runtime hiện tại (slot={slot_id})"
                            )
                            return True
                except Exception as e:
                    debug_logger.log_warning(
                        f"[BrowserCaptcha] project_id={project_id} tái dùng runtime vừa khôi phục thất bại, tiếp tục restart toàn trình duyệt: {e}"
                    )

            restarted = await self._restart_browser_for_project_unlocked(project_id)
            if restarted:
                self._mark_runtime_restart()
            return restarted

    async def _restart_browser_for_project_unlocked(self, project_id: str) -> bool:
        """Restart toàn trình duyệt nodriver và khôi phục pool Captcha chia sẻ."""
        async with self._resident_lock:
            restore_slots = max(1, min(self._max_resident_tabs, len(self._resident_tabs) or 1))
            restore_project_ids: list[str] = []
            seen_projects = set()
            for candidate in [project_id, *self._project_resident_affinity.keys()]:
                normalized_project_id = str(candidate or "").strip()
                if not normalized_project_id or normalized_project_id in seen_projects:
                    continue
                seen_projects.add(normalized_project_id)
                restore_project_ids.append(normalized_project_id)
                if len(restore_project_ids) >= restore_slots:
                    break

        debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id} chuẩn bị restart trình duyệt nodriver để khôi phục")
        await self._shutdown_browser_runtime(cancel_idle_reaper=False, reason=f"restart_project:{project_id}")

        warmed_slots = await self.warmup_resident_tabs(restore_project_ids, limit=restore_slots)
        if not warmed_slots:
            debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id} khôi phục tab chia sẻ sau restart trình duyệt thất bại")
            return False

        slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)
        if resident_info is None or not slot_id:
            debug_logger.log_warning(f"[BrowserCaptcha] project_id={project_id} không định vị được tab chia sẻ khả dụng sau restart trình duyệt")
            return False

        self._remember_project_affinity(project_id, slot_id, resident_info)
        self._resident_error_streaks.pop(slot_id, None)
        debug_logger.log_warning(
            f"[BrowserCaptcha] project_id={project_id} đã khôi phục pool tab chia sẻ sau restart trình duyệt "
            f"(slots={len(warmed_slots)}, active_slot={slot_id})"
        )
        return True

    async def report_flow_error(self, project_id: str, error_reason: str, error_message: str = ""):
        """Khi endpoint sinh nội dung upstream bị lỗi, thực hiện self-heal cho tab thường trú."""
        if not project_id:
            return

        async with self._resident_lock:
            slot_id, _ = self._resolve_resident_slot_for_project_locked(project_id)

        if not slot_id:
            return

        streak = self._resident_error_streaks.get(slot_id, 0) + 1
        self._resident_error_streaks[slot_id] = streak
        error_text = f"{error_reason or ''} {error_message or ''}".strip()
        error_lower = error_text.lower()
        debug_logger.log_warning(
            f"[BrowserCaptcha] project_id={project_id}, slot={slot_id} nhận ngoại lệ upstream, streak={streak}, reason={error_reason}, detail={error_message[:200]}"
        )

        if not self._initialized or not self.browser:
            return

        # Lỗi 403: dọn cache trước rồi rebuild
        if "403" in error_text or "forbidden" in error_lower or "recaptcha" in error_lower:
            debug_logger.log_warning(
                f"[BrowserCaptcha] project_id={project_id} phát hiện lỗi 403/reCAPTCHA, dọn cache và rebuild"
            )
            healed = await self._clear_resident_storage_and_reload(project_id)
            if not healed:
                await self._recreate_resident_tab(project_id)
            return

        # Lỗi server: quyết định chiến lược khôi phục theo số lần thất bại liên tiếp
        if self._is_server_side_flow_error(error_text):
            recreate_threshold = max(2, int(getattr(config, "browser_personal_recreate_threshold", 2) or 2))
            restart_threshold = max(3, int(getattr(config, "browser_personal_restart_threshold", 3) or 3))

            if streak >= restart_threshold:
                await self._restart_browser_for_project(project_id)
                return
            if streak >= recreate_threshold:
                await self._recreate_resident_tab(project_id)
                return

            healed = await self._clear_resident_storage_and_reload(project_id)
            if not healed:
                await self._recreate_resident_tab(project_id)
            return

        # Lỗi khác: rebuild tab trực tiếp
        await self._recreate_resident_tab(project_id)

    async def _wait_for_recaptcha(self, tab) -> bool:
        """Chờ reCAPTCHA load.

        Returns:
            True if reCAPTCHA loaded successfully
        """
        debug_logger.log_info("[BrowserCaptcha] Inject script reCAPTCHA...")

        # Inject script reCAPTCHA Enterprise
        await self._tab_evaluate(tab, f"""
            (() => {{
                if (document.querySelector('script[src*="recaptcha"]')) return;
                const script = document.createElement('script');
                script.src = 'https://www.google.com/recaptcha/enterprise.js?render={self.website_key}';
                script.async = true;
                document.head.appendChild(script);
            }})()
        """, label="inject_recaptcha_script", timeout_seconds=5.0)

        # Chờ reCAPTCHA load (giảm thời gian chờ)
        for i in range(15):  # Giảm xuống 15 lần, tối đa 7.5 giây
            try:
                is_ready = await self._tab_evaluate(
                    tab,
                    "typeof grecaptcha !== 'undefined' && "
                    "typeof grecaptcha.enterprise !== 'undefined' && "
                    "typeof grecaptcha.enterprise.execute === 'function'",
                    label="check_recaptcha_ready",
                    timeout_seconds=2.5,
                )

                if is_ready:
                    debug_logger.log_info(f"[BrowserCaptcha] reCAPTCHA đã sẵn sàng (đã chờ {i * 0.5}s)")
                    return True

                await tab.sleep(0.5)
            except Exception as e:
                debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi kiểm tra reCAPTCHA: {e}")
                await tab.sleep(0.3)  # Giảm thời gian chờ khi gặp ngoại lệ

        debug_logger.log_warning("[BrowserCaptcha] reCAPTCHA load timeout")
        return False

    async def _wait_for_custom_recaptcha(
        self,
        tab,
        website_key: str,
        enterprise: bool = False,
    ) -> bool:
        """Chờ reCAPTCHA load trên site bất kỳ, dùng cho kiểm tra score."""
        debug_logger.log_info("[BrowserCaptcha] Phát hiện reCAPTCHA tùy chỉnh...")

        ready_check = (
            "typeof grecaptcha !== 'undefined' && typeof grecaptcha.enterprise !== 'undefined' && "
            "typeof grecaptcha.enterprise.execute === 'function'"
        ) if enterprise else (
            "typeof grecaptcha !== 'undefined' && typeof grecaptcha.execute === 'function'"
        )
        script_path = "recaptcha/enterprise.js" if enterprise else "recaptcha/api.js"
        label = "Enterprise" if enterprise else "V3"

        is_ready = await self._tab_evaluate(
            tab,
            ready_check,
            label="check_custom_recaptcha_preloaded",
            timeout_seconds=2.5,
        )
        if is_ready:
            debug_logger.log_info(f"[BrowserCaptcha] reCAPTCHA tùy chỉnh {label} đã load")
            return True

        debug_logger.log_info("[BrowserCaptcha] Không phát hiện reCAPTCHA tùy chỉnh, inject script...")
        await self._tab_evaluate(tab, f"""
            (() => {{
                if (document.querySelector('script[src*="recaptcha"]')) return;
                const script = document.createElement('script');
                script.src = 'https://www.google.com/{script_path}?render={website_key}';
                script.async = true;
                document.head.appendChild(script);
            }})()
        """, label="inject_custom_recaptcha_script", timeout_seconds=5.0)

        await tab.sleep(3)
        for i in range(20):
            is_ready = await self._tab_evaluate(
                tab,
                ready_check,
                label="check_custom_recaptcha_ready",
                timeout_seconds=2.5,
            )
            if is_ready:
                debug_logger.log_info(f"[BrowserCaptcha] reCAPTCHA tùy chỉnh {label} đã load (chờ {i * 0.5} giây)")
                return True
            await tab.sleep(0.5)

        debug_logger.log_warning("[BrowserCaptcha] reCAPTCHA tùy chỉnh load timeout")
        return False

    async def _execute_recaptcha_on_tab(self, tab, action: str = "IMAGE_GENERATION") -> Optional[str]:
        """Thực thi reCAPTCHA trên tab chỉ định để lấy token.

        Args:
            tab: đối tượng tab nodriver
            action: loại action reCAPTCHA (IMAGE_GENERATION hoặc VIDEO_GENERATION)

        Returns:
            reCAPTCHA token hoặc None
        """
        execute_timeout_ms = int(max(1000, self._solve_timeout_seconds * 1000))
        execute_result = await self._tab_evaluate(
            tab,
            f"""
                (async () => {{
                    const finishError = (error) => {{
                        const message = error && error.message ? error.message : String(error || 'execute failed');
                        return {{ ok: false, error: message }};
                    }};

                    try {{
                        const token = await new Promise((resolve, reject) => {{
                            let settled = false;
                            const done = (handler, value) => {{
                                if (settled) return;
                                settled = true;
                                handler(value);
                            }};
                            const timer = setTimeout(() => {{
                                done(reject, new Error('execute timeout'));
                            }}, {execute_timeout_ms});

                            try {{
                                grecaptcha.enterprise.ready(() => {{
                                    grecaptcha.enterprise.execute({json.dumps(self.website_key)}, {{action: {json.dumps(action)}}})
                                        .then((token) => {{
                                            clearTimeout(timer);
                                            done(resolve, token);
                                        }})
                                        .catch((error) => {{
                                            clearTimeout(timer);
                                            done(reject, error);
                                        }});
                                }});
                            }} catch (error) {{
                                clearTimeout(timer);
                                done(reject, error);
                            }}
                        }});

                        return {{ ok: true, token }};
                    }} catch (error) {{
                        return finishError(error);
                    }}
                }})()
            """,
            label=f"execute_recaptcha:{action}",
            timeout_seconds=self._solve_timeout_seconds + 2.0,
            await_promise=True,
            return_by_value=True,
        )

        token = execute_result.get("token") if isinstance(execute_result, dict) else None
        if not token:
            error = execute_result.get("error") if isinstance(execute_result, dict) else execute_result
            if error:
                debug_logger.log_error(f"[BrowserCaptcha] Lỗi reCAPTCHA: {error}")

        if token:
            debug_logger.log_info(f"[BrowserCaptcha] ✅ Lấy Token thành công (độ dài: {len(token)})")
        else:
            debug_logger.log_warning("[BrowserCaptcha] Lấy Token thất bại, giao cho tầng trên thực thi khôi phục tab")

        return token

    async def _execute_custom_recaptcha_on_tab(
        self,
        tab,
        website_key: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> Optional[str]:
        """Thực thi reCAPTCHA trên site bất kỳ trong tab chỉ định."""
        ts = int(time.time() * 1000)
        token_var = f"_custom_recaptcha_token_{ts}"
        error_var = f"_custom_recaptcha_error_{ts}"
        execute_target = "grecaptcha.enterprise.execute" if enterprise else "grecaptcha.execute"

        execute_script = f"""
            (() => {{
                window.{token_var} = null;
                window.{error_var} = null;

                try {{
                    grecaptcha.ready(function() {{
                        {execute_target}('{website_key}', {{action: '{action}'}})
                            .then(function(token) {{
                                window.{token_var} = token;
                            }})
                            .catch(function(err) {{
                                window.{error_var} = err.message || 'execute failed';
                            }});
                    }});
                }} catch (e) {{
                    window.{error_var} = e.message || 'exception';
                }}
            }})()
        """

        await self._tab_evaluate(
            tab,
            execute_script,
            label=f"execute_custom_recaptcha:{action}",
            timeout_seconds=5.0,
        )

        token = None
        for _ in range(30):
            await tab.sleep(0.5)
            token = await self._tab_evaluate(
                tab,
                f"window.{token_var}",
                label=f"poll_custom_recaptcha_token:{action}",
                timeout_seconds=2.0,
            )
            if token:
                break
            error = await self._tab_evaluate(
                tab,
                f"window.{error_var}",
                label=f"poll_custom_recaptcha_error:{action}",
                timeout_seconds=2.0,
            )
            if error:
                debug_logger.log_error(f"[BrowserCaptcha] Lỗi reCAPTCHA tùy chỉnh: {error}")
                break

        try:
            await self._tab_evaluate(
                tab,
                f"delete window.{token_var}; delete window.{error_var};",
                label="cleanup_custom_recaptcha_temp_vars",
                timeout_seconds=5.0,
            )
        except:
            pass

        if token:
            post_wait_seconds = 3
            try:
                post_wait_seconds = float(getattr(config, "browser_recaptcha_settle_seconds", 3) or 3)
            except Exception:
                pass
            if post_wait_seconds > 0:
                debug_logger.log_info(
                    f"[BrowserCaptcha] reCAPTCHA tùy chỉnh đã xong, chờ thêm {post_wait_seconds:.1f}s rồi trả token"
                )
                await tab.sleep(post_wait_seconds)

        return token

    async def _verify_score_on_tab(self, tab, token: str, verify_url: str) -> Dict[str, Any]:
        """Đọc trực tiếp score hiển thị trên trang test, tránh lệch giữa verify.php và hiển thị trang."""
        _ = token
        _ = verify_url
        started_at = time.time()
        timeout_seconds = 25.0
        refresh_clicked = False
        last_snapshot: Dict[str, Any] = {}

        try:
            timeout_seconds = float(getattr(config, "browser_score_dom_wait_seconds", 25) or 25)
        except Exception:
            pass

        while (time.time() - started_at) < timeout_seconds:
            try:
                result = await self._tab_evaluate(tab, """
                    (() => {
                        const bodyText = ((document.body && document.body.innerText) || "")
                            .replace(/\\u00a0/g, " ")
                            .replace(/\\r/g, "");
                        const patterns = [
                            { source: "current_score", regex: /Your score is:\\s*([01](?:\\.\\d+)?)/i },
                            { source: "selected_score", regex: /Selected Score Test:[\\s\\S]{0,400}?Score:\\s*([01](?:\\.\\d+)?)/i },
                            { source: "history_score", regex: /(?:^|\\n)\\s*Score:\\s*([01](?:\\.\\d+)?)\\s*;/i },
                        ];
                        let score = null;
                        let source = "";
                        for (const item of patterns) {
                            const match = bodyText.match(item.regex);
                            if (!match) continue;
                            const parsed = Number(match[1]);
                            if (!Number.isNaN(parsed) && parsed >= 0 && parsed <= 1) {
                                score = parsed;
                                source = item.source;
                                break;
                            }
                        }
                        const uaMatch = bodyText.match(/Current User Agent:\\s*([^\\n]+)/i);
                        const ipMatch = bodyText.match(/Current IP Address:\\s*([^\\n]+)/i);
                        return {
                            score,
                            source,
                            raw_text: bodyText.slice(0, 4000),
                            current_user_agent: uaMatch ? uaMatch[1].trim() : "",
                            current_ip_address: ipMatch ? ipMatch[1].trim() : "",
                            title: document.title || "",
                            url: location.href || "",
                        };
                    })()
                """, label="verify_score_dom", timeout_seconds=10.0)
            except Exception as e:
                result = {"error": f"{type(e).__name__}: {str(e)[:200]}"}

            if isinstance(result, dict):
                last_snapshot = result
                score = result.get("score")
                if isinstance(score, (int, float)):
                    elapsed_ms = int((time.time() - started_at) * 1000)
                    return {
                        "verify_mode": "browser_page_dom",
                        "verify_elapsed_ms": elapsed_ms,
                        "verify_http_status": None,
                        "verify_result": {
                            "success": True,
                            "score": score,
                            "source": result.get("source") or "antcpt_dom",
                            "raw_text": result.get("raw_text") or "",
                            "current_user_agent": result.get("current_user_agent") or "",
                            "current_ip_address": result.get("current_ip_address") or "",
                            "page_title": result.get("title") or "",
                            "page_url": result.get("url") or "",
                        },
                    }

            if not refresh_clicked and (time.time() - started_at) >= 2:
                refresh_clicked = True
                try:
                    await self._tab_evaluate(tab, """
                        (() => {
                            const nodes = Array.from(
                                document.querySelectorAll('button, input[type="button"], input[type="submit"], a')
                            );
                            const target = nodes.find((node) => {
                                const text = (node.innerText || node.textContent || node.value || "").trim();
                                return /Refresh score now!?/i.test(text);
                            });
                            if (target) {
                                target.click();
                                return true;
                            }
                            return false;
                        })()
                    """, label="verify_score_click_refresh", timeout_seconds=5.0)
                except Exception:
                    pass

            await tab.sleep(0.5)

        elapsed_ms = int((time.time() - started_at) * 1000)
        if not isinstance(last_snapshot, dict):
            last_snapshot = {"raw": last_snapshot}

        return {
            "verify_mode": "browser_page_dom",
            "verify_elapsed_ms": elapsed_ms,
            "verify_http_status": None,
            "verify_result": {
                "success": False,
                "score": None,
                "source": "antcpt_dom_timeout",
                "raw_text": last_snapshot.get("raw_text") or "",
                "current_user_agent": last_snapshot.get("current_user_agent") or "",
                "current_ip_address": last_snapshot.get("current_ip_address") or "",
                "page_title": last_snapshot.get("title") or "",
                "page_url": last_snapshot.get("url") or "",
                "error": last_snapshot.get("error") or "Không đọc được score trong trang",
            },
        }

    async def _extract_tab_fingerprint(self, tab) -> Optional[Dict[str, Any]]:
        """Trích xuất thông tin fingerprint trình duyệt từ tab nodriver."""
        try:
            fingerprint = await self._tab_evaluate(tab, """
                (() => {
                    const ua = navigator.userAgent || "";
                    const lang = navigator.language || "";
                    const uaData = navigator.userAgentData || null;
                    let secChUa = "";
                    let secChUaMobile = "";
                    let secChUaPlatform = "";

                    if (uaData) {
                        if (Array.isArray(uaData.brands) && uaData.brands.length > 0) {
                            secChUa = uaData.brands
                                .map((item) => `"${item.brand}";v="${item.version}"`)
                                .join(", ");
                        }
                        secChUaMobile = uaData.mobile ? "?1" : "?0";
                        if (uaData.platform) {
                            secChUaPlatform = `"${uaData.platform}"`;
                        }
                    }

                    return {
                        user_agent: ua,
                        accept_language: lang,
                        sec_ch_ua: secChUa,
                        sec_ch_ua_mobile: secChUaMobile,
                        sec_ch_ua_platform: secChUaPlatform,
                    };
                })()
            """, label="extract_tab_fingerprint", timeout_seconds=8.0)
            if not isinstance(fingerprint, dict):
                return None

            result: Dict[str, Any] = {"proxy_url": self._proxy_url}
            for key in ("user_agent", "accept_language", "sec_ch_ua", "sec_ch_ua_mobile", "sec_ch_ua_platform"):
                value = fingerprint.get(key)
                if isinstance(value, str) and value:
                    result[key] = value
            return result
        except Exception as e:
            debug_logger.log_warning(f"[BrowserCaptcha] Trích xuất fingerprint nodriver thất bại: {e}")
            return None

    async def _refresh_last_fingerprint(self, tab) -> Optional[Dict[str, Any]]:
        """Cache fingerprint trình duyệt gần nhất, tránh mỗi lần giải Captcha thành công lại thêm một vòng JS."""
        if self._is_fingerprint_cache_fresh():
            return self._last_fingerprint

        fingerprint = await self._extract_tab_fingerprint(tab)
        self._last_fingerprint = fingerprint
        self._last_fingerprint_at = time.monotonic() if fingerprint else 0.0
        return fingerprint

    def _remember_fingerprint(self, fingerprint: Optional[Dict[str, Any]]):
        if isinstance(fingerprint, dict) and fingerprint:
            self._last_fingerprint = dict(fingerprint)
            self._last_fingerprint_at = time.monotonic()
        else:
            self._last_fingerprint = None
            self._last_fingerprint_at = 0.0

    async def _solve_with_resident_tab(
        self,
        slot_id: str,
        project_id: str,
        resident_info: Optional[ResidentTabInfo],
        action: str,
        *,
        success_label: str,
    ) -> Optional[str]:
        """Thực hiện một lần giải Captcha trên tab thường trú chia sẻ và cập nhật trạng thái thành công."""
        if not resident_info or not resident_info.tab or not resident_info.recaptcha_ready:
            return None

        start_time = time.time()
        async with resident_info.solve_lock:
            token = await self._run_with_timeout(
                self._execute_recaptcha_on_tab(resident_info.tab, action),
                timeout_seconds=self._solve_timeout_seconds,
                label=f"{success_label}:{slot_id}:{project_id}:{action}",
            )

        if not token:
            return None

        duration_ms = (time.time() - start_time) * 1000
        resident_info.last_used_at = time.time()
        resident_info.use_count += 1
        self._remember_project_affinity(project_id, slot_id, resident_info)
        self._resident_error_streaks.pop(slot_id, None)
        self._mark_browser_health(True)
        if resident_info.fingerprint:
            self._remember_fingerprint(resident_info.fingerprint)
        else:
            resident_info.fingerprint = await self._refresh_last_fingerprint(resident_info.tab)
        debug_logger.log_info(
            f"[BrowserCaptcha] ✅ Sinh Token thành công (slot={slot_id}, thời gian {duration_ms:.0f}ms, số lần dùng: {resident_info.use_count})"
        )
        return token

    # ========== API chính ==========

    async def get_token(self, project_id: str, action: str = "IMAGE_GENERATION") -> Optional[str]:
        """Lấy reCAPTCHA token.

        Dùng pool tab Captcha chia sẻ toàn cục. Tab không còn bind 1-1 theo project_id,
        ai lấy được tab rảnh thì dùng tab đó; chỉ có Session Token refresh/khôi phục lỗi mới ưu tiên tham chiếu mapping gần nhất.

        Args:
            project_id: ID project Flow
            action: loại action reCAPTCHA
                - IMAGE_GENERATION: sinh ảnh và phóng to ảnh 2K/4K (mặc định)
                - VIDEO_GENERATION: sinh video và phóng to video

        Returns:
            chuỗi reCAPTCHA token; nếu lấy thất bại trả None
        """
        debug_logger.log_info(f"[BrowserCaptcha] get_token bắt đầu: project_id={project_id}, action={action}, số tab hiện tại={len(self._resident_tabs)}/{self._max_resident_tabs}")

        # Đảm bảo trình duyệt đã khởi tạo
        await self.initialize()

        debug_logger.log_info(
            f"[BrowserCaptcha] Bắt đầu lấy tab từ pool Captcha chia sẻ (project: {project_id}, hiện tại: {len(self._resident_tabs)}/{self._max_resident_tabs})"
        )
        slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)
        if resident_info is None or not slot_id:
            if not await self._probe_browser_runtime():
                debug_logger.log_warning(
                    f"[BrowserCaptcha] Pool tab chia sẻ rỗng và trình duyệt nghi không hoạt động, thử restart để khôi phục (project: {project_id})"
                )
                if await self._recover_browser_runtime(project_id, reason="ensure_resident_tab"):
                    slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)

        if resident_info is None or not slot_id:
            debug_logger.log_warning(
                f"[BrowserCaptcha] Pool tab chia sẻ không khả dụng, fallback về chế độ truyền thống (project: {project_id})"
            )
            return await self._get_token_legacy(project_id, action)

        debug_logger.log_info(
            f"[BrowserCaptcha] ✅ Tab chia sẻ khả dụng (slot={slot_id}, project={project_id}, use_count={resident_info.use_count})"
        )

        if resident_info and resident_info.tab and not resident_info.recaptcha_ready:
            debug_logger.log_warning(
                f"[BrowserCaptcha] Tab chia sẻ chưa sẵn sàng, chuẩn bị rebuild cold slot={slot_id}, project={project_id}"
            )
            slot_id, resident_info = await self._rebuild_resident_tab(
                project_id,
                slot_id=slot_id,
                return_slot_key=True,
            )
            if resident_info is None:
                debug_logger.log_warning(
                    f"[BrowserCaptcha] Rebuild cold slot thất bại, nâng lên khôi phục cấp trình duyệt (slot={slot_id}, project={project_id})"
                )
                if await self._recover_browser_runtime(project_id, reason=f"cold_resident_tab:{slot_id or 'unknown'}"):
                    slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)

        # Dùng tab thường trú để sinh token (thực thi ngoài lock để tránh block)
        if resident_info and resident_info.recaptcha_ready and resident_info.tab:
            debug_logger.log_info(
                f"[BrowserCaptcha] Sinh token tức thì từ tab thường trú chia sẻ (slot={slot_id}, project={project_id}, action={action})..."
            )
            runtime_recovered = False
            try:
                token = await self._solve_with_resident_tab(
                    slot_id,
                    project_id,
                    resident_info,
                    action,
                    success_label="resident_solve",
                )
                if token:
                    return token
                debug_logger.log_warning(
                    f"[BrowserCaptcha] Tab chia sẻ sinh thất bại (slot={slot_id}, project={project_id}), thử rebuild..."
                )
            except Exception as e:
                debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ tab chia sẻ (slot={slot_id}): {e}, thử rebuild...")
                if self._is_browser_runtime_error(e):
                    runtime_recovered = await self._recover_browser_runtime(
                        project_id,
                        reason=f"resident_solve:{slot_id}",
                    )
                    if runtime_recovered:
                        slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)
                        if resident_info and slot_id:
                            try:
                                token = await self._solve_with_resident_tab(
                                    slot_id,
                                    project_id,
                                    resident_info,
                                    action,
                                    success_label="resident_solve_after_runtime_recover",
                                )
                                if token:
                                    return token
                            except Exception as retry_error:
                                debug_logger.log_warning(
                                    f"[BrowserCaptcha] Tab chia sẻ vẫn thất bại sau khi restart/khôi phục trình duyệt (slot={slot_id}): {retry_error}"
                                )

            if not runtime_recovered:
                # Tab thường trú mất hiệu lực, thử rebuild
                debug_logger.log_info(f"[BrowserCaptcha] Bắt đầu rebuild tab chia sẻ (slot={slot_id}, project={project_id})")
                slot_id, resident_info = await self._rebuild_resident_tab(
                    project_id,
                    slot_id=slot_id,
                    return_slot_key=True,
                )
                debug_logger.log_info(f"[BrowserCaptcha] Rebuild tab chia sẻ xong (slot={slot_id}, project={project_id})")
                if resident_info is None:
                    debug_logger.log_warning(
                        f"[BrowserCaptcha] Rebuild tab chia sẻ trả về rỗng, nâng lên khôi phục cấp trình duyệt (slot={slot_id}, project={project_id})"
                    )
                    if await self._recover_browser_runtime(project_id, reason=f"resident_rebuild_empty:{slot_id or 'unknown'}"):
                        slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)

                # Sau rebuild thử sinh ngay (thực thi ngoài lock)
                if resident_info:
                    try:
                        token = await self._solve_with_resident_tab(
                            slot_id,
                            project_id,
                            resident_info,
                            action,
                            success_label="resident_resolve_after_rebuild",
                        )
                        if token:
                            debug_logger.log_info(f"[BrowserCaptcha] ✅ Sinh Token sau rebuild thành công (slot={slot_id})")
                            return token
                    except Exception as rebuild_error:
                        debug_logger.log_warning(
                            f"[BrowserCaptcha] Vẫn không giải Captcha được sau khi rebuild tab (slot={slot_id}): {rebuild_error}"
                        )
                        if self._is_browser_runtime_error(rebuild_error):
                            if await self._recover_browser_runtime(project_id, reason=f"resident_rebuild:{slot_id}"):
                                slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)
                                if resident_info and slot_id:
                                    try:
                                        token = await self._solve_with_resident_tab(
                                            slot_id,
                                            project_id,
                                            resident_info,
                                            action,
                                            success_label="resident_resolve_after_browser_restart",
                                        )
                                        if token:
                                            return token
                                    except Exception as restart_error:
                                        debug_logger.log_warning(
                                            f"[BrowserCaptcha] Resident vẫn thất bại sau khi restart trình duyệt (slot={slot_id}): {restart_error}"
                                        )
                elif not await self._probe_browser_runtime():
                    if await self._recover_browser_runtime(project_id, reason=f"resident_rebuild_empty:{slot_id}"):
                        slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)
                        if resident_info and slot_id:
                            try:
                                token = await self._solve_with_resident_tab(
                                    slot_id,
                                    project_id,
                                    resident_info,
                                    action,
                                    success_label="resident_resolve_after_empty_recover",
                                )
                                if token:
                                    return token
                            except Exception as empty_recover_error:
                                debug_logger.log_warning(
                                    f"[BrowserCaptcha] Resident vẫn thất bại sau khi khôi phục rỗng trình duyệt (slot={slot_id}): {empty_recover_error}"
                                )

        # Fallback cuối: dùng chế độ truyền thống
        debug_logger.log_warning(f"[BrowserCaptcha] Mọi cách thường trú đều thất bại, fallback về chế độ truyền thống (project: {project_id})")
        legacy_token = await self._get_token_legacy(project_id, action)
        if legacy_token:
            if slot_id:
                self._resident_error_streaks.pop(slot_id, None)
        return legacy_token

    async def _create_resident_tab(self, slot_id: str, project_id: Optional[str] = None) -> Optional[ResidentTabInfo]:
        """Tạo một tab Captcha thường trú chia sẻ.

        Args:
            slot_id: ID slot tab chia sẻ
            project_id: ID project trigger tạo, chỉ dùng cho log và mapping gần nhất

        Returns:
            Đối tượng ResidentTabInfo, hoặc None (tạo thất bại)
        """
        try:
            # Dùng địa chỉ Flow API làm trang cơ sở
            website_url = "https://labs.google/fx/api/auth/providers"
            debug_logger.log_info(f"[BrowserCaptcha] Tạo tab thường trú chia sẻ slot={slot_id}, seed_project={project_id}")

            async with self._resident_lock:
                existing_tabs = [info.tab for info in self._resident_tabs.values() if info.tab]

            # Lấy hoặc tạo tab
            browser = self.browser
            if browser is None or getattr(browser, "stopped", False):
                debug_logger.log_warning(
                    f"[BrowserCaptcha] Trình duyệt không khả dụng trước khi tạo tab thường trú chia sẻ (slot={slot_id}, project={project_id})"
                )
                return None

            tabs = list(getattr(browser, "tabs", []) or [])
            available_tab = None

            # Tìm tab chưa bị chiếm
            for tab in tabs:
                if tab not in existing_tabs:
                    available_tab = tab
                    break

            if available_tab:
                tab = available_tab
                debug_logger.log_info(f"[BrowserCaptcha] Tái dùng tab chưa bị chiếm")
                await self._tab_get(
                    tab,
                    website_url,
                    label=f"resident_tab_get:{slot_id}",
                )
            else:
                debug_logger.log_info(f"[BrowserCaptcha] Tạo tab mới")
                tab = await self._browser_get(
                    website_url,
                    label=f"resident_browser_get:{slot_id}",
                    new_tab=True,
                )

            # Chờ trang load xong (giảm thời gian chờ)
            page_loaded = False
            for retry in range(10):  # Giảm xuống 10 lần, tối đa 5 giây
                try:
                    await asyncio.sleep(0.5)
                    ready_state = await self._tab_evaluate(
                        tab,
                        "document.readyState",
                        label=f"resident_document_ready:{slot_id}",
                        timeout_seconds=2.0,
                    )
                    if ready_state == "complete":
                        page_loaded = True
                        debug_logger.log_info(f"[BrowserCaptcha] Trang đã load")
                        break
                except Exception as e:
                    debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi chờ trang: {e}, retry {retry + 1}/10...")
                    await asyncio.sleep(0.3)  # Giảm khoảng cách retry

            if not page_loaded:
                debug_logger.log_error(f"[BrowserCaptcha] Trang load timeout (slot={slot_id}, project={project_id})")
                await self._close_tab_quietly(tab)
                return None

            # Chờ reCAPTCHA load
            recaptcha_ready = await self._wait_for_recaptcha(tab)

            if not recaptcha_ready:
                debug_logger.log_error(f"[BrowserCaptcha] reCAPTCHA load thất bại (slot={slot_id}, project={project_id})")
                await self._close_tab_quietly(tab)
                return None

            # Tạo đối tượng thông tin thường trú
            resident_info = ResidentTabInfo(tab, slot_id, project_id=project_id)
            resident_info.recaptcha_ready = True
            resident_info.fingerprint = await self._refresh_last_fingerprint(tab)
            self._mark_browser_health(True)

            debug_logger.log_info(f"[BrowserCaptcha] ✅ Tạo tab thường trú chia sẻ thành công (slot={slot_id}, project={project_id})")
            return resident_info

        except Exception as e:
            debug_logger.log_error(f"[BrowserCaptcha] Ngoại lệ khi tạo tab thường trú chia sẻ (slot={slot_id}, project={project_id}): {e}")
            return None

    async def _close_resident_tab(self, slot_id: str):
        """Đóng tab thường trú chia sẻ của slot chỉ định.

        Args:
            slot_id: ID slot tab chia sẻ
        """
        async with self._resident_lock:
            resident_info = self._resident_tabs.pop(slot_id, None)
            self._forget_project_affinity_for_slot_locked(slot_id)
            self._resident_error_streaks.pop(slot_id, None)
            self._sync_compat_resident_state()

        if resident_info and resident_info.tab:
            try:
                await self._close_tab_quietly(resident_info.tab)
                debug_logger.log_info(f"[BrowserCaptcha] Đã đóng tab thường trú chia sẻ slot={slot_id}")
            except Exception as e:
                debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi đóng tab: {e}")

    async def invalidate_token(self, project_id: str):
        """Gọi khi phát hiện token không hợp lệ, rebuild tab chia sẻ của mapping gần nhất của project hiện tại.

        Args:
            project_id: ID project
        """
        debug_logger.log_warning(
            f"[BrowserCaptcha] Token bị đánh dấu không hợp lệ (project: {project_id}), chỉ rebuild tab tương ứng trong pool chia sẻ để tránh xóa state trình duyệt toàn cục"
        )

        # Rebuild tab
        slot_id, resident_info = await self._rebuild_resident_tab(project_id, return_slot_key=True)
        if resident_info and slot_id:
            debug_logger.log_info(f"[BrowserCaptcha] ✅ Tab đã được rebuild (project: {project_id}, slot={slot_id})")
        else:
            debug_logger.log_error(f"[BrowserCaptcha] Rebuild tab thất bại (project: {project_id})")

    async def _get_token_legacy(self, project_id: str, action: str = "IMAGE_GENERATION") -> Optional[str]:
        """Chế độ truyền thống để lấy reCAPTCHA token (mỗi lần tạo tab mới).

        Args:
            project_id: ID project Flow
            action: loại action reCAPTCHA (IMAGE_GENERATION hoặc VIDEO_GENERATION)

        Returns:
            chuỗi reCAPTCHA token; nếu lấy thất bại trả None
        """
        max_attempts = 2
        async with self._legacy_lock:
            for attempt in range(max_attempts):
                if not self._initialized or not self.browser:
                    await self.initialize()

                start_time = time.time()
                tab = None

                try:
                    website_url = "https://labs.google/fx/api/auth/providers"
                    debug_logger.log_info(
                        f"[BrowserCaptcha] [Legacy] Tạo tab tạm độc lập để thực thi xác thực, tránh ảnh hưởng tab resident/custom: {website_url}"
                    )
                    tab = await self._browser_get(
                        website_url,
                        label=f"legacy_browser_get:{project_id}",
                        new_tab=True,
                    )

                    # Chờ trang load hoàn toàn (tăng thời gian chờ)
                    debug_logger.log_info("[BrowserCaptcha] [Legacy] Chờ trang load...")
                    await tab.sleep(3)

                    # Chờ DOM trang hoàn tất
                    for _ in range(10):
                        ready_state = await self._tab_evaluate(
                            tab,
                            "document.readyState",
                            label=f"legacy_document_ready:{project_id}",
                            timeout_seconds=2.0,
                        )
                        if ready_state == "complete":
                            break
                        await tab.sleep(0.5)

                    # Chờ reCAPTCHA load
                    recaptcha_ready = await self._wait_for_recaptcha(tab)

                    if not recaptcha_ready:
                        debug_logger.log_error("[BrowserCaptcha] [Legacy] Không load được reCAPTCHA")
                        return None

                    # Thực thi reCAPTCHA
                    debug_logger.log_info(f"[BrowserCaptcha] [Legacy] Thực thi xác thực reCAPTCHA (action: {action})...")
                    token = await self._run_with_timeout(
                        self._execute_recaptcha_on_tab(tab, action),
                        timeout_seconds=self._solve_timeout_seconds,
                        label=f"legacy_solve:{project_id}:{action}",
                    )

                    duration_ms = (time.time() - start_time) * 1000

                    if token:
                        self._mark_browser_health(True)
                        await self._refresh_last_fingerprint(tab)
                        debug_logger.log_info(f"[BrowserCaptcha] [Legacy] ✅ Lấy Token thành công (thời gian {duration_ms:.0f}ms)")
                        return token

                    debug_logger.log_error("[BrowserCaptcha] [Legacy] Lấy Token thất bại (trả null)")
                    return None

                except Exception as e:
                    if attempt < (max_attempts - 1) and self._is_browser_runtime_error(e):
                        debug_logger.log_warning(
                            f"[BrowserCaptcha] [Legacy] Runtime trình duyệt bất thường, thử restart khôi phục rồi retry: {e}"
                        )
                        await self._recover_browser_runtime(project_id, reason=f"legacy_attempt_{attempt + 1}")
                        continue

                    debug_logger.log_error(f"[BrowserCaptcha] [Legacy] Ngoại lệ khi lấy token: {str(e)}")
                    return None
                finally:
                    # Đóng tab tạm legacy (nhưng giữ trình duyệt)
                    if tab:
                        await self._close_tab_quietly(tab)

        return None

    def get_last_fingerprint(self) -> Optional[Dict[str, Any]]:
        """Trả về snapshot fingerprint trình duyệt lần giải Captcha gần nhất."""
        if not self._last_fingerprint:
            return None
        return dict(self._last_fingerprint)

    async def _clear_browser_cache(self):
        """Dọn toàn bộ cache trình duyệt."""
        if not self.browser:
            return

        try:
            debug_logger.log_info("[BrowserCaptcha] Bắt đầu dọn cache trình duyệt...")

            # Dùng Chrome DevTools Protocol để dọn cache
            # Dọn mọi loại dữ liệu cache
            await self._browser_send_command(
                "Network.clearBrowserCache",
                label="clear_browser_cache",
            )

            # Dọn Cookies
            await self._browser_send_command(
                "Network.clearBrowserCookies",
                label="clear_browser_cookies",
            )

            # Dọn dữ liệu storage (localStorage, sessionStorage, IndexedDB, v.v.)
            await self._browser_send_command(
                "Storage.clearDataForOrigin",
                {
                    "origin": "https://www.google.com",
                    "storageTypes": "all"
                },
                label="clear_browser_origin_storage",
            )

            debug_logger.log_info("[BrowserCaptcha] ✅ Đã dọn cache trình duyệt")

        except Exception as e:
            debug_logger.log_warning(f"[BrowserCaptcha] Ngoại lệ khi dọn cache: {e}")

    async def _shutdown_browser_runtime(self, cancel_idle_reaper: bool = False, reason: str = "shutdown"):
        if cancel_idle_reaper and self._idle_reaper_task and not self._idle_reaper_task.done():
            self._idle_reaper_task.cancel()
            try:
                await self._idle_reaper_task
            except asyncio.CancelledError:
                pass
            finally:
                self._idle_reaper_task = None

        async with self._browser_lock:
            try:
                await self._shutdown_browser_runtime_locked(reason=reason)
                debug_logger.log_info(f"[BrowserCaptcha] Đã dọn runtime trình duyệt ({reason})")
            except Exception as e:
                debug_logger.log_error(f"[BrowserCaptcha] Ngoại lệ khi dọn runtime trình duyệt ({reason}): {str(e)}")

    async def close(self):
        """Đóng trình duyệt."""
        await self._shutdown_browser_runtime(cancel_idle_reaper=True, reason="service_close")

    async def open_login_window(self):
        """Mở cửa sổ đăng nhập để user đăng nhập Google thủ công."""
        await self.initialize()
        tab = await self._browser_get(
            "https://accounts.google.com/",
            label="open_login_window",
            new_tab=True,
        )
        debug_logger.log_info("[BrowserCaptcha] Vui lòng đăng nhập tài khoản trong trình duyệt đã mở. Sau khi đăng nhập xong, không cần đóng trình duyệt; lần chạy script tiếp theo sẽ tự dùng trạng thái này.")
        print("Vui lòng đăng nhập tài khoản trong trình duyệt đã mở. Sau khi đăng nhập xong, không cần đóng trình duyệt; lần chạy script tiếp theo sẽ tự dùng trạng thái này.")

    # ========== Làm mới Session Token ==========

    async def refresh_session_token(self, project_id: str) -> Optional[str]:
        """Lấy Session Token mới nhất từ tab thường trú.
        
        Tái dùng tab Captcha chia sẻ, refresh trang và trích từ cookies
        __Secure-next-auth.session-token
        
        Args:
            project_id: ID project, dùng để định vị tab thường trú
            
        Returns:
            Session Token mới; nếu lấy thất bại trả None
        """
        for attempt in range(2):
            # Đảm bảo trình duyệt đã khởi tạo
            await self.initialize()

            start_time = time.time()
            debug_logger.log_info(f"[BrowserCaptcha] Bắt đầu làm mới Session Token (project: {project_id}, attempt={attempt + 1})...")

            async with self._resident_lock:
                slot_id = self._resolve_affinity_slot_locked(project_id)
                resident_info = self._resident_tabs.get(slot_id) if slot_id else None

            if resident_info is None or not slot_id:
                slot_id, resident_info = await self._ensure_resident_tab(project_id, return_slot_key=True)

            if resident_info is None or not slot_id:
                if attempt == 0 and not await self._probe_browser_runtime():
                    await self._recover_browser_runtime(project_id, reason="refresh_session_prepare")
                    continue
                debug_logger.log_warning(f"[BrowserCaptcha] Không lấy được tab thường trú chia sẻ cho project_id={project_id}")
                return None

            if not resident_info or not resident_info.tab:
                debug_logger.log_error(f"[BrowserCaptcha] Không lấy được tab thường trú")
                return None

            tab = resident_info.tab

            try:
                async with resident_info.solve_lock:
                    # Refresh trang để lấy cookies mới nhất
                    debug_logger.log_info(f"[BrowserCaptcha] Refresh tab thường trú để lấy cookies mới nhất...")
                    resident_info.recaptcha_ready = False
                    await self._run_with_timeout(
                        self._tab_reload(
                            tab,
                            label=f"refresh_session_reload:{slot_id}",
                        ),
                        timeout_seconds=self._session_refresh_timeout_seconds,
                        label=f"refresh_session_reload_total:{slot_id}",
                    )

                    # Chờ trang load xong
                    for _ in range(30):
                        await asyncio.sleep(1)
                        try:
                            ready_state = await self._tab_evaluate(
                                tab,
                                "document.readyState",
                                label=f"refresh_session_ready_state:{slot_id}",
                                timeout_seconds=2.0,
                            )
                            if ready_state == "complete":
                                break
                        except Exception:
                            pass

                    resident_info.recaptcha_ready = await self._wait_for_recaptcha(tab)
                    if not resident_info.recaptcha_ready:
                        debug_logger.log_warning(
                            f"[BrowserCaptcha] Sau khi làm mới Session Token, reCAPTCHA chưa sẵn sàng trở lại (slot={slot_id})"
                        )

                    # Chờ thêm để đảm bảo cookies đã được set
                    await asyncio.sleep(2)

                    # Trích __Secure-next-auth.session-token từ cookies
                    session_token = None

                    try:
                        cookies = await self._get_browser_cookies(
                            label=f"refresh_session_get_cookies:{slot_id}",
                        )

                        for cookie in cookies:
                            if cookie.name == "__Secure-next-auth.session-token":
                                session_token = cookie.value
                                break

                    except Exception as e:
                        debug_logger.log_warning(f"[BrowserCaptcha] Lấy qua cookies API thất bại: {e}, thử lấy từ document.cookie...")

                        try:
                            all_cookies = await self._tab_evaluate(
                                tab,
                                "document.cookie",
                                label=f"refresh_session_document_cookie:{slot_id}",
                            )
                            if all_cookies:
                                for part in all_cookies.split(";"):
                                    part = part.strip()
                                    if part.startswith("__Secure-next-auth.session-token="):
                                        session_token = part.split("=", 1)[1]
                                        break
                        except Exception as e2:
                            debug_logger.log_error(f"[BrowserCaptcha] Lấy document.cookie thất bại: {e2}")

                duration_ms = (time.time() - start_time) * 1000

                if session_token:
                    resident_info.last_used_at = time.time()
                    self._remember_project_affinity(project_id, slot_id, resident_info)
                    self._resident_error_streaks.pop(slot_id, None)
                    self._mark_browser_health(True)
                    debug_logger.log_info(f"[BrowserCaptcha] ✅ Lấy Session Token thành công (thời gian {duration_ms:.0f}ms)")
                    return session_token

                debug_logger.log_error(f"[BrowserCaptcha] ❌ Không tìm thấy cookie __Secure-next-auth.session-token")
                return None

            except Exception as e:
                debug_logger.log_error(f"[BrowserCaptcha] Ngoại lệ khi làm mới Session Token: {str(e)}")

                if attempt == 0 and self._is_browser_runtime_error(e):
                    if await self._recover_browser_runtime(project_id, reason=f"refresh_session:{slot_id}"):
                        continue

                slot_id, resident_info = await self._rebuild_resident_tab(project_id, slot_id=slot_id, return_slot_key=True)
                if resident_info and slot_id:
                    try:
                        async with resident_info.solve_lock:
                            cookies = await self._get_browser_cookies(
                                label=f"refresh_session_get_cookies_after_rebuild:{slot_id}",
                            )
                        for cookie in cookies:
                            if cookie.name == "__Secure-next-auth.session-token":
                                resident_info.last_used_at = time.time()
                                self._remember_project_affinity(project_id, slot_id, resident_info)
                                self._resident_error_streaks.pop(slot_id, None)
                                self._mark_browser_health(True)
                                debug_logger.log_info(f"[BrowserCaptcha] ✅ Lấy Session Token thành công sau rebuild")
                                return cookie.value
                    except Exception as rebuild_error:
                        if attempt == 0 and self._is_browser_runtime_error(rebuild_error):
                            if await self._recover_browser_runtime(project_id, reason=f"refresh_session_rebuild:{slot_id}"):
                                continue

                return None

        return None

    # ========== Truy vấn trạng thái ==========

    def is_resident_mode_active(self) -> bool:
        """Kiểm tra có tab thường trú nào đang hoạt động không."""
        return len(self._resident_tabs) > 0 or self._running

    def get_resident_count(self) -> int:
        """Lấy số lượng tab thường trú hiện tại."""
        return len(self._resident_tabs)

    def get_resident_project_ids(self) -> list[str]:
        """Lấy danh sách slot_id của mọi tab thường trú chia sẻ hiện tại."""
        return list(self._resident_tabs.keys())

    def get_resident_project_id(self) -> Optional[str]:
        """Lấy slot_id đầu tiên trong pool chia sẻ hiện tại (tương thích ngược)."""
        if self._resident_tabs:
            return next(iter(self._resident_tabs.keys()))
        return self.resident_project_id

    async def get_custom_token(
        self,
        website_url: str,
        website_key: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> Optional[str]:
        """Thực thi reCAPTCHA trên site bất kỳ, dùng cho tình huống kiểm tra score.

        Khác với chế độ legacy thông thường, ở đây sẽ tái dùng cùng một tab thường trú để tránh cold start tab mới mỗi lần.
        """
        await self.initialize()
        self._last_fingerprint = None

        cache_key = f"{website_url}|{website_key}|{1 if enterprise else 0}"
        warmup_seconds = float(getattr(config, "browser_score_test_warmup_seconds", 12) or 12)
        per_request_settle_seconds = float(
            getattr(config, "browser_score_test_settle_seconds", 2.5) or 2.5
        )
        max_retries = 2

        async with self._custom_lock:
            for attempt in range(max_retries):
                start_time = time.time()
                custom_info = self._custom_tabs.get(cache_key)
                tab = custom_info.get("tab") if isinstance(custom_info, dict) else None

                try:
                    if tab is None:
                        debug_logger.log_info(f"[BrowserCaptcha] [Custom] Tạo tab test thường trú: {website_url}")
                        tab = await self._browser_get(
                            website_url,
                            label="custom_browser_get",
                            new_tab=True,
                        )
                        custom_info = {
                            "tab": tab,
                            "recaptcha_ready": False,
                            "warmed_up": False,
                            "created_at": time.time(),
                        }
                        self._custom_tabs[cache_key] = custom_info

                    page_loaded = False
                    for _ in range(20):
                        ready_state = await self._tab_evaluate(
                            tab,
                            "document.readyState",
                            label="custom_document_ready",
                            timeout_seconds=2.0,
                        )
                        if ready_state == "complete":
                            page_loaded = True
                            break
                        await tab.sleep(0.5)

                    if not page_loaded:
                        raise RuntimeError("Trang tùy chỉnh load timeout")

                    if not custom_info.get("recaptcha_ready"):
                        recaptcha_ready = await self._wait_for_custom_recaptcha(
                            tab=tab,
                            website_key=website_key,
                            enterprise=enterprise,
                        )
                        if not recaptcha_ready:
                            raise RuntimeError("reCAPTCHA tùy chỉnh không load được")
                        custom_info["recaptcha_ready"] = True

                    try:
                        await self._tab_evaluate(tab, """
                            (() => {
                                try {
                                    const body = document.body || document.documentElement;
                                    const width = window.innerWidth || 1280;
                                    const height = window.innerHeight || 720;
                                    const x = Math.max(24, Math.floor(width * 0.38));
                                    const y = Math.max(24, Math.floor(height * 0.32));
                                    const moveEvent = new MouseEvent('mousemove', {
                                        bubbles: true,
                                        clientX: x,
                                        clientY: y
                                    });
                                    const overEvent = new MouseEvent('mouseover', {
                                        bubbles: true,
                                        clientX: x,
                                        clientY: y
                                    });
                                    window.focus();
                                    window.dispatchEvent(new Event('focus'));
                                    document.dispatchEvent(moveEvent);
                                    document.dispatchEvent(overEvent);
                                    if (body) {
                                        body.dispatchEvent(moveEvent);
                                        body.dispatchEvent(overEvent);
                                    }
                                    window.scrollTo(0, Math.min(320, document.body?.scrollHeight || 320));
                                } catch (e) {}
                            })()
                        """, label="custom_pre_warm_interaction", timeout_seconds=6.0)
                    except Exception:
                        pass

                    if not custom_info.get("warmed_up"):
                        if warmup_seconds > 0:
                            debug_logger.log_info(
                                f"[BrowserCaptcha] [Custom] Warm-up lần đầu trang test {warmup_seconds:.1f}s rồi mới thực thi token"
                            )
                            try:
                                await self._tab_evaluate(tab, """
                                    (() => {
                                        try {
                                            window.scrollTo(0, Math.min(240, document.body.scrollHeight || 240));
                                            window.dispatchEvent(new Event('mousemove'));
                                            window.dispatchEvent(new Event('focus'));
                                        } catch (e) {}
                                    })()
                                """, label="custom_warmup_interaction", timeout_seconds=6.0)
                            except Exception:
                                pass
                            await tab.sleep(warmup_seconds)
                        custom_info["warmed_up"] = True
                    elif per_request_settle_seconds > 0:
                        debug_logger.log_info(
                            f"[BrowserCaptcha] [Custom] Tái dùng tab test, chờ thêm {per_request_settle_seconds:.1f}s trước khi thực thi"
                        )
                        await tab.sleep(per_request_settle_seconds)

                    debug_logger.log_info(f"[BrowserCaptcha] [Custom] Dùng tab test thường trú để thực thi xác thực (action: {action})...")
                    token = await self._execute_custom_recaptcha_on_tab(
                        tab=tab,
                        website_key=website_key,
                        action=action,
                        enterprise=enterprise,
                    )

                    duration_ms = (time.time() - start_time) * 1000
                    if token:
                        extracted_fingerprint = await self._extract_tab_fingerprint(tab)
                        if not extracted_fingerprint:
                            try:
                                fallback_ua = await self._tab_evaluate(
                                    tab,
                                    "navigator.userAgent || ''",
                                    label="custom_fallback_ua",
                                )
                                fallback_lang = await self._tab_evaluate(
                                    tab,
                                    "navigator.language || ''",
                                    label="custom_fallback_lang",
                                )
                                extracted_fingerprint = {
                                    "user_agent": fallback_ua or "",
                                    "accept_language": fallback_lang or "",
                                    "proxy_url": self._proxy_url,
                                }
                            except Exception:
                                extracted_fingerprint = None
                        self._last_fingerprint = extracted_fingerprint
                        debug_logger.log_info(
                            f"[BrowserCaptcha] [Custom] ✅ Tab test thường trú lấy Token thành công (thời gian {duration_ms:.0f}ms)"
                        )
                        return token

                    raise RuntimeError("Lấy token tùy chỉnh thất bại (trả null)")
                except Exception as e:
                    debug_logger.log_warning(
                        f"[BrowserCaptcha] [Custom] Lần thử {attempt + 1}/{max_retries} thất bại: {str(e)}"
                    )
                    stale_info = self._custom_tabs.pop(cache_key, None)
                    stale_tab = stale_info.get("tab") if isinstance(stale_info, dict) else None
                    if stale_tab:
                        await self._close_tab_quietly(stale_tab)
                    if attempt >= max_retries - 1:
                        debug_logger.log_error(f"[BrowserCaptcha] [Custom] Ngoại lệ khi lấy token: {str(e)}")
                        return None

            return None

    async def get_custom_score(
        self,
        website_url: str,
        website_key: str,
        verify_url: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> Dict[str, Any]:
        """Lấy token trong cùng một tab thường trú và xác thực score trang trực tiếp."""
        token_started_at = time.time()
        token = await self.get_custom_token(
            website_url=website_url,
            website_key=website_key,
            action=action,
            enterprise=enterprise,
        )
        token_elapsed_ms = int((time.time() - token_started_at) * 1000)

        if not token:
            return {
                "token": None,
                "token_elapsed_ms": token_elapsed_ms,
                "verify_mode": "browser_page",
                "verify_elapsed_ms": 0,
                "verify_http_status": None,
                "verify_result": {},
            }

        cache_key = f"{website_url}|{website_key}|{1 if enterprise else 0}"
        async with self._custom_lock:
            custom_info = self._custom_tabs.get(cache_key)
            tab = custom_info.get("tab") if isinstance(custom_info, dict) else None
            if tab is None:
                raise RuntimeError("Tab kiểm tra score trang không tồn tại")
            verify_payload = await self._verify_score_on_tab(tab, token, verify_url)

        return {
            "token": token,
            "token_elapsed_ms": token_elapsed_ms,
            **verify_payload,
        }
