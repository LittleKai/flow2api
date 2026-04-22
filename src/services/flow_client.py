"""Flow API Client for VideoFX (Veo)"""
import asyncio
import json
import contextvars
import time
import uuid
import random
import base64
import ssl
from typing import Dict, Any, Optional, List, Union, Callable, Awaitable
from urllib.parse import quote
import urllib.error
import urllib.request
from curl_cffi.requests import AsyncSession
from ..core.logger import debug_logger
from ..core.config import config

try:
    import httpx
except ImportError:
    httpx = None


class FlowClient:
    """Client cho VideoFX API."""

    def __init__(self, proxy_manager, db=None):
        self.proxy_manager = proxy_manager
        self.db = db  # Database instance for captcha config
        self.labs_base_url = config.flow_labs_base_url  # https://labs.google/fx/api
        self.api_base_url = config.flow_api_base_url    # https://aisandbox-pa.googleapis.com/v1
        self.timeout = config.flow_timeout
        # Cache User-Agent theo từng tài khoản
        self._user_agent_cache = {}
        # Fingerprint trình duyệt gắn với luồng request hiện tại (dùng contextvar, tránh concurrent bị lẫn)
        self._request_fingerprint_ctx: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar(
            "flow_request_fingerprint",
            default=None
        )
        self._remote_browser_prefill_last_sent: Dict[str, float] = {}

        # Default "real browser" headers (Android Chrome style) to reduce upstream 4xx/5xx instability.
        # These will be applied as defaults (won't override caller-provided headers).
        self._default_client_headers = {
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": "\"Android\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "x-browser-channel": "stable",
            "x-browser-copyright": "Copyright 2026 Google LLC. All Rights reserved.",
            "x-browser-validation": "UujAs0GAwdnCJ9nvrswZ+O+oco0=",
            "x-browser-year": "2026",
            "x-client-data": "CJS2yQEIpLbJAQipncoBCNj9ygEIlKHLAQiFoM0BGP6lzwE="
        }
        # Chiến lược dispatch đổi thành "đến đâu gửi đó":
        # Không shape batch hay queue submit ở phía flow2api, tránh biến cùng batch thành nhiều bậc.

    def _generate_user_agent(self, account_id: str = None) -> str:
        """Sinh User-Agent cố định theo ID tài khoản.
        
        Args:
            account_id: định danh tài khoản (email hoặc token_id); cùng tài khoản trả cùng UA
            
        Returns:
            chuỗi User-Agent
        """
        # Nếu không có account_id, sinh UA ngẫu nhiên
        if not account_id:
            account_id = f"random_{random.randint(1, 999999)}"
        
        # Nếu đã cache thì trả luôn
        if account_id in self._user_agent_cache:
            return self._user_agent_cache[account_id]
        
        # Dùng account_id làm random seed để cùng tài khoản sinh cùng UA
        import hashlib
        seed = int(hashlib.md5(account_id.encode()).hexdigest()[:8], 16)
        rng = random.Random(seed)
        
        # Pool phiên bản Chrome
        chrome_versions = ["130.0.0.0", "131.0.0.0", "132.0.0.0", "129.0.0.0"]
        # Pool phiên bản Firefox
        firefox_versions = ["133.0", "132.0", "131.0", "134.0"]
        # Pool phiên bản Safari
        safari_versions = ["18.2", "18.1", "18.0", "17.6"]
        # Pool phiên bản Edge
        edge_versions = ["130.0.0.0", "131.0.0.0", "132.0.0.0"]

        # Cấu hình hệ điều hành
        os_configs = [
            # Windows
            {
                "platform": "Windows NT 10.0; Win64; x64",
                "browsers": [
                    lambda r: f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{r.choice(chrome_versions)} Safari/537.36",
                    lambda r: f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{r.choice(firefox_versions).split('.')[0]}.0) Gecko/20100101 Firefox/{r.choice(firefox_versions)}",
                    lambda r: f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{r.choice(chrome_versions)} Safari/537.36 Edg/{r.choice(edge_versions)}",
                ]
            },
            # macOS
            {
                "platform": "Macintosh; Intel Mac OS X 10_15_7",
                "browsers": [
                    lambda r: f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{r.choice(chrome_versions)} Safari/537.36",
                    lambda r: f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{r.choice(safari_versions)} Safari/605.1.15",
                    lambda r: f"Mozilla/5.0 (Macintosh; Intel Mac OS X 14.{r.randint(0, 7)}; rv:{r.choice(firefox_versions).split('.')[0]}.0) Gecko/20100101 Firefox/{r.choice(firefox_versions)}",
                ]
            },
            # Linux
            {
                "platform": "X11; Linux x86_64",
                "browsers": [
                    lambda r: f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{r.choice(chrome_versions)} Safari/537.36",
                    lambda r: f"Mozilla/5.0 (X11; Linux x86_64; rv:{r.choice(firefox_versions).split('.')[0]}.0) Gecko/20100101 Firefox/{r.choice(firefox_versions)}",
                    lambda r: f"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:{r.choice(firefox_versions).split('.')[0]}.0) Gecko/20100101 Firefox/{r.choice(firefox_versions)}",
                ]
            }
        ]

        # Dùng seed cố định để random chọn OS và trình duyệt
        os_config = rng.choice(os_configs)
        browser_generator = rng.choice(os_config["browsers"])
        user_agent = browser_generator(rng)
        
        # Cache kết quả
        self._user_agent_cache[account_id] = user_agent
        
        return user_agent

    def _set_request_fingerprint(self, fingerprint: Optional[Dict[str, Any]]):
        """Set context fingerprint trình duyệt cho luồng request hiện tại."""
        self._request_fingerprint_ctx.set(dict(fingerprint) if fingerprint else None)

    def get_request_fingerprint(self) -> Optional[Dict[str, Any]]:
        """Lấy snapshot fingerprint trình duyệt của luồng request hiện tại."""
        fingerprint = self._request_fingerprint_ctx.get()
        if not isinstance(fingerprint, dict) or not fingerprint:
            return None
        return dict(fingerprint)

    def clear_request_fingerprint(self):
        """Dọn dẹp fingerprint trình duyệt của luồng request."""
        self._set_request_fingerprint(None)

    async def _make_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        use_st: bool = False,
        st_token: Optional[str] = None,
        use_at: bool = False,
        at_token: Optional[str] = None,
        timeout: Optional[int] = None,
        use_media_proxy: bool = False,
        respect_fingerprint_proxy: bool = True
    ) -> Dict[str, Any]:
        """Xử lý HTTP request chung.

        Args:
            method: phương thức HTTP (GET/POST)
            url: URL đầy đủ
            headers: header request
            json_data: body JSON
            use_st: có dùng xác thực ST không (kiểu Cookie)
            st_token: Session Token
            use_at: có dùng xác thực AT không (kiểu Bearer)
            at_token: Access Token
            timeout: timeout tùy chỉnh (giây), không truyền sẽ dùng mặc định
            use_media_proxy: có dùng proxy tải lên/tải xuống ảnh không
            respect_fingerprint_proxy: có ưu tiên proxy trong fingerprint của trình duyệt giải Captcha không
        """
        fingerprint = self._request_fingerprint_ctx.get()

        proxy_url = None
        if self.proxy_manager:
            if use_media_proxy and hasattr(self.proxy_manager, "get_media_proxy_url"):
                proxy_url = await self.proxy_manager.get_media_proxy_url()
            elif hasattr(self.proxy_manager, "get_request_proxy_url"):
                proxy_url = await self.proxy_manager.get_request_proxy_url()
            else:
                proxy_url = await self.proxy_manager.get_proxy_url()

        if respect_fingerprint_proxy and isinstance(fingerprint, dict) and "proxy_url" in fingerprint:
            proxy_url = fingerprint.get("proxy_url")
            if proxy_url == "":
                proxy_url = None
        request_timeout = timeout or self.timeout

        if headers is None:
            headers = {}
        else:
            headers = dict(headers)

        # Xác thực ST - dùng Cookie
        if use_st and st_token:
            headers["Cookie"] = f"__Secure-next-auth.session-token={st_token}"

        # Xác thực AT - dùng Bearer
        if use_at and at_token:
            headers["authorization"] = f"Bearer {at_token}"

        # Xác định định danh tài khoản (ưu tiên dùng 16 ký tự đầu của token)
        account_id = None
        if st_token:
            account_id = st_token[:16]  # Dùng 16 ký tự đầu của ST
        elif at_token:
            account_id = at_token[:16]  # Dùng 16 ký tự đầu của AT

        # Header chung - ưu tiên UA trong fingerprint của trình duyệt giải Captcha
        fingerprint_user_agent = None
        if isinstance(fingerprint, dict):
            fingerprint_user_agent = fingerprint.get("user_agent")

        headers.update({
            "Content-Type": "application/json",
            "User-Agent": fingerprint_user_agent or self._generate_user_agent(account_id)
        })

        # Nếu có fingerprint trình duyệt giải Captcha, ghi đè client hint chính để request submit đồng nhất với lúc giải Captcha.
        if isinstance(fingerprint, dict):
            if fingerprint.get("accept_language"):
                headers.setdefault("Accept-Language", fingerprint["accept_language"])
            if fingerprint.get("sec_ch_ua"):
                headers["sec-ch-ua"] = fingerprint["sec_ch_ua"]
            if fingerprint.get("sec_ch_ua_mobile"):
                headers["sec-ch-ua-mobile"] = fingerprint["sec_ch_ua_mobile"]
            if fingerprint.get("sec_ch_ua_platform"):
                headers["sec-ch-ua-platform"] = fingerprint["sec_ch_ua_platform"]

        # Add default Chromium/Android client headers (do not override explicitly provided values).
        for key, value in self._default_client_headers.items():
            headers.setdefault(key, value)

        # Log request
        if config.debug_enabled:
            if isinstance(fingerprint, dict):
                proxy_for_log = proxy_url if proxy_url else "direct"
                debug_logger.log_info(
                    f"[FINGERPRINT] Submit request bằng fingerprint trình duyệt giải Captcha: UA={headers.get('User-Agent', '')[:120]}, proxy={proxy_for_log}"
                )
            debug_logger.log_request(
                method=method,
                url=url,
                headers=headers,
                body=json_data,
                proxy=proxy_url
            )

        start_time = time.time()

        try:
            async with AsyncSession() as session:
                if method.upper() == "GET":
                    response = await session.get(
                        url,
                        headers=headers,
                        proxy=proxy_url,
                        timeout=request_timeout,
                        impersonate="chrome110"
                    )
                else:  # POST
                    response = await session.post(
                        url,
                        headers=headers,
                        json=json_data,
                        proxy=proxy_url,
                        timeout=request_timeout,
                        impersonate="chrome110"
                    )

                duration_ms = (time.time() - start_time) * 1000

                # Log response
                if config.debug_enabled:
                    debug_logger.log_response(
                        status_code=response.status_code,
                        headers=dict(response.headers),
                        body=response.text,
                        duration_ms=duration_ms
                    )

                # Kiểm tra lỗi HTTP
                if response.status_code >= 400:
                    # Parse error response
                    error_reason = f"HTTP Error {response.status_code}"
                    try:
                        error_body = response.json()
                        # Trích reason từ error format của Google API
                        if "error" in error_body:
                            error_info = error_body["error"]
                            error_message = error_info.get("message", "")
                            # Trích reason từ details
                            details = error_info.get("details", [])
                            for detail in details:
                                if detail.get("reason"):
                                    error_reason = detail.get("reason")
                                    break
                            if error_message:
                                error_reason = f"{error_reason}: {error_message}"
                    except:
                        error_reason = f"HTTP Error {response.status_code}: {response.text[:200]}"
                    
                    # Khi thất bại, log body request và nội dung lỗi ra console
                    debug_logger.log_error(f"[API FAILED] URL: {url}")
                    debug_logger.log_error(f"[API FAILED] Request Body: {json_data}")
                    debug_logger.log_error(f"[API FAILED] Response: {response.text}")
                    
                    raise Exception(error_reason)

                return response.json()

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            error_msg = str(e)

            # Nếu không phải exception do ta ném thì ghi log
            if "HTTP Error" not in error_msg and not any(x in error_msg for x in ["PUBLIC_ERROR", "INVALID_ARGUMENT"]):
                debug_logger.log_error(f"[API FAILED] URL: {url}")
                debug_logger.log_error(f"[API FAILED] Request Body: {json_data}")
                debug_logger.log_error(f"[API FAILED] Exception: {error_msg}")

            if self._should_fallback_to_urllib(error_msg):
                debug_logger.log_warning(
                    f"[HTTP FALLBACK] curl_cffi request thất bại, fallback sang urllib: {method.upper()} {url}"
                )
                try:
                    return await asyncio.to_thread(
                        self._sync_json_request_via_urllib,
                        method.upper(),
                        url,
                        headers,
                        json_data,
                        proxy_url,
                        request_timeout,
                    )
                except Exception as fallback_error:
                    debug_logger.log_error(
                        f"[HTTP FALLBACK] Fallback urllib cũng thất bại: {fallback_error}"
                    )
                    raise Exception(
                        f"Flow API request failed: curl={error_msg}; urllib={fallback_error}"
                    )

            raise Exception(f"Flow API request failed: {error_msg}")

    def _should_fallback_to_urllib(self, error_message: str) -> bool:
        """Quyết định có fallback từ curl_cffi sang urllib không."""
        error_lower = (error_message or "").lower()
        return any(
            keyword in error_lower
            for keyword in [
                "curl: (6)",
                "curl: (7)",
                "curl: (28)",
                "curl: (35)",
                "curl: (52)",
                "curl: (56)",
                "connection timed out",
                "could not connect",
                "failed to connect",
                "ssl connect error",
                "tls connect error",
                "network is unreachable",
            ]
        )

    def _sync_json_request_via_urllib(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, Any]],
        json_data: Optional[Dict[str, Any]],
        proxy_url: Optional[str],
        timeout: int,
    ) -> Dict[str, Any]:
        """Thực thi JSON request bằng urllib, làm network fallback cho curl_cffi."""
        request_headers = dict(headers or {})
        request_headers.setdefault("Accept", "application/json")

        data = None
        if method.upper() != "GET" and json_data is not None:
            data = json.dumps(json_data, ensure_ascii=False).encode("utf-8")
            request_headers["Content-Type"] = "application/json"

        handlers = [urllib.request.HTTPSHandler(context=ssl.create_default_context())]
        if proxy_url:
            handlers.append(
                urllib.request.ProxyHandler(
                    {"http": proxy_url, "https": proxy_url}
                )
            )

        opener = urllib.request.build_opener(*handlers)
        request = urllib.request.Request(
            url=url,
            data=data,
            headers=request_headers,
            method=method.upper(),
        )

        try:
            with opener.open(
                request,
                timeout=timeout,
            ) as response:
                payload = response.read()
                status_code = int(response.getcode() or 0)
        except urllib.error.HTTPError as exc:
            payload = exc.read() if hasattr(exc, "read") else b""
            status_code = int(getattr(exc, "code", 500) or 500)
            body_text = payload.decode("utf-8", errors="replace")
            raise Exception(f"HTTP Error {status_code}: {body_text[:200]}") from exc
        except Exception as exc:
            raise Exception(str(exc)) from exc

        body_text = payload.decode("utf-8", errors="replace")
        if status_code >= 400:
            raise Exception(f"HTTP Error {status_code}: {body_text[:200]}")

        try:
            return json.loads(body_text) if body_text else {}
        except Exception as exc:
            raise Exception(f"Invalid JSON response: {body_text[:200]}") from exc

    def _is_timeout_error(self, error: Exception) -> bool:
        """Xác định có phải timeout mạng không để fail nhanh và retry."""
        error_lower = str(error).lower()
        return any(keyword in error_lower for keyword in [
            "timed out",
            "timeout",
            "curl: (28)",
            "connection timed out",
            "operation timed out",
        ])

    def _is_retryable_network_error(self, error_str: str) -> bool:
        """Nhận diện lỗi TLS/connection có thể retry."""
        error_lower = (error_str or "").lower()
        return any(keyword in error_lower for keyword in [
            "curl: (35)",
            "curl: (52)",
            "curl: (56)",
            "ssl_error_syscall",
            "tls connect error",
            "ssl connect error",
            "connection reset",
            "connection aborted",
            "connection was reset",
            "unexpected eof",
            "empty reply from server",
            "recv failure",
            "send failure",
            "connection refused",
            "network is unreachable",
            "remote host closed connection",
        ])

    def _get_control_plane_timeout(self) -> int:
        """Giới hạn timeout cho các request control-plane nhẹ, tránh endpoint auth/project treo lâu."""
        return max(5, min(int(self.timeout or 0) or 120, 10))

    async def _acquire_image_launch_gate(
        self,
        token_id: Optional[int],
        token_image_concurrency: Optional[int],
    ) -> tuple[bool, int, int]:
        """Request ảnh không còn queue dispatch cục bộ nữa, vào lấy token rồi submit upstream luôn."""
        return True, 0, 0

    async def _release_image_launch_gate(self, token_id: Optional[int]):
        """Giữ interface, hiện tại không cần release bất kỳ trạng thái dispatch cục bộ nào."""
        return

    async def _acquire_video_launch_gate(
        self,
        token_id: Optional[int],
        token_video_concurrency: Optional[int],
    ) -> tuple[bool, int, int]:
        """Request video không còn queue dispatch cục bộ nữa, vào lấy token rồi submit upstream luôn."""
        return True, 0, 0

    async def _release_video_launch_gate(self, token_id: Optional[int]):
        """Giữ interface, hiện tại không cần release bất kỳ trạng thái dispatch cục bộ nào."""
        return

    async def _make_image_generation_request(
        self,
        url: str,
        json_data: Dict[str, Any],
        at: str,
        attempt_trace: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Request sinh ảnh dùng timeout ngắn hơn và retry nhanh khi timeout mạng."""
        request_timeout = config.flow_image_request_timeout
        total_attempts = max(1, config.flow_image_timeout_retry_count + 1)
        retry_delay = config.flow_image_timeout_retry_delay

        # Với luồng giải Captcha qua trình duyệt/trình duyệt từ xa, ưu tiên giữ exit giống lúc giải Captcha.
        # Nếu không, khi hop đầu chuyển sang media proxy dễ trigger reCAPTCHA verify fail và khuếch đại long-tail.
        fingerprint = self._request_fingerprint_ctx.get()
        has_fingerprint_context = bool(isinstance(fingerprint, dict) and fingerprint)

        has_media_proxy = False
        if self.proxy_manager and config.flow_image_timeout_use_media_proxy_fallback:
            try:
                has_media_proxy = bool(await self.proxy_manager.get_media_proxy_url())
            except Exception:
                has_media_proxy = False
        prefer_media_first = bool(has_media_proxy and config.flow_image_prefer_media_proxy)

        if has_fingerprint_context and prefer_media_first:
            prefer_media_first = False
            debug_logger.log_info(
                "[IMAGE] Phát hiện context fingerprint trình duyệt giải Captcha, hop đầu cố định đi qua luồng Captcha; "
                "media proxy chỉ làm fallback phòng hờ khi timeout mạng."
            )

        last_error: Optional[Exception] = None

        for attempt_index in range(total_attempts):
            if has_media_proxy:
                # Lần retry thứ hai áp dụng chiến lược "luồng chính + luồng dự phòng" để tránh lần nào cũng kẹt ở luồng lỗi.
                if attempt_index == 0:
                    prefer_media_proxy = prefer_media_first
                elif attempt_index == 1:
                    prefer_media_proxy = not prefer_media_first
                else:
                    prefer_media_proxy = prefer_media_first
            else:
                prefer_media_proxy = False
            route_label = "Luồng media proxy" if prefer_media_proxy else "Luồng Captcha"
            http_attempt_started_at = time.time()
            http_attempt_info: Optional[Dict[str, Any]] = None
            if isinstance(attempt_trace, dict):
                http_attempt_info = {
                    "attempt": attempt_index + 1,
                    "route": route_label,
                    "timeout_seconds": request_timeout,
                    "used_media_proxy": bool(prefer_media_proxy),
                }
            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at,
                    timeout=request_timeout,
                    use_media_proxy=prefer_media_proxy,
                    respect_fingerprint_proxy=not prefer_media_proxy,
                )
                if http_attempt_info is not None:
                    http_attempt_info["duration_ms"] = int((time.time() - http_attempt_started_at) * 1000)
                    http_attempt_info["success"] = True
                    attempt_trace.setdefault("http_attempts", []).append(http_attempt_info)
                return result
            except Exception as e:
                last_error = e
                if http_attempt_info is not None:
                    http_attempt_info["duration_ms"] = int((time.time() - http_attempt_started_at) * 1000)
                    http_attempt_info["success"] = False
                    http_attempt_info["timeout_error"] = bool(self._is_timeout_error(e))
                    http_attempt_info["error"] = str(e)[:240]
                    attempt_trace.setdefault("http_attempts", []).append(http_attempt_info)
                if not self._is_timeout_error(e) or attempt_index >= total_attempts - 1:
                    raise

                if has_media_proxy and total_attempts > 1:
                    next_prefer_media_proxy = (
                        not prefer_media_proxy if attempt_index == 0 else prefer_media_proxy
                    )
                else:
                    next_prefer_media_proxy = prefer_media_proxy
                next_route_label = "Luồng media proxy" if next_prefer_media_proxy else "Luồng Captcha"
                debug_logger.log_warning(
                    f"[IMAGE] Request sinh ảnh timeout mạng, chuẩn bị retry nhanh "
                    f"({attempt_index + 2}/{total_attempts}), luồng hiện tại={route_label}, "
                    f"luồng tiếp theo={next_route_label}, timeout={request_timeout}s"
                )
                if retry_delay > 0:
                    await asyncio.sleep(retry_delay)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Request sinh ảnh thất bại")

    # ========== Xác thực (dùng ST) ==========

    async def st_to_at(self, st: str) -> dict:
        """Chuyển ST thành AT.

        Args:
            st: Session Token

        Returns:
            {
                "access_token": "AT",
                "expires": "2025-11-15T04:46:04.000Z",
                "user": {...}
            }
        """
        url = f"{self.labs_base_url}/auth/session"
        result = await self._make_request(
            method="GET",
            url=url,
            use_st=True,
            st_token=st,
            timeout=self._get_control_plane_timeout(),
        )
        return result

    # ========== Quản lý project (dùng ST) ==========

    async def create_project(self, st: str, title: str) -> str:
        """Tạo project, trả về project_id.

        Args:
            st: Session Token
            title: tiêu đề project

        Returns:
            project_id (UUID)
        """
        url = f"{self.labs_base_url}/trpc/project.createProject"
        json_data = {
            "json": {
                "projectTitle": title,
                "toolName": "PINHOLE"
            }
        }
        max_retries = config.flow_max_retries
        request_timeout = max(self._get_control_plane_timeout(), min(self.timeout, 15))
        last_error: Optional[Exception] = None

        for retry_attempt in range(max_retries):
            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_st=True,
                    st_token=st,
                    timeout=request_timeout,
                )
                project_result = (
                    result.get("result", {})
                    .get("data", {})
                    .get("json", {})
                    .get("result", {})
                )
                project_id = project_result.get("projectId")
                if not project_id:
                    raise Exception("Invalid project.createProject response: missing projectId")
                return project_id
            except Exception as e:
                last_error = e
                retry_reason = "Timeout mạng" if self._is_timeout_error(e) else self._get_retry_reason(str(e))
                if retry_reason and retry_attempt < max_retries - 1:
                    debug_logger.log_warning(
                        f"[PROJECT] Tạo project thất bại, chuẩn bị retry ({retry_attempt + 2}/{max_retries}) "
                        f"title={title!r}, reason={retry_reason}: {e}"
                    )
                    await asyncio.sleep(1)
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("Tạo project thất bại")

    async def delete_project(self, st: str, project_id: str):
        """Xóa project.

        Args:
            st: Session Token
            project_id: ID project
        """
        url = f"{self.labs_base_url}/trpc/project.deleteProject"
        json_data = {
            "json": {
                "projectToDeleteId": project_id
            }
        }

        await self._make_request(
            method="POST",
            url=url,
            json_data=json_data,
            use_st=True,
            st_token=st,
            timeout=self._get_control_plane_timeout(),
        )

    # ========== Truy vấn số dư (dùng AT) ==========

    async def get_credits(self, at: str) -> dict:
        """Truy vấn số dư.

        Args:
            at: Access Token

        Returns:
            {
                "credits": 920,
                "userPaygateTier": "PAYGATE_TIER_ONE"
            }
        """
        url = f"{self.api_base_url}/credits"
        result = await self._make_request(
            method="GET",
            url=url,
            use_at=True,
            at_token=at,
            timeout=self._get_control_plane_timeout(),
        )
        return result

    # ========== Tải ảnh lên (dùng AT) ==========

    def _detect_image_mime_type(self, image_bytes: bytes) -> str:
        """Phát hiện MIME type của ảnh thông qua magic bytes ở header file.

        Args:
            image_bytes: bytes ảnh

        Returns:
            chuỗi MIME type, mặc định image/jpeg
        """
        if len(image_bytes) < 12:
            return "image/jpeg"

        # WebP: RIFF....WEBP
        if image_bytes[:4] == b'RIFF' and image_bytes[8:12] == b'WEBP':
            return "image/webp"
        # PNG: 89 50 4E 47
        if image_bytes[:4] == b'\x89PNG':
            return "image/png"
        # JPEG: FF D8 FF
        if image_bytes[:3] == b'\xff\xd8\xff':
            return "image/jpeg"
        # GIF: GIF87a hoặc GIF89a
        if image_bytes[:6] in (b'GIF87a', b'GIF89a'):
            return "image/gif"
        # BMP: BM
        if image_bytes[:2] == b'BM':
            return "image/bmp"
        # JPEG 2000: 00 00 00 0C 6A 50
        if image_bytes[:6] == b'\x00\x00\x00\x0cjP':
            return "image/jp2"

        return "image/jpeg"

    def _convert_to_jpeg(self, image_bytes: bytes) -> bytes:
        """Chuyển ảnh sang định dạng JPEG.

        Args:
            image_bytes: bytes ảnh gốc

        Returns:
            bytes ảnh JPEG
        """
        from io import BytesIO
        from PIL import Image

        img = Image.open(BytesIO(image_bytes))
        # Nếu có kênh alpha thì chuyển sang RGB
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        
        output = BytesIO()
        img.save(output, format='JPEG', quality=95)
        return output.getvalue()

    async def upload_image(
        self,
        at: str,
        image_bytes: bytes,
        aspect_ratio: str = "IMAGE_ASPECT_RATIO_LANDSCAPE",
        project_id: Optional[str] = None
    ) -> str:
        """Tải ảnh lên, trả về mediaId.

        Args:
            at: Access Token
            image_bytes: bytes ảnh
            aspect_ratio: tỉ lệ khung ảnh hoặc video (sẽ tự chuyển sang định dạng ảnh)
            project_id: ID project (endpoint upload mới có thể dùng)

        Returns:
            mediaId
        """
        # Chuyển aspect_ratio video sang aspect_ratio ảnh
        # VIDEO_ASPECT_RATIO_LANDSCAPE -> IMAGE_ASPECT_RATIO_LANDSCAPE
        # VIDEO_ASPECT_RATIO_PORTRAIT -> IMAGE_ASPECT_RATIO_PORTRAIT
        if aspect_ratio.startswith("VIDEO_"):
            aspect_ratio = aspect_ratio.replace("VIDEO_", "IMAGE_")

        # Tự phát hiện MIME type của ảnh
        mime_type = self._detect_image_mime_type(image_bytes)

        # Encode base64 (bỏ prefix)
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')

        # Ưu tiên thử endpoint upload mới: /v1/flow/uploadImage
        # Nếu thất bại tự fallback về endpoint cũ để giữ tương thích
        ext = "png" if "png" in mime_type else "jpg"
        upload_file_name = f"flow2api_upload_{int(time.time() * 1000)}.{ext}"
        new_url = f"{self.api_base_url}/flow/uploadImage"
        normalized_project_id = str(project_id or "").strip()
        new_client_context = {
            "tool": "PINHOLE"
        }
        if normalized_project_id:
            new_client_context["projectId"] = normalized_project_id

        new_json_data = {
            "clientContext": new_client_context,
            "fileName": upload_file_name,
            "imageBytes": image_base64,
            "isHidden": False,
            "isUserUploaded": True,
            "mimeType": mime_type
        }

        # Fallback tương thích: endpoint cũ :uploadUserImage
        legacy_url = f"{self.api_base_url}:uploadUserImage"
        legacy_json_data = {
            "imageInput": {
                "rawImageBytes": image_base64,
                "mimeType": mime_type,
                "isUserUploaded": True,
                "aspectRatio": aspect_ratio
            },
            "clientContext": {
                "sessionId": self._generate_session_id(),
                "tool": "ASSET_MANAGER"
            }
        }
        max_retries = config.flow_max_retries
        last_error: Optional[Exception] = None

        for retry_attempt in range(max_retries):
            try:
                new_result = await self._make_request(
                    method="POST",
                    url=new_url,
                    json_data=new_json_data,
                    use_at=True,
                    at_token=at,
                    use_media_proxy=True
                )
                media_id = (
                    new_result.get("media", {}).get("name")
                    or new_result.get("mediaGenerationId", {}).get("mediaGenerationId")
                )
                if media_id:
                    return media_id
                raise Exception(f"Invalid upload response: missing media id, keys={list(new_result.keys())}")
            except Exception as new_upload_error:
                last_error = new_upload_error
                retry_reason = "Timeout mạng" if self._is_timeout_error(new_upload_error) else self._get_retry_reason(str(new_upload_error))

                # Endpoint cũ không có projectId; nếu upload có context project mà fallback sẽ có thể gán ảnh nhầm project.
                if normalized_project_id:
                    if retry_reason and retry_attempt < max_retries - 1:
                        debug_logger.log_warning(
                            f"[UPLOAD] Upload kèm project gặp {retry_reason}, chuẩn bị retry endpoint mới "
                            f"({retry_attempt + 2}/{max_retries}, project_id={normalized_project_id})..."
                        )
                        await asyncio.sleep(1)
                        continue
                    raise RuntimeError(
                        "Project-scoped image upload failed via /flow/uploadImage; "
                        "legacy :uploadUserImage fallback is disabled because it may attach media "
                        f"to a different project (project_id={normalized_project_id})."
                    ) from new_upload_error

                debug_logger.log_warning(
                    f"[UPLOAD] New upload API failed, fallback to legacy endpoint: {new_upload_error}"
                )

            try:
                legacy_result = await self._make_request(
                    method="POST",
                    url=legacy_url,
                    json_data=legacy_json_data,
                    use_at=True,
                    at_token=at,
                    use_media_proxy=True
                )

                media_id = (
                    legacy_result.get("mediaGenerationId", {}).get("mediaGenerationId")
                    or legacy_result.get("media", {}).get("name")
                )
                if media_id:
                    return media_id
                raise Exception(f"Legacy upload response missing media id: keys={list(legacy_result.keys())}")
            except Exception as legacy_upload_error:
                last_error = legacy_upload_error
                retry_reason = self._get_retry_reason(str(legacy_upload_error))
                if retry_reason and retry_attempt < max_retries - 1:
                    debug_logger.log_warning(
                        f"[UPLOAD] Upload gặp {retry_reason}, chuẩn bị retry ({retry_attempt + 2}/{max_retries})..."
                    )
                    await asyncio.sleep(1)
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("Tải ảnh lên thất bại")

    # ========== Sinh ảnh (dùng AT) - trả đồng bộ ==========

    async def generate_image(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_name: str,
        aspect_ratio: str,
        image_inputs: Optional[List[Dict]] = None,
        token_id: Optional[int] = None,
        token_image_concurrency: Optional[int] = None,
        progress_callback: Optional[Callable[[str, int], Awaitable[None]]] = None,
    ) -> tuple[dict, str, Dict[str, Any]]:
        """Sinh ảnh (trả đồng bộ).

        Args:
            at: Access Token
            project_id: ID project
            prompt: nội dung prompt
            model_name: NARWHAL / GEM_PIX / GEM_PIX_2 / IMAGEN_3_5
            aspect_ratio: tỉ lệ khung ảnh
            image_inputs: danh sách ảnh tham chiếu (dùng khi image-to-image)

        Returns:
            (result, session_id, perf_trace)
            result: kết quả do upstream trả về
            session_id: sessionId cho request sinh ảnh thành công lần này
            perf_trace: dấu vết thời gian retry và thời gian các hop
        """
        url = f"{self.api_base_url}/projects/{project_id}/flowMedia:batchGenerateImages"

        # Logic retry cho 403/reCAPTCHA
        max_retries = config.flow_max_retries
        last_error = None
        perf_trace: Dict[str, Any] = {
            "max_retries": max_retries,
            "generation_attempts": [],
        }
        
        for retry_attempt in range(max_retries):
            attempt_trace: Dict[str, Any] = {
                "attempt": retry_attempt + 1,
                "recaptcha_ok": False,
            }
            attempt_started_at = time.time()
            # Mỗi lần retry đều lấy lại reCAPTCHA token
            recaptcha_started_at = time.time()
            if progress_callback is not None:
                await progress_callback("solving_image_captcha", 38)
            launch_gate_acquired = False
            launch_ok, launch_queue_ms, launch_stagger_ms = await self._acquire_image_launch_gate(
                token_id=token_id,
                token_image_concurrency=token_image_concurrency,
            )
            attempt_trace["launch_queue_ms"] = launch_queue_ms
            attempt_trace["launch_stagger_ms"] = launch_stagger_ms
            if not launch_ok:
                last_error = Exception("Image launch queue wait timeout")
                attempt_trace["success"] = False
                attempt_trace["error"] = str(last_error)
                attempt_trace["duration_ms"] = int((time.time() - attempt_started_at) * 1000)
                perf_trace["generation_attempts"].append(attempt_trace)
                raise last_error

            launch_gate_acquired = True
            try:
                recaptcha_token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="IMAGE_GENERATION",
                    token_id=token_id
                )
            finally:
                if launch_gate_acquired:
                    await self._release_image_launch_gate(token_id)
            attempt_trace["recaptcha_ms"] = int((time.time() - recaptcha_started_at) * 1000)
            attempt_trace["recaptcha_ok"] = bool(recaptcha_token)
            if not recaptcha_token:
                last_error = Exception("Failed to obtain reCAPTCHA token")
                attempt_trace["success"] = False
                attempt_trace["error"] = str(last_error)
                attempt_trace["duration_ms"] = int((time.time() - attempt_started_at) * 1000)
                perf_trace["generation_attempts"].append(attempt_trace)
                should_retry = await self._handle_missing_recaptcha_token(
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[IMAGE] Sinh",
                )
                if should_retry:
                    continue
                raise last_error
            if progress_callback is not None:
                await progress_callback("submitting_image", 48)
            session_id = self._generate_session_id()

            # Dựng request - endpoint mới có clientContext cả ở ngoài và bên trong requests
            client_context = {
                "recaptchaContext": {
                    "token": recaptcha_token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"
                },
                "sessionId": session_id,
                "projectId": project_id,
                "tool": "PINHOLE"
            }

            # Endpoint ảnh mới dùng prompt có cấu trúc + switch new media
            request_data = {
                "clientContext": client_context,
                "seed": random.randint(1, 999999),
                "imageModelName": model_name,
                "imageAspectRatio": aspect_ratio,
                "structuredPrompt": {
                    "parts": [{
                        "text": prompt
                    }]
                },
                "imageInputs": image_inputs or []
            }

            json_data = {
                "clientContext": client_context,
                "mediaGenerationContext": {
                    "batchId": str(uuid.uuid4())
                },
                "useNewMedia": True,
                "requests": [request_data]
            }

            try:
                result = await self._make_image_generation_request(
                    url=url,
                    json_data=json_data,
                    at=at,
                    attempt_trace=attempt_trace,
                )
                attempt_trace["success"] = True
                attempt_trace["duration_ms"] = int((time.time() - attempt_started_at) * 1000)
                perf_trace["generation_attempts"].append(attempt_trace)
                perf_trace["final_success_attempt"] = retry_attempt + 1
                return result, session_id, perf_trace
            except Exception as e:
                last_error = e
                attempt_trace["success"] = False
                attempt_trace["error"] = str(e)[:240]
                attempt_trace["duration_ms"] = int((time.time() - attempt_started_at) * 1000)
                perf_trace["generation_attempts"].append(attempt_trace)
                should_retry = await self._handle_retryable_generation_error(
                    error=e,
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[IMAGE] Sinh",
                )
                if should_retry:
                    continue
                raise
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)
        
        # Mọi lần retry đều thất bại
        perf_trace["final_success_attempt"] = None
        raise last_error

    async def upsample_image(
        self,
        at: str,
        project_id: str,
        media_id: str,
        target_resolution: str = "UPSAMPLE_IMAGE_RESOLUTION_4K",
        user_paygate_tier: str = "PAYGATE_TIER_NOT_PAID",
        session_id: Optional[str] = None,
        token_id: Optional[int] = None
    ) -> str:
        """Phóng ảnh lên 2K/4K.

        Args:
            at: Access Token
            project_id: ID project
            media_id: mediaId của ảnh (từ media[0]["name"] do batchGenerateImages trả về)
            target_resolution: UPSAMPLE_IMAGE_RESOLUTION_2K hoặc UPSAMPLE_IMAGE_RESOLUTION_4K
            user_paygate_tier: tier user (ví dụ PAYGATE_TIER_NOT_PAID / PAYGATE_TIER_ONE)
            session_id: tùy chọn, tái dùng sessionId của request sinh ảnh

        Returns:
            dữ liệu ảnh đã encode base64
        """
        url = f"{self.api_base_url}/flow/upsampleImage"

        # Logic retry cho 403/reCAPTCHA/500 - dùng số retry tối đa trong cấu hình
        max_retries = config.flow_max_retries
        last_error = None

        for retry_attempt in range(max_retries):
            # Lấy reCAPTCHA token - dùng action IMAGE_GENERATION
            recaptcha_token, browser_id = await self._get_recaptcha_token(
                project_id,
                action="IMAGE_GENERATION",
                token_id=token_id
            )
            if not recaptcha_token:
                last_error = Exception("Failed to obtain reCAPTCHA token")
                should_retry = await self._handle_missing_recaptcha_token(
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[IMAGE UPSAMPLE] Phóng to",
                )
                if should_retry:
                    continue
                raise last_error
            upsample_session_id = session_id or self._generate_session_id()

            json_data = {
                "mediaId": media_id,
                "targetResolution": target_resolution,
                "clientContext": {
                    "recaptchaContext": {
                        "token": recaptcha_token,
                        "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"
                    },
                    "sessionId": upsample_session_id,
                    "projectId": project_id,
                    "tool": "PINHOLE",
                    "userPaygateTier": user_paygate_tier
                }
            }

            # Phóng to 4K/2K dùng timeout riêng vì dữ liệu base64 trả về rất lớn
            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at,
                    timeout=config.upsample_timeout
                )

                # Trả về ảnh đã encode base64
                return result.get("encodedImage", "")
            except Exception as e:
                last_error = e
                should_retry = await self._handle_retryable_generation_error(
                    error=e,
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[IMAGE UPSAMPLE] Phóng to",
                )
                if should_retry:
                    continue
                raise
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)

        raise last_error

    # ========== Sinh video (dùng AT) - trả bất đồng bộ ==========

    def _build_video_text_input(self, prompt: str, use_v2_model_config: bool = False) -> Dict[str, Any]:
        if use_v2_model_config:
            return {
                "structuredPrompt": {
                    "parts": [{
                        "text": prompt
                    }]
                }
            }
        return {
            "prompt": prompt
        }

    async def generate_video_text(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        use_v2_model_config: bool = False,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
    ) -> dict:
        """Text-to-Video, trả task_id.

        Args:
            at: Access Token
            project_id: ID project
            prompt: nội dung prompt
            model_key: veo_3_1_t2v_fast v.v.
            aspect_ratio: tỉ lệ khung video
            user_paygate_tier: tier user

        Returns:
            {
                "operations": [{
                    "operation": {"name": "task_id"},
                    "sceneId": "uuid",
                    "status": "MEDIA_GENERATION_STATUS_PENDING"
                }],
                "remainingCredits": 900
            }
        """
        url = f"{self.api_base_url}/video:batchAsyncGenerateVideoText"

        # Logic retry cho 403/reCAPTCHA - dùng số retry tối đa trong cấu hình
        max_retries = config.flow_max_retries
        last_error = None
        
        for retry_attempt in range(max_retries):
            # Mỗi lần retry đều lấy lại reCAPTCHA token - video dùng action VIDEO_GENERATION
            launch_gate_acquired = False
            launch_ok, _, _ = await self._acquire_video_launch_gate(
                token_id=token_id,
                token_video_concurrency=token_video_concurrency,
            )
            if not launch_ok:
                last_error = Exception("Video launch queue wait timeout")
                raise last_error

            launch_gate_acquired = True
            try:
                recaptcha_token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="VIDEO_GENERATION",
                    token_id=token_id
                )
            finally:
                if launch_gate_acquired:
                    await self._release_video_launch_gate(token_id)
            if not recaptcha_token:
                last_error = Exception("Failed to obtain reCAPTCHA token")
                should_retry = await self._handle_missing_recaptcha_token(
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO T2V] Sinh",
                )
                if should_retry:
                    continue
                raise last_error
            session_id = self._generate_session_id()
            scene_id = str(uuid.uuid4())
            client_context = {
                "recaptchaContext": {
                    "token": recaptcha_token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"
                },
                "sessionId": session_id,
                "projectId": project_id,
                "tool": "PINHOLE",
                "userPaygateTier": user_paygate_tier
            }
            request_data = {
                "aspectRatio": aspect_ratio,
                "seed": random.randint(1, 99999),
                "textInput": self._build_video_text_input(prompt, use_v2_model_config=use_v2_model_config),
                "videoModelKey": model_key,
                "metadata": {
                    "sceneId": scene_id
                }
            }
            json_data = {
                "clientContext": client_context,
                "requests": [request_data]
            }
            if use_v2_model_config:
                json_data["mediaGenerationContext"] = {
                    "batchId": str(uuid.uuid4())
                }
                json_data["useV2ModelConfig"] = True

            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at
                )
                return result
            except Exception as e:
                last_error = e
                should_retry = await self._handle_retryable_generation_error(
                    error=e,
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO T2V] Sinh",
                )
                if should_retry:
                    continue
                raise
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)
        
        # Mọi lần retry đều thất bại
        raise last_error

    async def generate_video_reference_images(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        reference_images: List[Dict],
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
    ) -> dict:
        """Image-to-Video, trả task_id.

        Args:
            at: Access Token
            project_id: ID project
            prompt: nội dung prompt
            model_key: veo_3_1_r2v_fast_landscape
            aspect_ratio: tỉ lệ khung video
            reference_images: danh sách ảnh tham chiếu [{"imageUsageType": "IMAGE_USAGE_TYPE_ASSET", "mediaId": "..."}]
            user_paygate_tier: tier user

        Returns:
            giống generate_video_text
        """
        url = f"{self.api_base_url}/video:batchAsyncGenerateVideoReferenceImages"

        # Logic retry cho 403/reCAPTCHA - dùng số retry tối đa trong cấu hình
        max_retries = config.flow_max_retries
        last_error = None
        
        for retry_attempt in range(max_retries):
            # Mỗi lần retry đều lấy lại reCAPTCHA token - video dùng action VIDEO_GENERATION
            launch_gate_acquired = False
            launch_ok, _, _ = await self._acquire_video_launch_gate(
                token_id=token_id,
                token_video_concurrency=token_video_concurrency,
            )
            if not launch_ok:
                last_error = Exception("Video launch queue wait timeout")
                raise last_error

            launch_gate_acquired = True
            try:
                recaptcha_token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="VIDEO_GENERATION",
                    token_id=token_id
                )
            finally:
                if launch_gate_acquired:
                    await self._release_video_launch_gate(token_id)
            if not recaptcha_token:
                last_error = Exception("Failed to obtain reCAPTCHA token")
                should_retry = await self._handle_missing_recaptcha_token(
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO R2V] Sinh",
                )
                if should_retry:
                    continue
                raise last_error
            session_id = self._generate_session_id()
            batch_id = str(uuid.uuid4())
            scene_id = str(uuid.uuid4())

            json_data = {
                "mediaGenerationContext": {
                    "batchId": batch_id
                },
                "clientContext": {
                    "recaptchaContext": {
                        "token": recaptcha_token,
                        "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"
                    },
                    "sessionId": session_id,
                    "projectId": project_id,
                    "tool": "PINHOLE",
                    "userPaygateTier": user_paygate_tier
                },
                "requests": [{
                    "aspectRatio": aspect_ratio,
                    "seed": random.randint(1, 99999),
                    "textInput": {
                        "structuredPrompt": {
                            "parts": [{
                                "text": prompt
                            }]
                        }
                    },
                    "videoModelKey": model_key,
                    "referenceImages": reference_images,
                    "metadata": {
                        "sceneId": scene_id
                    }
                }],
                "useV2ModelConfig": True
            }

            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at
                )
                return result
            except Exception as e:
                last_error = e
                should_retry = await self._handle_retryable_generation_error(
                    error=e,
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO R2V] Sinh",
                )
                if should_retry:
                    continue
                raise
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)
        
        # Mọi lần retry đều thất bại
        raise last_error

    async def generate_video_start_end(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        start_media_id: str,
        end_media_id: str,
        use_v2_model_config: bool = False,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
    ) -> dict:
        """Sinh video với khung đầu/cuối, trả task_id.

        Args:
            at: Access Token
            project_id: ID project
            prompt: nội dung prompt
            model_key: veo_3_1_i2v_s_fast_fl
            aspect_ratio: tỉ lệ khung video
            start_media_id: mediaId khung đầu
            end_media_id: mediaId khung cuối
            user_paygate_tier: tier user

        Returns:
            giống generate_video_text
        """
        url = f"{self.api_base_url}/video:batchAsyncGenerateVideoStartAndEndImage"

        # Logic retry cho 403/reCAPTCHA - dùng số retry tối đa trong cấu hình
        max_retries = config.flow_max_retries
        last_error = None
        
        for retry_attempt in range(max_retries):
            # Mỗi lần retry đều lấy lại reCAPTCHA token - video dùng action VIDEO_GENERATION
            launch_gate_acquired = False
            launch_ok, _, _ = await self._acquire_video_launch_gate(
                token_id=token_id,
                token_video_concurrency=token_video_concurrency,
            )
            if not launch_ok:
                last_error = Exception("Video launch queue wait timeout")
                raise last_error

            launch_gate_acquired = True
            try:
                recaptcha_token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="VIDEO_GENERATION",
                    token_id=token_id
                )
            finally:
                if launch_gate_acquired:
                    await self._release_video_launch_gate(token_id)
            if not recaptcha_token:
                last_error = Exception("Failed to obtain reCAPTCHA token")
                should_retry = await self._handle_missing_recaptcha_token(
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO I2V] Sinh khung đầu/cuối",
                )
                if should_retry:
                    continue
                raise last_error
            session_id = self._generate_session_id()
            scene_id = str(uuid.uuid4())
            client_context = {
                "recaptchaContext": {
                    "token": recaptcha_token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"
                },
                "sessionId": session_id,
                "projectId": project_id,
                "tool": "PINHOLE",
                "userPaygateTier": user_paygate_tier
            }
            request_data = {
                "aspectRatio": aspect_ratio,
                "seed": random.randint(1, 99999),
                "textInput": self._build_video_text_input(prompt, use_v2_model_config=use_v2_model_config),
                "videoModelKey": model_key,
                "startImage": {
                    "mediaId": start_media_id
                },
                "endImage": {
                    "mediaId": end_media_id
                },
                "metadata": {
                    "sceneId": scene_id
                }
            }
            json_data = {
                "clientContext": client_context,
                "requests": [request_data]
            }
            if use_v2_model_config:
                json_data["mediaGenerationContext"] = {
                    "batchId": str(uuid.uuid4())
                }
                json_data["useV2ModelConfig"] = True

            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at
                )
                return result
            except Exception as e:
                last_error = e
                should_retry = await self._handle_retryable_generation_error(
                    error=e,
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO I2V] Sinh khung đầu/cuối",
                )
                if should_retry:
                    continue
                raise
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)
        
        # Mọi lần retry đều thất bại
        raise last_error

    async def generate_video_start_image(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        start_media_id: str,
        use_v2_model_config: bool = False,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
    ) -> dict:
        """Sinh video chỉ với khung đầu, trả task_id.

        Args:
            at: Access Token
            project_id: ID project
            prompt: nội dung prompt
            model_key: veo_3_1_i2v_s_fast_fl v.v.
            aspect_ratio: tỉ lệ khung video
            start_media_id: mediaId khung đầu
            user_paygate_tier: tier user

        Returns:
            giống generate_video_text
        """
        url = f"{self.api_base_url}/video:batchAsyncGenerateVideoStartImage"

        # Logic retry cho 403/reCAPTCHA - dùng số retry tối đa trong cấu hình
        max_retries = config.flow_max_retries
        last_error = None
        
        for retry_attempt in range(max_retries):
            # Mỗi lần retry đều lấy lại reCAPTCHA token - video dùng action VIDEO_GENERATION
            launch_gate_acquired = False
            launch_ok, _, _ = await self._acquire_video_launch_gate(
                token_id=token_id,
                token_video_concurrency=token_video_concurrency,
            )
            if not launch_ok:
                last_error = Exception("Video launch queue wait timeout")
                raise last_error

            launch_gate_acquired = True
            try:
                recaptcha_token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="VIDEO_GENERATION",
                    token_id=token_id
                )
            finally:
                if launch_gate_acquired:
                    await self._release_video_launch_gate(token_id)
            if not recaptcha_token:
                last_error = Exception("Failed to obtain reCAPTCHA token")
                should_retry = await self._handle_missing_recaptcha_token(
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO I2V] Sinh khung đầu",
                )
                if should_retry:
                    continue
                raise last_error
            session_id = self._generate_session_id()
            scene_id = str(uuid.uuid4())
            client_context = {
                "recaptchaContext": {
                    "token": recaptcha_token,
                    "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"
                },
                "sessionId": session_id,
                "projectId": project_id,
                "tool": "PINHOLE",
                "userPaygateTier": user_paygate_tier
            }
            request_data = {
                "aspectRatio": aspect_ratio,
                "seed": random.randint(1, 99999),
                "textInput": self._build_video_text_input(prompt, use_v2_model_config=use_v2_model_config),
                "videoModelKey": model_key,
                "startImage": {
                    "mediaId": start_media_id
                },
                # Lưu ý: không có field endImage, chỉ dùng khung đầu
                "metadata": {
                    "sceneId": scene_id
                }
            }
            json_data = {
                "clientContext": client_context,
                "requests": [request_data]
            }
            if use_v2_model_config:
                json_data["mediaGenerationContext"] = {
                    "batchId": str(uuid.uuid4())
                }
                json_data["useV2ModelConfig"] = True

            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at
                )
                return result
            except Exception as e:
                last_error = e
                should_retry = await self._handle_retryable_generation_error(
                    error=e,
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO I2V] Sinh khung đầu",
                )
                if should_retry:
                    continue
                raise
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)
        
        # Mọi lần retry đều thất bại
        raise last_error

    # ========== Phóng to video (Video Upsampler) ==========

    async def upsample_video(
        self,
        at: str,
        project_id: str,
        video_media_id: str,
        aspect_ratio: str,
        resolution: str,
        model_key: str,
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
    ) -> dict:
        """Phóng to video lên 4K/1080P, trả task_id.

        Args:
            at: Access Token
            project_id: ID project
            video_media_id: mediaId của video
            aspect_ratio: tỉ lệ khung video VIDEO_ASPECT_RATIO_PORTRAIT/LANDSCAPE
            resolution: VIDEO_RESOLUTION_4K hoặc VIDEO_RESOLUTION_1080P
            model_key: veo_3_1_upsampler_4k hoặc veo_3_1_upsampler_1080p

        Returns:
            giống generate_video_text
        """
        url = f"{self.api_base_url}/video:batchAsyncGenerateVideoUpsampleVideo"

        # Logic retry cho 403/reCAPTCHA - dùng số retry tối đa trong cấu hình
        max_retries = config.flow_max_retries
        last_error = None
        
        for retry_attempt in range(max_retries):
            launch_gate_acquired = False
            launch_ok, _, _ = await self._acquire_video_launch_gate(
                token_id=token_id,
                token_video_concurrency=token_video_concurrency,
            )
            if not launch_ok:
                last_error = Exception("Video launch queue wait timeout")
                raise last_error

            launch_gate_acquired = True
            try:
                recaptcha_token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="VIDEO_GENERATION",
                    token_id=token_id
                )
            finally:
                if launch_gate_acquired:
                    await self._release_video_launch_gate(token_id)
            if not recaptcha_token:
                last_error = Exception("Failed to obtain reCAPTCHA token")
                should_retry = await self._handle_missing_recaptcha_token(
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO UPSAMPLE] Phóng to",
                )
                if should_retry:
                    continue
                raise last_error
            session_id = self._generate_session_id()
            scene_id = str(uuid.uuid4())

            json_data = {
                "requests": [{
                    "aspectRatio": aspect_ratio,
                    "resolution": resolution,
                    "seed": random.randint(1, 99999),
                    "videoInput": {
                        "mediaId": video_media_id
                    },
                    "videoModelKey": model_key,
                    "metadata": {
                        "sceneId": scene_id
                    }
                }],
                "clientContext": {
                    "recaptchaContext": {
                        "token": recaptcha_token,
                        "applicationType": "RECAPTCHA_APPLICATION_TYPE_WEB"
                    },
                    "sessionId": session_id
                }
            }

            try:
                result = await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at
                )
                return result
            except Exception as e:
                last_error = e
                should_retry = await self._handle_retryable_generation_error(
                    error=e,
                    retry_attempt=retry_attempt,
                    max_retries=max_retries,
                    browser_id=browser_id,
                    project_id=project_id,
                    log_prefix="[VIDEO UPSAMPLE] Phóng to",
                )
                if should_retry:
                    continue
                raise
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)
        
        raise last_error

    # ========== Polling tác vụ (dùng AT) ==========

    async def check_video_status(self, at: str, operations: List[Dict]) -> dict:
        """Truy vấn trạng thái sinh video.

        Args:
            at: Access Token
            operations: danh sách operation [{"operation": {"name": "task_id"}, "sceneId": "...", "status": "..."}]

        Returns:
            {
                "operations": [{
                    "operation": {
                        "name": "task_id",
                        "metadata": {...}  # Khi hoàn tất sẽ có thông tin video
                    },
                    "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL"
                }]
            }
        """
        url = f"{self.api_base_url}/video:batchCheckAsyncVideoGenerationStatus"

        json_data = {
            "operations": operations
        }
        max_retries = config.flow_max_retries
        last_error: Optional[Exception] = None

        for retry_attempt in range(max_retries):
            try:
                return await self._make_request(
                    method="POST",
                    url=url,
                    json_data=json_data,
                    use_at=True,
                    at_token=at
                )
            except Exception as e:
                last_error = e
                retry_reason = self._get_retry_reason(str(e))
                if retry_reason and retry_attempt < max_retries - 1:
                    debug_logger.log_warning(
                        f"[VIDEO POLL] Truy vấn trạng thái gặp {retry_reason}, chuẩn bị retry ({retry_attempt + 2}/{max_retries})..."
                    )
                    await asyncio.sleep(1)
                    continue
                raise

        if last_error is not None:
            raise last_error
        raise RuntimeError("Truy vấn trạng thái video thất bại")

    # ========== Xóa media (dùng ST) ==========

    async def delete_media(self, st: str, media_names: List[str]):
        """Xóa media.

        Args:
            st: Session Token
            media_names: danh sách ID media
        """
        url = f"{self.labs_base_url}/trpc/media.deleteMedia"
        json_data = {
            "json": {
                "names": media_names
            }
        }

        await self._make_request(
            method="POST",
            url=url,
            json_data=json_data,
            use_st=True,
            st_token=st
        )

    # ========== Phương thức phụ ==========

    async def _handle_retryable_generation_error(
        self,
        error: Exception,
        retry_attempt: int,
        max_retries: int,
        browser_id: Optional[Union[int, str]],
        project_id: str,
        log_prefix: str,
    ) -> bool:
        """Xử lý chung logic retry và self-heal Captcha cho luồng sinh nội dung."""
        error_str = str(error)
        retry_reason = self._get_retry_reason(error_str)
        notify_reason = retry_reason or error_str[:120] or type(error).__name__
        await self._notify_browser_captcha_error(
            browser_id=browser_id,
            project_id=project_id,
            error_reason=notify_reason,
            error_message=error_str,
        )
        if not retry_reason:
            return False

        is_terminal_attempt = retry_attempt >= max_retries - 1

        if is_terminal_attempt:
            debug_logger.log_warning(
                f"{log_prefix} gặp {retry_reason}, đã đạt số retry tối đa ({max_retries}), request này thất bại và thực thi đóng/thu hồi."
            )
            return False

        debug_logger.log_warning(
            f"{log_prefix} gặp {retry_reason}, đang lấy lại Captcha để retry ({retry_attempt + 2}/{max_retries})..."
        )
        await asyncio.sleep(1)
        return True

    async def _handle_missing_recaptcha_token(
        self,
        retry_attempt: int,
        max_retries: int,
        browser_id: Optional[Union[int, str]],
        project_id: str,
        log_prefix: str,
    ) -> bool:
        token_error = Exception("Failed to obtain reCAPTCHA token")
        return await self._handle_retryable_generation_error(
            error=token_error,
            retry_attempt=retry_attempt,
            max_retries=max_retries,
            browser_id=browser_id,
            project_id=project_id,
            log_prefix=log_prefix,
        )

    def _get_retry_reason(self, error_str: str) -> Optional[str]:
        """Quyết định có cần retry không, trả về text gợi ý cho log."""
        error_lower = error_str.lower()
        if "403" in error_lower:
            return "Lỗi 403"
        if "429" in error_lower or "too many requests" in error_lower:
            return "Rate limit 429"
        if self._is_retryable_network_error(error_str):
            return "Lỗi mạng/TLS"
        if "recaptcha evaluation failed" in error_lower:
            return "reCAPTCHA verify thất bại"
        if "recaptcha" in error_lower:
            return "Lỗi reCAPTCHA"
        if any(keyword in error_lower for keyword in [
            "http error 500",
            "public_error",
            "internal error",
            "reason=internal",
            "reason: internal",
            "\"reason\":\"internal\"",
            "server error",
            "upstream error",
        ]):
            return "500/Lỗi nội bộ"
        return None

    async def _notify_browser_captcha_error(
        self,
        browser_id: Optional[Union[int, str]] = None,
        project_id: Optional[str] = None,
        error_reason: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        """Thông báo dịch vụ Captcha trình duyệt self-heal khi thất bại.
        
        Args:
            browser_id: ID trình duyệt dùng trong chế độ browser
            project_id: project_id dùng trong chế độ personal
            error_reason: lý do lỗi đã được phân loại
            error_message: text lỗi gốc
        """
        if config.captcha_method == "browser":
            try:
                from .browser_captcha import BrowserCaptchaService
                service = await BrowserCaptchaService.get_instance(self.db)
                await service.report_error(
                    browser_id,
                    error_reason=error_reason or error_message or "upstream_error"
                )
            except Exception:
                pass
        elif config.captcha_method == "personal" and project_id:
            try:
                from .browser_captcha_personal import BrowserCaptchaService
                service = await BrowserCaptchaService.get_instance(self.db)
                await service.report_flow_error(
                    project_id=project_id,
                    error_reason=error_reason or "",
                    error_message=error_message or "",
                )
            except Exception:
                pass
        elif config.captcha_method == "remote_browser" and browser_id:
            try:
                session_id = quote(str(browser_id), safe="")
                await self._call_remote_browser_service(
                    method="POST",
                    path=f"/api/v1/sessions/{session_id}/error",
                    json_data={"error_reason": error_reason or error_message or "upstream_error"},
                    timeout_override=2,
                )
            except Exception as e:
                debug_logger.log_warning(f"[reCAPTCHA RemoteBrowser] Báo cáo error thất bại: {e}")

    async def _notify_browser_captcha_request_finished(self, browser_id: Optional[Union[int, str]] = None):
        """Thông báo trình duyệt có giao diện: upstream request ảnh/video đã kết thúc, có thể đóng trình duyệt giải Captcha tương ứng."""
        if config.captcha_method == "browser":
            try:
                from .browser_captcha import BrowserCaptchaService
                service = await BrowserCaptchaService.get_instance(self.db)
                await service.report_request_finished(browser_id)
            except Exception:
                pass
        elif config.captcha_method == "remote_browser" and browser_id:
            try:
                session_id = quote(str(browser_id), safe="")
                await self._call_remote_browser_service(
                    method="POST",
                    path=f"/api/v1/sessions/{session_id}/finish",
                    json_data={"status": "success"},
                    timeout_override=2,
                )
            except Exception as e:
                debug_logger.log_warning(f"[reCAPTCHA RemoteBrowser] Báo cáo finish thất bại: {e}")

    def _generate_session_id(self) -> str:
        """Sinh sessionId: ;timestamp."""
        return f";{int(time.time() * 1000)}"

    def _generate_scene_id(self) -> str:
        """Sinh sceneId: UUID."""
        return str(uuid.uuid4())

    def _get_remote_browser_service_config(self) -> tuple[str, str, int]:
        base_url = (config.remote_browser_base_url or "").strip().rstrip("/")
        api_key = (config.remote_browser_api_key or "").strip()
        timeout = max(5, int(config.remote_browser_timeout or 60))

        if not base_url:
            raise RuntimeError("Chưa cấu hình địa chỉ dịch vụ remote_browser")
        if not api_key:
            raise RuntimeError("Chưa cấu hình API Key cho remote_browser")

        if not (base_url.startswith("http://") or base_url.startswith("https://")):
            raise RuntimeError("Địa chỉ dịch vụ remote_browser sai định dạng")

        return base_url, api_key, timeout

    @staticmethod
    def _build_remote_browser_http_timeout(read_timeout: float) -> Any:
        read_value = max(3.0, float(read_timeout))
        write_value = min(10.0, max(3.0, read_value))
        if httpx is None:
            return read_value
        return httpx.Timeout(
            connect=2.5,
            read=read_value,
            write=write_value,
            pool=2.5,
        )

    @staticmethod
    def _parse_json_response_text(text: str) -> Optional[Any]:
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return None

    @staticmethod
    async def _stdlib_json_http_request(
        method: str,
        url: str,
        headers: Dict[str, str],
        payload: Optional[Dict[str, Any]],
        timeout: int,
    ) -> tuple[int, Optional[Any], str]:
        req_headers = dict(headers or {})
        req_headers.setdefault("Accept", "application/json")
        request_method = (method or "GET").upper()
        request_data: Optional[bytes] = None

        if payload is not None:
            req_headers["Content-Type"] = "application/json; charset=utf-8"
            if request_method != "GET":
                request_data = json.dumps(payload).encode("utf-8")

        def do_request() -> tuple[int, str]:
            request = urllib.request.Request(
                url=url,
                data=request_data,
                headers=req_headers,
                method=request_method,
            )
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            try:
                with opener.open(request, timeout=max(1.0, float(timeout))) as response:
                    status_code = int(getattr(response, "status", 0) or response.getcode() or 0)
                    body = response.read()
                    charset = response.headers.get_content_charset() or "utf-8"
                    return status_code, body.decode(charset, errors="replace")
            except urllib.error.HTTPError as exc:
                body = exc.read()
                charset = exc.headers.get_content_charset() if exc.headers else None
                return int(getattr(exc, "code", 0) or 0), body.decode(charset or "utf-8", errors="replace")

        try:
            status_code, text = await asyncio.to_thread(do_request)
        except Exception as e:
            raise RuntimeError(f"Request remote_browser thất bại: {e}") from e

        return status_code, FlowClient._parse_json_response_text(text), text

    @staticmethod
    async def _sync_json_http_request(
        method: str,
        url: str,
        headers: Dict[str, str],
        payload: Optional[Dict[str, Any]],
        timeout: int,
    ) -> tuple[int, Optional[Any], str]:
        req_headers = dict(headers or {})
        req_headers.setdefault("Accept", "application/json")
        request_method = (method or "GET").upper()
        request_kwargs: Dict[str, Any] = {
            "headers": req_headers,
            "timeout": FlowClient._build_remote_browser_http_timeout(timeout),
        }

        if payload is not None:
            req_headers["Content-Type"] = "application/json; charset=utf-8"
            if request_method != "GET":
                request_kwargs["json"] = payload

        if httpx is None:
            return await FlowClient._stdlib_json_http_request(
                method=method,
                url=url,
                headers=req_headers,
                payload=payload,
                timeout=timeout,
            )

        try:
            # Control-plane của remote_browser chỉ cần truyền JSON ổn định, không cần giả browser fingerprint.
            # Dùng httpx tránh được việc curl_cffi trong môi trường hiện tại nuốt mất POST body.
            async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as session:
                response = await session.request(
                    method=request_method,
                    url=url,
                    **request_kwargs,
                )
        except Exception as e:
            raise RuntimeError(f"Request remote_browser thất bại: {e}") from e

        status_code = int(getattr(response, "status_code", 0) or 0)
        text = response.text or ""
        parsed = FlowClient._parse_json_response_text(text)

        return status_code, parsed, text

    async def _call_remote_browser_service(
        self,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        timeout_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        base_url, api_key, timeout = self._get_remote_browser_service_config()
        url = f"{base_url}{path}"
        effective_timeout = max(5, int(timeout_override or timeout))

        status_code, payload, response_text = await self._sync_json_http_request(
            method=method,
            url=url,
            headers={"Authorization": f"Bearer {api_key}"},
            payload=json_data,
            timeout=effective_timeout,
        )

        if status_code >= 400:
            detail = ""
            if isinstance(payload, dict):
                detail = payload.get("detail") or payload.get("message") or str(payload)
            if not detail:
                detail = (response_text or "").strip() or f"HTTP {status_code}"
            raise RuntimeError(f"Request remote_browser thất bại: {detail}")

        if not isinstance(payload, dict):
            raise RuntimeError("remote_browser trả về sai định dạng")

        return payload

    async def prefill_remote_browser_pool(
        self,
        project_id: str,
        action: str = "IMAGE_GENERATION",
        token_id: Optional[int] = None,
        *,
        cooldown_seconds: float = 8.0,
    ) -> bool:
        """Cho dịch vụ remote_browser cục bộ bắt đầu fill pool sớm để dịch thời gian chờ lấy token về phía trước."""
        if config.captcha_method != "remote_browser":
            return False

        normalized_project = str(project_id or "").strip()
        normalized_action = str(action or "IMAGE_GENERATION").strip() or "IMAGE_GENERATION"
        if not normalized_project:
            return False

        cache_key = f"{normalized_project}|{normalized_action}|{int(token_id or 0)}"
        now_value = time.monotonic()
        last_sent = float(self._remote_browser_prefill_last_sent.get(cache_key, 0.0) or 0.0)
        if (now_value - last_sent) < max(0.5, float(cooldown_seconds)):
            return False

        try:
            await self._call_remote_browser_service(
                method="POST",
                path="/api/v1/prefill",
                json_data={
                    "project_id": normalized_project,
                    "action": normalized_action,
                    "token_id": token_id,
                },
                timeout_override=3,
            )
            self._remote_browser_prefill_last_sent[cache_key] = now_value
            return True
        except Exception as e:
            debug_logger.log_warning(f"[reCAPTCHA RemoteBrowser] prefill thất bại: {e}")
            return False

    async def prefill_remote_browser_for_tokens(self, tokens: List[Any], action: str = "IMAGE_GENERATION") -> int:
        if config.captcha_method != "remote_browser":
            return 0

        unique_projects: List[str] = []
        seen_projects = set()
        for token in tokens or []:
            project_id = str(getattr(token, "current_project_id", "") or "").strip()
            if not project_id or project_id in seen_projects:
                continue
            seen_projects.add(project_id)
            unique_projects.append(project_id)

        warmed = 0
        for project_id in unique_projects:
            if await self.prefill_remote_browser_pool(project_id, action=action):
                warmed += 1
        return warmed

    def _resolve_remote_browser_solve_timeout(self, action: str) -> int:
        base_timeout = max(5, int(config.remote_browser_timeout or 60))
        action_name = str(action or "").strip().upper()

        # Ở đây chỉ lấy reCAPTCHA token, không nên dùng chung timeout vài trăm giây của cả luồng sinh nội dung.
        target_timeout = 45 if action_name == "VIDEO_GENERATION" else 35
        return max(12, min(base_timeout, target_timeout))

    async def _get_recaptcha_token(
        self,
        project_id: str,
        action: str = "IMAGE_GENERATION",
        token_id: Optional[int] = None
    ) -> tuple[Optional[str], Optional[Union[int, str]]]:
        """Lấy reCAPTCHA token - hỗ trợ nhiều phương thức giải Captcha.
        
        Args:
            project_id: ID project
            action: loại action reCAPTCHA
                - IMAGE_GENERATION: sinh ảnh và phóng ảnh 2K/4K (mặc định)
                - VIDEO_GENERATION: sinh video và phóng video
            token_id: ID token nghiệp vụ hiện tại (chế độ browser dùng để đọc proxy Captcha cấp token)
        
        Returns:
            tuple (token, browser_id).
            - chế độ browser: browser_id là ID trình duyệt cục bộ
            - chế độ remote_browser: browser_id là session_id từ xa
            - chế độ khác: browser_id là None
        """
        captcha_method = config.captcha_method
        debug_logger.log_info(f"[reCAPTCHA] Bắt đầu lấy token: method={captcha_method}, project_id={project_id}, action={action}")

        # Captcha trình duyệt tích hợp (nodriver)
        if captcha_method == "personal":
            debug_logger.log_info(f"[reCAPTCHA] Dùng chế độ personal")
            try:
                from .browser_captcha_personal import BrowserCaptchaService
                debug_logger.log_info(f"[reCAPTCHA] Import BrowserCaptchaService thành công")
                service = await BrowserCaptchaService.get_instance(self.db)
                debug_logger.log_info(f"[reCAPTCHA] Lấy instance dịch vụ thành công, chuẩn bị gọi get_token")
                token = await service.get_token(project_id, action)
                debug_logger.log_info(f"[reCAPTCHA] get_token trả về: {token[:50] if token else None}...")
                fingerprint = service.get_last_fingerprint() if token else None
                self._set_request_fingerprint(fingerprint if token else None)
                return token, None
            except RuntimeError as e:
                # Bắt các lỗi rõ ràng trong môi trường Docker hoặc thiếu dependency
                error_msg = str(e)
                debug_logger.log_error(f"[reCAPTCHA Personal] {error_msg}")
                print(f"[reCAPTCHA] ❌ Captcha trình duyệt tích hợp thất bại: {error_msg}")
                self._set_request_fingerprint(None)
                return None, None
            except ImportError as e:
                debug_logger.log_error(f"[reCAPTCHA Personal] Import thất bại: {str(e)}")
                print(f"[reCAPTCHA] ❌ Chưa cài nodriver, chạy: pip install nodriver")
                self._set_request_fingerprint(None)
                return None, None
            except Exception as e:
                debug_logger.log_error(f"[reCAPTCHA Personal] Lỗi: {str(e)}")
                self._set_request_fingerprint(None)
                return None, None
        # Captcha trình duyệt có giao diện (playwright)
        elif captcha_method == "browser":
            try:
                from .browser_captcha import BrowserCaptchaService
                service = await BrowserCaptchaService.get_instance(self.db)
                token, browser_id = await service.get_token(project_id, action, token_id=token_id)
                fingerprint = await service.get_fingerprint(browser_id) if token else None
                self._set_request_fingerprint(fingerprint if token else None)
                return token, browser_id
            except RuntimeError as e:
                # Bắt các lỗi rõ ràng trong môi trường Docker hoặc thiếu dependency
                error_msg = str(e)
                debug_logger.log_error(f"[reCAPTCHA Browser] {error_msg}")
                print(f"[reCAPTCHA] ❌ Captcha trình duyệt có giao diện thất bại: {error_msg}")
                self._set_request_fingerprint(None)
                return None, None
            except ImportError as e:
                debug_logger.log_error(f"[reCAPTCHA Browser] Import thất bại: {str(e)}")
                print(f"[reCAPTCHA] ❌ Chưa cài playwright, chạy: pip install playwright && python -m playwright install chromium")
                self._set_request_fingerprint(None)
                return None, None
            except Exception as e:
                debug_logger.log_error(f"[reCAPTCHA Browser] Lỗi: {str(e)}")
                self._set_request_fingerprint(None)
                return None, None
        elif captcha_method == "remote_browser":
            try:
                solve_timeout = self._resolve_remote_browser_solve_timeout(action)
                payload = await self._call_remote_browser_service(
                    method="POST",
                    path="/api/v1/solve",
                    json_data={
                        "project_id": project_id,
                        "action": action,
                        "token_id": token_id,
                    },
                    timeout_override=solve_timeout,
                )
                token = payload.get("token")
                session_id = payload.get("session_id")
                fingerprint = payload.get("fingerprint") if isinstance(payload.get("fingerprint"), dict) else None
                self._set_request_fingerprint(fingerprint if token else None)
                if not token or not session_id:
                    raise RuntimeError(f"remote_browser thiếu token/session_id: {payload}")
                return token, str(session_id)
            except Exception as e:
                debug_logger.log_error(f"[reCAPTCHA RemoteBrowser] Lỗi: {str(e)}")
                self._set_request_fingerprint(None)
                return None, None
        # Dịch vụ Captcha API
        elif captcha_method in ["yescaptcha", "capmonster", "ezcaptcha", "capsolver"]:
            self._set_request_fingerprint(None)
            token = await self._get_api_captcha_token(captcha_method, project_id, action)
            return token, None
        else:
            debug_logger.log_info(f"[reCAPTCHA] Phương thức Captcha không xác định: {captcha_method}")
            self._set_request_fingerprint(None)
            return None, None

    async def _get_api_captcha_token(self, method: str, project_id: str, action: str = "IMAGE_GENERATION") -> Optional[str]:
        """Dịch vụ Captcha API chung.
        
        Args:
            method: loại dịch vụ Captcha
            project_id: ID project
            action: loại action reCAPTCHA (IMAGE_GENERATION hoặc VIDEO_GENERATION)
        """
        # Lấy cấu hình
        if method == "yescaptcha":
            client_key = config.yescaptcha_api_key
            base_url = config.yescaptcha_base_url
            task_type = "RecaptchaV3TaskProxylessM1"
        elif method == "capmonster":
            client_key = config.capmonster_api_key
            base_url = config.capmonster_base_url
            task_type = "RecaptchaV3TaskProxyless"
        elif method == "ezcaptcha":
            client_key = config.ezcaptcha_api_key
            base_url = config.ezcaptcha_base_url
            task_type = "ReCaptchaV3TaskProxylessS9"
        elif method == "capsolver":
            client_key = config.capsolver_api_key
            base_url = config.capsolver_base_url
            task_type = "ReCaptchaV3EnterpriseTaskProxyLess"
        else:
            debug_logger.log_error(f"[reCAPTCHA] Unknown API method: {method}")
            return None

        if not client_key:
            debug_logger.log_info(f"[reCAPTCHA] {method} API key not configured, skipping")
            return None

        website_key = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
        website_url = f"https://labs.google/fx/tools/flow/project/{project_id}"
        page_action = action

        try:
            # Do not use curl_cffi impersonation for captcha API JSON endpoints: some ASGI
            # servers (for example FastAPI/Uvicorn) may receive an empty body and return 422.
            async with AsyncSession() as session:
                create_url = f"{base_url}/createTask"
                create_data = {
                    "clientKey": client_key,
                    "task": {
                        "websiteURL": website_url,
                        "websiteKey": website_key,
                        "type": task_type,
                        "pageAction": page_action
                    }
                }

                result = await session.post(create_url, json=create_data)
                result_json = result.json()
                task_id = result_json.get('taskId')

                debug_logger.log_info(f"[reCAPTCHA {method}] created task_id: {task_id}")

                if not task_id:
                    error_desc = result_json.get('errorDescription', 'Unknown error')
                    debug_logger.log_error(f"[reCAPTCHA {method}] Failed to create task: {error_desc}")
                    return None

                get_url = f"{base_url}/getTaskResult"
                for i in range(40):
                    get_data = {
                        "clientKey": client_key,
                        "taskId": task_id
                    }
                    result = await session.post(get_url, json=get_data)
                    result_json = result.json()

                    debug_logger.log_info(f"[reCAPTCHA {method}] polling #{i+1}: {result_json}")

                    status = result_json.get('status')
                    if status == 'ready':
                        solution = result_json.get('solution', {})
                        response = solution.get('gRecaptchaResponse')
                        if response:
                            debug_logger.log_info(f"[reCAPTCHA {method}] Lấy Token thành công")
                            return response

                    await asyncio.sleep(3)

                debug_logger.log_error(f"[reCAPTCHA {method}] Timeout waiting for token")
                return None

        except Exception as e:
            debug_logger.log_error(f"[reCAPTCHA {method}] error: {str(e)}")
            return None
