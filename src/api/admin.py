"""Admin API routes"""
import asyncio
import json
from fastapi import APIRouter, Depends, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import secrets
import time
import re
import urllib.error
import urllib.request
from urllib.parse import urlparse
from curl_cffi.requests import AsyncSession
from ..core.auth import AuthManager
from ..core.database import Database
from ..core.config import config
from ..services.token_manager import TokenManager
from ..services.proxy_manager import ProxyManager
from ..services.concurrency_manager import ConcurrencyManager

try:
    import httpx
except ImportError:
    httpx = None

router = APIRouter()

# Dependency injection
token_manager: TokenManager = None
proxy_manager: ProxyManager = None
db: Database = None
concurrency_manager: Optional[ConcurrencyManager] = None

# Store active admin session tokens (in production, use Redis or database)
active_admin_tokens = set()
SUPPORTED_API_CAPTCHA_METHODS = {"yescaptcha", "capmonster", "ezcaptcha", "capsolver"}


def _mask_token(token: Optional[str]) -> str:
    if not token:
        return ""
    if len(token) <= 24:
        return token
    return f"{token[:18]}...{token[-8:]}"


def _truncate_text(text: Any, limit: int = 240) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return f"{value[:limit - 3]}..."


def _extract_error_summary(payload: Any) -> str:
    """Trích xuất tóm tắt lỗi dễ đọc từ response body."""
    if payload is None:
        return ""

    if isinstance(payload, str):
        raw = payload.strip()
        if not raw:
            return ""
        try:
            return _extract_error_summary(json.loads(raw))
        except Exception:
            return _truncate_text(raw)

    if isinstance(payload, dict):
        for key in ("error_summary", "error_message", "detail", "message"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return _truncate_text(value)

        error_value = payload.get("error")
        if isinstance(error_value, dict):
            for key in ("message", "detail", "reason", "code"):
                value = error_value.get(key)
                if isinstance(value, str) and value.strip():
                    return _truncate_text(value)
        elif isinstance(error_value, str) and error_value.strip():
            return _truncate_text(error_value)

        for nested_key in ("response", "data"):
            nested = payload.get(nested_key)
            if isinstance(nested, (dict, list, str)):
                summary = _extract_error_summary(nested)
                if summary:
                    return summary

        return ""

    if isinstance(payload, list):
        for item in payload:
            summary = _extract_error_summary(item)
            if summary:
                return summary
        return ""

    return _truncate_text(payload)


def _guess_client_hints_from_user_agent(user_agent: str) -> Dict[str, str]:
    """Bổ sung các header sec-ch-* phổ biến dựa trên UA."""
    ua = (user_agent or "").strip()
    if not ua:
        return {}

    headers: Dict[str, str] = {}
    major_match = re.search(r"(?:Chrome|Chromium|Edg|EdgA|EdgiOS)/(\d+)", ua)
    is_mobile = any(token in ua for token in ("Android", "iPhone", "iPad", "Mobile"))
    headers["sec-ch-ua-mobile"] = "?1" if is_mobile else "?0"

    if "Windows" in ua:
        headers["sec-ch-ua-platform"] = '"Windows"'
    elif "Macintosh" in ua or "Mac OS X" in ua:
        headers["sec-ch-ua-platform"] = '"macOS"'
    elif "Android" in ua:
        headers["sec-ch-ua-platform"] = '"Android"'
    elif "iPhone" in ua or "iPad" in ua:
        headers["sec-ch-ua-platform"] = '"iOS"'
    elif "Linux" in ua:
        headers["sec-ch-ua-platform"] = '"Linux"'

    if major_match:
        major = major_match.group(1)
        if "Edg/" in ua:
            headers["sec-ch-ua"] = (
                f'"Not:A-Brand";v="99", "Microsoft Edge";v="{major}", "Chromium";v="{major}"'
            )
        else:
            headers["sec-ch-ua"] = (
                f'"Not:A-Brand";v="99", "Google Chrome";v="{major}", "Chromium";v="{major}"'
            )

    return headers


def _guess_impersonate_from_user_agent(user_agent: str) -> str:
    """Chọn phiên bản browser fingerprint khả dụng của curl_cffi dựa trên UA."""
    ua = (user_agent or "").strip()
    major_match = re.search(r"(?:Chrome|Chromium|Edg|EdgA|EdgiOS)/(\d+)", ua)
    if not major_match:
        return "chrome120"

    try:
        major = int(major_match.group(1))
    except Exception:
        return "chrome120"

    if major >= 124:
        return "chrome124"
    if major >= 120:
        return "chrome120"
    return "chrome120"


def _build_proxy_map(proxy_url: str) -> Optional[Dict[str, str]]:
    normalized = (proxy_url or "").strip()
    if not normalized:
        return None
    return {"http": normalized, "https": normalized}


def _normalize_http_base_url(base_url: str) -> str:
    normalized = (base_url or "").strip().rstrip("/")
    if not normalized:
        raise RuntimeError("Chưa cấu hình địa chỉ dịch vụ giải Captcha từ xa")

    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError("Địa chỉ dịch vụ giải Captcha từ xa sai định dạng, phải là http(s)://host[:port]")

    return normalized


def _get_remote_browser_client_config() -> tuple[str, str, int]:
    base_url = _normalize_http_base_url(config.remote_browser_base_url)
    api_key = (config.remote_browser_api_key or "").strip()
    if not api_key:
        raise RuntimeError("Chưa cấu hình API Key cho dịch vụ giải Captcha từ xa")
    timeout = max(5, int(config.remote_browser_timeout or 60))
    return base_url, api_key, timeout


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


def _parse_json_response_text(text: str) -> Optional[Any]:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


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
        raise RuntimeError(f"Yêu cầu dịch vụ giải Captcha từ xa thất bại: {e}") from e

    return status_code, _parse_json_response_text(text), text


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
        "timeout": _build_remote_browser_http_timeout(timeout),
    }

    if payload is not None:
        req_headers["Content-Type"] = "application/json; charset=utf-8"
        if request_method != "GET":
            request_kwargs["json"] = payload

    if httpx is None:
        return await _stdlib_json_http_request(
            method=method,
            url=url,
            headers=req_headers,
            payload=payload,
            timeout=timeout,
        )

    try:
        # control plane của remote_browser là JSON API giữa các dịch vụ, dùng httpx để tránh curl_cffi trong
        # tình huống Windows + impersonate khi POST body bị mất khiến FastAPI coi body là thiếu.
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as session:
            response = await session.request(
                method=request_method,
                url=url,
                **request_kwargs,
            )
    except Exception as e:
        raise RuntimeError(f"Yêu cầu dịch vụ giải Captcha từ xa thất bại: {e}") from e

    status_code = int(getattr(response, "status_code", 0) or 0)
    text = response.text or ""
    parsed = _parse_json_response_text(text)

    return status_code, parsed, text


async def _resolve_score_test_verify_proxy(
    captcha_method: str,
    browser_proxy_enabled: bool,
    browser_proxy_url: str
) -> tuple[Optional[Dict[str, str]], bool, str, str]:
    """
    Chọn proxy verify cho score-test, ưu tiên đồng bộ với proxy Captcha trình duyệt.
    Trả về: (proxies, used, source, proxy_url)
    """
    # Chế độ Captcha trình duyệt ưu tiên dùng browser_proxy để đồng nhất exit với lúc lấy token
    if captcha_method in {"browser", "personal"} and browser_proxy_enabled and browser_proxy_url:
        proxy_map = _build_proxy_map(browser_proxy_url)
        if proxy_map:
            return proxy_map, True, "captcha_browser_proxy", browser_proxy_url

    # Fallback về cấu hình proxy request
    try:
        if proxy_manager:
            proxy_cfg = await proxy_manager.get_proxy_config()
            if proxy_cfg and proxy_cfg.enabled and proxy_cfg.proxy_url:
                proxy_map = _build_proxy_map(proxy_cfg.proxy_url)
                if proxy_map:
                    return proxy_map, True, "request_proxy", proxy_cfg.proxy_url
    except Exception:
        pass

    return None, False, "none", ""


async def _solve_recaptcha_with_api_service(
    method: str,
    website_url: str,
    website_key: str,
    action: str,
    enterprise: bool = False
) -> Optional[str]:
    """Dùng dịch vụ giải Captcha bên thứ ba đang cấu hình để lấy token."""
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
        task_type = "ReCaptchaV3EnterpriseTaskProxyLess" if enterprise else "ReCaptchaV3TaskProxyLess"
    else:
        raise RuntimeError(f"Phương thức giải Captcha không hỗ trợ: {method}")

    if not client_key:
        raise RuntimeError(f"Chưa cấu hình API Key cho {method}")

    task: Dict[str, Any] = {
        "websiteURL": website_url,
        "websiteKey": website_key,
        "type": task_type,
        "pageAction": action,
    }

    if enterprise and method == "capsolver":
        task["isEnterprise"] = True

    create_url = f"{base_url.rstrip('/')}/createTask"
    get_url = f"{base_url.rstrip('/')}/getTaskResult"

    # Do not use curl_cffi impersonation for captcha API JSON endpoints: some ASGI servers
    # (for example FastAPI/Uvicorn) may receive an empty body and return 422.
    async with AsyncSession() as session:
        create_resp = await session.post(
            create_url,
            json={"clientKey": client_key, "task": task},
            timeout=30
        )
        create_json = create_resp.json()
        task_id = create_json.get("taskId")

        if not task_id:
            error_desc = create_json.get("errorDescription") or create_json.get("errorMessage") or str(create_json)
            raise RuntimeError(f"{method} createTask thất bại: {error_desc}")

        for _ in range(40):
            poll_resp = await session.post(
                get_url,
                json={"clientKey": client_key, "taskId": task_id},
                timeout=30
            )
            poll_json = poll_resp.json()
            if poll_json.get("status") == "ready":
                solution = poll_json.get("solution", {}) or {}
                token = solution.get("gRecaptchaResponse") or solution.get("token")
                if token:
                    return token
                raise RuntimeError(f"{method} trả về kết quả thiếu token: {poll_json}")

            if poll_json.get("errorId") not in (None, 0):
                error_desc = poll_json.get("errorDescription") or poll_json.get("errorMessage") or str(poll_json)
                raise RuntimeError(f"{method} getTaskResult thất bại: {error_desc}")

            await asyncio.sleep(3)

    raise RuntimeError(f"{method} lấy token timeout")


async def _score_test_with_remote_browser_service(
    website_url: str,
    website_key: str,
    verify_url: str,
    action: str,
    enterprise: bool = False,
) -> Dict[str, Any]:
    """Gọi dịch vụ giải Captcha từ xa (có giao diện) để thực hiện giải Captcha trong trang + xác thực score."""
    base_url, api_key, timeout = _get_remote_browser_client_config()
    endpoint = f"{base_url}/api/v1/custom-score"
    request_payload = {
        "website_url": website_url,
        "website_key": website_key,
        "verify_url": verify_url,
        "action": action,
        "enterprise": enterprise,
    }

    status_code, response_payload, response_text = await _sync_json_http_request(
        method="POST",
        url=endpoint,
        headers={"Authorization": f"Bearer {api_key}"},
        payload=request_payload,
        timeout=timeout,
    )

    if status_code >= 400:
        detail = ""
        if isinstance(response_payload, dict):
            detail = response_payload.get("detail") or response_payload.get("message") or str(response_payload)
        if not detail:
            detail = (response_text or "").strip()
        raise RuntimeError(f"Yêu cầu dịch vụ giải Captcha từ xa thất bại (HTTP {status_code}): {detail or 'Lỗi không xác định'}")

    if not isinstance(response_payload, dict):
        raise RuntimeError("Dịch vụ giải Captcha từ xa trả về sai định dạng")
    return response_payload


def set_dependencies(tm: TokenManager, pm: ProxyManager, database: Database, cm: Optional[ConcurrencyManager] = None):
    """Set service instances"""
    global token_manager, proxy_manager, db, concurrency_manager
    token_manager = tm
    proxy_manager = pm
    db = database
    concurrency_manager = cm


# ========== Request Models ==========

class LoginRequest(BaseModel):
    username: str
    password: str


class AddTokenRequest(BaseModel):
    st: str
    project_id: Optional[str] = None  # project_id tùy chọn do user nhập
    project_name: Optional[str] = None
    remark: Optional[str] = None
    captcha_proxy_url: Optional[str] = None
    image_enabled: bool = True
    video_enabled: bool = True
    image_concurrency: int = -1
    video_concurrency: int = -1


class UpdateTokenRequest(BaseModel):
    st: str  # Session Token (bắt buộc, dùng để làm mới AT)
    project_id: Optional[str] = None  # project_id tùy chọn do user nhập
    project_name: Optional[str] = None
    remark: Optional[str] = None
    captcha_proxy_url: Optional[str] = None
    image_enabled: Optional[bool] = None
    video_enabled: Optional[bool] = None
    image_concurrency: Optional[int] = None
    video_concurrency: Optional[int] = None


class ProxyConfigRequest(BaseModel):
    proxy_enabled: bool
    proxy_url: Optional[str] = None
    media_proxy_enabled: Optional[bool] = None
    media_proxy_url: Optional[str] = None


class ProxyTestRequest(BaseModel):
    proxy_url: str
    test_url: Optional[str] = "https://labs.google/"
    timeout_seconds: Optional[int] = 15


class CaptchaScoreTestRequest(BaseModel):
    website_url: Optional[str] = "https://antcpt.com/score_detector/"
    website_key: Optional[str] = "6LcR_okUAAAAAPYrPe-HK_0RULO1aZM15ENyM-Mf"
    action: Optional[str] = "homepage"
    verify_url: Optional[str] = "https://antcpt.com/score_detector/verify.php"
    enterprise: Optional[bool] = False


class GenerationConfigRequest(BaseModel):
    image_timeout: Optional[int] = None
    video_timeout: Optional[int] = None
    max_retries: Optional[int] = None


class CallLogicConfigRequest(BaseModel):
    call_mode: str


class ChangePasswordRequest(BaseModel):
    username: Optional[str] = None
    old_password: str
    new_password: str


class UpdateAPIKeyRequest(BaseModel):
    new_api_key: str


class UpdateDebugConfigRequest(BaseModel):
    enabled: bool


class UpdateAdminConfigRequest(BaseModel):
    error_ban_threshold: int


class ST2ATRequest(BaseModel):
    """Request chuyển ST thành AT."""
    st: str


class ImportTokenItem(BaseModel):
    """Một mục khi nhập Token."""
    email: Optional[str] = None
    access_token: Optional[str] = None
    session_token: Optional[str] = None
    is_active: bool = True
    captcha_proxy_url: Optional[str] = None
    image_enabled: bool = True
    video_enabled: bool = True
    image_concurrency: int = -1
    video_concurrency: int = -1


class ImportTokensRequest(BaseModel):
    """Request nhập Token."""
    tokens: List[ImportTokenItem]


# ========== Auth Middleware ==========

async def verify_admin_token(authorization: str = Header(None)):
    """Verify admin session token (NOT API key)"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization")

    token = authorization[7:]

    # Check if token is in active session tokens
    if token not in active_admin_tokens:
        raise HTTPException(status_code=401, detail="Invalid or expired admin token")

    return token


# ========== Auth Endpoints ==========

@router.post("/api/admin/login")
async def admin_login(request: LoginRequest):
    """Admin login - returns session token (NOT API key)"""
    admin_config = await db.get_admin_config()

    if not AuthManager.verify_admin(request.username, request.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Generate independent session token
    session_token = f"admin-{secrets.token_urlsafe(32)}"

    # Store in active tokens
    active_admin_tokens.add(session_token)

    return {
        "success": True,
        "token": session_token,  # Session token (NOT API key)
        "username": admin_config.username
    }


@router.post("/api/admin/logout")
async def admin_logout(token: str = Depends(verify_admin_token)):
    """Admin logout - invalidate session token"""
    active_admin_tokens.discard(token)
    return {"success": True, "message": "Đăng xuất thành công"}


@router.post("/api/admin/change-password")
async def change_password(
    request: ChangePasswordRequest,
    token: str = Depends(verify_admin_token)
):
    """Change admin password"""
    admin_config = await db.get_admin_config()

    # Verify old password
    if not AuthManager.verify_admin(admin_config.username, request.old_password):
        raise HTTPException(status_code=400, detail="Mật khẩu cũ sai")

    # Update password and username in database
    update_params = {"password": request.new_password}
    if request.username:
        update_params["username"] = request.username

    await db.update_admin_config(**update_params)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    # 🔑 Invalidate all admin session tokens (force re-login for security)
    active_admin_tokens.clear()

    return {"success": True, "message": "Đổi mật khẩu thành công, vui lòng đăng nhập lại"}


# ========== Token Management ==========

@router.get("/api/tokens")
async def get_tokens(token: str = Depends(verify_admin_token)):
    """Get all tokens with statistics"""
    token_rows = await db.get_all_tokens_with_stats()
    to_iso = lambda value: value.isoformat() if hasattr(value, "isoformat") else value

    return [{
        "id": row.get("id"),
        "st": row.get("st"),  # Session Token for editing
        "at": row.get("at"),  # Access Token dùng để chỉnh sửa (chuyển từ ST)
        "at_expires": to_iso(row.get("at_expires")) if row.get("at_expires") else None,  # 🆕 Thời điểm AT hết hạn
        "token": row.get("at"),  # Tương thích cách truy cập token.token ở frontend
        "email": row.get("email"),
        "name": row.get("name"),
        "remark": row.get("remark"),
        "is_active": bool(row.get("is_active")),
        "created_at": to_iso(row.get("created_at")) if row.get("created_at") else None,
        "last_used_at": to_iso(row.get("last_used_at")) if row.get("last_used_at") else None,
        "use_count": row.get("use_count"),
        "credits": row.get("credits"),  # 🆕 Số dư
        "user_paygate_tier": row.get("user_paygate_tier"),
        "current_project_id": row.get("current_project_id"),  # 🆕 Project ID
        "current_project_name": row.get("current_project_name"),  # 🆕 Tên project
        "captcha_proxy_url": row.get("captcha_proxy_url") or "",
        "image_enabled": bool(row.get("image_enabled")),
        "video_enabled": bool(row.get("video_enabled")),
        "image_concurrency": row.get("image_concurrency"),
        "video_concurrency": row.get("video_concurrency"),
        "image_count": row.get("image_count", 0),
        "video_count": row.get("video_count", 0),
        "error_count": row.get("error_count", 0)
    } for row in token_rows]  # Trả về mảng trực tiếp để tương thích frontend


@router.post("/api/tokens")
async def add_token(
    request: AddTokenRequest,
    token: str = Depends(verify_admin_token)
):
    """Add a new token"""
    try:
        new_token = await token_manager.add_token(
            st=request.st,
            project_id=request.project_id,  # 🆕 Hỗ trợ user chỉ định project_id
            project_name=request.project_name,
            remark=request.remark,
            captcha_proxy_url=request.captcha_proxy_url.strip() if request.captcha_proxy_url is not None else None,
            image_enabled=request.image_enabled,
            video_enabled=request.video_enabled,
            image_concurrency=request.image_concurrency,
            video_concurrency=request.video_concurrency
        )

        # Hot-update giới hạn concurrency để không phải restart service
        if concurrency_manager:
            await concurrency_manager.reset_token(
                new_token.id,
                image_concurrency=new_token.image_concurrency,
                video_concurrency=new_token.video_concurrency
            )

        return {
            "success": True,
            "message": "Thêm Token thành công",
            "token": {
                "id": new_token.id,
                "email": new_token.email,
                "credits": new_token.credits,
                "project_id": new_token.current_project_id,
                "project_name": new_token.current_project_name
            }
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Thêm Token thất bại: {str(e)}")


@router.put("/api/tokens/{token_id}")
async def update_token(
    token_id: int,
    request: UpdateTokenRequest,
    token: str = Depends(verify_admin_token)
):
    """Update token - tự động làm mới AT dùng ST."""
    try:
        # Chuyển ST sang AT trước
        result = await token_manager.flow_client.st_to_at(request.st)
        at = result["access_token"]
        expires = result.get("expires")

        # Parse thời điểm hết hạn
        from datetime import datetime
        at_expires = None
        if expires:
            try:
                at_expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
            except:
                pass

        # Update token (bao gồm AT, ST, thời điểm AT hết hạn, project_id và project_name)
        await token_manager.update_token(
            token_id=token_id,
            st=request.st,
            at=at,
            at_expires=at_expires,  # 🆕 Cập nhật thời điểm AT hết hạn
            project_id=request.project_id,
            project_name=request.project_name,
            remark=request.remark,
            captcha_proxy_url=request.captcha_proxy_url.strip() if request.captcha_proxy_url is not None else None,
            image_enabled=request.image_enabled,
            video_enabled=request.video_enabled,
            image_concurrency=request.image_concurrency,
            video_concurrency=request.video_concurrency
        )

        # Hot-update giới hạn concurrency để thay đổi từ admin panel có hiệu lực ngay
        if concurrency_manager:
            updated_token = await token_manager.get_token(token_id)
            if updated_token:
                await concurrency_manager.reset_token(
                    token_id,
                    image_concurrency=updated_token.image_concurrency,
                    video_concurrency=updated_token.video_concurrency
                )

        return {"success": True, "message": "Cập nhật Token thành công"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/tokens/{token_id}")
async def delete_token(
    token_id: int,
    token: str = Depends(verify_admin_token)
):
    """Delete token"""
    try:
        await token_manager.delete_token(token_id)
        if concurrency_manager:
            await concurrency_manager.remove_token(token_id)
        return {"success": True, "message": "Xóa Token thành công"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/tokens/{token_id}/enable")
async def enable_token(
    token_id: int,
    token: str = Depends(verify_admin_token)
):
    """Enable token"""
    await token_manager.enable_token(token_id)
    return {"success": True, "message": "Đã bật Token"}


@router.post("/api/tokens/{token_id}/disable")
async def disable_token(
    token_id: int,
    token: str = Depends(verify_admin_token)
):
    """Disable token"""
    await token_manager.disable_token(token_id)
    return {"success": True, "message": "Đã vô hiệu Token"}


@router.post("/api/tokens/{token_id}/refresh-credits")
async def refresh_credits(
    token_id: int,
    token: str = Depends(verify_admin_token)
):
    """Làm mới số dư Token 🆕."""
    try:
        credits = await token_manager.refresh_credits(token_id)
        return {
            "success": True,
            "message": "Làm mới số dư thành công",
            "credits": credits
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Làm mới số dư thất bại: {str(e)}")


@router.post("/api/tokens/{token_id}/refresh-at")
async def refresh_at(
    token_id: int,
    token: str = Depends(verify_admin_token)
):
    """Thủ công làm mới AT của Token (dùng ST để chuyển đổi) 🆕.

    Nếu làm mới AT thất bại và đang ở chế độ personal, sẽ tự thử làm mới ST qua trình duyệt.
    """
    from ..core.logger import debug_logger
    from ..core.config import config
    
    debug_logger.log_info(f"[API] Yêu cầu làm mới AT thủ công: token_id={token_id}, captcha_method={config.captcha_method}")
    
    try:
        # Gọi phương thức làm mới nội bộ của token_manager (bao gồm logic tự làm mới ST)
        success = await token_manager._refresh_at(token_id)

        if success:
            # Lấy thông tin token sau cập nhật
            updated_token = await token_manager.get_token(token_id)
            
            message = "Làm mới AT thành công"
            if config.captcha_method == "personal":
                message += " (hỗ trợ tự làm mới ST)"
            
            debug_logger.log_info(f"[API] Làm mới AT thành công: token_id={token_id}")
            
            return {
                "success": True,
                "message": message,
                "token": {
                    "id": updated_token.id,
                    "email": updated_token.email,
                    "at_expires": updated_token.at_expires.isoformat() if updated_token.at_expires else None
                }
            }
        else:
            debug_logger.log_error(f"[API] Làm mới AT thất bại: token_id={token_id}")
            
            error_detail = "Làm mới AT thất bại"
            if config.captcha_method != "personal":
                error_detail += f" (chế độ Captcha hiện tại: {config.captcha_method}, tự làm mới ST chỉ khả dụng ở chế độ personal)"
            
            raise HTTPException(status_code=500, detail=error_detail)
    except HTTPException:
        raise
    except Exception as e:
        debug_logger.log_error(f"[API] Ngoại lệ khi làm mới AT: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Làm mới AT thất bại: {str(e)}")


@router.post("/api/tokens/st2at")
async def st_to_at(
    request: ST2ATRequest,
    token: str = Depends(verify_admin_token)
):
    """Chuyển Session Token thành Access Token (chỉ chuyển đổi, không thêm vào DB)."""
    try:
        result = await token_manager.flow_client.st_to_at(request.st)
        return {
            "success": True,
            "message": "ST converted to AT successfully",
            "access_token": result["access_token"],
            "email": result.get("user", {}).get("email"),
            "expires": result.get("expires")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/tokens/import")
async def import_tokens(
    request: ImportTokensRequest,
    token: str = Depends(verify_admin_token)
):
    """Nhập hàng loạt Token."""
    from datetime import datetime, timezone

    added = 0
    updated = 0
    errors = []
    # Giữ nhất quán với logic cũ: trong kết quả sắp theo created_at DESC, ưu tiên khớp "bản ghi mới nhất" cùng email
    existing_by_email = {}
    for existing_token in await token_manager.get_all_tokens():
        if existing_token.email and existing_token.email not in existing_by_email:
            existing_by_email[existing_token.email] = existing_token

    for idx, item in enumerate(request.tokens):
        try:
            st = item.session_token

            if not st:
                errors.append(f"Mục {idx+1}: thiếu session_token")
                continue

            # Dùng ST chuyển sang AT để lấy thông tin user
            try:
                result = await token_manager.flow_client.st_to_at(st)
                at = result["access_token"]
                email = result.get("user", {}).get("email")
                expires = result.get("expires")

                if not email:
                    errors.append(f"Mục {idx+1}: không lấy được thông tin email")
                    continue

                # Parse thời điểm hết hạn
                at_expires = None
                is_expired = False
                if expires:
                    try:
                        at_expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                        # Kiểm tra đã hết hạn chưa
                        now = datetime.now(timezone.utc)
                        is_expired = at_expires <= now
                    except:
                        pass

                # Dùng email để kiểm tra đã tồn tại chưa
                existing = existing_by_email.get(email)

                if existing:
                    # Cập nhật Token đã có
                    await token_manager.update_token(
                        token_id=existing.id,
                        st=st,
                        at=at,
                        at_expires=at_expires,
                        captcha_proxy_url=item.captcha_proxy_url.strip() if item.captcha_proxy_url is not None else None,
                        image_enabled=item.image_enabled,
                        video_enabled=item.video_enabled,
                        image_concurrency=item.image_concurrency,
                        video_concurrency=item.video_concurrency
                    )
                    # Nếu hết hạn thì vô hiệu
                    if is_expired:
                        await token_manager.disable_token(existing.id)
                        existing.is_active = False
                    existing.st = st
                    existing.at = at
                    existing.at_expires = at_expires
                    existing.captcha_proxy_url = item.captcha_proxy_url
                    existing.image_enabled = item.image_enabled
                    existing.video_enabled = item.video_enabled
                    existing.image_concurrency = item.image_concurrency
                    existing.video_concurrency = item.video_concurrency
                    updated += 1
                else:
                    # Thêm Token mới
                    new_token = await token_manager.add_token(
                        st=st,
                        captcha_proxy_url=item.captcha_proxy_url.strip() if item.captcha_proxy_url is not None else None,
                        image_enabled=item.image_enabled,
                        video_enabled=item.video_enabled,
                        image_concurrency=item.image_concurrency,
                        video_concurrency=item.video_concurrency
                    )
                    # Nếu hết hạn thì vô hiệu
                    if is_expired:
                        await token_manager.disable_token(new_token.id)
                        new_token.is_active = False
                    existing_by_email[email] = new_token
                    added += 1

            except Exception as e:
                errors.append(f"Mục {idx+1}: {str(e)}")

        except Exception as e:
            errors.append(f"Mục {idx+1}: {str(e)}")

    return {
        "success": True,
        "added": added,
        "updated": updated,
        "errors": errors if errors else None,
        "message": f"Nhập xong: thêm mới {added}, cập nhật {updated}" + (f", thất bại {len(errors)}" if errors else "")
    }


# ========== Config Management ==========

@router.get("/api/config/proxy")
async def get_proxy_config(token: str = Depends(verify_admin_token)):
    """Get proxy configuration"""
    config = await proxy_manager.get_proxy_config()
    return {
        "success": True,
        "config": {
            "enabled": config.enabled,
            "proxy_url": config.proxy_url,
            "media_proxy_enabled": config.media_proxy_enabled,
            "media_proxy_url": config.media_proxy_url
        }
    }


@router.get("/api/proxy/config")
async def get_proxy_config_alias(token: str = Depends(verify_admin_token)):
    """Get proxy configuration (alias for frontend compatibility)"""
    config = await proxy_manager.get_proxy_config()
    return {
        "proxy_enabled": config.enabled,  # Frontend expects proxy_enabled
        "proxy_url": config.proxy_url,
        "media_proxy_enabled": config.media_proxy_enabled,
        "media_proxy_url": config.media_proxy_url
    }


@router.post("/api/proxy/config")
async def update_proxy_config_alias(
    request: ProxyConfigRequest,
    token: str = Depends(verify_admin_token)
):
    """Update proxy configuration (alias for frontend compatibility)"""
    try:
        await proxy_manager.update_proxy_config(
            enabled=request.proxy_enabled,
            proxy_url=request.proxy_url,
            media_proxy_enabled=request.media_proxy_enabled,
            media_proxy_url=request.media_proxy_url
        )
    except ValueError as e:
        return {"success": False, "message": str(e)}
    return {"success": True, "message": "Cập nhật cấu hình proxy thành công"}


@router.post("/api/config/proxy")
async def update_proxy_config(
    request: ProxyConfigRequest,
    token: str = Depends(verify_admin_token)
):
    """Update proxy configuration"""
    try:
        await proxy_manager.update_proxy_config(
            enabled=request.proxy_enabled,
            proxy_url=request.proxy_url,
            media_proxy_enabled=request.media_proxy_enabled,
            media_proxy_url=request.media_proxy_url
        )
    except ValueError as e:
        return {"success": False, "message": str(e)}
    return {"success": True, "message": "Cập nhật cấu hình proxy thành công"}


@router.post("/api/proxy/test")
async def test_proxy_connectivity(
    request: ProxyTestRequest,
    token: str = Depends(verify_admin_token)
):
    """Kiểm tra proxy có truy cập được trang đích hay không (mặc định https://labs.google/)."""
    proxy_input = (request.proxy_url or "").strip()
    test_url = (request.test_url or "https://labs.google/").strip()
    timeout_seconds = int(request.timeout_seconds or 15)
    timeout_seconds = max(5, min(timeout_seconds, 60))

    if not proxy_input:
        return {
            "success": False,
            "message": "Địa chỉ proxy đang trống",
            "test_url": test_url
        }

    try:
        proxy_url = proxy_manager.normalize_proxy_url(proxy_input)
    except ValueError as e:
        return {
            "success": False,
            "message": str(e),
            "test_url": test_url
        }

    start_time = time.time()
    try:
        proxies = {"http": proxy_url, "https": proxy_url}
        async with AsyncSession() as session:
            resp = await session.get(
                test_url,
                proxies=proxies,
                timeout=timeout_seconds,
                impersonate="chrome120",
                allow_redirects=True,
                verify=False
            )

        elapsed_ms = int((time.time() - start_time) * 1000)
        status_code = resp.status_code
        final_url = str(resp.url)
        ok = 200 <= status_code < 400

        return {
            "success": ok,
            "message": "Proxy khả dụng" if ok else f"Proxy kết nối được nhưng đích trả về mã trạng thái {status_code}",
            "test_url": test_url,
            "final_url": final_url,
            "status_code": status_code,
            "elapsed_ms": elapsed_ms
        }
    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        return {
            "success": False,
            "message": f"Kiểm tra proxy thất bại: {str(e)}",
            "test_url": test_url,
            "elapsed_ms": elapsed_ms
        }


@router.get("/api/config/generation")
async def get_generation_config(token: str = Depends(verify_admin_token)):
    """Get generation timeout configuration"""
    config = await db.get_generation_config()
    return {
        "success": True,
        "config": {
            "image_timeout": config.image_timeout,
            "video_timeout": config.video_timeout,
            "max_retries": config.max_retries,
        }
    }


@router.post("/api/config/generation")
async def update_generation_config(
    request: GenerationConfigRequest,
    token: str = Depends(verify_admin_token)
):
    """Update generation timeout configuration"""
    await db.update_generation_config(
        image_timeout=request.image_timeout,
        video_timeout=request.video_timeout,
        max_retries=request.max_retries,
    )

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    return {"success": True, "message": "Cập nhật cấu hình sinh nội dung thành công"}


@router.get("/api/call-logic/config")
async def get_call_logic_config(token: str = Depends(verify_admin_token)):
    """Get token call logic configuration."""
    config_obj = await db.get_call_logic_config()
    call_mode = getattr(config_obj, "call_mode", None)
    if call_mode not in ("default", "polling"):
        call_mode = "polling" if getattr(config_obj, "polling_mode_enabled", False) else "default"
    return {
        "success": True,
        "config": {
            "call_mode": call_mode,
            "polling_mode_enabled": call_mode == "polling",
        }
    }


@router.post("/api/call-logic/config")
async def update_call_logic_config(
    request: CallLogicConfigRequest,
    token: str = Depends(verify_admin_token)
):
    """Update token call logic configuration."""
    call_mode = request.call_mode if request.call_mode in ("default", "polling") else None
    if call_mode is None:
        raise HTTPException(status_code=400, detail="Invalid call_mode")

    await db.update_call_logic_config(call_mode)
    await db.reload_config_to_memory()

    return {
        "success": True,
        "message": "Lưu chế độ luân chuyển Token thành công",
        "config": {
            "call_mode": call_mode,
            "polling_mode_enabled": call_mode == "polling",
        }
    }


# ========== System Info ==========

@router.get("/api/system/info")
async def get_system_info(token: str = Depends(verify_admin_token)):
    """Get system information"""
    stats = await db.get_system_info_stats()

    return {
        "success": True,
        "info": {
            "total_tokens": stats["total_tokens"],
            "active_tokens": stats["active_tokens"],
            "total_credits": stats["total_credits"],
            "version": "1.0.0"
        }
    }


# ========== Additional Routes for Frontend Compatibility ==========

@router.post("/api/login")
async def login(request: LoginRequest):
    """Login endpoint (alias for /api/admin/login)"""
    return await admin_login(request)


@router.post("/api/logout")
async def logout(token: str = Depends(verify_admin_token)):
    """Logout endpoint (alias for /api/admin/logout)"""
    return await admin_logout(token)


@router.get("/health")
async def health_check():
    """Public health check endpoint - no auth required"""
    try:
        stats = await db.get_dashboard_stats()
        has_active_tokens = stats.get("active_tokens", 0) > 0
    except Exception:
        return {"backend_running": True, "has_active_tokens": False}
    return {"backend_running": True, "has_active_tokens": has_active_tokens}


@router.get("/api/stats")
async def get_stats(token: str = Depends(verify_admin_token)):
    """Get statistics for dashboard"""
    return await db.get_dashboard_stats()


@router.get("/api/logs")
async def get_logs(
    limit: int = 100,
    token: str = Depends(verify_admin_token)
):
    """Get lightweight request logs for list view"""
    limit = max(1, min(limit, 100))
    logs = await db.get_logs(limit=limit, include_payload=False)

    result = []
    for log in logs:
        raw_status_code = log.get("status_code")
        try:
            status_code = int(raw_status_code) if raw_status_code is not None else None
        except (TypeError, ValueError):
            status_code = None
        result.append({
            "id": log.get("id"),
            "token_id": log.get("token_id"),
            "token_email": log.get("token_email"),
            "token_username": log.get("token_username"),
            "operation": log.get("operation"),
            "status_code": status_code if status_code is not None else raw_status_code,
            "duration": log.get("duration"),
            "status_text": log.get("status_text") or "",
            "progress": log.get("progress") or 0,
            "created_at": log.get("created_at"),
            "updated_at": log.get("updated_at"),
            "error_summary": _extract_error_summary(log.get("response_body_excerpt")) if status_code is not None and status_code >= 400 else "",
        })
    return result


@router.get("/api/logs/{log_id}")
async def get_log_detail(
    log_id: int,
    token: str = Depends(verify_admin_token)
):
    """Get single request log detail (payload loaded on demand)"""
    log = await db.get_log_detail(log_id)
    if not log:
        raise HTTPException(status_code=404, detail="Nhật ký không tồn tại")

    error_summary = _extract_error_summary(log.get("response_body"))

    return {
        "id": log.get("id"),
        "token_id": log.get("token_id"),
        "token_email": log.get("token_email"),
        "token_username": log.get("token_username"),
        "operation": log.get("operation"),
        "status_code": log.get("status_code"),
        "duration": log.get("duration"),
        "status_text": log.get("status_text") or "",
        "progress": log.get("progress") or 0,
        "created_at": log.get("created_at"),
        "updated_at": log.get("updated_at"),
        "error_summary": error_summary,
        "request_body": log.get("request_body"),
        "response_body": log.get("response_body")
    }


@router.delete("/api/logs")
async def clear_logs(token: str = Depends(verify_admin_token)):
    """Clear all logs"""
    try:
        await db.clear_all_logs()
        return {"success": True, "message": "Đã xóa toàn bộ nhật ký"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/admin/config")
async def get_admin_config(token: str = Depends(verify_admin_token)):
    """Get admin configuration"""
    admin_config = await db.get_admin_config()

    return {
        "admin_username": admin_config.username,
        "api_key": admin_config.api_key,
        "error_ban_threshold": admin_config.error_ban_threshold,
        "debug_enabled": config.debug_enabled  # Return actual debug status
    }


@router.post("/api/admin/config")
async def update_admin_config(
    request: UpdateAdminConfigRequest,
    token: str = Depends(verify_admin_token)
):
    """Update admin configuration (error_ban_threshold)"""
    # Update error_ban_threshold in database
    await db.update_admin_config(error_ban_threshold=request.error_ban_threshold)

    return {"success": True, "message": "Cập nhật cấu hình thành công"}


@router.post("/api/admin/password")
async def update_admin_password(
    request: ChangePasswordRequest,
    token: str = Depends(verify_admin_token)
):
    """Update admin password"""
    return await change_password(request, token)


@router.post("/api/admin/apikey")
async def update_api_key(
    request: UpdateAPIKeyRequest,
    token: str = Depends(verify_admin_token)
):
    """Update API key (for external API calls, NOT for admin login)"""
    # Update API key in database
    await db.update_admin_config(api_key=request.new_api_key)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    return {"success": True, "message": "Cập nhật API Key thành công"}


@router.post("/api/admin/debug")
async def update_debug_config(
    request: UpdateDebugConfigRequest,
    token: str = Depends(verify_admin_token)
):
    """Update debug configuration"""
    try:
        # Update in-memory config only (not database)
        # This ensures debug mode is automatically disabled on restart
        config.set_debug_enabled(request.enabled)

        status = "enabled" if request.enabled else "disabled"
        return {"success": True, "message": f"Debug mode {status}", "enabled": request.enabled}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update debug config: {str(e)}")


@router.get("/api/generation/timeout")
async def get_generation_timeout(token: str = Depends(verify_admin_token)):
    """Get generation timeout configuration"""
    return await get_generation_config(token)


@router.post("/api/generation/timeout")
async def update_generation_timeout(
    request: GenerationConfigRequest,
    token: str = Depends(verify_admin_token)
):
    """Update generation timeout configuration"""
    await db.update_generation_config(
        image_timeout=request.image_timeout,
        video_timeout=request.video_timeout,
        max_retries=request.max_retries,
    )

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    return {"success": True, "message": "Cập nhật cấu hình sinh nội dung thành công"}


# ========== AT Auto Refresh Config ==========

@router.get("/api/token-refresh/config")
async def get_token_refresh_config(token: str = Depends(verify_admin_token)):
    """Get AT auto refresh configuration (mặc định bật)."""
    return {
        "success": True,
        "config": {
            "at_auto_refresh_enabled": True  # Flow2API mặc định bật tự làm mới AT
        }
    }


@router.post("/api/token-refresh/enabled")
async def update_token_refresh_enabled(
    token: str = Depends(verify_admin_token)
):
    """Update AT auto refresh enabled (Flow2API luôn bật, endpoint này chỉ để tương thích frontend)."""
    return {
        "success": True,
        "message": "AT auto-refresh của Flow2API bật mặc định và không thể tắt"
    }


async def _sync_runtime_cache_config():
    from . import routes
    if routes.generation_handler and routes.generation_handler.file_cache:
        file_cache = routes.generation_handler.file_cache
        file_cache.set_timeout(config.cache_timeout)
        await file_cache.refresh_cleanup_task()

# ========== Cache Configuration Endpoints ==========

@router.get("/api/cache/config")
async def get_cache_config(token: str = Depends(verify_admin_token)):
    """Get cache configuration"""
    cache_config = await db.get_cache_config()

    # Calculate effective base URL
    effective_base_url = cache_config.cache_base_url if cache_config.cache_base_url else f"http://127.0.0.1:8000"

    return {
        "success": True,
        "config": {
            "enabled": cache_config.cache_enabled,
            "timeout": cache_config.cache_timeout,
            "base_url": cache_config.cache_base_url or "",
            "effective_base_url": effective_base_url
        }
    }


@router.post("/api/cache/enabled")
async def update_cache_enabled(
    request: dict,
    token: str = Depends(verify_admin_token)
):
    """Update cache enabled status"""
    enabled = request.get("enabled", False)
    await db.update_cache_config(enabled=enabled)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()
    await _sync_runtime_cache_config()

    return {"success": True, "message": f"Cache đã {'bật' if enabled else 'tắt'}"}


@router.post("/api/cache/config")
async def update_cache_config_full(
    request: dict,
    token: str = Depends(verify_admin_token)
):
    """Update complete cache configuration"""
    enabled = request.get("enabled")
    timeout = request.get("timeout")
    base_url = request.get("base_url")

    if timeout is not None:
        try:
            timeout = int(timeout)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Timeout cache phải là số nguyên")
        if timeout < 0:
            raise HTTPException(status_code=400, detail="Timeout cache không được nhỏ hơn 0")

    await db.update_cache_config(enabled=enabled, timeout=timeout, base_url=base_url)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()
    await _sync_runtime_cache_config()

    return {"success": True, "message": "Cập nhật cấu hình cache thành công"}


@router.post("/api/cache/base-url")
async def update_cache_base_url(
    request: dict,
    token: str = Depends(verify_admin_token)
):
    """Update cache base URL"""
    base_url = request.get("base_url", "")
    await db.update_cache_config(base_url=base_url)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()
    await _sync_runtime_cache_config()

    return {"success": True, "message": "Cập nhật Base URL cache thành công"}


@router.post("/api/captcha/config")
async def update_captcha_config(
    request: dict,
    token: str = Depends(verify_admin_token)
):
    """Update captcha configuration"""
    from ..services.browser_captcha import validate_browser_proxy_url

    captcha_method = request.get("captcha_method")
    yescaptcha_api_key = request.get("yescaptcha_api_key")
    yescaptcha_base_url = request.get("yescaptcha_base_url")
    capmonster_api_key = request.get("capmonster_api_key")
    capmonster_base_url = request.get("capmonster_base_url")
    ezcaptcha_api_key = request.get("ezcaptcha_api_key")
    ezcaptcha_base_url = request.get("ezcaptcha_base_url")
    capsolver_api_key = request.get("capsolver_api_key")
    capsolver_base_url = request.get("capsolver_base_url")
    remote_browser_base_url = request.get("remote_browser_base_url")
    remote_browser_api_key = request.get("remote_browser_api_key")
    remote_browser_timeout = request.get("remote_browser_timeout", 60)
    browser_proxy_enabled = request.get("browser_proxy_enabled", False)
    browser_proxy_url = request.get("browser_proxy_url", "")
    browser_count = request.get("browser_count", 1)
    personal_project_pool_size = request.get("personal_project_pool_size")
    personal_max_resident_tabs = request.get("personal_max_resident_tabs")
    personal_idle_tab_ttl_seconds = request.get("personal_idle_tab_ttl_seconds")

    # Xác thực định dạng URL proxy của trình duyệt
    if browser_proxy_enabled and browser_proxy_url:
        is_valid, error_msg = validate_browser_proxy_url(browser_proxy_url)
        if not is_valid:
            return {"success": False, "message": error_msg}

    if remote_browser_base_url:
        try:
            remote_browser_base_url = _normalize_http_base_url(remote_browser_base_url)
        except RuntimeError as e:
            return {"success": False, "message": str(e)}

    try:
        remote_browser_timeout = max(5, int(remote_browser_timeout or 60))
    except Exception:
        return {"success": False, "message": "Timeout giải Captcha từ xa phải là số nguyên giây"}

    if captcha_method == "remote_browser":
        if not (remote_browser_base_url or "").strip():
            return {"success": False, "message": "Chế độ remote_browser cần cấu hình địa chỉ dịch vụ giải Captcha từ xa"}
        if not (remote_browser_api_key or "").strip():
            return {"success": False, "message": "Chế độ remote_browser cần cấu hình API Key dịch vụ giải Captcha từ xa"}

    await db.update_captcha_config(
        captcha_method=captcha_method,
        yescaptcha_api_key=yescaptcha_api_key,
        yescaptcha_base_url=yescaptcha_base_url,
        capmonster_api_key=capmonster_api_key,
        capmonster_base_url=capmonster_base_url,
        ezcaptcha_api_key=ezcaptcha_api_key,
        ezcaptcha_base_url=ezcaptcha_base_url,
        capsolver_api_key=capsolver_api_key,
        capsolver_base_url=capsolver_base_url,
        remote_browser_base_url=remote_browser_base_url,
        remote_browser_api_key=remote_browser_api_key,
        remote_browser_timeout=remote_browser_timeout,
        browser_proxy_enabled=browser_proxy_enabled,
        browser_proxy_url=browser_proxy_url if browser_proxy_enabled else None,
        browser_count=max(1, int(browser_count)) if browser_count else 1,
        personal_project_pool_size=personal_project_pool_size,
        personal_max_resident_tabs=personal_max_resident_tabs,
        personal_idle_tab_ttl_seconds=personal_idle_tab_ttl_seconds
    )

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    # Nếu dùng Captcha bằng trình duyệt, hot-reload cấu hình số lượng trình duyệt
    if captcha_method == "browser":
        try:
            from ..services.browser_captcha import BrowserCaptchaService
            service = await BrowserCaptchaService.get_instance(db)
            await service.reload_browser_count()
        except Exception:
            pass

    # Nếu dùng Captcha bằng trình duyệt cá nhân, hot-reload cấu hình
    if captcha_method == "personal":
        try:
            from ..services.browser_captcha_personal import BrowserCaptchaService
            service = await BrowserCaptchaService.get_instance(db)
            await service.reload_config()
        except Exception as e:
            print(f"[Admin] Hot-update cấu hình Personal thất bại: {e}")

    return {"success": True, "message": "Cập nhật cấu hình Captcha thành công"}


@router.get("/api/captcha/config")
async def get_captcha_config(token: str = Depends(verify_admin_token)):
    """Get captcha configuration"""
    captcha_config = await db.get_captcha_config()
    return {
        "captcha_method": captcha_config.captcha_method,
        "yescaptcha_api_key": captcha_config.yescaptcha_api_key,
        "yescaptcha_base_url": captcha_config.yescaptcha_base_url,
        "capmonster_api_key": captcha_config.capmonster_api_key,
        "capmonster_base_url": captcha_config.capmonster_base_url,
        "ezcaptcha_api_key": captcha_config.ezcaptcha_api_key,
        "ezcaptcha_base_url": captcha_config.ezcaptcha_base_url,
        "capsolver_api_key": captcha_config.capsolver_api_key,
        "capsolver_base_url": captcha_config.capsolver_base_url,
        "remote_browser_base_url": captcha_config.remote_browser_base_url,
        "remote_browser_api_key": captcha_config.remote_browser_api_key,
        "remote_browser_timeout": captcha_config.remote_browser_timeout,
        "browser_proxy_enabled": captcha_config.browser_proxy_enabled,
        "browser_proxy_url": captcha_config.browser_proxy_url or "",
        "browser_count": captcha_config.browser_count,
        "personal_project_pool_size": captcha_config.personal_project_pool_size,
        "personal_max_resident_tabs": captcha_config.personal_max_resident_tabs,
        "personal_idle_tab_ttl_seconds": captcha_config.personal_idle_tab_ttl_seconds
    }


@router.post("/api/captcha/score-test")
async def test_captcha_score(
    _request: Optional[CaptchaScoreTestRequest] = None,
    _token: str = Depends(verify_admin_token)
):
    """Kiểm tra score đã bị vô hiệu."""
    raise HTTPException(status_code=403, detail="Kiểm tra score đã bị vô hiệu")


# ========== Plugin Configuration Endpoints ==========

@router.get("/api/plugin/config")
async def get_plugin_config(request: Request, token: str = Depends(verify_admin_token)):
    """Get plugin configuration"""
    plugin_config = await db.get_plugin_config()

    # Get the actual domain and port from the request
    # This allows the connection URL to reflect the user's actual access path
    host_header = request.headers.get("host", "")

    # Generate connection URL based on actual request
    if host_header:
        # Use the actual domain/IP and port from the request
        connection_url = f"http://{host_header}/api/plugin/update-token"
    else:
        # Fallback to config-based URL
        from ..core.config import config
        server_host = config.server_host
        server_port = config.server_port

        if server_host == "0.0.0.0":
            connection_url = f"http://127.0.0.1:{server_port}/api/plugin/update-token"
        else:
            connection_url = f"http://{server_host}:{server_port}/api/plugin/update-token"

    return {
        "success": True,
        "config": {
            "connection_token": plugin_config.connection_token,
            "connection_url": connection_url,
            "auto_enable_on_update": plugin_config.auto_enable_on_update
        }
    }


@router.post("/api/plugin/config")
async def update_plugin_config(
    request: dict,
    token: str = Depends(verify_admin_token)
):
    """Update plugin configuration"""
    connection_token = request.get("connection_token", "")
    auto_enable_on_update = request.get("auto_enable_on_update", True)  # Bật mặc định

    # Generate random token if empty
    if not connection_token:
        connection_token = secrets.token_urlsafe(32)

    await db.update_plugin_config(
        connection_token=connection_token,
        auto_enable_on_update=auto_enable_on_update
    )

    return {
        "success": True,
        "message": "Cập nhật cấu hình plugin thành công",
        "connection_token": connection_token,
        "auto_enable_on_update": auto_enable_on_update
    }


@router.post("/api/plugin/update-token")
async def plugin_update_token(request: dict, authorization: Optional[str] = Header(None)):
    """Receive token update from Chrome extension (no admin auth required, uses connection_token)"""
    # Verify connection token
    plugin_config = await db.get_plugin_config()

    # Extract token from Authorization header
    provided_token = None
    if authorization:
        if authorization.startswith("Bearer "):
            provided_token = authorization[7:]
        else:
            provided_token = authorization

    # Check if token matches
    if not plugin_config.connection_token or provided_token != plugin_config.connection_token:
        raise HTTPException(status_code=401, detail="Invalid connection token")

    # Extract session token from request
    session_token = request.get("session_token")

    if not session_token:
        raise HTTPException(status_code=400, detail="Missing session_token")

    # Step 1: Convert ST to AT to get user info (including email)
    try:
        result = await token_manager.flow_client.st_to_at(session_token)
        at = result["access_token"]
        expires = result.get("expires")
        user_info = result.get("user", {})
        email = user_info.get("email", "")

        if not email:
            raise HTTPException(status_code=400, detail="Failed to get email from session token")

        # Parse expiration time
        from datetime import datetime
        at_expires = None
        if expires:
            try:
                at_expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
            except:
                pass

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid session token: {str(e)}")

    # Step 2: Check if token with this email exists
    existing_token = await db.get_token_by_email(email)

    if existing_token:
        # Update existing token
        try:
            # Update token
            await token_manager.update_token(
                token_id=existing_token.id,
                st=session_token,
                at=at,
                at_expires=at_expires
            )

            # Check if auto-enable is enabled and token is disabled
            if plugin_config.auto_enable_on_update and not existing_token.is_active:
                await token_manager.enable_token(existing_token.id)
                return {
                    "success": True,
                    "message": f"Token updated and auto-enabled for {email}",
                    "action": "updated",
                    "auto_enabled": True
                }

            return {
                "success": True,
                "message": f"Token updated for {email}",
                "action": "updated"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update token: {str(e)}")
    else:
        # Add new token
        try:
            new_token = await token_manager.add_token(
                st=session_token,
                remark="Added by Chrome Extension"
            )

            return {
                "success": True,
                "message": f"Token added for {new_token.email}",
                "action": "added",
                "token_id": new_token.id
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to add token: {str(e)}")
