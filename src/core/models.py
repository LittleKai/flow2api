"""Data models for Flow2API"""

from pydantic import BaseModel, ConfigDict
from typing import Optional, List, Union, Any, Literal
from datetime import datetime


class Token(BaseModel):
    """Token model for Flow2API"""

    id: Optional[int] = None

    # Thông tin xác thực (chính)
    st: str  # Session Token (__Secure-next-auth.session-token)
    at: Optional[str] = None  # Access Token (được chuyển từ ST)
    at_expires: Optional[datetime] = None  # Thời điểm AT hết hạn

    # Thông tin cơ bản
    email: str
    name: Optional[str] = ""
    remark: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    use_count: int = 0

    # Trường riêng của VideoFX
    credits: int = 0  # Credits còn lại
    user_paygate_tier: Optional[str] = None  # PAYGATE_TIER_ONE

    # Quản lý project
    current_project_id: Optional[str] = None  # UUID của project đang dùng
    current_project_name: Optional[str] = None  # Tên project

    # Công tắc tính năng
    image_enabled: bool = True
    video_enabled: bool = True

    # Giới hạn concurrency
    image_concurrency: int = -1  # -1 nghĩa là không giới hạn
    video_concurrency: int = -1  # -1 nghĩa là không giới hạn

    # Proxy giải Captcha (cấp Token, có thể ghi đè proxy Captcha trình duyệt toàn cục)
    captcha_proxy_url: Optional[str] = None

    # Các trường liên quan tới vô hiệu do 429
    ban_reason: Optional[str] = None  # Lý do vô hiệu: "429_rate_limit" hoặc None
    banned_at: Optional[datetime] = None  # Thời điểm vô hiệu


class Project(BaseModel):
    """Project model for VideoFX"""

    id: Optional[int] = None
    project_id: str  # UUID project của VideoFX
    token_id: int  # ID Token liên kết
    project_name: str  # Tên project
    tool_name: str = "PINHOLE"  # Tên tool, cố định là PINHOLE
    is_active: bool = True
    created_at: Optional[datetime] = None


class TokenStats(BaseModel):
    """Token statistics"""

    token_id: int
    image_count: int = 0
    video_count: int = 0
    success_count: int = 0
    error_count: int = 0  # Historical total errors (never reset)
    last_success_at: Optional[datetime] = None
    last_error_at: Optional[datetime] = None
    # Thống kê hôm nay
    today_image_count: int = 0
    today_video_count: int = 0
    today_error_count: int = 0
    today_date: Optional[str] = None
    # Đếm lỗi liên tiếp (dùng cho logic tự vô hiệu hóa)
    consecutive_error_count: int = 0


class Task(BaseModel):
    """Generation task"""

    id: Optional[int] = None
    task_id: str  # operation name Flow API trả về
    token_id: int
    model: str
    prompt: str
    status: str  # processing, completed, failed
    progress: int = 0  # 0-100
    result_urls: Optional[List[str]] = None
    error_message: Optional[str] = None
    scene_id: Optional[str] = None  # sceneId của Flow API
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class RequestLog(BaseModel):
    """API request log"""

    id: Optional[int] = None
    token_id: Optional[int] = None
    operation: str
    request_body: Optional[str] = None
    response_body: Optional[str] = None
    status_code: int
    duration: float
    status_text: Optional[str] = None
    progress: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AdminConfig(BaseModel):
    """Admin configuration"""

    id: int = 1
    username: str
    password: str
    api_key: str
    error_ban_threshold: int = 3  # Auto-disable token after N consecutive errors


class ProxyConfig(BaseModel):
    """Proxy configuration"""

    id: int = 1
    enabled: bool = False  # Công tắc proxy cho request
    proxy_url: Optional[str] = None  # Địa chỉ proxy request
    media_proxy_enabled: bool = False  # Công tắc proxy tải lên/tải xuống ảnh
    media_proxy_url: Optional[str] = None  # Địa chỉ proxy tải lên/tải xuống ảnh


class GenerationConfig(BaseModel):
    """Generation timeout configuration"""

    id: int = 1
    image_timeout: int = 300  # seconds
    video_timeout: int = 1500  # seconds
    max_retries: int = 3  # Số lần retry tối đa cho request


class CallLogicConfig(BaseModel):
    """Token selection call logic configuration"""

    id: int = 1
    call_mode: str = "default"
    polling_mode_enabled: bool = False
    updated_at: Optional[datetime] = None


class CacheConfig(BaseModel):
    """Cache configuration"""

    id: int = 1
    cache_enabled: bool = False
    cache_timeout: int = 7200  # seconds (2 hours), 0 means never expire
    cache_base_url: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class DebugConfig(BaseModel):
    """Debug configuration"""

    id: int = 1
    enabled: bool = False
    log_requests: bool = True
    log_responses: bool = True
    mask_token: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CaptchaConfig(BaseModel):
    """Captcha configuration"""

    id: int = 1
    captcha_method: str = "browser"  # yescaptcha/capmonster/ezcaptcha/capsolver/browser/personal/remote_browser
    yescaptcha_api_key: str = ""
    yescaptcha_base_url: str = "https://api.yescaptcha.com"
    capmonster_api_key: str = ""
    capmonster_base_url: str = "https://api.capmonster.cloud"
    ezcaptcha_api_key: str = ""
    ezcaptcha_base_url: str = "https://api.ez-captcha.com"
    capsolver_api_key: str = ""
    capsolver_base_url: str = "https://api.capsolver.com"
    remote_browser_base_url: str = ""
    remote_browser_api_key: str = ""
    remote_browser_timeout: int = 60
    website_key: str = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
    page_action: str = "IMAGE_GENERATION"
    browser_proxy_enabled: bool = False  # Trình duyệt giải Captcha có bật proxy không
    browser_proxy_url: Optional[str] = None  # URL proxy trình duyệt giải Captcha
    browser_count: int = 1  # Số instance trình duyệt giải Captcha
    personal_project_pool_size: int = 4  # Số project pool mặc định của mỗi Token (chỉ ảnh hưởng luân chuyển project)
    personal_max_resident_tabs: int = 5  # Số tab giải Captcha dùng chung tối đa của trình duyệt tích hợp
    personal_idle_tab_ttl_seconds: int = 600  # Timeout nhàn rỗi của tab trình duyệt tích hợp (giây)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class PluginConfig(BaseModel):
    """Plugin connection configuration"""

    id: int = 1
    connection_token: str = ""  # Token kết nối của plugin
    auto_enable_on_update: bool = True  # Tự bật lại khi cập nhật token (mặc định bật)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


# OpenAI Compatible Request Models
class ChatMessage(BaseModel):
    """Chat message"""

    role: str
    content: Union[str, List[dict]]  # string or multimodal array


class ImageConfig(BaseModel):
    """Gemini imageConfig parameters"""

    aspectRatio: Optional[str] = None  # "16:9", "9:16", "1:1", "4:3", "3:4"
    imageSize: Optional[str] = None  # "2k", "4k"

    # Tương thích các trường size/quality hoặc snake_case mà upstream OpenAI/NewAPI có thể truyền qua
    model_config = ConfigDict(extra="allow")


class GenerationConfigParam(BaseModel):
    """Gemini generationConfig parameters (for model name resolution)"""

    responseModalities: Optional[List[str]] = None  # ["IMAGE", "TEXT"]
    imageConfig: Optional[ImageConfig] = None

    model_config = ConfigDict(extra="allow")


class GeminiInlineData(BaseModel):
    """Gemini inline binary data."""

    mimeType: str
    data: str


class GeminiFileData(BaseModel):
    """Gemini file reference."""

    fileUri: str
    mimeType: Optional[str] = None


class GeminiPart(BaseModel):
    """Gemini content part."""

    text: Optional[str] = None
    inlineData: Optional[GeminiInlineData] = None
    fileData: Optional[GeminiFileData] = None

    model_config = ConfigDict(extra="allow")


class GeminiContent(BaseModel):
    """Gemini content block."""

    role: Optional[Literal["user", "model"]] = None
    parts: List[GeminiPart]


class GeminiGenerateContentRequest(BaseModel):
    """Gemini official generateContent request."""

    contents: List[GeminiContent]
    generationConfig: Optional[GenerationConfigParam] = None
    systemInstruction: Optional[GeminiContent] = None

    model_config = ConfigDict(extra="allow")


class ChatCompletionRequest(BaseModel):
    """Chat completion request (OpenAI compatible + Gemini extension)"""

    model: str
    messages: Optional[List[ChatMessage]] = None
    stream: bool = False
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    # Flow2API specific parameters
    image: Optional[str] = None  # Base64 encoded image (deprecated, use messages)
    video: Optional[str] = None  # Base64 encoded video (deprecated)
    # Gemini extension parameters (from extra_body or top-level)
    generationConfig: Optional[GenerationConfigParam] = None
    contents: Optional[List[Any]] = None  # Gemini native contents

    model_config = ConfigDict(extra="allow")  # Allow extra fields like extra_body passthrough
