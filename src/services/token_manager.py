"""Token manager for Flow2API with AT auto-refresh"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from ..core.database import Database
from ..core.config import config
from ..core.models import Token, Project
from ..core.logger import debug_logger
from .flow_client import FlowClient
from .proxy_manager import ProxyManager


class TokenManager:
    """Token lifecycle manager with AT auto-refresh"""

    def __init__(self, db: Database, flow_client: FlowClient):
        self.db = db
        self.flow_client = flow_client
        self._refresh_lock_guard = asyncio.Lock()
        self._project_lock_guard = asyncio.Lock()
        self._refresh_locks: dict[int, asyncio.Lock] = {}
        self._project_locks: dict[int, asyncio.Lock] = {}
        self._refresh_futures: dict[int, asyncio.Task] = {}

    async def _get_token_lock(
        self,
        lock_map: dict[int, asyncio.Lock],
        guard: asyncio.Lock,
        token_id: int,
    ) -> asyncio.Lock:
        """Lấy lock theo chiều token, tránh các token khác bị chặn tuần tự."""
        async with guard:
            lock = lock_map.get(token_id)
            if lock is None:
                lock = asyncio.Lock()
                lock_map[token_id] = lock
            return lock

    def _get_project_pool_size(self) -> int:
        """Đọc cấu hình kích thước project pool hiện tại cho mỗi Token."""
        try:
            return max(1, min(50, int(config.personal_project_pool_size or 4)))
        except Exception:
            return 4

    def _sort_projects(self, projects: List[Project]) -> List[Project]:
        """Sort projects in a stable order for round-robin selection."""
        return sorted(projects, key=lambda project: (project.id or 0, project.project_id))

    def _normalize_project_name_base(self, project_name: Optional[str] = None) -> str:
        """Normalize a project base name for pooled creation."""
        raw_name = (project_name or "").strip()
        if raw_name:
            parts = raw_name.rsplit(" ", 1)
            if len(parts) == 2 and parts[1].startswith("P") and parts[1][1:].isdigit():
                return parts[0]
            return raw_name
        return datetime.now().strftime("%b %d - %H:%M")

    def _build_project_name(self, pool_index: int, base_name: Optional[str] = None) -> str:
        """Build a project name for the pool."""
        normalized_base = self._normalize_project_name_base(base_name)
        return f"{normalized_base} P{pool_index}"

    async def get_personal_warmup_project_ids(
        self,
        tokens: Optional[List[Token]] = None,
        limit: Optional[int] = None,
    ) -> List[str]:
        """Trả về danh sách project ID nên warm-up khi khởi động chế độ personal."""
        token_list = tokens if tokens is not None else await self.get_all_tokens()
        pool_size = self._get_project_pool_size()
        warmup_ids: List[str] = []
        seen_projects: set[str] = set()

        try:
            warmup_limit = None if limit is None else max(1, int(limit))
        except Exception:
            warmup_limit = None

        for token in token_list:
            if not token or not token.is_active:
                continue

            candidate_ids: List[str] = []
            current_project_id = str(token.current_project_id or "").strip()
            if current_project_id:
                candidate_ids.append(current_project_id)

            projects = [project for project in await self.db.get_projects_by_token(token.id) if project.is_active]
            for project in self._sort_projects(projects):
                project_id = str(project.project_id or "").strip()
                if project_id and project_id not in candidate_ids:
                    candidate_ids.append(project_id)

            for project_id in candidate_ids[:pool_size]:
                if project_id in seen_projects:
                    continue
                seen_projects.add(project_id)
                warmup_ids.append(project_id)
                if warmup_limit is not None and len(warmup_ids) >= warmup_limit:
                    return warmup_ids

        return warmup_ids

    async def _create_project_for_token(self, token: Token, pool_index: int, base_name: Optional[str] = None) -> Project:
        """Create a new pooled project for a token and persist it."""
        project_name = self._build_project_name(pool_index, base_name)
        project_id = await self.flow_client.create_project(token.st, project_name)
        debug_logger.log_info(
            f"[PROJECT] Created pooled project for token {token.id}: {project_name} ({project_id})"
        )
        project = Project(
            project_id=project_id,
            token_id=token.id,
            project_name=project_name,
        )
        project.id = await self.db.add_project(project)
        return project

    def _select_next_project(self, token: Token, projects: List[Project]) -> Project:
        """Select the next project from the pool in round-robin order."""
        ordered_projects = self._sort_projects(projects)
        if not ordered_projects:
            raise ValueError("No available projects for token")

        if len(ordered_projects) == 1:
            return ordered_projects[0]

        if token.current_project_id:
            for index, project in enumerate(ordered_projects):
                if project.project_id == token.current_project_id:
                    return ordered_projects[(index + 1) % len(ordered_projects)]

        return ordered_projects[0]

    # ========== Token CRUD ==========

    async def get_all_tokens(self) -> List[Token]:
        """Get all tokens"""
        return await self.db.get_all_tokens()

    async def get_active_tokens(self) -> List[Token]:
        """Get all active tokens"""
        return await self.db.get_active_tokens()

    async def get_token(self, token_id: int) -> Optional[Token]:
        """Get token by ID"""
        return await self.db.get_token(token_id)

    async def delete_token(self, token_id: int):
        """Delete token"""
        token = await self.db.get_token(token_id)
        project_ids: List[str] = []
        if token:
            current_project_id = str(token.current_project_id or "").strip()
            if current_project_id:
                project_ids.append(current_project_id)

        for project in await self.db.get_projects_by_token(token_id):
            project_id = str(project.project_id or "").strip()
            if project_id and project_id not in project_ids:
                project_ids.append(project_id)

        await self.db.delete_token(token_id)

        refresh_task = self._refresh_futures.pop(token_id, None)
        if refresh_task and not refresh_task.done():
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        self._refresh_locks.pop(token_id, None)
        self._project_locks.pop(token_id, None)

        if config.captcha_method == "personal" and project_ids:
            try:
                from .browser_captcha_personal import BrowserCaptchaService
                service = await BrowserCaptchaService.get_instance(self.db)
                for project_id in project_ids:
                    await service.stop_resident_mode(project_id)
            except Exception as e:
                debug_logger.log_warning(f"[DELETE_TOKEN] Dọn dẹp trạng thái trình duyệt personal thất bại: {e}")

    async def enable_token(self, token_id: int):
        """Enable a token and reset error count"""
        # Enable the token
        await self.db.update_token(token_id, is_active=True)
        # Reset error count when enabling (only reset total error_count, keep today_error_count)
        await self.db.reset_error_count(token_id)

    async def disable_token(self, token_id: int):
        """Disable a token"""
        await self.db.update_token(token_id, is_active=False)

    # ========== Thêm Token (hỗ trợ tạo Project) ==========

    async def add_token(
        self,
        st: str,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        remark: Optional[str] = None,
        image_enabled: bool = True,
        video_enabled: bool = True,
        image_concurrency: int = -1,
        video_concurrency: int = -1,
        captcha_proxy_url: Optional[str] = None
    ) -> Token:
        """Add a new token and prepare its pooled projects."""
        existing_token = await self.db.get_token_by_st(st)
        if existing_token:
            raise ValueError(f"Token ??????: {existing_token.email}?")

        debug_logger.log_info(f"[ADD_TOKEN] Converting ST to AT...")
        try:
            result = await self.flow_client.st_to_at(st)
            at = result["access_token"]
            expires = result.get("expires")
            user_info = result.get("user", {})
            email = user_info.get("email", "")
            name = user_info.get("name", email.split("@")[0] if email else "")
            at_expires = None
            if expires:
                try:
                    at_expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                except Exception:
                    pass
        except Exception as e:
            raise ValueError(f"ST?AT??: {str(e)}")

        try:
            credits_result = await self.flow_client.get_credits(at)
            credits = credits_result.get("credits", 0)
            user_paygate_tier = credits_result.get("userPaygateTier")
        except Exception:
            credits = 0
            user_paygate_tier = None

        base_project_name = self._normalize_project_name_base(project_name)
        project_pool_size = self._get_project_pool_size()
        pooled_projects: List[Project] = []

        if project_id:
            first_project_name = self._build_project_name(1, base_project_name)
            debug_logger.log_info(f"[ADD_TOKEN] Using provided project_id as pooled project #1: {project_id}")
            pooled_projects.append(Project(
                project_id=project_id,
                token_id=0,
                project_name=first_project_name,
                tool_name="PINHOLE"
            ))
        else:
            try:
                first_project_name = self._build_project_name(1, base_project_name)
                first_project_id = await self.flow_client.create_project(st, first_project_name)
                debug_logger.log_info(f"[ADD_TOKEN] Created pooled project #1: {first_project_name} (ID: {first_project_id})")
                pooled_projects.append(Project(
                    project_id=first_project_id,
                    token_id=0,
                    project_name=first_project_name,
                    tool_name="PINHOLE"
                ))
            except Exception as e:
                raise ValueError(f"??????: {str(e)}")

        token = Token(
            st=st,
            at=at,
            at_expires=at_expires,
            email=email,
            name=name,
            remark=remark,
            is_active=True,
            credits=credits,
            user_paygate_tier=user_paygate_tier,
            current_project_id=pooled_projects[0].project_id,
            current_project_name=pooled_projects[0].project_name,
            image_enabled=image_enabled,
            video_enabled=video_enabled,
            image_concurrency=image_concurrency,
            video_concurrency=video_concurrency,
            captcha_proxy_url=captcha_proxy_url
        )

        token_id = await self.db.add_token(token)
        token.id = token_id

        pooled_projects[0].token_id = token_id
        pooled_projects[0].id = await self.db.add_project(pooled_projects[0])

        while len(pooled_projects) < project_pool_size:
            new_project = await self._create_project_for_token(token, len(pooled_projects) + 1, base_project_name)
            pooled_projects.append(new_project)

        debug_logger.log_info(
            f"[ADD_TOKEN] Token added successfully (ID: {token_id}, Email: {email}, pooled_projects={len(pooled_projects)})"
        )
        return token
    async def update_token(
        self,
        token_id: int,
        st: Optional[str] = None,
        at: Optional[str] = None,
        at_expires: Optional[datetime] = None,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        remark: Optional[str] = None,
        image_enabled: Optional[bool] = None,
        video_enabled: Optional[bool] = None,
        image_concurrency: Optional[int] = None,
        video_concurrency: Optional[int] = None,
        captcha_proxy_url: Optional[str] = None
    ):
        """Update token (hỗ trợ chỉnh sửa project_id và project_name)

        Khi user chỉnh sửa token và lưu, nếu token chưa hết hạn sẽ tự xóa trạng thái bị vô hiệu do 429
        """
        update_fields = {}

        if st is not None:
            update_fields["st"] = st
        if at is not None:
            update_fields["at"] = at
        if at_expires is not None:
            update_fields["at_expires"] = at_expires
        if project_id is not None:
            update_fields["current_project_id"] = project_id
        if project_name is not None:
            update_fields["current_project_name"] = project_name
        if remark is not None:
            update_fields["remark"] = remark
        if image_enabled is not None:
            update_fields["image_enabled"] = image_enabled
        if video_enabled is not None:
            update_fields["video_enabled"] = video_enabled
        if image_concurrency is not None:
            update_fields["image_concurrency"] = image_concurrency
        if video_concurrency is not None:
            update_fields["video_concurrency"] = video_concurrency
        if captcha_proxy_url is not None:
            update_fields["captcha_proxy_url"] = captcha_proxy_url

        # Kiểm tra token có bị vô hiệu do 429 không; nếu có và chưa hết hạn thì xóa trạng thái 429
        token = await self.db.get_token(token_id)
        if token and token.ban_reason == "429_rate_limit":
            # Kiểm tra token có hết hạn không
            is_expired = False
            if token.at_expires:
                now = datetime.now(timezone.utc)
                if token.at_expires.tzinfo is None:
                    at_expires_aware = token.at_expires.replace(tzinfo=timezone.utc)
                else:
                    at_expires_aware = token.at_expires
                is_expired = at_expires_aware <= now

            # Nếu chưa hết hạn, xóa trạng thái vô hiệu do 429
            if not is_expired:
                debug_logger.log_info(f"[UPDATE_TOKEN] Token {token_id} đã được chỉnh sửa và lưu, xóa trạng thái vô hiệu do 429")
                update_fields["ban_reason"] = None
                update_fields["banned_at"] = None

        if update_fields:
            await self.db.update_token(token_id, **update_fields)

    # ========== Logic tự động làm mới AT (chính) ==========

    def _should_refresh_at(self, token: Token) -> bool:
        """Dựa vào snapshot hiện tại của token để quyết định có cần làm mới AT không."""
        if not token.at:
            debug_logger.log_info(f"[AT_CHECK] Token {token.id}: AT không tồn tại, cần làm mới")
            return True

        if not token.at_expires:
            debug_logger.log_info(f"[AT_CHECK] Token {token.id}: không rõ hạn AT, thử làm mới")
            return True

        now = datetime.now(timezone.utc)
        if token.at_expires.tzinfo is None:
            at_expires_aware = token.at_expires.replace(tzinfo=timezone.utc)
        else:
            at_expires_aware = token.at_expires

        time_until_expiry = at_expires_aware - now
        if time_until_expiry.total_seconds() < 3600:
            debug_logger.log_info(
                f"[AT_CHECK] Token {token.id}: AT sắp hết hạn "
                f"(còn {time_until_expiry.total_seconds():.0f} giây), cần làm mới"
            )
            return True

        return False

    def needs_at_refresh(self, token: Optional[Token]) -> bool:
        """Cho scheduler nhanh chóng đánh giá token hiện tại có khả năng trigger làm mới AT không."""
        if not token:
            return True
        return self._should_refresh_at(token)

    async def ensure_valid_token(self, token: Optional[Token]) -> Optional[Token]:
        """Đảm bảo AT của token khả dụng, khi cần thiết trả về object mới nhất sau khi làm mới."""
        if not token:
            return None

        if not self._should_refresh_at(token):
            return token

        if not await self._refresh_at(token.id):
            return None

        return await self.db.get_token(token.id)

    async def is_at_valid(self, token_id: int, token: Optional[Token] = None) -> bool:
        """Kiểm tra AT có hợp lệ không; nếu không hợp lệ hoặc sắp hết hạn thì tự động làm mới.

        Returns:
            True if AT is valid or refreshed successfully
            False if AT cannot be refreshed
        """
        token_obj = token if token and token.id == token_id else await self.db.get_token(token_id)
        if not token_obj:
            return False

        valid_token = await self.ensure_valid_token(token_obj)
        return valid_token is not None


    async def _refresh_at_inner(self, token_id: int) -> bool:
        """Perform exactly one real AT refresh attempt."""
        refresh_lock = await self._get_token_lock(
            self._refresh_locks,
            self._refresh_lock_guard,
            token_id,
        )
        async with refresh_lock:
            token = await self.db.get_token(token_id)
            if not token:
                return False

            result = await self._do_refresh_at(token_id, token.st)
            if result:
                return True

            debug_logger.log_info(f"[AT_REFRESH] Token {token_id}: first AT refresh failed, trying ST refresh...")
            new_st = await self._try_refresh_st(token_id, token)
            if new_st:
                debug_logger.log_info(f"[AT_REFRESH] Token {token_id}: ST refreshed, retrying AT refresh...")
                result = await self._do_refresh_at(token_id, new_st)
                if result:
                    return True

            debug_logger.log_error(f"[AT_REFRESH] Token {token_id}: all refresh attempts failed, disabling token")
            await self.disable_token(token_id)
            return False

    async def _refresh_at(self, token_id: int) -> bool:
        """Coalesce concurrent AT refresh calls for the same token."""
        existing_task = self._refresh_futures.get(token_id)
        if existing_task:
            return await existing_task

        async def runner() -> bool:
            try:
                return await self._refresh_at_inner(token_id)
            finally:
                current = self._refresh_futures.get(token_id)
                if current is task:
                    self._refresh_futures.pop(token_id, None)

        task = asyncio.create_task(runner())
        self._refresh_futures[token_id] = task
        return await task

    async def _do_refresh_at(self, token_id: int, st: str) -> bool:
        """Logic chính để thực hiện làm mới AT.

        Args:
            token_id: Token ID
            st: Session Token

        Returns:
            True if refresh successful AND AT is valid, False otherwise
        """
        try:
            debug_logger.log_info(f"[AT_REFRESH] Token {token_id}: Bắt đầu làm mới AT...")

            # Dùng ST để chuyển thành AT
            result = await self.flow_client.st_to_at(st)
            new_at = result["access_token"]
            expires = result.get("expires")

            # Parse thời điểm hết hạn
            new_at_expires = None
            if expires:
                try:
                    new_at_expires = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                except:
                    pass

            # Cập nhật database
            await self.db.update_token(
                token_id,
                at=new_at,
                at_expires=new_at_expires
            )

            debug_logger.log_info(f"[AT_REFRESH] Token {token_id}: Làm mới AT thành công")
            debug_logger.log_info(f"  - Hạn mới: {new_at_expires}")

            # Xác thực AT: thử qua get_credits
            try:
                credits_result = await self.flow_client.get_credits(new_at)
                await self.db.update_token(
                    token_id,
                    credits=credits_result.get("credits", 0),
                    user_paygate_tier=credits_result.get("userPaygateTier"),
                )
                debug_logger.log_info(f"[AT_REFRESH] Token {token_id}: Xác thực AT thành công (số dư: {credits_result.get('credits', 0)})")
                return True
            except Exception as verify_err:
                # Xác thực AT thất bại (có thể trả về 401), nghĩa là ST đã hết hạn
                error_msg = str(verify_err)
                if "401" in error_msg or "UNAUTHENTICATED" in error_msg:
                    debug_logger.log_warning(f"[AT_REFRESH] Token {token_id}: Xác thực AT thất bại (401), ST có thể đã hết hạn")
                    return False
                else:
                    # Lỗi khác (như mạng), vẫn coi là thành công
                    debug_logger.log_warning(f"[AT_REFRESH] Token {token_id}: Xác thực AT gặp lỗi không phải xác thực: {error_msg}")
                    return True

        except Exception as e:
            debug_logger.log_error(f"[AT_REFRESH] Token {token_id}: Làm mới AT thất bại - {str(e)}")
            return False

    async def _try_refresh_st(self, token_id: int, token) -> Optional[str]:
        """Thử làm mới Session Token qua trình duyệt.

        Dùng tab thường trú để lấy __Secure-next-auth.session-token mới

        Args:
            token_id: Token ID
            token: object Token

        Returns:
            Chuỗi ST mới; nếu thất bại trả về None
        """
        try:
            from ..core.config import config

            # Chỉ hỗ trợ tự động làm mới ST trong chế độ personal
            if config.captcha_method != "personal":
                debug_logger.log_info(f"[ST_REFRESH] Không phải chế độ personal, bỏ qua tự động làm mới ST")
                return None

            if not token.current_project_id:
                debug_logger.log_warning(f"[ST_REFRESH] Token {token_id} không có project_id, không làm mới được ST")
                return None

            debug_logger.log_info(f"[ST_REFRESH] Token {token_id}: Thử làm mới ST qua trình duyệt...")

            from .browser_captcha_personal import BrowserCaptchaService
            service = await BrowserCaptchaService.get_instance(self.db)

            refresh_timeout_seconds = 45.0
            try:
                new_st = await asyncio.wait_for(
                    service.refresh_session_token(token.current_project_id),
                    timeout=refresh_timeout_seconds,
                )
            except asyncio.TimeoutError:
                debug_logger.log_error(
                    f"[ST_REFRESH] Token {token_id}: Làm mới ST timeout ({refresh_timeout_seconds:.0f}s)"
                )
                return None
            if new_st and new_st != token.st:
                # Cập nhật ST trong database
                await self.db.update_token(token_id, st=new_st)
                debug_logger.log_info(f"[ST_REFRESH] Token {token_id}: ST đã được cập nhật tự động")
                return new_st
            elif new_st == token.st:
                debug_logger.log_warning(f"[ST_REFRESH] Token {token_id}: ST thu được giống ST cũ, có thể session đăng nhập đã mất")
                return None
            else:
                debug_logger.log_warning(f"[ST_REFRESH] Token {token_id}: Không lấy được ST mới")
                return None

        except Exception as e:
            debug_logger.log_error(f"[ST_REFRESH] Token {token_id}: Làm mới ST thất bại - {str(e)}")
            return None

    async def ensure_project_exists(self, token_id: int) -> str:
        """Ensure a token has a pooled set of projects and return one in round-robin order."""
        project_lock = await self._get_token_lock(
            self._project_locks,
            self._project_lock_guard,
            token_id,
        )
        async with project_lock:
            token = await self.db.get_token(token_id)
            if not token:
                raise ValueError("Token not found")

            projects = [project for project in await self.db.get_projects_by_token(token_id) if project.is_active]
            projects = self._sort_projects(projects)

            try:
                project_pool_size = self._get_project_pool_size()
                while len(projects) < project_pool_size:
                    new_project = await self._create_project_for_token(token, len(projects) + 1)
                    projects.append(new_project)
                    projects = self._sort_projects(projects)

                selectable_projects = projects[:project_pool_size]
                selected_project = self._select_next_project(token, selectable_projects)
                await self.db.update_token(
                    token_id,
                    current_project_id=selected_project.project_id,
                    current_project_name=selected_project.project_name,
                )
                return selected_project.project_id
            except Exception as e:
                raise ValueError(f"Failed to prepare project pool: {str(e)}")

    async def record_usage(self, token_id: int, is_video: bool = False):
        """Record token usage"""
        await self.db.update_token(token_id, use_count=1, last_used_at=datetime.now())

        if is_video:
            await self.db.increment_token_stats(token_id, "video")
        else:
            await self.db.increment_token_stats(token_id, "image")

    async def record_error(self, token_id: int):
        """Record token error and auto-disable if threshold reached"""
        await self.db.increment_token_stats(token_id, "error")

        # Check if should auto-disable token (based on consecutive errors)
        stats = await self.db.get_token_stats(token_id)
        admin_config = await self.db.get_admin_config()

        if stats and stats.consecutive_error_count >= admin_config.error_ban_threshold:
            debug_logger.log_warning(
                f"[TOKEN_BAN] Token {token_id} consecutive error count ({stats.consecutive_error_count}) "
                f"reached threshold ({admin_config.error_ban_threshold}), auto-disabling"
            )
            await self.disable_token(token_id)

    async def record_success(self, token_id: int):
        """Record successful request (reset consecutive error count)

        This method resets error_count to 0, which is used for auto-disable threshold checking.
        Note: today_error_count and historical statistics are NOT reset.
        """
        await self.db.reset_error_count(token_id)

    async def ban_token_for_429(self, token_id: int):
        """Vô hiệu token ngay lập tức do lỗi 429.

        Args:
            token_id: Token ID
        """
        debug_logger.log_warning(f"[429_BAN] Vô hiệu Token {token_id} (lý do: 429 Rate Limit)")
        await self.db.update_token(
            token_id,
            is_active=False,
            ban_reason="429_rate_limit",
            banned_at=datetime.now(timezone.utc)
        )

    async def auto_unban_429_tokens(self):
        """Tự động bỏ chặn các token bị vô hiệu do 429.

        Quy tắc:
        - Tự bỏ chặn sau 12 giờ kể từ thời điểm vô hiệu
        - Chỉ bỏ chặn token chưa hết hạn
        - Chỉ bỏ chặn token bị vô hiệu do 429
        """
        all_tokens = await self.db.get_all_tokens()
        now = datetime.now(timezone.utc)

        for token in all_tokens:
            # Bỏ qua token bị vô hiệu không phải do 429
            if token.ban_reason != "429_rate_limit":
                continue

            # Bỏ qua token chưa bị vô hiệu
            if token.is_active:
                continue

            # Bỏ qua token không có thời điểm vô hiệu
            if not token.banned_at:
                continue

            # Kiểm tra token có hết hạn chưa
            if token.at_expires:
                # Đảm bảo timezone đồng nhất
                if token.at_expires.tzinfo is None:
                    at_expires_aware = token.at_expires.replace(tzinfo=timezone.utc)
                else:
                    at_expires_aware = token.at_expires

                # Nếu đã hết hạn thì bỏ qua
                if at_expires_aware <= now:
                    debug_logger.log_info(f"[AUTO_UNBAN] Token {token.id} đã hết hạn, bỏ qua việc bỏ chặn")
                    continue

            # Đảm bảo timezone của banned_at đồng nhất
            if token.banned_at.tzinfo is None:
                banned_at_aware = token.banned_at.replace(tzinfo=timezone.utc)
            else:
                banned_at_aware = token.banned_at

            # Kiểm tra đã qua 12 giờ chưa
            time_since_ban = now - banned_at_aware
            if time_since_ban.total_seconds() >= 12 * 3600:  # 12 giờ
                debug_logger.log_info(
                    f"[AUTO_UNBAN] Bỏ chặn Token {token.id} (vô hiệu lúc: {banned_at_aware}, "
                    f"đã qua {time_since_ban.total_seconds() / 3600:.1f} giờ)"
                )
                await self.db.update_token(
                    token.id,
                    is_active=True,
                    ban_reason=None,
                    banned_at=None
                )
                # Reset bộ đếm lỗi
                await self.db.reset_error_count(token.id)

    # ========== Làm mới số dư ==========

    async def refresh_credits(self, token_id: int) -> int:
        """Làm mới số dư Token.

        Returns:
            credits
        """
        token = await self.db.get_token(token_id)
        if not token:
            return 0

        # Đảm bảo AT hợp lệ
        token = await self.ensure_valid_token(token)
        if not token:
            return 0

        try:
            result = await self.flow_client.get_credits(token.at)
            credits = result.get("credits", 0)
            user_paygate_tier = result.get("userPaygateTier")

            # Cập nhật database
            await self.db.update_token(
                token_id,
                credits=credits,
                user_paygate_tier=user_paygate_tier,
            )

            return credits
        except Exception as e:
            debug_logger.log_error(f"Failed to refresh credits for token {token_id}: {str(e)}")
            return 0
