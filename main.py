import asyncio
import csv
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_chatcsv",
    "ChatCSV",
    "将聊天记录保存为 CSV 文件。",
    "1.0.0",
    "https://github.com/Puiching-Memory/ChatCSV",
)
class ChatCSVLogger(Star):
    """监听所有消息并将聊天记录持久化到 CSV 文件。"""

    def __init__(self, context: Context):
        super().__init__(context)
        self._csv_headers = [
            "timestamp_iso",
            "platform",
            "session_id",
            "message_id",
            "group_id",
            "sender_id",
            "sender_name",
            "message_text",
        ]
        base_dir = getattr(self.context, "data_dir", None) or os.getcwd()
        self._csv_base_dir = os.path.join(base_dir, "chatcsv")
        self._global_init_lock = asyncio.Lock()
        self._file_locks: Dict[str, asyncio.Lock] = {}

    async def initialize(self) -> None:
        """保证 CSV 路径存在并写入表头。"""
        await asyncio.to_thread(self._ensure_dir_ready, self._csv_base_dir)

    @filter.on(AstrMessageEvent)
    async def record_message(self, event: AstrMessageEvent):
        """捕获所有消息事件并追加到 CSV。"""
        try:
            timestamp_iso = self._to_iso(event.timestamp)
            msg_obj: Any = getattr(event, "message_obj", None)
            session_id = getattr(msg_obj, "session_id", "")
            message_id = getattr(msg_obj, "message_id", "")
            group_id = getattr(msg_obj, "group_id", "")
            sender = getattr(msg_obj, "sender", None)
            sender_id = getattr(sender, "user_id", "") or getattr(sender, "id", "")
            sender_name = event.get_sender_name()
            platform = event.get_platform_name()
            message_text = event.message_str or ""

            row = [
                timestamp_iso,
                platform,
                session_id,
                message_id,
                group_id,
                sender_id,
                sender_name,
                message_text,
            ]
            csv_path = await self._resolve_csv_path(group_id, session_id, sender_id)
            await self._append_row(csv_path, row)
        except Exception:
            logger.exception("记录聊天消息到 CSV 失败。")

    async def terminate(self) -> None:
        """插件卸载时无需额外清理。"""

    async def _append_row(self, file_path: str, row: list[str]) -> None:
        lock = await self._get_file_lock(file_path)
        async with lock:
            await asyncio.to_thread(self._write_row_sync, file_path, row)

    def _write_row_sync(self, file_path: str, row: list[str]) -> None:
        with open(file_path, mode="a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(row)

    @staticmethod
    def _to_iso(timestamp: Any) -> str:
        try:
            ts = float(timestamp)
        except (TypeError, ValueError):
            return datetime.now(timezone.utc).isoformat()
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    async def _resolve_csv_path(
        self, group_id: Any, session_id: Any, sender_id: Any
    ) -> str:
        if group_id:
            folder = os.path.join(
                self._csv_base_dir, "groups", self._sanitize_component(group_id)
            )
            file_name = "chat_history.csv"
        else:
            identifier = (
                self._sanitize_component(session_id)
                or self._sanitize_component(sender_id)
                or "private_unknown"
            )
            folder = os.path.join(self._csv_base_dir, "privates", identifier)
            file_name = "chat_history.csv"

        file_path = os.path.join(folder, file_name)
        await asyncio.to_thread(self._ensure_csv_ready, folder, file_path)
        return file_path

    def _ensure_dir_ready(self, directory: str) -> None:
        os.makedirs(directory, exist_ok=True)

    def _ensure_csv_ready(self, folder: str, file_path: str) -> None:
        os.makedirs(folder, exist_ok=True)
        if not os.path.exists(file_path):
            with open(file_path, mode="w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(self._csv_headers)

    async def _get_file_lock(self, file_path: str) -> asyncio.Lock:
        async with self._global_init_lock:
            lock = self._file_locks.get(file_path)
            if lock is None:
                lock = asyncio.Lock()
                self._file_locks[file_path] = lock
            return lock

    @staticmethod
    def _sanitize_component(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r"[^\w.-]", "_", text)
        return text[:100]
