import asyncio
import csv
import os
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_chatcsv",
    "ChatCSV",
    "将群聊消息保存为 CSV 文件。",
    "1.0.0",
    "https://github.com/Puiching-Memory/ChatCSV",
)
class ChatCSVLogger(Star):
    """仅监听群聊消息并将聊天记录持久化到 CSV 文件。"""

    def __init__(self, context: Context):
        super().__init__(context)
        self._csv_headers = [
            "timestamp_iso",
            "timestamp_unix",
            "platform",
            "message_type",
            "self_id",
            "session_id",
            "message_id",
            "group_id",
            "sender_id",
            "sender_name",
            "sender_repr",
            "message_text",
            "message_components",
            "raw_message",
        ]
        base_dir = Path(getattr(self.context, "data_dir", None) or os.getcwd())
        if base_dir.name != "data":
            base_dir /= "data"
        self._csv_base_dir = base_dir / "plugin_data" / "chatcsv" / "groups"
        self._global_init_lock = asyncio.Lock()
        self._file_locks: Dict[str, asyncio.Lock] = {}
        self._zip_lock = asyncio.Lock()
        self._zip_pending = False

    async def initialize(self) -> None:
        """保证 CSV 路径存在并写入表头。"""
        await asyncio.to_thread(self._csv_base_dir.mkdir, parents=True, exist_ok=True)

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def record_group_message(self, event: AstrMessageEvent):
        """捕获群聊消息并追加到 CSV。"""
        msg_obj: Any = getattr(event, "message_obj", None)
        group_id = getattr(msg_obj, "group_id", "") if msg_obj else ""
        if not group_id:
            return
        timestamp = getattr(msg_obj, "timestamp", None)
        timestamp_value = timestamp or datetime.now(timezone.utc).timestamp()
        timestamp_iso = self._to_iso(timestamp_value)
        session_id = getattr(msg_obj, "session_id", "")
        message_id = getattr(msg_obj, "message_id", "")
        sender = getattr(msg_obj, "sender", None)
        sender_id = getattr(sender, "user_id", "") or getattr(sender, "id", "")
        sender_name = event.get_sender_name()
        platform = event.get_platform_name()
        message_text = event.message_str or ""
        message_type = self._stringify(getattr(msg_obj, "type", ""))
        self_id = getattr(msg_obj, "self_id", "")
        sender_repr = self._stringify(sender)
        message_components = self._stringify(getattr(msg_obj, "message", ""))
        raw_message = self._stringify(getattr(msg_obj, "raw_message", ""))

        row = [
            timestamp_iso,
            str(timestamp_value),
            platform,
            message_type,
            self_id,
            session_id,
            message_id,
            group_id,
            sender_id,
            sender_name,
            sender_repr,
            message_text,
            message_components,
            raw_message,
        ]
        csv_path = await asyncio.to_thread(self._prepare_csv_path, group_id)
        await self._append_row(csv_path, row)

    async def _append_row(self, file_path: Path, row: list[str]) -> None:
        lock = await self._get_file_lock(str(file_path))
        async with lock:
            await asyncio.to_thread(self._write_row, file_path, row)
        await self._package_groups_zip()

    def _write_row(self, file_path: Path, row: list[str]) -> None:
        with file_path.open(mode="a", newline="", encoding="utf-8") as csv_file:
            csv.writer(csv_file).writerow(row)
        logger.info("已写入群聊 CSV: %s -> %s", file_path, row)

    @staticmethod
    def _to_iso(timestamp: Any) -> str:
        try:
            ts = float(timestamp)
        except (TypeError, ValueError):
            return datetime.now(timezone.utc).isoformat()
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    @staticmethod
    def _stringify(value: Any) -> str:
        if value is None:
            return ""
        try:
            return str(value)
        except Exception:
            return repr(value)

    def _prepare_csv_path(self, group_id: Any) -> Path:
        sanitized = self._sanitize_component(group_id) or "unknown_group"
        file_path = self._csv_base_dir / f"{sanitized}.csv"
        self._csv_base_dir.mkdir(parents=True, exist_ok=True)
        if not file_path.exists():
            with file_path.open(mode="w", newline="", encoding="utf-8-sig") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(self._csv_headers)
            logger.info("已创建群聊 CSV 文件: %s", file_path)
        return file_path

    async def _get_file_lock(self, file_path: str) -> asyncio.Lock:
        lock = self._file_locks.get(file_path)
        if lock is not None:
            return lock
        async with self._global_init_lock:
            return self._file_locks.setdefault(file_path, asyncio.Lock())

    async def _package_groups_zip(self) -> None:
        self._zip_pending = True
        if self._zip_lock.locked():
            return
        async with self._zip_lock:
            while self._zip_pending:
                self._zip_pending = False
                try:
                    await asyncio.to_thread(self._create_groups_zip)
                except Exception as exc:
                    logger.exception("打包群聊 CSV 失败: %s", exc)
                    break

    def _create_groups_zip(self) -> None:
        if not self._csv_base_dir.exists():
            return
        target_zip = self._csv_base_dir.parent / "groups.zip"
        temp_zip = target_zip.with_suffix(".tmp.zip")
        with zipfile.ZipFile(temp_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for root, _, files in os.walk(self._csv_base_dir):
                for file_name in files:
                    file_path = Path(root) / file_name
                    arcname = Path("groups") / file_path.relative_to(self._csv_base_dir)
                    archive.write(file_path, arcname.as_posix())
        temp_zip.replace(target_zip)
        logger.info("已更新群聊 ZIP: %s", target_zip)

    @staticmethod
    def _sanitize_component(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = re.sub(r"[^\w.-]", "_", text)
        return text[:100]

