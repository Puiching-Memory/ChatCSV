## Chat CSV Logger

将所有 AstrBot 会话消息实时记录到 CSV 文件，方便进行数据分析或备份。

### 功能特性

- 自动监听所有 `AstrMessageEvent` 消息
- 首次运行自动创建 CSV 并写入表头
- 记录时间戳、平台、会话、消息 ID、群组 ID、发送者 ID/昵称及消息文本
- 群聊按 `group_id` 分目录存储，私聊按会话/发送者拆分
- 使用异步锁防止并发写入导致的数据损坏

### 部署与使用

1. 将插件放置在 AstrBot 的 `data/plugins/astrbot_plugin_chatcsv` 目录下。
2. 启动 AstrBot 并在 WebUI 插件管理中启用本插件。
3. CSV 文件默认保存在 AstrBot 数据目录下的 `chatcsv/` 目录：
   - 群聊：`chatcsv/groups/<group_id>/chat_history.csv`
   - 私聊：`chatcsv/privates/<session_or_sender>/chat_history.csv`

### CSV 字段说明

| 字段名          | 说明                         |
| --------------- | ---------------------------- |
| `timestamp_iso` | UTC ISO8601 格式的消息时间   |
| `platform`      | 消息来源平台标识             |
| `session_id`    | 会话 ID（若适用）            |
| `message_id`    | 消息 ID                      |
| `group_id`      | 群组 ID（私聊则为空）        |
| `sender_id`     | 发送者 ID（若适用）          |
| `sender_name`   | 发送者名称                   |
| `message_text`  | 文本消息内容                 |

### 许可证

本项目遵循 [MIT License](LICENSE)。
