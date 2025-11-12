import csv
import shutil
import re
import tempfile
from pathlib import Path


IMAGE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"Image\([^)]*\)", re.IGNORECASE | re.DOTALL),
    re.compile(r"\[CQ:image[^\]]*\]", re.IGNORECASE | re.DOTALL),
    re.compile(r"\{[^{}]*['\"]type['\"]\s*:\s*['\"]image['\"][^{}]*\}", re.IGNORECASE | re.DOTALL),
    re.compile(r"https?://[^\s\"\']+(?:\.jpg|\.jpeg|\.png|\.gif|\.webp)(?:\?[^\s\"\'\)]*)?", re.IGNORECASE),
)

COLUMNS_TO_DROP: tuple[str, ...] = ("timestamp_iso",
                                    "timestamp_unix",
                                    "platform",
                                    "message_type",
                                    "self_id",
                                    "session_id",
                                    "sender_repr",
                                    "message_components",
                                    "raw_message")


def normalize_cell(value: str) -> str:
    if not value:
        return value
    text = value
    for pattern in IMAGE_PATTERNS:
        text = pattern.sub("image", text)
    return text


def normalize_csv(input_path: Path | str, output_path: Path | str | None = None) -> None:
    input_path = Path(input_path)
    output_path = Path(output_path) if output_path is not None else input_path
    with input_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        rows = [row for row in reader]

    normalized_rows = [
        [normalize_cell(cell) for cell in row]
        for row in rows
    ]

    normalized_rows = drop_columns(normalized_rows, COLUMNS_TO_DROP)

    tmp_dir = input_path.parent
    with tempfile.NamedTemporaryFile("w", encoding="utf-8-sig", newline="", dir=tmp_dir, delete=False) as tmp_file:
        writer = csv.writer(tmp_file)
        writer.writerows(normalized_rows)
        tmp_name = Path(tmp_file.name)

    shutil.move(tmp_name, output_path)


def run(csv_path: Path | str, output_path: Path | str | None = None) -> None:
    """直接调用此函数即可完成替换。"""
    normalize_csv(csv_path, output_path)


def drop_columns(rows: list[list[str]], target_columns: tuple[str, ...]) -> list[list[str]]:
    if not rows:
        return rows
    header = rows[0]
    drop_indices = {idx for idx, name in enumerate(header) if name in target_columns}
    if not drop_indices:
        return rows

    kept_indices = [idx for idx in range(len(header)) if idx not in drop_indices]
    pruned_rows: list[list[str]] = []
    for row in rows:
        pruned_rows.append([row[idx] for idx in kept_indices if idx < len(row)])
    return pruned_rows

if __name__ == "__main__":
    run("./chat_history.csv","./chat_history_normalized.csv")