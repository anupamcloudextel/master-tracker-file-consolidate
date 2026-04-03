"""
Read every .xlsx under Wireless/, take the "General Info" sheet (header on Excel row 5),
and write one merged workbook under output/.

If the output file already exists, it is removed first so each run produces a fresh file.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import traceback
from datetime import datetime

import pandas as pd


def read_general_info(
    path: Path,
    sheet_name: str,
    header_row_1_indexed: int,
) -> pd.DataFrame:
    header_row_0_indexed = header_row_1_indexed - 1
    return pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=header_row_0_indexed,
        engine="openpyxl",
    )


def append_log(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def iter_excel_files(folder: Path) -> list[Path]:
    if not folder.is_dir():
        raise NotADirectoryError(f"Not a directory: {folder}")
    files = sorted(folder.glob("*.xlsx"))
    # Skip Excel temporary lock files
    return [p for p in files if not p.name.startswith("~$")]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge 'General Info' from all Excel files in a folder into one workbook."
    )
    parser.add_argument(
        "--input-dir",
        default="Wireless",
        help="Folder containing input .xlsx files (default: Wireless).",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Folder for the consolidated workbook (default: output).",
    )
    parser.add_argument(
        "--output-file",
        default="consolidated_general_info.xlsx",
        help="Output filename (default: consolidated_general_info.xlsx).",
    )
    parser.add_argument(
        "--sheet",
        default="General Info",
        help="Sheet name to read (default: General Info).",
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=5,
        help="1-indexed Excel row used as column headers (default: 5).",
    )
    parser.add_argument(
        "--no-source-column",
        action="store_true",
        help="Do not add a Source_File column with the originating workbook name.",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / args.output_file
    log_path = output_dir / "consolidate_wireless.log.txt"
    # Start fresh every run.
    if log_path.exists():
        try:
            log_path.unlink()
        except PermissionError:
            # If it can't be deleted (rare), we'll just append.
            pass

    append_log(
        log_path,
        f"[{datetime.now().isoformat(timespec='seconds')}] Start run. "
        f"input_dir={input_dir.resolve()} sheet={args.sheet!r} header_row={args.header_row} "
        f"output_path={output_path.resolve()}",
    )

    paths = iter_excel_files(input_dir)
    if not paths:
        raise SystemExit(f"No .xlsx files found in {input_dir.resolve()}")

    frames: list[pd.DataFrame] = []
    skipped: list[tuple[str, str]] = []

    for path in paths:
        try:
            df = read_general_info(path, args.sheet, args.header_row)
        except ValueError as e:
            append_log(
                log_path,
                f"[ERROR] {path.name}: ValueError while reading sheet {args.sheet!r} "
                f"(header_row={args.header_row}). Message={e}",
            )
            append_log(log_path, traceback.format_exc().rstrip())
            skipped.append((path.name, str(e)))
            continue
        except Exception as e:
            append_log(
                log_path,
                f"[ERROR] {path.name}: Exception while reading sheet {args.sheet!r} "
                f"(header_row={args.header_row}). Type={type(e).__name__} Message={e}",
            )
            append_log(log_path, traceback.format_exc().rstrip())
            skipped.append((path.name, str(e)))
            continue

        if not args.no_source_column:
            df = df.copy()
            df.insert(0, "Source_File", path.name)
        frames.append(df)

    if skipped:
        print("Skipped files:")
        for name, reason in skipped:
            print(f"  - {name}: {reason}")
            append_log(log_path, f"[SKIPPED] {name}: {reason}")

    if not frames:
        raise SystemExit("No sheets could be read; nothing to write.")

    merged = pd.concat(frames, ignore_index=True, sort=False)

    if output_path.is_file():
        try:
            output_path.unlink()
        except PermissionError:
            raise SystemExit(
                f"Cannot replace output file (close it in Excel if open): {output_path}"
            ) from None

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        merged.to_excel(writer, sheet_name="General Info", index=False)

    print(
        f"Wrote {len(merged)} rows from {len(frames)} file(s) -> {output_path.resolve()}"
    )
    append_log(
        log_path,
        f"[OK] Wrote {len(merged)} rows from {len(frames)} file(s) -> {output_path.resolve()}",
    )


if __name__ == "__main__":
    main()
