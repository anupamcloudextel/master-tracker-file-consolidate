import argparse
from pathlib import Path

import pandas as pd


def extract_general_info(
    input_path: Path,
    output_path: Path,
    sheet_name: str,
    header_row_1_indexed: int,
    output_sheet_name: str,
) -> None:
    """
    Reads `sheet_name` from `input_path` using `header_row_1_indexed` as the header row
    (1-indexed Excel row), then writes the resulting table to `output_path`.

    If that row is blank or only merged placeholders, pandas cannot read column names and
    will label columns ``Unnamed: 0``, ``Unnamed: 1``, … — use the row that actually
    contains the real headers (for these trackers, that is usually row 5).
    """

    if not input_path.exists():
        raise FileNotFoundError(f"Input Excel not found: {input_path}")

    # pandas uses 0-indexed header row number, so subtract 1.
    header_row_0_indexed = header_row_1_indexed - 1

    df = pd.read_excel(
        input_path,
        sheet_name=sheet_name,
        header=header_row_0_indexed,
        engine="openpyxl",
    )

    # Write to row 1 in Excel (startrow=0) so the header becomes the first row.
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(
            writer,
            sheet_name=output_sheet_name,
            index=False,
            header=True,
            startrow=0,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract 'General Info' sheet using the header row and write master.xlsx"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input Excel file.",
    )
    parser.add_argument(
        "--sheet",
        default="General Info",
        help="Sheet name to read from the input workbook.",
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=5,
        help="1-indexed Excel row to use as column headers (default: 5 for standard trackers).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Excel path. If omitted, uses --output-dir/master.xlsx",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory to write the output file into (default: ./output).",
    )
    parser.add_argument(
        "--output-sheet",
        default="master",
        help="Output sheet name (default: master).",
    )

    args = parser.parse_args()

    input_path = Path(args.input)

    if args.output:
        output_path = Path(args.output)
    else:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "master.xlsx"

    extract_general_info(
        input_path=input_path,
        output_path=output_path,
        sheet_name=args.sheet,
        header_row_1_indexed=args.header_row,
        output_sheet_name=args.output_sheet,
    )

    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()

