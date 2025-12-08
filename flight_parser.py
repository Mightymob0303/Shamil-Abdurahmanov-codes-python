#instructions to run this in my github repository, create a data folder using "mkdir data"
#create the csv file "nano data/db.csv"
#paste this sample flight_id,origin,destination,departure_datetime,arrival_datetime,price


# === Valid flights ===
#BA2490,LHR,JFK,2025-11-14 10:30,2025-11-14 13:05,489.99
#H172,FRA,RIX,2025-11-12 07:15,2025-11-12 10:30,159.50
#FR1234,RIX,OSL,2025-11-15 08:00,2025-11-15 08:55,99.99
#BT102,RIX,HEL,2025-11-14 09:40,2025-11-14 10:25,120.00
#AA9999,JFK,LHR,2025-11-15 20:15,2025-11-16 08:10,550.00
#DY4501,OSL,ARN,2025-12-01 06:00,2025-12-01 07:10,75.00
#AF112,CDG,DXB,2025-11-20 21:10,2025-11-21 05:45,620.00

# === Invalid flights (for testing validation) ===
#BADLINE,NO_DATE,NO_TIME
#BA_BAD,RIX,LON,2025-11-15 11:00,INVALID_DATE,250.00
#SK404,OSL,RIX,2025-11-15 14:00,2025-11-15 12:00,120.00
#W61025,XXX,RIX,2025-11-16 11:00,2025-11-16 13:00,80.00
#QR1,DOH,SYD,INVALID_DATETIME,2025-11-17 23:30,980.00
#KL1999,AMS,,2025-11-14 09:00,2025-11-14 11:15,180.00
#AY503,HEL,RIX,2025-11-15 13:20,2025-11-15 14:15,-10.00
#LH999999999,FRA,LAX,2025-11-13 09:30,2025-11-13 18:10,700.00
#SN2902,BRU,LHR,2025-13-40 10:00,2025-13-40 12:00,99.99


#then save using "CTRL + O, press Enter, CTRL + X"
# now run the code using "python flight_parser.py -i data/db.csv"
# run "python3 flight_parser.py -i data/db.csv -o flights_output.json"
#run "python3 flight_parser.py -d data


import argparse
import csv
import json
import os
from datetime import datetime

CSV_HEADER = [
    "flight_id",
    "origin",
    "destination",
    "departure_datetime",
    "arrival_datetime",
    "price",
]

DATETIME_FORMAT = "%Y-%m-%d %H:%M"

DISALLOWED_AIRPORT_CODES = {"XXX"}


def parse_args():
    """Set up and parse command-line arguments using argparse."""
    parser = argparse.ArgumentParser(
        description="Flight schedule parser"
    )

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "-i", "--input", metavar="CSV_FILE",
        help="Parse a single CSV file.",
    )
    source_group.add_argument(
        "-d", "--directory", metavar="CSV_DIR",
        help="Parse all .csv files in a folder (non-recursive).",
    )

    parser.add_argument(
        "-o", "--output", metavar="OUTPUT_JSON",
        help="Optional custom output path for valid flights JSON (default: db.json).",
    )

    return parser.parse_args()


def ensure_file_exists(path):
    """Check that a CSV file exists and is readable."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"ERROR: File not found: {path}")
    if not path.lower().endswith(".csv"):
        raise ValueError(f"ERROR: Not a CSV file: {path}")


def ensure_directory_exists(path):
    """Check that directory exists and contains at least one .csv file."""
    if not os.path.isdir(path):
        raise NotADirectoryError(f"ERROR: Directory not found: {path}")

    csv_files = [
        f for f in os.listdir(path)
        if f.lower().endswith(".csv")
    ]
    if not csv_files:
        raise FileNotFoundError(f"ERROR: Directory '{path}' contains no CSV files.")


def is_header_row(row):
    """Return True if the row is exactly the expected header."""
    return [cell.strip() for cell in row] == CSV_HEADER


def validate_flight_row(fields):
    """
    Validate one CSV data row.

    fields: list of strings

    Returns:
        (is_valid, record_dict_or_None, list_of_error_messages)
    """
    errors = []

    if len(fields) != 6:
        errors.append("missing required fields")
        return False, None, errors

    flight_id, origin, destination, dep_str, arr_str, price_str = [f.strip() for f in fields]

    # flight_id rules
    if len(flight_id) < 2 or not flight_id.isalnum():
        errors.append("invalid flight_id")
    if len(flight_id) > 8:
        errors.append("flight_id too long (more than 8 characters)")

    # origin
    if not origin:
        errors.append("missing origin field")
    elif len(origin) != 3 or not origin.isupper() or not origin.isalpha():
        errors.append("invalid origin code")
    elif origin in DISALLOWED_AIRPORT_CODES:
        errors.append("invalid origin code")

    # destination
    if not destination:
        errors.append("missing destination field")
    elif len(destination) != 3 or not destination.isupper() or not destination.isalpha():
        errors.append("invalid destination code")
    elif destination in DISALLOWED_AIRPORT_CODES:
        errors.append("invalid destination code")

    # datetime validation
    dep_dt = None
    arr_dt = None
    dep_ok = True
    arr_ok = True

    try:
        dep_dt = datetime.strptime(dep_str, DATETIME_FORMAT)
    except ValueError:
        dep_ok = False

    try:
        arr_dt = datetime.strptime(arr_str, DATETIME_FORMAT)
    except ValueError:
        arr_ok = False

    if not dep_ok and not arr_ok:
        errors.append("invalid date format")
    else:
        if not dep_ok:
            errors.append("invalid departure datetime")
        if not arr_ok:
            errors.append("invalid arrival datetime")

    if dep_dt and arr_dt and arr_dt <= dep_dt:
        errors.append("arrival before departure")

    # price validation
    try:
        price_val = float(price_str)
        if price_val <= 0:
            errors.append("negative price value")
    except ValueError:
        errors.append("invalid price value")
        price_val = 0.0

    # if errors → invalid record
    if errors:
        return False, None, errors

    return True, {
        "flight_id": flight_id,
        "origin": origin,
        "destination": destination,
        "departure_datetime": dep_str,
        "arrival_datetime": arr_str,
        "price": price_val,
    }, []


def parse_csv_file(path, single_file=True):
    """Parse a single CSV file and report valid + invalid lines."""
    valid_records = []
    error_lines = []
    seen_header = False

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw_line = line.rstrip("\n")

            if raw_line.strip() == "":
                continue

            stripped = raw_line.lstrip()

            if stripped.startswith("#"):
                msg = "comment line, ignored for data parsing"
                prefix = f"{'Line' if single_file else os.path.basename(path)+' Line'} {line_no}: "
                error_lines.append(f"{prefix}{raw_line} → {msg}")
                continue

            reader = csv.reader([raw_line])
            try:
                row = next(reader)
            except:
                prefix = f"{'Line' if single_file else os.path.basename(path)+' Line'} {line_no}: "
                error_lines.append(f"{prefix}{raw_line} → could not parse CSV line")
                continue

            if not seen_header and is_header_row(row):
                seen_header = True
                continue

            is_valid, record, errs = validate_flight_row(row)
            if is_valid:
                valid_records.append(record)
            else:
                prefix = f"{'Line' if single_file else os.path.basename(path)+' Line'} {line_no}: "
                error_lines.append(f"{prefix}{raw_line} → {', '.join(errs)}")

    return valid_records, error_lines


def parse_directory(path):
    """Parse all CSV files in a directory with robust error handling."""
    ensure_directory_exists(path)

    all_valid = []
    all_errors = []

    for filename in sorted(os.listdir(path)):
        if filename.lower().endswith(".csv"):
            file_path = os.path.join(path, filename)
            print(f"Processing file: {filename}")
            valid, errors = parse_csv_file(file_path, single_file=False)
            all_valid.extend(valid)
            all_errors.extend(errors)

    return all_valid, all_errors


def save_json(records, path="db.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


def save_errors(errors, path="errors.txt"):
    with open(path, "w", encoding="utf-8") as f:
        for e in errors:
            f.write(e + "\n")


def main():
    args = parse_args()

    try:
        if args.input:
            ensure_file_exists(args.input)
            valid, errors = parse_csv_file(args.input, single_file=True)
            source_desc = args.input
        else:
            ensure_directory_exists(args.directory)
            valid, errors = parse_directory(args.directory)
            source_desc = f"all CSVs in {args.directory}"
    except Exception as e:
        print(e)
        return

    output_json = args.output or "db.json"
    save_json(valid, output_json)
    save_errors(errors)

    print("\nSUMMARY")
    print(f"Parsed source: {source_desc}")
    print(f"Valid flights:   {len(valid)} (saved to {output_json})")
    print(f"Invalid/comment: {len(errors)} lines (saved to errors.txt)")


if __name__ == "__main__":
    main()
