"""
Flight Schedule Parser and Query Tool
-------------------------------------

Features:
- Parse one or more flight schedule CSV files.
- Validate and separate valid and invalid records.
- Export valid flights as db.json and invalid lines as errors.txt.
- Optionally load an existing JSON database instead of parsing CSVs.
- Execute queries from a JSON file and save results.

Command-line arguments:
  -i path/to/file.csv   Parse a single CSV file.
  -d path/to/folder/    Parse all .csv files in a folder and combine results.
  -o path/to/output.json  Optional custom output path for valid flights JSON.
  -j path/to/db.json    Load existing JSON database instead of parsing CSVs.
  -q path/to/query.json Execute queries on the loaded database.
  -h                    Show help message.

Query output file:
  response_<studentid>_<name>_<lastname>_<YYYYMMDD_HHMM>.json
"""

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

# Special invalid airport codes (for sample data like "XXX")
DISALLOWED_AIRPORT_CODES = {"XXX"}


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Flight schedule parser and query tool."
    )

    source_group = parser.add_mutually_exclusive_group(required=False)
    source_group.add_argument(
        "-i",
        "--input",
        metavar="CSV_FILE",
        help="Parse a single CSV file.",
    )
    source_group.add_argument(
        "-d",
        "--directory",
        metavar="CSV_DIR",
        help="Parse all .csv files in a folder (non-recursive).",
    )
    source_group.add_argument(
        "-j",
        "--jsondb",
        metavar="DB_JSON",
        help="Load existing JSON database instead of parsing CSVs.",
    )

    parser.add_argument(
        "-o",
        "--output",
        metavar="OUTPUT_JSON",
        help="Optional custom output path for valid flights JSON (default: db.json).",
    )
    parser.add_argument(
        "-q",
        "--query",
        metavar="QUERY_JSON",
        help="Execute queries defined in a JSON file on the loaded database.",
    )

    # For naming the response JSON file
    parser.add_argument(
        "--studentid",
        metavar="STUDENT_ID",
        help="Student ID for naming response JSON file.",
    )
    parser.add_argument(
        "--name",
        metavar="NAME",
        help="First name for naming response JSON file.",
    )
    parser.add_argument(
        "--lastname",
        metavar="LASTNAME",
        help="Last name for naming response JSON file.",
    )

    return parser.parse_args()


def is_header_row(row):
    """Return True if the row matches the expected CSV header."""
    return [cell.strip() for cell in row] == CSV_HEADER


def validate_flight_row(fields):
    """
    Validate a list of fields representing a flight row.

    Returns:
        is_valid (bool),
        record (dict or None),
        errors (list of error message strings)
    """
    errors = []

    # Wrong number of fields
    if len(fields) != 6:
        errors.append("missing required fields")
        return False, None, errors

    flight_id, origin, destination, dep_str, arr_str, price_str = [
        f.strip() for f in fields
    ]

    # flight_id: 2–8 alphanumeric characters
    if len(flight_id) < 2 or not flight_id.isalnum():
        errors.append("invalid flight_id")
    if len(flight_id) > 8:
        errors.append("flight_id too long (more than 8 characters)")

    # origin: 3 uppercase letters, not in disallowed set
    if not origin:
        errors.append("missing origin field")
    elif len(origin) != 3 or not origin.isupper() or not origin.isalpha():
        errors.append("invalid origin code")
    elif origin in DISALLOWED_AIRPORT_CODES:
        errors.append("invalid origin code")

    # destination: 3 uppercase letters, not in disallowed set
    if not destination:
        errors.append("missing destination field")
    elif len(destination) != 3 or not destination.isupper() or not destination.isalpha():
        errors.append("invalid destination code")
    elif destination in DISALLOWED_AIRPORT_CODES:
        errors.append("invalid destination code")

    # departure and arrival datetime
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

    # Specific messages to match examples
    if not dep_ok and not arr_ok:
        errors.append("invalid date format")
    else:
        if not dep_ok:
            errors.append("invalid departure datetime")
        if not arr_ok:
            errors.append("invalid arrival datetime")

    # arrival must be after departure
    if dep_dt and arr_dt:
        if arr_dt <= dep_dt:
            errors.append("arrival before departure")

    # price: positive float number
    try:
        price_val = float(price_str)
        if price_val <= 0:
            errors.append("negative price value")
    except ValueError:
        errors.append("invalid price value")
        price_val = 0.0

    if errors:
        return False, None, errors

    record = {
        "flight_id": flight_id,
        "origin": origin,
        "destination": destination,
        "departure_datetime": dep_str,
        "arrival_datetime": arr_str,
        "price": price_val,
    }
    return True, record, []


def parse_csv_file(path, single_file=True):
    """
    Parse a single CSV file.

    Returns:
        valid_records (list of dicts),
        error_lines (list of strings for errors.txt)
    """
    valid_records = []
    error_lines = []
    seen_header = False

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw_line = line.rstrip("\n")

            # Ignore completely blank lines
            if raw_line.strip() == "":
                continue

            stripped = raw_line.lstrip()

            # Comment line → record in errors.txt
            if stripped.startswith("#"):
                msg = "comment line, ignored for data parsing"
                if single_file:
                    prefix = f"Line {line_no}: "
                else:
                    prefix = f"{os.path.basename(path)} Line {line_no}: "
                error_lines.append(f"{prefix}{raw_line} \u2192 {msg}")
                continue

            # Use csv.reader for robust splitting
            reader = csv.reader([raw_line])
            try:
                row = next(reader)
            except Exception:
                if single_file:
                    prefix = f"Line {line_no}: "
                else:
                    prefix = f"{os.path.basename(path)} Line {line_no}: "
                error_lines.append(
                    f"{prefix}{raw_line} \u2192 could not parse CSV line"
                )
                continue

            # Skip header line (once per file)
            if not seen_header and is_header_row(row):
                seen_header = True
                continue

            is_valid, record, row_errors = validate_flight_row(row)
            if is_valid and record is not None:
                valid_records.append(record)
            else:
                if single_file:
                    prefix = f"Line {line_no}: "
                else:
                    prefix = f"{os.path.basename(path)} Line {line_no}: "
                error_lines.append(
                    f"{prefix}{raw_line} \u2192 {', '.join(row_errors)}"
                )

    return valid_records, error_lines


def parse_directory(dir_path):
    """
    Parse all .csv files in a directory (non-recursive) and combine results.
    """
    all_valid = []
    all_errors = []

    csv_files = [
        os.path.join(dir_path, name)
        for name in os.listdir(dir_path)
        if name.lower().endswith(".csv")
    ]

    for csv_path in sorted(csv_files):
        valid, errors = parse_csv_file(csv_path, single_file=False)
        all_valid.extend(valid)
        all_errors.extend(errors)

    return all_valid, all_errors


def load_json_database(path):
    """Load an existing JSON database (must be a list of flight objects)."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("JSON database must be a list of flight objects")
    return data


def save_json_database(records, path):
    """Save flight records as JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


def load_queries(path):
    """
    Load query JSON.

    Accepts:
      - single object → converted to [object]
      - list of objects → returned as-is
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("Query JSON must be an object or a list of objects")


def flight_matches_query(flight, query):
    """
    Check if a flight matches a query based on rules:

    - flight_id, origin, destination: exact match
    - departure_datetime: flight departure >= query departure
    - arrival_datetime: flight arrival <= query arrival
    - price: flight price <= query price
    """
    # Exact matches for strings
    for key in ("flight_id", "origin", "destination"):
        if key in query:
            if str(flight.get(key)) != str(query[key]):
                return False

    # departure_datetime filter
    if "departure_datetime" in query:
        try:
            q_dep = datetime.strptime(str(query["departure_datetime"]), DATETIME_FORMAT)
            f_dep = datetime.strptime(str(flight["departure_datetime"]), DATETIME_FORMAT)
        except Exception:
            return False
        if f_dep < q_dep:
            return False

    # arrival_datetime filter
    if "arrival_datetime" in query:
        try:
            q_arr = datetime.strptime(str(query["arrival_datetime"]), DATETIME_FORMAT)
            f_arr = datetime.strptime(str(flight["arrival_datetime"]), DATETIME_FORMAT)
        except Exception:
            return False
        if f_arr > q_arr:
            return False

    # price filter
    if "price" in query:
        try:
            q_price = float(query["price"])
            f_price = float(flight["price"])
        except Exception:
            return False
        if f_price > q_price:
            return False

    return True


def run_queries(flights, queries):
    """
    Run all queries over the given list of flights.

    Returns a list of results:
    [
      {
        "query": {...},
        "matches": [...],
      },
      ...
    ]
    """
    results = []
    for query in queries:
        matches = [f for f in flights if flight_matches_query(f, query)]
        results.append({"query": query, "matches": matches})
    return results


def make_response_filename(studentid, name, lastname):
    """Build response_<studentid>_<name>_<lastname>_<YYYYMMDD_HHMM>.json."""
    now_str = datetime.now().strftime("%Y%m%d_%H%M")

    sid = studentid or "studentid"
    nm = name or "name"
    ln = lastname or "lastname"

    def safe(s):
        return "".join(c for c in str(s) if c.isalnum() or c in ("-", "_"))

    sid = safe(sid)
    nm = safe(nm)
    ln = safe(ln)

    return f"response_{sid}_{nm}_{ln}_{now_str}.json"


def main():
    args = parse_args()

    flights = []
    errors = []

    # Decide how to get the database: from JSON (-j) or from CSV(s) (-i/-d)
    if args.jsondb:
        flights = load_json_database(args.jsondb)
    else:
        if not args.input and not args.directory:
            print("You must provide either -i, -d, or -j.")
            return

        if args.input:
            flights, errors = parse_csv_file(args.input, single_file=True)
        elif args.directory:
            flights, errors = parse_directory(args.directory)

        # Save errors.txt if there were invalid lines or comments
        if errors:
            with open("errors.txt", "w", encoding="utf-8") as ef:
                for line in errors:
                    ef.write(line + "\n")

        # Save JSON database from parsed flights
        output_path = args.output or "db.json"
        save_json_database(flights, output_path)

    # If queries are provided, run them on the loaded flight database
    if args.query:
        queries = load_queries(args.query)
        response = run_queries(flights, queries)
        response_filename = make_response_filename(
            args.studentid, args.name, args.lastname
        )
        with open(response_filename, "w", encoding="utf-8") as rf:
            json.dump(response, rf, indent=2)
        print(f"Query results saved to {response_filename}")
    else:
        # No queries: just basic summary
        if args.jsondb:
            print(f"Loaded {len(flights)} flights from {args.jsondb}")
        else:
            output_path = args.output or "db.json"
            print(f"Parsed {len(flights)} valid flights. Saved to {output_path}")
            if errors:
                print(f"{len(errors)} invalid lines written to errors.txt")


if __name__ == "__main__":
    main()
