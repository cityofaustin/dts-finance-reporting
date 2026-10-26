import argparse
from datetime import datetime
import logging
import os
import re

from mstrio.project_objects.report import Report
from mstrio.connection import Connection
from sodapy import Socrata

from config import MICROSTRATEGY_DATASET_CONFIG
import utils

# MicroStrategy secrets
BASE_URL = os.getenv("BASE_URL")
MSTRO_USERNAME = os.getenv("MSTRO_USERNAME")
MSTRO_PASSWORD = os.getenv("MSTRO_PASSWORD")
PROJECT_ID = os.getenv("PROJECT_ID")

# Socrata Secrets
SOCRATA_ENDPOINT = os.getenv("SOCRATA_ENDPOINT")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
SOCRATA_KEY_ID = os.getenv("SOCRATA_KEY_ID")
SOCRATA_KEY_SECRET = os.getenv("SOCRATA_KEY_SECRET")


def connect_microstrategy():
    conn = Connection(
        base_url=BASE_URL,
        username=MSTRO_USERNAME,
        password=MSTRO_PASSWORD,
        project_id=PROJECT_ID,
        login_mode=1,
    )
    return conn


def get_report_data(conn, report_id):
    report = Report(conn, id=report_id)
    df = report.to_dataframe()
    return df.to_dict(orient="records")


def data_to_socrata(rows, dataset):
    soda = Socrata(
        SOCRATA_ENDPOINT,
        SOCRATA_APP_TOKEN,
        username=SOCRATA_KEY_ID,
        password=SOCRATA_KEY_SECRET,
        timeout=15,
    )
    response = soda.replace(dataset, rows)
    return response


def snake_case_keys(rows):
    """Lowercases all keys and replaces spaces/punctuation with underscores."""

    def clean(key):
        key = re.sub(r"[^0-9a-zA-Z]+", "_", str(key).strip())
        return key.strip("_").lower()

    # Build the mapping once, since every row has the same keys
    if not rows:
        return rows
    mapping = {k: clean(k) for k in rows[0]}
    if len(set(mapping.values())) != len(mapping):
        raise ValueError(f"Column names collide after cleaning: {mapping}")

    return [{mapping[k]: v for k, v in row.items()} for row in rows]


def socrata_datetime(rows, column, date_format="%m/%d/%Y"):
    for row in rows:
        value = row.get(column)
        d = datetime.strptime(value, date_format)
        row[column] = d.strftime("%Y-%m-%dT%H:%M:%S")
    return rows


def add_fiscal_quarter(
    rows, column, date_format="%Y-%m-%dT%H:%M:%S", fiscal_year_start_month=10
):
    """Adds <column>_fiscal_quarter (e.g. "Q4") and <column>_fiscal_year to each row.
    Default start month is 10 (Oct 1), matching the City of Austin fiscal year."""
    out_col = f"{column}_fiscal_quarter"
    fy_col = f"{column}_fiscal_year"

    for row in rows:
        value = row.get(column)
        if not value:
            raise ValueError("No date value provided")
        d = datetime.strptime(value, date_format)
        quarter = ((d.month - fiscal_year_start_month) % 12) // 3 + 1
        row[out_col] = f"Q{quarter}"
        # FY is named for the year it ends in
        row[fy_col] = d.year + 1 if d.month >= fiscal_year_start_month > 1 else d.year
    return rows


def apply_transforms(rows, steps):
    for step in steps:
        func = TRANSFORMS[step["name"]]
        rows = func(rows, **step.get("params", {}))
    return rows


def main(args):
    if args.report not in MICROSTRATEGY_DATASET_CONFIG:
        raise Exception(f"The report name {args.report} was not defined.")
    report = MICROSTRATEGY_DATASET_CONFIG[args.report]

    # Run report from microstrategy
    conn = connect_microstrategy()
    data = get_report_data(conn, report["report_id"])
    logger.info(f"{len(data)} rows received from MicroStrategy.")

    # Apply transforms
    data = apply_transforms(data, steps=report["transforms"])

    # Send data to socrata
    reply = data_to_socrata(data, report["dataset_id"])
    logger.info(reply)


if __name__ == "__main__":
    # CLI arguments definition
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--report",
        type=str,
        help="The name of the report to upload to socrata, as defined in config.py under MICROSTRATEGY_DATASET_CONFIG",
    )

    args = parser.parse_args()

    logger = utils.get_logger(
        __name__,
        level=logging.INFO,
    )

    TRANSFORMS = {
        "fiscal_quarter": add_fiscal_quarter,
        "snake_case_keys": snake_case_keys,
        "socrata_datetime": socrata_datetime,
    }

    main(args)
