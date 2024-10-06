import os
import sys
import requests
import sqlite3


def delete_db_if_exists() -> None:
    if os.path.exists("nuclear_outages.db"):
        os.remove("nuclear_outages.db")


def get_data() -> dict:
    api_key = "api_key"

    url = f"https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/?api_key={api_key}&frequency=daily&data[0]=capacity&data[1]=outage&data[2]=percentOutage&start=2007-01-01&end=2024-10-03&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    response = requests.get(url)
    if response.status_code != 200:
        sys.exit(f"Failed to retrieve data. Status code: {response.status_code}")

    return response.json()


def init_schema() -> None:
    conn = None
    try:
        conn = sqlite3.connect("nuclear_outages.db")
        cursor = conn.cursor()

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS nuclear_outages (
                period TEXT, 
                capacity REAL,
                outage REAL,
                percent_outage REAL,
                capacity_units TEXT,
                outage_units TEXT,
                percent_outage_units TEXT
            )"""
        )

        conn.commit()
    except sqlite3.Error as e:
        sys.exit(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


def dump_data(data: dict) -> None:
    conn = None

    try:
        conn = sqlite3.connect("nuclear_outages.db")
        cursor = conn.cursor()

        if not data.get("response", {}):
            sys.exit("No data to dump")

        if not data.get("response", {}).get("data", {}):
            sys.exit("No data to dump")

        for item in data["response"]["data"]:
            period = item.get("period", "")
            capacity = float(item.get("capacity", 0))
            outage = float(item.get("outage", 0))
            percent_outage = float(item.get("percentOutage", 0))
            capacity_units = item.get("capacity-units", "")
            outage_units = item.get("outage-units", "")
            percent_outage_units = item.get("percentOutage-units", "")

            cursor.execute(
                "INSERT INTO nuclear_outages (period, capacity, outage, percent_outage, capacity_units, outage_units, percent_outage_units) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    period,
                    capacity,
                    outage,
                    percent_outage,
                    capacity_units,
                    outage_units,
                    percent_outage_units,
                ),
            )

        conn.commit()
    except sqlite3.Error as e:
        sys.exit(f"SQLite error: {e}")
    finally:
        if conn:
            conn.close()


def main():
    delete_db_if_exists()

    data = get_data()

    init_schema()

    dump_data(data)


if __name__ == "__main__":
    main()
