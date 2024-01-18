from datetime import datetime as dt
import pymysql
import asyncio

import pytz
from dotenv import dotenv_values
from fastapi import HTTPException

from tqdm import tqdm

env = dotenv_values(".env")

# Source database connection parameters
source_host = env["SOURCE_HOST"]
source_user = env["SOURCE_USER"]
source_password = env["SOURCE_PASSWORD"]
source_db = env["SOURCE_DB"]

# Destination database connection parameters
destination_host = env["DESTINATION_HOST"]
destination_port = int(env["DESTINATION_PORT"])
destination_user = env["DESTINATION_USER"]
destination_password = env["DESTINATION_PASSWORD"]
destination_db = env["DESTINATION_DB"]


# ######### helper function #########
def get_connection():
    # Connect to the source database
    source_conn = pymysql.connect(host=source_host, user=source_user, password=source_password, db=source_db)
    source_cursor = source_conn.cursor()

    # try check connection source
    try:
        source_conn.ping()
        print("Connected to source database")
    except Exception as e:
        print(f"Error: {e}")

    # Connect to the destination database
    destination_conn = pymysql.connect(host=destination_host, port=destination_port, user=destination_user,
                                       password=destination_password,
                                       db=destination_db)
    destination_cursor = destination_conn.cursor()

    # try check connection destination
    try:
        destination_conn.ping()
        print("Connected to destination database")
    except Exception as e:
        print(f"Error: {e}")

    return source_conn, source_cursor, destination_conn, destination_cursor


def thai_datetime():
    # datetime thailand
    time = dt.now()
    tz = pytz.timezone('Asia/Bangkok')
    thai_time = time.astimezone(tz)
    datetime_now = thai_time.strftime('%Y-%m-%d %H:%M:%S')
    return datetime_now


# ######### main function #########
def transfer_table(source_tables, destination_tables):
    # Connect to databases
    global values_clause, row_values
    source_conn, source_cursor, destination_conn, destination_cursor = get_connection()

    try:
        datetime_now = thai_datetime()
        print(f"{datetime_now} - Starting transfer of table {source_tables} to {destination_tables}")

        # Fetch data from the source table
        source_cursor.execute(f"SELECT * FROM {source_tables}")
        rows = source_cursor.fetchall()

        # Start a transaction on the destination connection
        destination_conn.begin()

        # Calculate total rows for progress bar
        total_rows = len(rows)

        # Insert data into the destination table using a loop and batch inserts
        chunk_size = 1500  # Adjust the chunk size based on your preference

        with tqdm(total=total_rows, desc="Inserting Rows", unit="row") as pbar:
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                value_placeholders = ', '.join(['%s'] * len(chunk[0]))

                # Construct the VALUES clause for the chunk dynamically
                values_clause = ', '.join([f"({value_placeholders})" for _ in range(len(chunk))])

                # Construct the query
                columns = ', '.join([f"`{column[0]}`" for column in source_cursor.description])

                query = f"REPLACE INTO `{destination_tables}` ({columns}) VALUES {values_clause}"
                # print(query)

                try:
                    # Flatten the chunk list to pass as a single parameter to executemany
                    flat_chunk = [value for row in chunk for value in row]
                    destination_cursor.executemany(query, [flat_chunk])

                except Exception as e:
                    print(f"Error: {e}")

                finally:
                    destination_conn.rollback()

                pbar.update(len(chunk))

    # Commit the transaction on success or rollback on error
    finally:
        destination_conn.commit()
        print("\nData transfer complete!")
        source_conn.close()
        destination_conn.close()


async def run_transfer_table(source_tables: list, destination_tables: list):
    # count member and loop for transfer tables
    count_member = len(destination_tables)
    for i in range(count_member):
        transfer_table(source_tables[i], destination_tables[i])

    datetime_now = thai_datetime()
    print(f"{datetime_now} - All tables transferred successfully!")


def toggle_process(option) -> str | dict[str, str]:
    # async process
    if option == '1':
        source_tables = [
            "ddc_person_nkp",
            "ddc_epidem_report_nkp",
            "ddc_lab_report_nkp"
        ]

        destination_tables = [
            "ddc1_person",
            "ddc1_epidem_report",
            "ddc1_lab_report"
        ]
    else:
        # return code 400 in https response
        raise HTTPException(status_code=400, detail="Invalid option")

    try:
        asyncio.create_task(run_transfer_table(source_tables, destination_tables))

    except Exception as e:
        error = f"Error: {e}"
        print(error)
        return error

    return {"status": "success", "detail": "api was run in background"}


if __name__ == "__main__":
    asyncio.run(toggle_process())
    print("Main program continues executing...")
