import requests
from fastapi import FastAPI, Depends, HTTPException, Query
import pymysql
from pymysql import MySQLError
from dotenv import dotenv_values
import asyncio

from controller.transfer_nkp_controller import toggle_process
from controller.send_ddc_controller import call_api_send_async

config_env = dotenv_values(".env")

app = FastAPI()

# Database Configuration
DB_CONFIG = {
    'host': config_env['DB_HOST'],
    'port': int(config_env['DB_PORT']),
    'user': config_env['DB_USERNAME'],
    'password': config_env['DB_PASSWORD'],
    'db': config_env['DB_DATABASE'],
}


def get_db():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        yield connection
    finally:
        connection.close()


@app.post("/gen_token")
async def create_token(db: pymysql.connections.Connection = Depends(get_db)):
    sql = "SELECT * FROM user_moph"
    try:
        with db.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()

            for row in result:
                user = row['user']
                password_hash = row['pass_hash']
                hoscode = row['hoscode']

                url = f"{config_env['URL_MOPH_GEN_TOKEN']}&user={user}&password_hash={password_hash}&hospital_code={hoscode}"
                payload = {}
                headers = {}

                response = requests.request("POST", url, headers=headers, data=payload)

                print(response.text)

                #             loop for insert token to database
                sql = "UPDATE user_moph SET token = %s, created_at = now() WHERE hoscode = %s"
                with db.cursor() as cursor:
                    cursor.execute(sql, (response.text, hoscode))
                    db.commit()
                    print(cursor.rowcount, "record inserted.")

                row_count = cursor.rowcount
                print(f"insert {row_count} record")

    except MySQLError as e:
        print(e)
        raise HTTPException(status_code=500, detail="Database error")

    return {"status": "success", "detail": f"insert {row_count} record"}


@app.post("/send_ddc/{option}")
async def send_ddc(db: pymysql.connections.Connection = Depends(get_db), option: str = "0", hoscode: str = Query(None)):
    try:
        asyncio.create_task(call_api_send_async(db, option, hoscode))
    except requests.RequestException as e:
        error = f"Error: {e}"
        print(error)
        return error

    return {"status": "success", "detail": "api was run in background"}


if __name__ == "__main__":
    asyncio.run(send_ddc())
    print("Main program continues executing...")


@app.post("/sent_to_cmpho/{option}")
async def sent_to_cmpho(option: str):
    return toggle_process(option)
