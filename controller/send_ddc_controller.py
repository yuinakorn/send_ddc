import json
from datetime import datetime

import pymysql
import pytz
import requests
from dotenv import dotenv_values
from fastapi import HTTPException
from pymysql import MySQLError

from controller.send_option_controller import select_sql_by_option

# from send_option_controller import select_sql_by_option

config_env = dotenv_values("../.env")


def replace_none_with_empty_string(data):
    if isinstance(data, list):
        return [replace_none_with_empty_string(item) for item in data]
    elif isinstance(data, dict):
        return {key: replace_none_with_empty_string(value) if value is not None else "" for key, value in data.items()}
    else:
        return data if data is not None else ""


async def call_api_send_async(db, option):

    time = datetime.now()
    tz = pytz.timezone('Asia/Bangkok')
    thai_time = time.astimezone(tz)
    print("Start at: ", thai_time.strftime('%Y-%m-%d %H:%M:%S'))

    url = config_env['URL_SEND_DDC']

    # ไปเอาคิวรี่ที่ option
    query = select_sql_by_option(option)

    # "AND e.diagnosis_icd10 in ('U071','U072') " \
    # send_ddc_moph = '0'

    try:
        with db.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            rows = 0
            # datetime now
            print("start time = " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            for row in results:
                # Assuming 'birth_date' is the column containing datetime objects
                # row['birth_date'] = row['birth_date'].strftime('%Y-%m-%d')
                # row['birth_date'] = row['birth_date'].strftime('%Y-%m-%d') if row['birth_date'] is not None else None

                json_data = {
                    "hospital": {
                        "hospital_code": row['hoscode'],
                        "hospital_name": row['hosname'],
                        "his_identifier": "ihims | epidem-cm"
                    },
                    "person": {
                        "cid": row['cid'],
                        "passport_no": row['passport_no'],
                        "prefix": row['prefix'],
                        "first_name": row['first_name'],
                        "last_name": row['last_name'],
                        "nationality": row['nationality'],
                        "gender": row['gender'],
                        "birth_date": row['birth_date'],
                        "age_y": row['age_y'],
                        "age_m": row['age_m'],
                        "age_d": row['age_d'],
                        "marital_status_id": row['marital_status_id'],
                        "address": row['address'],
                        "moo": row['moo'],
                        "road": row['road'],
                        "chw_code": row['chw_code'],
                        "amp_code": row['amp_code'],
                        "tmb_code": row['tmb_code'],
                        "mobile_phone": row['mobile_phone'],
                        "occupation": row['occupation'],
                    },
                    "epidem_report": {
                        "epidem_report_guid": row['epidem_report_guid'],
                        "epidem_report_group_code": row['epidem_report_group_id'],
                        "treated_hospital_code": row['treated_hospital_code'],
                        "report_datetime": row['report_datetime'],
                        "onset_date": row['onset_date'],
                        "treated_date": row['treated_date'],
                        "diagnosis_date": row['diagnosis_date'],
                        "death_date": row["death_date"],
                        "organism": row["organism"],
                        "complication": row["complication"],
                        "cdeath": row["Cdeath"],
                        "informer_name": row["informer_name"],
                        "principal_diagnosis_icd10": row["diagnosis_icd10"],
                        "diagnosis_icd10_list": row["diagnosis_icd10_list"],
                        "epidem_person_status_id": row["epidem_person_status_id"],
                        "epidem_symptom_type_id": row["epidem_symptom_type_id"],
                        "municipal": row["municipal"],
                        "respirator_status": row["respirator_status"],
                        "vaccinated_status": row["vaccinated_status"],
                        "epidem_address": row["epidem_address"],
                        "epidem_moo": row["epidem_moo"],
                        "epidem_road": row["epidem_road"],
                        "epidem_chw_code": row["epidem_chw_code"],
                        "epidem_amp_code": row["epidem_amp_code"],
                        "epidem_tmb_code": row["epidem_tmb_code"],
                        "location_gis_latitude": row["location_gis_latitude"],
                        "location_gis_longitude": row["location_gis_longitude"],
                        "isolate_chw_code": row["isolate_chw_code"],
                        "patient_type": row["patient_type"],
                        "active_case_finding": row["active_case_finding"],
                        "epidem_cluster_type_id": row["epidem_cluster_type_id"],
                        "cluster_latitude": row["cluster_latitude"],
                        "cluster_longitude": row["cluster_longitude"],
                        "comment": row["comment"]
                    },
                    "lab_report": {
                        "epidem_lab_confirm_type_id": row["epidem_lab_confirm_type_id"],
                        "specimen_date": row["specimen_date"],
                        "specimen_place_id": row["specimen_place_id"],
                        # short if
                        "lab_report_date": row["lab_report_date"],
                        "lab_report_result": row["lab_report_result"],
                        "lab_his_ref_code": row["lab_his_ref_code"],
                        "lab_his_ref_name": row["lab_his_ref_name"],
                        "tmlt_code": row["tmlt_code"]
                    },
                }

                headers = {
                    'Content-type': 'Application/json',
                    'Authorization': 'Bearer ' + row['token']
                }

                modified_json_data = replace_none_with_empty_string(json_data)
                # print("this is payload = " + json.dumps(modified_json_data))
                response = requests.request("POST", url, headers=headers, data=json.dumps(modified_json_data))
                # print("this is payload = " + json.dumps(modified_json_data))

                # all_json_data.append(modified_json_data)
                print(response.text)

                json_response = json.loads(response.text)
                message = json_response['Message']
                message_code = json_response['MessageCode']

                sql = "UPDATE ddc_final_person d " \
                      "SET send_ddc_moph = '1', message_from_ddc = %s, message_code = %s, d_update = %s " \
                      "WHERE d.hoscode = %s AND d.vn = %s"

                cursor.execute(sql, (
                    message, message_code, thai_time.strftime('%Y-%m-%d %H:%M:%S'), row['hoscode'], row['vn']))
                try:
                    db.commit()
                    rows += cursor.rowcount
                    print("this is rowcount =", rows)
                    print(rows, "record updated.")
                except MySQLError as e:
                    print("Database Error: ", e)
                    db.rollback()  # Rollback the transaction in case of an error

        print("End at: ", thai_time.strftime('%Y-%m-%d %H:%M:%S'))

    except MySQLError as e:
        print(e)
        # raise HTTPException(status_code=500, detail="Database error")
