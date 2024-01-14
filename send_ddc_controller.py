import json
from datetime import datetime

import pymysql
import pytz
import requests
from dotenv import dotenv_values
from fastapi import HTTPException
from pymysql import MySQLError

config_env = dotenv_values(".env")


def replace_none_with_empty_string(data):
    if isinstance(data, list):
        return [replace_none_with_empty_string(item) for item in data]
    elif isinstance(data, dict):
        return {key: replace_none_with_empty_string(value) if value is not None else "" for key, value in data.items()}
    else:
        return data if data is not None else ""


async def call_api_send_async(db):
    time = datetime.now()
    tz = pytz.timezone('Asia/Bangkok')
    thai_time = time.astimezone(tz)
    print("Start at: ", thai_time.strftime('%Y-%m-%d %H:%M:%S'))

    url = config_env['URL_SEND_DDC']
    query = "SELECT " \
            "p.hoscode, " \
            "p.hosname, " \
            "p.vn, " \
            "p.cid, " \
            "p.passport_no, " \
            "p.prefix, " \
            "p.first_name, " \
            "p.last_name, " \
            "p.nationality, " \
            "p.gender, " \
            "p.birth_date, " \
            "p.age_y, " \
            "p.age_m, " \
            "p.age_d, " \
            "p.marital_status_id, " \
            "p.address, " \
            "p.moo, " \
            "p.road, " \
            "p.chw_code, " \
            "p.amp_code, " \
            "p.tmb_code, " \
            "p.mobile_phone, " \
            "p.occupation, " \
            "e.epidem_report_guid, " \
            "e.epidem_report_group_id, " \
            "p.hoscode as treated_hospital_code, " \
            "e.report_datetime, " \
            "e.onset_date, " \
            "e.treated_date, " \
            "e.diagnosis_date, " \
            "e.death_date, " \
            "e.Cdeath, " \
            "e.informer_name, " \
            "e.diagnosis_icd10, " \
            "e.diagnosis_icd10_list, " \
            "e.organism, " \
            "e.complication, " \
            "e.epidem_person_status_id, " \
            "e.epidem_symptom_type_id, " \
            "e.respirator_status, " \
            "e.vaccinated_status, " \
            "'3' as municipal,  " \
            "e.epidem_address, " \
            "e.epidem_moo, " \
            "e.epidem_road, " \
            "e.epidem_chw_code, " \
            "e.epidem_amp_code, " \
            "e.epidem_tmb_code, " \
            "e.location_gis_latitude, " \
            "e.location_gis_longitude, " \
            "'50' as isolate_chw_code, " \
            "e.patient_type, " \
            "e.active_case_finding, " \
            "e.epidem_cluster_type_id, " \
            "e.cluster_latitude, " \
            "e.cluster_longitude, " \
            "e.`comment`, " \
            "l.epidem_lab_confirm_type_id, " \
            "l.lab_report_date, " \
            "l.lab_report_result, " \
            "l.specimen_date, " \
            "l.specimen_place_id, " \
            "l.lab_his_ref_code, " \
            "l.lab_his_ref_name, " \
            "l.tmlt_code ," \
            "p.message_from_ddc, " \
            "p.send_ddc_moph, " \
            "user_moph.token " \
            " FROM	ddc_final_person AS p " \
            "INNER JOIN	ddc_final_epidem_report AS e	ON p.hoscode = e.hoscode AND p.vn = e.vn AND p.hn = e.hn " \
            "LEFT JOIN	ddc_final_lab_report AS l ON e.hoscode = l.hoscode AND e.vn = l.vn AND e.hn = l.hn " \
            "INNER JOIN user_moph on user_moph.hoscode = p.hoscode and user_moph.active = 1 " \
            "WHERE (p.message_from_ddc <> 'OK' OR p.message_from_ddc IS NULL) " \
            " GROUP BY e.hoscode, e.vn, e.hn "

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
                row['birth_date'] = row['birth_date'].strftime('%Y-%m-%d')

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

                sql = "UPDATE ddc_final_person d " \
                      "SET send_ddc_moph = '1' , message_from_ddc = %s, d_update = %s " \
                      "WHERE d.hoscode = %s AND d.vn = %s"
                #         not in with
                cursor.execute(sql, (message, thai_time.strftime('%Y-%m-%d %H:%M:%S'), row['hoscode'], row['vn']))
                try:
                    if db.commit():
                        rows += cursor.rowcount
                        print("this is rowcount = " + str(rows))
                        print(rows, "record inserted.")
                    else:
                        print("error commit insert")
                except MySQLError as e:
                    print(e)

        print("End at: ", thai_time.strftime('%Y-%m-%d %H:%M:%S'))

    except MySQLError as e:
        print(e)
        # raise HTTPException(status_code=500, detail="Database error")
