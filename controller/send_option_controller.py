def select_sql_by_option(choice: str, hoscode: str = None):
    query = ""
    if choice == '0':
        #     ส่งทั้งหมดที่ยังไม่ ok
        print("Your choice is 0: to send all data")
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
                "INNER JOIN	ddc_final_epidem_report AS e ON p.hoscode = e.hoscode AND p.vn = e.vn AND p.hn = e.hn " \
                "LEFT JOIN	ddc_final_lab_report AS l ON e.hoscode = l.hoscode AND e.vn = l.vn AND e.hn = l.hn " \
                "INNER JOIN user_moph on user_moph.hoscode = p.hoscode and user_moph.active = 1 AND left(user_moph.token,1) <> '{' " \
                "WHERE (p.message_from_ddc <> 'OK' OR p.message_from_ddc IS NULL) and p.hoscode in ('14550') " \
                " GROUP BY e.hoscode, e.vn, e.hn " \
                " limit 10"

    elif choice == '1':
        #     ส่งทั้งหมดที่ยังไม่ได้ส่ง ไม่ส่งซ้ำที่ error
        print("Your choice is 1: to send all data that not send yet")
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
                "INNER JOIN	ddc_final_epidem_report AS e ON p.hoscode = e.hoscode AND p.vn = e.vn AND p.hn = e.hn " \
                "LEFT JOIN	ddc_final_lab_report AS l ON e.hoscode = l.hoscode AND e.vn = l.vn AND e.hn = l.hn " \
                "INNER JOIN user_moph on user_moph.hoscode = p.hoscode and user_moph.active = 1 AND left(user_moph.token,1) <> '{' " \
                "WHERE (p.send_ddc_moph <> '1' OR p.message_from_ddc IS NULL) " \
                " GROUP BY e.hoscode, e.vn, e.hn "


    elif choice == '20':
        #     ส่งทั้งหมดที่ยังไม่ OK ระบุโรงพยาบาล
        print("Your choice is 20: to send all data that not send yet by hospital code")
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
                "INNER JOIN	ddc_final_epidem_report AS e ON p.hoscode = e.hoscode AND p.vn = e.vn AND p.hn = e.hn " \
                "LEFT JOIN	ddc_final_lab_report AS l ON e.hoscode = l.hoscode AND e.vn = l.vn AND e.hn = l.hn " \
                "INNER JOIN user_moph on user_moph.hoscode = p.hoscode and user_moph.active = 1 AND left(user_moph.token,1) <> '{' " \
                "WHERE (p.message_from_ddc <> 'OK' OR p.message_from_ddc IS NULL) " \
                "AND p.hoscode = %s " \
                " GROUP BY e.hoscode, e.vn, e.hn "
        query = query % hoscode

    return query
