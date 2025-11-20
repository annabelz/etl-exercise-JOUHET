from data_pipeline.utils.db_util import get_connection
from data_pipeline.utils.logger_util import get_logger
from data_pipeline.domain.snds_patient import SndsPatient
from data_pipeline.domain.i2b2_patient import I2b2Patient
from data_pipeline.domain.patient_mapping import PatientMapping
from typing import List

logger = get_logger(__name__)

def get_snds_patient():
    conn = get_connection()
    sql_statement = """
        SELECT pat_id,
                pat_birth_date,
                pat_dpt_res,
                pat_sex_cod,
                pat_hea_insur,
                pat_loc_iris,
                pat_death_date
        FROM synthetic_snds.tab_patient
        """

    with conn.cursor() as cur: 
        logger.info(f"Sending the query {sql_statement} to the database")
        cur.execute(sql_statement)
        rows = cur.fetchall()
        snds_patient_list: List[SndsPatient] = []

        for row in rows:
            logger.info(row)
            snds_patient = SndsPatient(
                pat_id = row[0],
                pat_birth_date = row[1],
                pat_dpt_res = row[2],
                pat_sex_cod = row[3],
                pat_hea_insur = row[4],
                pat_loc_iris = row[5],
                pat_death_date = row[6]
                )
            snds_patient_list.append(snds_patient)
        logger.info(f"Number of patients (fetched rows) is: {len(snds_patient_list)}")
        logger.info(f"The first patient is {snds_patient_list[0].pat_id}")
    conn.close()
    return snds_patient_list

def map_snds_patients_to_i2b2_patients(snds_patient_list):
    logger.info("Starting transformation part")
    generated_id = 0
    i2b2_patient_list : list[I2b2Patient] = []
    patient_mapping_list : list[PatientMapping] = []

    for snds_patient in snds_patient_list:
        generated_id += 1
        i2b2_patient : I2b2Patient = snds_patient.to_i2b2_patient(generated_id)
        patient_mapping = PatientMapping(
            patient_ide = snds_patient.pat_id,
            patient_num = i2b2_patient.patient_num,
            patient_ide_source = "synthetic_snds",
            project_id = "phds"
        )

        i2b2_patient_list.append(i2b2_patient)
        patient_mapping_list.append(patient_mapping)

        # logger.info(f"SNDS_Patient id: {snds_patient.pat_id}")
        # logger.info(f"I2B2_Patient num: {i2b2_patient.patient_num}")

        logger.info(f"Number of i2b2 patients is: {len(i2b2_patient_list)}")
        logger.info(f"Number of patient mappings is: {len(patient_mapping_list)}")

    return (i2b2_patient_list, patient_mapping_list)

def load_i2b2_patients(i2b2_patient_list: List[I2b2Patient]):

    conn_target = get_connection(
        port=5436,
        dbname="postgres",
        user="postgres",
        password="phds"
    )

    truncate_statement = """
        truncate table patient_dimension
        """
    with conn_target.cursor() as cur:
        cur.execute(truncate_statement)
        conn_target.commit()

    sql_insert_statement = """
        INSERT INTO public.patient_dimension (
            patient_num,
            vital_status_cd,
            birth_date,
            death_date,
            sex_cd,
            age_in_years_num,
            language_cd,
            race_cd,
            marital_status_cd,
            religion_cd,
            zip_cd,
            statecityzip_path,
            income_cd,
            patient_blob,
            update_date,
            download_date,
            import_date,
            sourcesystem_cd,
            upload_id
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )

        """
    with conn_target.cursor() as cur:
        # Sending query to the database with the values to put in each variable
        for i2b2_patient in i2b2_patient_list:
            cur.execute(
                sql_insert_statement, 
                (
                    i2b2_patient.patient_num, 
                    i2b2_patient.vital_status_cd, 
                    i2b2_patient.birth_date, 
                    i2b2_patient.death_date,
                    i2b2_patient.sex_cd, 
                    i2b2_patient.age_in_years_num, 
                    i2b2_patient.language_cd, 
                    i2b2_patient.race_cd,
                    i2b2_patient.marital_status_cd, 
                    i2b2_patient.religion_cd, 
                    i2b2_patient.zip_cd, 
                    i2b2_patient.statecityzip_path,
                    i2b2_patient.income_cd, 
                    i2b2_patient.patient_blob, 
                    i2b2_patient.update_date, 
                    i2b2_patient.download_date,
                    i2b2_patient.import_date, 
                    i2b2_patient.sourcesystem_cd, 
                    i2b2_patient.upload_id
                )
            )
    conn_target.commit()
    conn_target.close()
    
def load_patient_mappings(patient_mapping_list: List[PatientMapping]):

    conn_target = get_connection(
        port=5436,
        dbname="postgres",
        user="postgres",
        password="phds"
    )

    truncate_statement = """
        truncate table patient_mapping
        """
    with conn_target.cursor() as cur:
        cur.execute(truncate_statement)
        conn_target.commit()

    sql_insert_statement = """
        INSERT INTO public.patient_mapping (
            patient_ide,
            patient_ide_source,
            patient_num,
            project_id
        )
        VALUES (
        %s, %s, %s, %s
        )

        """
    with conn_target.cursor() as cur:
        # Sending query to the database with the values to put in each variable
        for patient_mapping in patient_mapping_list:
            cur.execute(
                sql_insert_statement, 
                (
                    patient_mapping.patient_ide,
                    patient_mapping.patient_ide_source,
                    patient_mapping.patient_num,
                    patient_mapping.project_id,
        
                )
            )
    conn_target.commit()
    conn_target.close()