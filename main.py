from data_pipeline.utils.logger_util import get_logger
from data_pipeline.jobs.load_patients import get_snds_patient, map_snds_patients_to_i2b2_patients
from data_pipeline.jobs.load_patients import load_i2b2_patients, load_patient_mappings

logger = get_logger(__name__)

logger.info("Starting the data pipeline...")

snds_patient_list = get_snds_patient()
(i2b2_patient_list, patient_mapping_list) = map_snds_patients_to_i2b2_patients(snds_patient_list)

load_i2b2_patients(i2b2_patient_list)

load_patient_mappings(patient_mapping_list)
