from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from data_pipeline.domain.i2b2_patient import I2b2Patient

@dataclass
class SndsPatient:
    pat_id: str                     # bpchar(17), primary key
    pat_birth_date: Optional[datetime] = None
    pat_dpt_res: Optional[int] = None          # int4
    pat_sex_cod: Optional[int] = None          # int2
    pat_hea_insur: Optional[str] = None        # bpchar(9)
    pat_loc_iris: Optional[str] = None         # bpchar(3)
    pat_death_date: Optional[datetime] = None

    def to_i2b2_patient(self, patient_num: int):
        return I2b2Patient(
            patient_num = patient_num,
            vital_status_cd = "dead" if self.pat_death_date else "alive",
            birth_date = self.pat_birth_date,
            sex_cd = "F" if self.pat_sex_cod == 2 else "M" 
        )