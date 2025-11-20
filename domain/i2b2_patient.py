from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class I2b2Patient:
    patient_num: int                                 # primary key, NOT NULL
    vital_status_cd: Optional[str] = None
    birth_date: Optional[datetime] = None
    death_date: Optional[datetime] = None
    sex_cd: Optional[str] = None
    age_in_years_num: Optional[int] = None
    language_cd: Optional[str] = None
    race_cd: Optional[str] = None
    marital_status_cd: Optional[str] = None
    religion_cd: Optional[str] = None
    zip_cd: Optional[str] = None
    statecityzip_path: Optional[str] = None
    income_cd: Optional[str] = None
    patient_blob: Optional[str] = None              # SQL `text` → str
    update_date: Optional[datetime] = None
    download_date: Optional[datetime] = None
    import_date: Optional[datetime] = None
    sourcesystem_cd: Optional[str] = None
    upload_id: Optional[int] = None
