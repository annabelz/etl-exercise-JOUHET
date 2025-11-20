from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class PatientMapping:
    patient_ide: str                     # NOT NULL, part of composite PK
    patient_ide_source: str              # NOT NULL, part of composite PK
    patient_num: int                     # NOT NULL
    project_id: str = ""                 # NOT NULL, part of composite PK
    
    patient_ide_status: Optional[str] = None
    upload_date: Optional[datetime] = None
    update_date: Optional[datetime] = None
    download_date: Optional[datetime] = None
    import_date: Optional[datetime] = None
    sourcesystem_cd: Optional[str] = None
    upload_id: Optional[int] = None
