SELECT MODEL2.is_mutagen,ATOM3.charge, count(distinct MODEL2.model_id ) FROM MODEL MODEL2, ATOM ATOM3  WHERE MODEL2.model_id=ATOM3.model_id group by ATOM3.charge , MODEL2.is_mutagen;
