SELECT MODEL2.is_mutagen,ATOM3.type, count(distinct MODEL2.model_id ) FROM MODEL MODEL2, ATOM ATOM3  WHERE MODEL2.model_id=ATOM3.model_id group by ATOM3.type , MODEL2.is_mutagen;
