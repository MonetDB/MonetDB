SELECT MODEL1.is_mutagen, count(distinct MODEL1.model_id ) FROM MODEL MODEL1, ATOM T1008290346560  WHERE MODEL1.model_id=T1008290346560.model_id group by MODEL1.is_mutagen;
