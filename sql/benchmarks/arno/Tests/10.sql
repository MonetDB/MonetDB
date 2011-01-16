SELECT MODEL1.is_mutagen, count(distinct MODEL1.model_id ) FROM MODEL MODEL1, BOND T1008290375670  WHERE MODEL1.model_id=T1008290375670.model_id group by MODEL1.is_mutagen;
