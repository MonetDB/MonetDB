SELECT MODEL1.is_mutagen,MODEL1.logp, count(distinct MODEL1.model_id ) FROM MODEL MODEL1  group by MODEL1.logp , MODEL1.is_mutagen;
