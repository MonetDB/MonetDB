SELECT MODEL297.is_mutagen,MODEL297.logp, count(distinct MODEL297.model_id ) FROM MODEL MODEL297  WHERE MODEL297.logp='1' group by MODEL297.logp , MODEL297.is_mutagen;
