SELECT MODEL306.is_mutagen,MODEL306.is_mutagen, count(distinct MODEL306.model_id ) FROM MODEL MODEL306  WHERE MODEL306.logp='0' group by MODEL306.is_mutagen , MODEL306.is_mutagen;
