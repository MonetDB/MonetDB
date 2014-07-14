SELECT MODEL232.is_mutagen,MODEL232.logp, count(distinct MODEL232.model_id ) FROM MODEL MODEL232  WHERE MODEL232.logp='8' group by MODEL232.logp , MODEL232.is_mutagen;
