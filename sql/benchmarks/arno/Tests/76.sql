SELECT MODEL275.is_mutagen,MODEL275.logp, count(distinct MODEL275.model_id ) FROM MODEL MODEL275  WHERE MODEL275.logp='3' group by MODEL275.logp , MODEL275.is_mutagen;
