SELECT MODEL264.is_mutagen,MODEL264.logp, count(distinct MODEL264.model_id ) FROM MODEL MODEL264  WHERE MODEL264.logp='4' group by MODEL264.logp , MODEL264.is_mutagen;
