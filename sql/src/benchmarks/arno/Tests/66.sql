SELECT MODEL255.is_mutagen,MODEL255.logp, count(distinct MODEL255.model_id ) FROM MODEL MODEL255  WHERE MODEL255.logp='5' group by MODEL255.logp , MODEL255.is_mutagen;
