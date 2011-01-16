SELECT MODEL255.is_mutagen,MODEL255.lumo, count(distinct MODEL255.model_id ) FROM MODEL MODEL255  WHERE MODEL255.logp='5' group by MODEL255.lumo , MODEL255.is_mutagen;
