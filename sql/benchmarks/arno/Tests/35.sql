SELECT MODEL192.is_mutagen,MODEL192.logp, count(distinct MODEL192.model_id ) FROM MODEL MODEL192  WHERE MODEL192.lumo='-3' group by MODEL192.logp , MODEL192.is_mutagen;
