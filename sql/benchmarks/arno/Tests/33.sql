SELECT MODEL192.is_mutagen,MODEL192.is_mutagen, count(distinct MODEL192.model_id ) FROM MODEL MODEL192  WHERE MODEL192.lumo='-3' group by MODEL192.is_mutagen , MODEL192.is_mutagen;
