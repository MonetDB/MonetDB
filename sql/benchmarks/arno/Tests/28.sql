SELECT MODEL178.is_mutagen,MODEL178.lumo, count(distinct MODEL178.model_id ) FROM MODEL MODEL178  WHERE MODEL178.is_mutagen='F' group by MODEL178.lumo , MODEL178.is_mutagen;
