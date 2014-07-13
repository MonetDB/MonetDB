SELECT MODEL1.is_mutagen,MODEL1.lumo, count(distinct MODEL1.model_id ) FROM MODEL MODEL1  group by MODEL1.lumo , MODEL1.is_mutagen;
