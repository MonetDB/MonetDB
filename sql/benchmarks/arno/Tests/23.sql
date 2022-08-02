SELECT MODEL162.is_mutagen,MODEL162.lumo, count(distinct MODEL162.model_id ) FROM MODEL MODEL162  WHERE MODEL162.is_mutagen='T' group by MODEL162.lumo , MODEL162.is_mutagen;
