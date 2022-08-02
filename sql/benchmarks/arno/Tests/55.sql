SELECT MODEL237.is_mutagen,MODEL237.lumo, count(distinct MODEL237.model_id ) FROM MODEL MODEL237  WHERE MODEL237.logp='7' group by MODEL237.lumo , MODEL237.is_mutagen;
