SELECT MODEL237.is_mutagen,MODEL237.logp, count(distinct MODEL237.model_id ) FROM MODEL MODEL237  WHERE MODEL237.logp='7' group by MODEL237.logp , MODEL237.is_mutagen;
