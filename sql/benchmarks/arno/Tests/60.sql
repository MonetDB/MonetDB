SELECT MODEL247.is_mutagen,MODEL247.lumo, count(distinct MODEL247.model_id ) FROM MODEL MODEL247  WHERE MODEL247.logp='6' group by MODEL247.lumo , MODEL247.is_mutagen;
