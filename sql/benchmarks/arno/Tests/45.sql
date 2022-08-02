SELECT MODEL217.is_mutagen,MODEL217.logp, count(distinct MODEL217.model_id ) FROM MODEL MODEL217  WHERE MODEL217.lumo='-1' group by MODEL217.logp , MODEL217.is_mutagen;
