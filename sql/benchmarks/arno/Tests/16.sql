SELECT MODEL110.is_mutagen,MODEL110.lumo, count(distinct MODEL110.model_id ) FROM MODEL MODEL110, BOND BOND111  WHERE MODEL110.model_id=BOND111.model_id group by MODEL110.lumo , MODEL110.is_mutagen;
