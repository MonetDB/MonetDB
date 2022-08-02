SELECT MODEL110.is_mutagen,BOND111.type, count(distinct MODEL110.model_id ) FROM MODEL MODEL110, BOND BOND111  WHERE MODEL110.model_id=BOND111.model_id group by BOND111.type , MODEL110.is_mutagen;
