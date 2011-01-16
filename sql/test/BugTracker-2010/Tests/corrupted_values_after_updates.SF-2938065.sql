CREATE TABLE change (
    new_value VARCHAR(32),
    old_value VARCHAR(32),
    name VARCHAR(32),
    PRIMARY KEY (name,old_value)
);
CREATE TABLE base (
    value VARCHAR(32),
    name VARCHAR(32),
    state INTEGER DEFAULT 0,
    PRIMARY KEY (name)
);
CREATE TABLE edit (
    value VARCHAR(32),
    name VARCHAR(32),
    state INTEGER DEFAULT 0,
    PRIMARY KEY (name)
);
INSERT
    INTO base (name,value)
    SELECT 'token1','initial';
INSERT
    INTO edit (value,name,state)
    SELECT value,name,-1
        FROM base
        WHERE state=0;
DELETE
    FROM base
    WHERE EXISTS (
        SELECT true
            FROM edit
            WHERE base.name=edit.name
    )
;
INSERT
    INTO base (value,name,state)
    SELECT value,name,state
    FROM edit;
DELETE
    FROM edit;
INSERT
    INTO change (new_value,old_value,name)
    SELECT 'modified','initial','token1';
INSERT
    INTO edit (value,name,state)
    SELECT max(
            new_value
        ),base.name,0
        FROM base,change
        WHERE base.name=change.name
        AND base.value=old_value
        AND NOT new_value='modified'
        GROUP BY base.name,state;
INSERT
    INTO edit (value,name,state)
    SELECT max(
            new_value
        ),base.name,state
        FROM base,change
        WHERE base.name=change.name
        AND base.value=old_value
        AND new_value='modified'
        GROUP BY base.name,state;
DELETE
    FROM base
    WHERE EXISTS (
        SELECT true
            FROM edit
            WHERE base.name=edit.name
    )
;
INSERT
    INTO base (value,name,state)
    SELECT value,name,state
        FROM edit;
SELECT *
    FROM base;

DROP TABLE edit;
DROP TABLE base;
DROP TABLE change;
