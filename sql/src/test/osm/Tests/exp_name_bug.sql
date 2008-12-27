CREATE TABLE relation_members_relation (relation integer, idx integer, to_relation integer, role varchar(255), primary key(relation, idx));

WITH a(relation, to_relation, indent) AS (SELECT relation, to_relation,
        0 FROM relation_members_relation) SELECT * FROM a;

DROP table relation_members_relation;
