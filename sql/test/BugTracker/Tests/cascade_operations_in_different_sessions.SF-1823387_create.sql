CREATE TABLE parent (key int PRIMARY KEY);
CREATE TABLE child (parent_key int REFERENCES parent (key) ON DELETE
CASCADE);

INSERT INTO parent VALUES (1);
INSERT INTO child VALUES (1);
