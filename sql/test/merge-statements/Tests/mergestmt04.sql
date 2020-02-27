CREATE TABLE food_store (id INT, "name" TEXT, new_item BOOL);
INSERT INTO food_store VALUES (1, 'bread', false), (2, 'coffee', false), (3, 'tea', false), (4, 'butter', false), (5, 'chocolate', false);

CREATE TABLE releases (id INT, reason CLOB);
INSERT INTO releases VALUES (5, 'too warm to eat chocolate'), (6, 'end of chestnuts season');

CREATE TABLE incoming (id INT, "name" CLOB);
INSERT INTO incoming VALUES (6, 'ice cream');

MERGE INTO food_store AS to_update USING releases AS food_updates 
  ON to_update.id = food_updates.id 
  WHEN MATCHED THEN DELETE;

MERGE INTO food_store USING (SELECT id, "name" FROM incoming) AS new_food 
  ON food_store.id = new_food.id 
  WHEN NOT MATCHED THEN INSERT VALUES (new_food.id, new_food."name", true);

SELECT id, "name", new_item FROM food_store;

TRUNCATE incoming;
INSERT INTO incoming VALUES (1, 'cookies'), (2, 'cake'), (7, 'peanuts');

MERGE INTO food_store USING (SELECT id, "name" FROM incoming) AS food_updates 
  ON food_store.id = food_updates.id 
  WHEN NOT MATCHED THEN INSERT VALUES (food_updates.id, food_updates."name", true) 
  WHEN MATCHED THEN UPDATE SET "name" = food_updates."name", new_item = true; 

SELECT id, "name", new_item FROM food_store;

TRUNCATE releases;
INSERT INTO releases VALUES (1, 'chocolate cookies'), (1, 'gluten cookies');

MERGE INTO food_store USING (SELECT id, reason FROM releases) AS food_updates 
  ON food_store.id = food_updates.id 
  WHEN MATCHED THEN UPDATE SET new_item = true; --error, more than one row match

SELECT id, "name", new_item FROM food_store;

DROP TABLE food_store;
DROP TABLE releases;
DROP TABLE incoming;
