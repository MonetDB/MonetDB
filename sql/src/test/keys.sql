CREATE TABLE allnewtriples (id integer NOT NULL, subject integer NOT
NULL, predicate integer NOT NULL, "object" integer NOT NULL, explicit
boolean NOT NULL, CONSTRAINT unique_key UNIQUE(subject, predicate, "object"));
CREATE INDEX allnewtriples_subject_idx ON allnewtriples (subject);
CREATE INDEX allnewtriples_predicate_idx ON allnewtriples (predicate);
CREATE INDEX allnewtriples_object_idx ON allnewtriples ("object");

SELECT idxs.name, idxs."type", keys.name, keys."type" 
FROM idxs LEFT JOIN keys on idxs.name = keys.name;
SELECT idxs.name, idxs."type", keys.name, keys."type" 
FROM idxs JOIN keys on idxs.name = keys.name;
