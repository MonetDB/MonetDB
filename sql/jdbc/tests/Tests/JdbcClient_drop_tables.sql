DROP View   subject_stats;
DROP View predicate_stats;
DROP View object_stats;

DROP INDEX allnewtriples_subject_idx;
DROP INDEX allnewtriples_predicate_idx;
DROP INDEX allnewtriples_object_idx;

DROP TABLE allnewtriples CASCADE;

DROP TABLE "foreign" CASCADE;

DROP INDEX "triples_object_idx";
DROP INDEX "triples_predicate_idx";
DROP INDEX "triples_predicate_object_idx";
DROP INDEX "triples_subject_idx";
DROP INDEX "triples_subject_object_idx";
DROP INDEX "triples_subject_predicate_idx";

DROP TABLE "triples" CASCADE;

DROP TABLE mt;
DROP TABLE remt;
DROP TABLE replt;

DROP TABLE gtmpt;

