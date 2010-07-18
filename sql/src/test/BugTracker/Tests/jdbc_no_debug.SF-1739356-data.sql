debug select count(*) from tables;
plan select count(*) from tables;
-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='inline,remap,evaluate,costModel,coercions,emptySet,aliases,mergetable,deadcode,commonTerms,joinPath,reorder,deadcode,reduce,history,multiplex,garbageCollector';
explain select count(*) from tables;
