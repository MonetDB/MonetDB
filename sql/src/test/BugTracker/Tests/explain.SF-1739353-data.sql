-- disable parallelism (mitosis & dataflow) to avoid ambiguous results 
set optimizer='inline,remap,evaluate,costModel,coercions,emptySet,aliases,mergetable,deadcode,commonTerms,joinPath,reorder,deadcode,reduce,history,multiplex,garbageCollector';
EXPLAIN SELECT "name" FROM "tables";
