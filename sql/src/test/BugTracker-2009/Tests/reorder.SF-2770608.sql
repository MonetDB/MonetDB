-- query causes problems in specific pipeline
set optimizer='inline,remap,evaluate,costModel,coercions,emptySet,mitosis,aliases,mergetable,deadcode,constants,commonTerms,joinPath,reorder,deadcode,reduce,garbageCollector,dataflow,history,multiplex';
select * from types t1, types t2 where t1.id = t2.id;
