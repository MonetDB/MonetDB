-- perform an aggregate using the octopus
set optimizer='inline,remap,evaluate,costModel,coercions,emptySet,mitosis,aliases,mergetable,commonTerms,accumulators,octopus,deadcode,reduce,garbageCollector,dataflow,history,multiplex';
explain select count(*) from squidA;
select count(*) from squidA;
