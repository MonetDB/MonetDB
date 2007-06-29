-- This test is meant to exercise a few permutations and random
-- selections of the optimizers. The purpose is to detect major
-- errors due to dependencies.

select 'optimizer test:',optimizer;

set optimizer='off';
select 'optimizer test:',optimizer;

set optimizer='';
select 'optimizer test:',optimizer;

set optimizer='costModel,coercions,emptySet,accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='coercions,emptySet,accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='emptySet,accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='costModel,coercions,emptySet,accessmode,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;

set optimizer='costModel,coercions,emptySet,accessmode,commonTerms,accumulators,joinPath,deadcode,garbageCollector,';
select 'optimizer test:',optimizer;

select 'done';
