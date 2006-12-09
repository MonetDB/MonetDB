-- This test is meant to exercise a few permutations and random
-- selections of the optimizers. The purpose is to detect major
-- errors due to dependencies.

select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='off';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='costModel,coercions,emptySet,modes,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='coercions,emptySet,modes,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='emptySet,modes,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='modes,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='costModel,coercions,emptySet,modes,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

set optimizer='costModel,coercions,emptySet,modes,commonTerms,accumulators,joinPath,deadcode,garbageCollector,';
select 'optimizer test:',optimizer;
select count(*) from tables;

select 'done';
