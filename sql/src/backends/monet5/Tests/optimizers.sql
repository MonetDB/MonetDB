-- This test is meant to exercise a few permutations and random
-- selections of the optimizers. The purpose is to detect major
-- errors due to dependencies.

select 'optimizer test:',optimizer;

set optimizer='off';
select 'optimizer test:',optimizer;

set optimizer='on';
select 'optimizer test:',optimizer;

set optimizer='';
select 'optimizer test:',optimizer;

set optimizer='costModel,coercions,emptySet,accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='coercions,emptySet,accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='emptySet,accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='accessmode,aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='aliases,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='joinPath,deadcode,reduce,garbageCollector';

set optimizer='deadcode,reduce,garbageCollector';

set optimizer='reduce,garbageCollector';

set optimizer='garbageCollector';

set optimizer='costModel,coercions,emptySet,accessmode,commonTerms,accumulators,joinPath,deadcode,reduce,garbageCollector';

set optimizer='costModel,coercions,emptySet,accessmode,commonTerms,accumulators,joinPath,deadcode,garbageCollector';

select 'done';
