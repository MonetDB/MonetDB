-- This test is meant to exercise a few permutations and random
-- selections of the optimizers. The purpose is to detect major
-- errors due to dependencies.

select 'optimizer test:',optimizer;

set optimizer='off';
select 'optimizer off test:',optimizer;

set optimizer='on';
select 'optimizer on test:',optimizer;

set optimizer='';
select 'optimizer <empty> test:',optimizer;

set optimizer='optimizer.costModel();optimizer.coercions();optimizer.aliases();optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.coercions();optimizer.aliases();optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.aliases();optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.aliases();optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.aliases();optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.garbageCollector();';

set optimizer='optimizer.costModel();optimizer.coercions();optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.reduce();optimizer.garbageCollector();';

set optimizer='optimizer.costModel();optimizer.coercions();optimizer.commonTerms();optimizer.joinPath();optimizer.deadcode();optimizer.garbageCollector();';

set optimizer='optimizer.inline();';

set optimizer='optimizer.inline();optimizer.multiplex();';

set optimizer='optimizer.inline();optimizer.multiplex();optimizer.deadcode();';

set optimizer='optimizer.inline();optimizer.deadcode();optimizer.multiplex();';

set optimizer='optimizer.deadcode();optimizer.inline();optimizer.multiplex();optimizer.garbageCollector();';

set optimizer='optimizer.inline();optimizer.deadcode();optimizer.garbageCollector();optimizer.multiplex();';

set optimizer='optimizer.inline();optimizer.multiplex();optimizer.deadcode();optimizer.garbageCollector();';

set optimizer='optimizer.inline();optimizer.deadcode();optimizer.multiplex();optimizer.garbageCollector();';

select 'done';
