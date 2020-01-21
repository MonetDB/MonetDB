-- show behavior of optimizer plans
set optimizer='default_pipe'; -- overrule others

set optimizer='minimal_pipe';
select optimizer;

set optimizer=' optimizer.inline(); optimizer.remap(); optimizer.evaluate(); optimizer.costModel(); optimizer.coercions(); optimizer.emptySet(); optimizer.aliases(); optimizer.mergetable(); optimizer.deadcode(); optimizer.commonTerms(); optimizer.joinPath(); optimizer.reorder(); optimizer.deadcode(); optimizer.reduce(); optimizer.querylog(); optimizer.multiplex(); optimizer.garbageCollector();';
select optimizer;

-- and some errors
set optimizer='myfamous_pipe';
select optimizer;

select * from optimizers();
