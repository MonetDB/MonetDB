from MonetDBtesting.sqltest import SQLTestCase
import os

with SQLTestCase() as tc:
    tc.connect()
    with open('foreign_key_outer_join_dead_code_elimination-plan-3join-query.sql') as f:
        suffix = '.32bit' if os.getenv('TST_BITS', '') == '32bit' else ''
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='foreign_key_outer_join_dead_code_elimination-plan-3join-query.stable.out%s' % (suffix))
