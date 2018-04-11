import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

class RSSTestConfig:

    # CAUTION: switch implementation without default.
    test_2_gdk_mem_maxsize = {
        "cap_rss_64" : 20000000,
        "cap_rss_32" : 10000000
    }

    sql_template = \
"""
create function getrss()
returns bigint external name status.rss_cursize;

create table test(a int, b int, c double);

insert into test values (1, 0, 1);

create procedure loop_insert(maximum_size bigint)
begin
    declare size bigint;
    set size = (select count(*) from test);

    while size < maximum_size do
        insert into test (select a+1, b+2, rand()*c from test);

        set size = (select count(*) from test);
    end while;
end;

call loop_insert(1000000);

-- it seems that it requires an analytical query to keep memory in ram.
select getrss() < {0} as resident_set_size_is_less_then_{0}_kB, quantile(c/a, 0.8) * 0  from test;

drop table test cascade;
drop function getrss;
"""

    def __init__(self, test):
        self.rss_max_in_Bytes = RSSTestConfig.test_2_gdk_mem_maxsize[test]

    def prepare_server_options(self):
        return ["--set", "gdk_mem_maxsize={}".format(self.rss_max_in_Bytes)]

    def prepare_sql_script(self):
            return RSSTestConfig.sql_template.format(self.rss_max_in_Bytes / 1000)

def build_test_config():
        test = sys.argv[1]
        return RSSTestConfig(test)

def server_start(args):
    sys.stderr.write('#mserver: "%s"\n' % ' '.join(args))
    sys.stderr.flush()
    srv = process.server(args = args, stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return srv

def client(lang, query):
    clt = process.client(lang.lower(), stdin = process.PIPE,
                         stdout = process.PIPE, stderr = process.PIPE)
    return clt.communicate(input = query)

def main():

    test_config = build_test_config()

    options = test_config.prepare_server_options()

    srv = server_start(options)

    script = test_config.prepare_sql_script()

    out, err = client('SQL', script)
    sys.stdout.write(out)
    sys.stderr.write(err)
    out, err = srv.communicate()
    sys.stdout.write(out)
    sys.stderr.write(err)

main()
