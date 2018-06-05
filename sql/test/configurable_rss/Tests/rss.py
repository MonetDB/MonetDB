import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

class RSSTestConfig:
    CAPPED = True

    # CAUTION: switch implementation without default.
    test_2_gdk_mem_maxsize = {
        "cap_rss_64" : (25000000,CAPPED),
        "cap_rss_32" : (30000000,CAPPED),
        "no_cap_rss_64" : (25000000,not CAPPED),
        "no_cap_rss_32" : (30000000,not CAPPED)
    }

    sql_template = \
"""
create function getrss()
returns bigint external name status.rss_cursize;

create function printf(message string)
returns void external name io.printf;

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
select printf('#~BeginVariableOutput~#');
select getrss() as resident_set_size_in_kB, quantile(c/a, 0.8) * 0  from test;
select printf('#~EndVariableOutput~#');

select getrss() {compare_sign} {cap_in_kB} as resident_set_size_is_{compare_string}_then_{cap_in_kB}_kB, quantile(c/a, 0.8) * 0  from test;

drop table test cascade;
drop function getrss;
drop function printf;
"""

    def __init__(self, test):
        self.rss_max_in_Bytes, self.is_capped = RSSTestConfig.test_2_gdk_mem_maxsize[test]

    def prepare_server_options(self):
        return ["--set", "gdk_mem_maxsize={}".format(self.rss_max_in_Bytes)] if self.is_capped else []

    def get_template_parameters(self):

        compare_sign, compare_string = ("<", "less") if self.is_capped else (">", "bigger")

        return (self.rss_max_in_Bytes / 1000, compare_sign, compare_string)


    def prepare_sql_script(self):
            cap_in_kB, compare_sign, compare_string = self.get_template_parameters()

            return RSSTestConfig.sql_template.format(cap_in_kB=cap_in_kB, compare_sign=compare_sign, compare_string=compare_string)

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
