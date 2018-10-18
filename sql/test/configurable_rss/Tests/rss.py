import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

class RSSTestConfig:

    sql_template = \
"""
create function test_rss(bound bigint)
returns bigint external name test_config_rss.run_test_config_rss;

select test_rss({gdk_mem_maxsize});

drop function test_rss;
"""

    def __init__(self, test):
        self.rss_max_in_Bytes = 30000000

    def prepare_server_options(self):
        return ["--set", "gdk_mem_maxsize={}".format(self.rss_max_in_Bytes)]

    def prepare_sql_script(self):
        return RSSTestConfig.sql_template.format(gdk_mem_maxsize=self.rss_max_in_Bytes)

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
