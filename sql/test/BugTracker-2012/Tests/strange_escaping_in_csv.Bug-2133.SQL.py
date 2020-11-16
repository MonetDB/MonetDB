import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

queries = r'''
\f csv
select 'bla';
select '"'||'bla';
select '"bla"' '"blah"';
select 'blah''s nork';
select E'blah\'nork';
select E'blah\,blah';
select E'blah \tthe \n black';
select E'blah \t the \\n black';
select E'blah\\blah';
select E'\n';
select E'\t';
select E'\\n';
select E'\\t';
select E'\"blah\"';
select '"blah"';
'''

output = r'''bla
"""bla"
"""bla""""blah"""
blah's nork
blah'nork
"blah,blah"
"blah \tthe \n black"
"blah \t the \\n black"
"blah\\blah"
"\n"
"\t"
"\\n"
"\\t"
"""blah"""
"""blah"""
'''

with process.client('sql', stdout=process.PIPE, stderr=process.PIPE,
                    stdin=process.PIPE, interactive=True, echo=False) as c:
    c.stdin.write(queries)
    out, err = c.communicate()
    if out != output:
        print(out)
    if err:
        print(err, file=sys.stderr)
