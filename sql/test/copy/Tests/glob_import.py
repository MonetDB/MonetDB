import os
import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

server = process.server(stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
client = process.client('sql', server=server, stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE)
cout, cerr = client.communicate("""
start transaction;
create table "ratings" (movie_id BIGINT, customer_id BIGINT, rating TINYINT, rating_date DATE);
copy into "ratings" from '%s' using delimiters ',','\n';
copy into "ratings" from '%s' using delimiters ',','\n';
copy into "ratings" from '%s' using delimiters ',','\n';
copy into "ratings" from '%s' using delimiters ',','\n';
copy into "ratings" from '%s' using delimiters ',','\n'; --error
rollback;
""" % (os.getenv("TSTDATAPATH")+"/netflix_data/ratings_sample_0.csv", os.getenv("TSTDATAPATH")+"/netflix_data/ratings_sample_1.csv", os.getenv("TSTDATAPATH")+"/netflix_data/ratings_sample_?.csv", os.getenv("TSTDATAPATH")+"/netflix_data/ratings_sample_*", os.getenv("TSTDATAPATH")+"/netflix_data/idontexist*"))

sout, serr = server.communicate()
sys.stdout.write(sout)
sys.stderr.write(serr)

sys.stdout.write(cout)
sys.stderr.write(cerr)
