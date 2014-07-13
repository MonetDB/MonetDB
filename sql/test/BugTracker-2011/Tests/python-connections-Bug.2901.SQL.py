import monetdb.sql
import os

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'))
c.close()

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'),
                           user = 'monetdb')
c.close()

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'),
                           user = 'monetdb',
                           username = 'bogus_user_name')
c.close()

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'),
                           username = 'monetdb')
c.close()

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'))
c.close()

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'),
                           host = os.getenv('HOST'))
c.close()

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'),
                           host = os.getenv('HOST'),
                           hostname = 'bogus_host_name')
c.close()

c = monetdb.sql.Connection(port = int(os.getenv('MAPIPORT')),
                           database = os.getenv('TSTDB'),
                           hostname = os.getenv('HOST'))
c.close()
