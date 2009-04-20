from distutils.core import setup

setup(name='monetdb.sql',
      version='1.0',
      description='Native MonetDB driver',
      author='Gijs Molenaar',
      author_email='gijs.molenaar@cw.nl',
      url='http://www.monetdb.nl',
      packages=['monetdb', 'monetdb.sql'],
     )

