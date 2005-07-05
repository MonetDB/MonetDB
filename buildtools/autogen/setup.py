from distutils.core import setup

setup(name = "autogen",
      version = "1.0",
      description = "MonetDB autogen script",
      author = "MonetDB Team, CWI",
      author_email = "monet@cwi.nl",
      url = "http://monetdb.cwi.nl/",
      maintainer = "Sjoerd Mullender",
      maintainer_email = "monet@cwi.nl",
      license = "MonetDB Public License",
      packages = ['autogen'],
      scripts = ['autogen.py'])
