==How to use mSEED data vault==

- libmseed library is needed.
	- Download it from http://www.iris.edu/pub/programs/libmseed-2.7.tar.gz
	- Install under LIBMSEED_LOCATION of your choice by referring to the file named 'INSTALL'. A simple 'make' on most Unix-like systems should build the library.
	- Create 'lib' and 'include' directories under LIBMSEED_LOCATION both pointing to the directory that contains 'libmseed.h'.

- MonetDB 'DVframework' branch is needed.
	- Refer to 'Building from Mercurial (HG) Sources' section of the wiki page http://www.monetdb.org/wiki/MonetDB:Building_from_sources
	- Check out the 'DVframework' branch by running "hg clone -u DVframework http://dev.monetdb.org/hg/MonetDB/"
	- Follow the steps on the wiki page and note that 'configure' should be run with this additional option: '--with-mseed=LIBMSEED_LOCATION'

- How to run an example
	- Go and Refer to .sql and .py files in the Tests/ directory under monetdb5/extras/dvf/.
	- You need a file containing a list of mSEED files with their absolute paths that you would like to have in the repository. Run "Mtest create_file_list" to get a small example of mSEED file repository.
	- Run "export LD_LIBRARY_PATH=$LIBMSEED_LOCATION/libmseed".
	- Run "mserver5" to start mserver with the default settings.
	- Run "mclient" in another terminal to get a client connection to the database server using 'demo' database.
	- In the mclient sql interface,
		- run 'mseed_schema.sql' script to create the normalized schema of mSEED.
		- run "CALL register_repo('TESTDIR/example_mseed_file_list.txt', 0);" where TESTDIR is the relative or absolute path of the aforementioned Tests/ directory. This will employ the registrar module to load (only) metadata of mSEED files.
	- Leave mclient and stop mserver5. Then run "mserver5 --readonly" to restart in readonly mode. The current incomplete version works only in readonly mode.
	- Run "mclient" again and in the mclient sql interface,
		- run 'initializer.sql' script to modify MonetDB optimizer pipeline in order to include DVframework optimizer in the pipeline. This is need to me done after every restart of mclient, unfortunately.
		- run example queries like "SELECT AVG(sample_value) FROM mseed.dataview WHERE network = 'FR';". You may also prefix the queries with 'EXPLAIN ' to see the query plan after the DVframework optimizer runs.
	- You may always refer to man pages of commands beginning with 'm' or 'M' for more information about them.
- E-mail to kargin@cwi.nl for questions, problems, etc.

