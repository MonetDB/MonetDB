select sys.getUser('https://me:pw@www.monetdb.org/Doc');
	-- me
select sys.getUser('http://mk@www.cwi.nl/vision2011.pdf');
	-- mk
select sys.getUser('http://www.cwi.nl/~mk/vision2011.pdf');
	-- NULL
