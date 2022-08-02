CREATE TABLE tempEntries (
	url varchar(255),
	psize int
);

CREATE TABLE entries (
	id int,
	url varchar(255) not null unique,
	doc date,
	dom date,
	psize int,
	ranking int,
	primary key (id)
);

CREATE TABLE links (
	entryid int,
	url varchar(255) not null,
	primary key (entryid, url)
);

select locallinks.url, allentries.psize
from links locallinks inner join (
	select url, psize
		from entries union
			select url, psize
				from tempEntries
	) as allentries
	on locallinks.url=allentries.url;
