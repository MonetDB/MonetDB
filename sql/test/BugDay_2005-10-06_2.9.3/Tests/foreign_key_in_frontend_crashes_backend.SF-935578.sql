  create table studenten(
    sid integer,
    email varchar(40),
    name varchar(40),
    sno varchar(9),
    primary key (sid)
  );
  create table classes(
    cid integer,
    cdate date,
    tstart time,
    tend time,
    primary key (cid)
  );
  create table groups(
    cid integer,
    sid integer,
    foreign key (cid) references classes(cid),
    foreign key (sid) references studenten(sid)
  );

  copy 2 records into studenten 
       from stdin
       using delimiters ',','\n','"';
1,"k.zheng@ewi.tudelft.nl","Kang Zheng","1190857"
2,"yaleyoung109@hotmail.com","Yang Yang","1194887"
  copy 1 records into classes
       from stdin
       using delimiters ',','\n','''';
1,'2004-4-20','13:45','14:30'
  copy 2 records into groups
       from stdin
       using delimiters ',','\n';
1,1
1,2

