
--create type json external name json;

create table minitable(j json);

insert into minitable values('
{"_id" : 1,
"name" : { "first" : "John", "last" : "Backus" },
"contribs" : [ "Fortran", "ALGOL", "Backus-Naur Form", "FP" ],
"awards" : [
           {
             "award" : "W.W. McDowell Award",
             "year" : 1967,
             "by" : "IEEE Computer Society"
           },
           { "award" : "Draper Prize",
             "year" : 1993,
             "by" : "National Academy of Engineering"
           }
]
}');

select * from minitable;

select json.filter(j,'_id') from minitable;
select json.filter(j,'name') from minitable;
select json.filter(j,'contribs') from minitable;
select json.filter(j,'awards') from minitable;

-- returns a table with two json columns
select json.filter(j,'name'), json_filter(j,'contribs') from minitable;

-- returns a table with a single json column

select json.keys(j) from minitable;
select json.names(j) from minitable;
select json.values(j) from minitable;

drop table minitable;
