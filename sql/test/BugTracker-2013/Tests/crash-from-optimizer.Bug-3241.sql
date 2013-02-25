START TRANSACTION;

CREATE TABLE "_dict" (
        "idstr" CHARACTER LARGE OBJECT,
        "id"    INTEGER       NOT NULL,
        CONSTRAINT "_dict_id_pkey" PRIMARY KEY ("id")
);
COPY 7 RECORDS INTO "_dict" FROM stdin USING DELIMITERS ' ','\n','"';
"entity.department-dummy:11be0f0ab68e55dbbb205fa871914d89" 0
"entity.person:e1c055925f3783a7631a2efcde7d4413" 1
"entity.section:3673f53d1a8e1e5981b8bb6726923e68" 2
"entity.department-dummy:d57100c1f843d4d0f5a611ba7ac711f3" 3
"entity.department-dummy:2895b2eae68f91e3240e109e1df3296f" 4
"entity.department-dummy:c3f049405e2d621ff271bb92b4f921b9" 5
"entity.department-dummy:ce5f9e283849737fcebebd0c973ed0ac" 6

create function pcre_index(pat string, s string) returns int external name pcre."patindex";

create function gettype(str string) returns string
begin
  return substring(str, pcre_index('.', str) + 1,pcre_index(':', str) - 1 - pcre_index('.', str));
end; 

create view dict as select *, gettype(idstr) as type, 1.0e0 as prob from _dict;


SELECT * FROM dict WHERE type='company';

ROLLBACK;
