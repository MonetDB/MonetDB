start transaction;

create table unicodetest (
  U&"&015D" UESCAPE '&' varchar(4)   -- LATIN SMALL LETTER S WITH CIRCUMFLEX
);
-- three times the same string in three different ways
insert into unicodetest (U&"\015D") values (U&'&+01F525&+01F692&+01F6F1&+01F9EF' UESCAPE '&');
insert into unicodetest (U&"&015D" UESCAPE '&') values (U&'&+01F525' '&+01F692' '&+01F6F1' '&+01F9EF' UESCAPE '&');
insert into unicodetest (U&"\015D" UESCAPE R'\') values (U&'\+01F525\+01F692\+01F6F1\+01F9EF');
insert into unicodetest (U&"\015D" UESCAPE E'\\') values (U&'\23ba\23bb\23bc\23bd');
insert into unicodetest values (U&'\23ba' '\23bb' '\23bc' '\23bd' UESCAPE r'\');
insert into unicodetest values (U&'%23ba%23bb%23bc%23bd' UESCAPE '%');

select * from unicodetest;

rollback;
