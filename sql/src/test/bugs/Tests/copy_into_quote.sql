
CREATE TABLE test_copyinto (
--
  HouseholdID                      INT,
  IndividualID                     INT,
  Address1                         VARCHAR(60),
  Address2                         VARCHAR(60),
  City                             VARCHAR(30),
  State                            VARCHAR(2),
  ZIPCode                          VARCHAR(5),
  ZIP4                             VARCHAR(4),
  FirstName                        VARCHAR(30),
  MiddleName                       VARCHAR(30),
  LastName                         VARCHAR(30),
  Gender                           VARCHAR(1),
  HeadOfHouseholdFlag              VARCHAR(1)
--
) ;

copy 1 records INTO test_copyinto FROM stdin USING DELIMITERS '|','\n', '';
2413949|8117533|1234 ANOTHER DISK||ABCDE|TT|23456|7860|" S||LAST|-|N

copy 1 records INTO test_copyinto FROM stdin USING DELIMITERS '|','\n', '';
2413949|8117533|1234 ANOTHER DISK||ABCDE|TT|23456|7860|' S||LAST|-|N

select * from test_copyinto;
drop table test_copyinto;
