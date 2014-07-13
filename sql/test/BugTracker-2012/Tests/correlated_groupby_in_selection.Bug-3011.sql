
CREATE TABLE "sys"."test" (
        "version"            DECIMAL(2)    NOT NULL,
        "implicit"           BOOLEAN       NOT NULL,
        "dataownercode"      VARCHAR(10)   NOT NULL,
        "lineplanningnumber" VARCHAR(10)   NOT NULL,
        "journeypatterncode" VARCHAR(10)   NOT NULL,
        "timinglinkorder"    DECIMAL(3)    NOT NULL,
        "userstopcodebegin"  VARCHAR(10)   NOT NULL,
        "userstopcodeend"    VARCHAR(10)   NOT NULL,
        "confinrelcode"      VARCHAR(10)   NOT NULL,
        "destcode"           VARCHAR(10)   NOT NULL,
        "istimingstop"       BOOLEAN       NOT NULL,
        "displaypublicline"  VARCHAR(4),
        "productformulatype" VARCHAR(4)
);


select dataownercode, lineplanningnumber, journeypatterncode, timinglinkorder,
userstopcodebegin, istimingstop,
       (select count(*) from test as counter where
            counter.dataownercode = test.dataownercode and
            counter.lineplanningnumber = test.lineplanningnumber and
            counter.journeypatterncode = test.journeypatterncode and
            counter.timinglinkorder < test.timinglinkorder and
            counter.userstopcodebegin = test.userstopcodebegin
            group by dataownercode, lineplanningnumber, journeypatterncode) as
passagesequencenumber
from test order by dataownercode, lineplanningnumber, journeypatterncode,
timinglinkorder limit 20;

select dataownercode, lineplanningnumber, journeypatterncode, timinglinkorder,
userstopcodebegin, istimingstop,
       (select count(*) from test as counter where
            counter.dataownercode = test.dataownercode and
            counter.lineplanningnumber = test.lineplanningnumber and
            counter.journeypatterncode = test.journeypatterncode and
            counter.timinglinkorder < test.timinglinkorder and
            counter.userstopcodebegin = test.userstopcodebegin) as
passagesequencenumber 
from test order by dataownercode, lineplanningnumber, journeypatterncode,
timinglinkorder limit 20;


COPY 10 RECORDS INTO "test" FROM STDIN USING DELIMITERS ',', '\n';
1,true,CXX,A001,0,0,40000010,40004015,santro,A00100998,true,,34
1,true,CXX,A001,0,1,40004015,40004021,santro,A00100998,false,,34
1,true,CXX,A001,0,2,40004021,40002570,santro,A00100998,false,,34
1,true,CXX,A001,0,3,40002570,40002550,santro,A00100998,false,,34
1,true,CXX,A001,0,4,40002550,40002590,santro,A00100998,false,,34
1,true,CXX,A001,0,5,40002590,40002610,santro,A00100998,false,,34
1,true,CXX,A001,0,6,40002610,40002630,santro,A00100998,false,,34
1,true,CXX,A001,0,7,40002630,40002690,santro,A00100998,false,,34
1,true,CXX,A001,0,8,40002690,40002770,santro,A00100998,false,,34
1,true,CXX,A001,0,9,40002770,40009591,santro,A00100998,false,,34


select dataownercode, lineplanningnumber, journeypatterncode, timinglinkorder,
userstopcodebegin, istimingstop,
       (select count(*) from test as counter where
            counter.dataownercode = test.dataownercode and
            counter.lineplanningnumber = test.lineplanningnumber and
            counter.journeypatterncode = test.journeypatterncode and
            counter.timinglinkorder < test.timinglinkorder and
            counter.userstopcodebegin = test.userstopcodebegin
            group by dataownercode, lineplanningnumber, journeypatterncode) as
passagesequencenumber
from test order by dataownercode, lineplanningnumber, journeypatterncode,
timinglinkorder limit 20;

select dataownercode, lineplanningnumber, journeypatterncode, timinglinkorder,
userstopcodebegin, istimingstop,
       (select count(*) from test as counter where
            counter.dataownercode = test.dataownercode and
            counter.lineplanningnumber = test.lineplanningnumber and
            counter.journeypatterncode = test.journeypatterncode and
            counter.timinglinkorder < test.timinglinkorder and
            counter.userstopcodebegin = test.userstopcodebegin) as
passagesequencenumber 
from test order by dataownercode, lineplanningnumber, journeypatterncode,
timinglinkorder limit 20;

DROP table test;
