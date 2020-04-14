-- This is a simple global variable and aims for all kinds of ambiguous use cases
DECLARE Gvar string;
SET Gvar='Gvar';

CREATE SCHEMA A;
SET SCHEMA A;
DECLARE Avar string;
SET Avar='Avar';

CREATE OR REPLACE FUNCTION foo(i string) RETURNS INT
BEGIN DECLARE i string; set i ='1'; return i; END; --error, i re-declared

CREATE OR REPLACE FUNCTION foo(gvar string) RETURNS INT
BEGIN DECLARE i string; set i ='1'; return i; END;

CREATE OR REPLACE FUNCTION foo() RETURNS INT
BEGIN DECLARE i string; set i ='1'; return i; END;
SELECT foo();
    -- 1

CREATE OR REPLACE FUNCTION Gvar() RETURNS string
BEGIN DECLARE i string; set i ='function i'; return i; END;
SELECT Gvar();
    -- function i

-- now scopes
CREATE OR REPLACE FUNCTION foo() RETURNS string
BEGIN DECLARE i string; set i =gvar; return i; END; --error, gvar was declared on sys schema, so it doesn't exist here
SELECT foo();
    -- 1

CREATE OR REPLACE FUNCTION Gvar() RETURNS string
BEGIN DECLARE i string; set i =gvar; return i; END; --error, gvar was declared on sys schema, so it doesn't exist here
SELECT Gvar();
    -- function i

CREATE OR REPLACE FUNCTION foo() RETURNS string
BEGIN DECLARE i string; set i = sys.gvar; return i; END;
SELECT foo();
    -- Gvar

CREATE OR REPLACE FUNCTION Gvar() RETURNS string
BEGIN DECLARE i string; set i =sys.gvar; return i; END;
SELECT Gvar();
    -- Gvar

CREATE OR REPLACE FUNCTION foo() RETURNS string
BEGIN DECLARE i string; set i = A.gvar; return i; END; --error, variable a.gvar doesn't exist
SELECT foo();
    -- Gvar

CREATE OR REPLACE FUNCTION Gvar() RETURNS string
BEGIN DECLARE i string; set i =A.gvar; return i; END; --error, variable a.gvar doesn't exist
SELECT Gvar();
    -- Gvar

-- procedures
CREATE OR REPLACE PROCEDURE foo() 
BEGIN DECLARE i string; set i = 'iassigned'; END;
CALL foo();

CREATE OR REPLACE PROCEDURE gvar() 
BEGIN DECLARE i string; set i = 'iassigned'; END;
CALL foo();

CREATE OR REPLACE PROCEDURE foo() 
BEGIN set A.avar = 'avar_assigned'; END;
CALL foo();

CREATE OR REPLACE PROCEDURE avar() 
BEGIN set A.avar = 'avar_assigned'; END;
CALL avar();

-- play around with schema changes
CREATE OR REPLACE PROCEDURE avar() 
BEGIN SET SCHEMA A; set avar = 'avar_assigned'; END;
CALL avar();

CREATE OR REPLACE PROCEDURE avar() 
BEGIN SET SCHEMA A; set sys.avar = 'avar_assigned'; END; --error, variable sys.avar doesn't exist
CALL avar();

SET SCHEMA "sys";

DROP SCHEMA A CASCADE;
