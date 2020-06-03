-- experiments with variables used within and between schemas.

CREATE SCHEMA A;
CREATE SCHEMA B;

DECLARE "outer" string;
SET "outer" = 'outer';
SELECT "outer";
SELECT sys."outer"; -- same as above
SELECT tmp."outer"; -- unkown variable within sys

SET SCHEMA A;
DECLARE "Avar" string;
SET "Avar" = 'Avar';
SELECT "Avar";

SET SCHEMA B;
DECLARE "Bvar" string;
SET "Bvar" = 'Bvar';
SELECT "Bvar";

SET SCHEMA sys;
SELECT "outer"; -- should be known
SELECT "Avar";	-- unknown
SELECT "Bvar";	-- unknown

SELECT sys."outer";
SELECT A."outer"; -- unknown
SELECT B."outer"; -- unknown
SELECT A."Avar"; -- known
SELECT B."Avar"; -- unknown
SELECT A."Bvar"; -- unknown
SELECT B."Bvar"; -- known

SET SCHEMA A;
SELECT "outer"; -- unkown
SELECT "Avar";	-- known
SELECT "Bvar";	-- unknown

SELECT sys."outer";
SELECT A."outer"; -- unknown
SELECT B."outer"; -- unknown
SELECT A."Avar"; -- known
SELECT B."Avar"; -- unknown
SELECT A."Bvar"; -- unknown
SELECT B."Bvar"; -- known

SET SCHEMA "sys";

DROP SCHEMA A CASCADE;
DROP SCHEMA B CASCADE;
