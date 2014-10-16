START TRANSACTION;
CREATE FUNCTION shouldbeint() RETURNS TABLE (i integer) LANGUAGE R {as.numeric(42.0)};
SELECT * FROM shouldbeint();
ROLLBACK;

START TRANSACTION;
CREATE FUNCTION throwwarning() RETURNS TABLE (i integer) LANGUAGE R {
	warning("this is the wrong kind of handbag")
	as.integer(42)
};
SELECT * FROM throwwarning();
ROLLBACK;

START TRANSACTION;
CREATE FUNCTION throwerror() RETURNS TABLE (i integer) LANGUAGE R {stop("goodbye cruel world")};
SELECT * FROM throwerror();
ROLLBACK;

START TRANSACTION;
CREATE FUNCTION suicide() RETURNS TABLE (i integer) LANGUAGE R {quit(save="no")};
SELECT * FROM suicide();
ROLLBACK;

START TRANSACTION;
CREATE FUNCTION suicide2() RETURNS TABLE (i integer) LANGUAGE R {base::quit(save="no")};
SELECT * FROM suicide2();
ROLLBACK;

START TRANSACTION;
CREATE FUNCTION suicide3() RETURNS TABLE (i integer) LANGUAGE R {.Internal(quit("no", 0,F))};
SELECT * FROM suicide3();
ROLLBACK;

SELECT 1;




