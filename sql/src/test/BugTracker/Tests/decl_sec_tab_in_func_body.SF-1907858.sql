CREATE FUNCTION fR(x float, types float)
RETURNS float
BEGIN
	DECLARE TABLE typesTable(
		type varchar(16)
	);

	INSERT into typesTable (type) values ('asdasd');

	DECLARE TABLE region(
		regionid bigint,
		type varchar(16)
	);
	Return x;
END;

drop function fR;
