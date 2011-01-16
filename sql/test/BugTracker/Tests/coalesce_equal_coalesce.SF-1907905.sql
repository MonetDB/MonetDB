CREATE TABLE syscolumns(
		id int,
		length int,
		uid int,
		nme char(100),
		xtype char(100)
);

CREATE TABLE spt_datatype_info(
		length int,
		ss_dtype varchar(100),
		"AUTO_INCREMENT" varchar(100)
);

CREATE FUNCTION fDocColumnsWithRank(TableName varchar(400))
RETURNS float
BEGIN
	return table ( SELECT c.nme as nme
			FROM
				spt_datatype_info d,
				syscolumns c
			WHERE
			coalesce(d."AUTO_INCREMENT",0) =
			coalesce(ColumnProperty (c.id, c.nme,'IsIdentity'),0) );
END;

drop function fDocColumnsWithRank;
drop table syscolumns;
drop table spt_datatype_info;
