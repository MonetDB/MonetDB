create table dbl_sht_cast_bug (
	r real,
	i real,
	g real
);

insert into dbl_sht_cast_bug values (118778.78787, 11999.99888, 12345.678901);
SELECT count(g)
FROM dbl_sht_cast_bug
WHERE ( r - i < (0.08 + 0.42 * (g - r - 0.96)) or g - r > 2.26 );

drop table dbl_sht_cast_bug;
