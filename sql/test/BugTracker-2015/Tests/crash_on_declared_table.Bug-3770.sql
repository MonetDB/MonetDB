start transaction;
create table uniquecatalog11(id bigint PRIMARY KEY
	, targetid bigint
	, ra_avg double
	, decl_avg double
	, flux_ref double
	, datapoints int
	, zone smallint
	, x double
	, y double
	, z double
	, INACTIVE BOOLEAN
);

create function neighbor30()
returns table (ra_avg double)
begin
	declare table uzone (id bigint, ra_avg double);
	insert into uzone select id, ra_avg from uniquecatalog11;
	return table( select ra_avg
		from uzone as u0
		where id between 10 and 20
		and ra_avg between 10 and 20
	);
end;

rollback;
