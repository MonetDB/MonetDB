declare epsilon double;
set epsilon=0.00001;

create table alphatest(expected double, computed double);
insert into alphatest values(1.1547200925865859,Alpha(cast(30.0 as double),cast(1.0 as double)));
insert into alphatest values(1.4142854109042293,Alpha(cast(45.0 as double),cast(1.0 as double)));
insert into alphatest values(2.0003048809350625,Alpha(cast(60.0 as double),cast(1.0 as double)));

select abs(expected - computed) as err
from alphatest 
where abs(expected - computed)>epsilon;

---------Spatial Zone Index-------------
create table photoobj (
    objID       bigint      not null, -- object Identifier in table
    ra          float       not null, 
    "dec"       float       not null,
    mode        tinyint	    not null,
    primary key (objID)
    );

insert into photoobj
values( 687726014001184891,	193.754250579787,	1.4851688900683999,	0);
insert into photoobj
values( 687726014001184892,	193.755094586886,	1.4846298308572601,	0);
insert into photoobj
values( 687726014001184894,	193.75653243396599,	1.4825263586962001,	0);
insert into photoobj
values( 687726014001184895,	193.75663392621499,	1.61956881843685,	0);
insert into photoobj
values( 687726014001184896,	193.75664598583401,	1.6195802985663199,	0);


create table ZoneIndex (
    objID       bigint         not null, -- object Identifier in table
    zone        int         not null, -- zone number (using 10 arcminu
    ra          float       not null, -- sperical coordinates
    "dec"       float       not null,
    x           float       not null, -- cartesian coordinates
    y           float       not null,
    z           float       not null,
    mode        tinyint	    not null,       
    margin      int         not null, 
    primary key (zone, ra, objID)
    );

---Parameterizing the algo with zoneHeight
create table ZoneHeight( "value" double not null); -- zone height in degrees.
insert into ZoneHeight values(cast (0.1  as double));

declare zHeight double;
set zHeight = (select min("value") from ZoneHeight);


insert into zoneindex
     select objID,
            cast(floor("dec"/zHeight) as int) as zone,
            ra, "dec",
              cos(radians("dec"))*cos(radians(ra)) as x,
              cos(radians("dec"))*sin(radians(ra)) as y,
              sin(radians("dec")) as z,
              mode, 0 as margin
      from photoobj;

------margins

insert into zoneindex
select objid,zone,ra-360.0,"dec",x,y,z,mode,1 as margin
from zoneindex where ra>=180.0;

insert into zoneindex
select objid,zone,ra+360.0,"dec",x,y,z,mode,1 as margin
from zoneindex where ra<180.0 and margin=0;

-------------------------------------
create function GetNearbyObjects(
                       pra double, pdec double, -- in degrees
                       ptheta double)           -- radius in degrees
returns Table (objID bigint, distance double) 
begin
    declare zoneHeight float, alpha1 float,
            x1 float, y1 float, z1 float;

    -- compute “alpha” expansion and cartesian coordinates.
    set alpha1 = Alpha(ptheta, pdec);
    set x1 = cos(radians(pdec))*cos(radians(pra));
    set y1 = cos(radians(pdec))*sin(radians(pra));
    set z1 = sin(radians(pdec));

    return TABLE(select objID,
            case when(x1*x +y1*y + z1*z) < 1   -- avoid domain error on acos
                 then degrees(acos(x1*x +y1*y + z1*z))
                 else 0 end as distance                   -- when angle is tiny.
    from zoneindex 
    where
       ra between pra -alpha1      -- restrict to a 2 Alpha wide
       and  pra + alpha1   -- ragitude band in the zone
       and "dec" between pdec - ptheta      -- and roughly correct "dec"itude
       and    pdec + ptheta
       and (x1*x +y1*y + z1*z)             -- and then a careful distance
                   > cos(radians(ptheta)) 
	);
end;

select count(S.objID) 
from GetNearbyObjects(193.75, 1.48, 0.1) as S;

create table zonetest (
    objID    bigint,
    expected double);

insert into zonetest values( 687726014001184891,	0.00669124492169760);
insert into zonetest values( 687726014001184892,	0.00688278877005443);
insert into zonetest values( 687726014001184894,	0.00700190450261338);
insert into zonetest values( 687726014001184895,	0.13972627471136584);
insert into zonetest values( 687726014001184896,	0.13973831452158963);

select S.objID,S.distance, T.expected 
from GetNearbyObjects(193.75,1.48, 1.0) as S, zonetest as T
where S.objID=T.objID and abs(S.distance-T.expected)>epsilon;

-----------------------------------
create function GetNearestObject(
                         pra double, pdec double) -- in degrees
returns Table (objID bigint, distance double) 
begin
	return TABLE(select objID,distance
	from GetNearbyObjects(pra,pdec,0.1) G
	where distance = (select min(distance) 
	from GetNearbyObjects(pra,pdec,0.1) G1));
end;

create function fGetNearestObjIdAllEq(
                         pra double, pdec double, pr double)
returns bigint
begin
	declare ob bigint;
	set ob= (select G.objID
	from GetNearestObject(pra,pdec) G);
        return ob;
end;

declare nearest bigint;
set nearest=687726014001184891;

select fGetNearestObjIdAllEq(193.75,1.48,0.1) - nearest;

---------------------------------------------
drop function fGetNearestObjIdAllEq;
drop function GetNearestObject;
drop function GetNearbyObjects;
drop table alphatest;
drop table zonetest;
drop table zoneindex;

