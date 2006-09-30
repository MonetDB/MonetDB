declare epsilon double;
set epsilon=0.00001;

create function Alpha(theta double, decl double) returns double
    begin
    if abs(decl)+theta > 89.9 then return cast(180.0 as double);
    else 
    return(degrees(abs(atan(sin(radians(theta)) /
                             sqrt(abs( cos(radians(decl-theta))
                                      * cos(radians(decl+theta))
           )       )   )   )     ));
	end if;
    end;

create table alphatest(expected double, computed double);
insert into alphatest values(1.1547200925865859,Alpha(cast(1.0 as double),cast(30.0 as double)));
insert into alphatest values(1.4142854109042293,Alpha(cast(1.0 as double),cast(45.0 as double)));
insert into alphatest values(2.0003048809350625,Alpha(cast(1.0 as double),cast(60.0 as double)));

select abs(expected - computed) as err
from alphatest 
where abs(expected - computed)>epsilon;

---------Spatial Zone Index-------------
create table ZoneIndex (
    objID       bigint         not null, -- object Identifier in table
    zone        int         not null, -- zone number (using 10 arcminu
    ra          float       not null, -- sperical coordinates
    "dec"       float       not null,
    x           float       not null, -- cartesian coordinates
    y           float       not null,
    z           float       not null,
    margin      int         not null, -- "margin" or "native" elements, bit
    primary key (zone, ra, objID)
    );

insert into zoneindex
values( 687726014001184891,	1,	193.754250579787,	1.4851688900683999,	-0.97099811213370246,	-0.23767817297968644,	0.025918185156838715,	0);
insert into zoneindex
values( 687726014001184892,	1,	193.755094586886,	1.4846298308572601,	-0.97099484767300204,	-0.2376925344085847,	0.0259087799579065,	0);
insert into zoneindex
values( 687726014001184894,	1,	193.75653243396599,	1.4825263586962001,	-0.97098980567114368,	-0.2377171276374572,	0.025872079748478348,	0);
insert into zoneindex
values( 687726014001184895,	1,	193.75663392621499,	1.61956881843685,	-0.97092650035194339,	-0.23770345222609343,	0.028263045025703246,	0);
insert into zoneindex
values( 687726014001184896,	1,	193.75664598583401,	1.6195802985663199,	-0.97092644481955426,	-0.23770365524011272,	0.028263245311723893,	0);

------margins

insert into zoneindex
select objid,zone,ra-360.0,"dec",x,y,z,1 as margin
from zoneindex where ra>=180.0;

insert into zoneindex
select objid,zone,ra+360.0,"dec",x,y,z,1 as margin
from zoneindex where ra<180.0 and margin=0;

-------------------------------------
create function GetNearbyObjects(
                       pdec double, pra double, -- in degrees
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
from GetNearbyObjects(cast(1.48 as double),cast(193.75 as double),cast(0.1 as double)) as S;

create table zonetest (
    objID    bigint,
    expected double);

insert into zonetest values( 687726014001184891,	0.00669124492169760);
insert into zonetest values( 687726014001184892,	0.00688278877005443);
insert into zonetest values( 687726014001184894,	0.00700190450261338);
insert into zonetest values( 687726014001184895,	0.13972627471136584);
insert into zonetest values( 687726014001184896,	0.13973831452158963);

select S.objID,S.distance, T.expected 
from GetNearbyObjects(cast(1.48 as double),cast(193.75 as double),cast(1.0 as double)) as S, zonetest as T
where S.objID=T.objID and abs(S.distance-T.expected)>epsilon;


drop function GetNearbyObjects;
drop function Alpha;
drop table alphatest;
drop table zonetest;
drop table zoneindex;

