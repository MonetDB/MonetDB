START TRANSACTION;

create table urlsTest (urlid integer not null, suburl0x0 bigint, suburl0x1 bigint, suburl1x0 bigint, suburl1x1 bigint, suburl2x0 bigint, suburl2x1 bigint, suburl3x0 bigint, suburl3x1 bigint, suburl4x0 bigint, suburl4x1 bigint, suburl5x0 bigint, suburl5x1 bigint, suburl6x0 bigint, suburl6x1 bigint, suburl7x0 bigint, suburl7x1 bigint, suburl8x0 bigint, suburl8x1 bigint, suburl9x0 bigint, suburl9x1 bigint, suburl10x0 bigint, suburl10x1 bigint, suburl11x0 bigint, suburl11x1 bigint, suburl12x0 bigint, suburl12x1 bigint, suburl13x0 bigint, suburl13x1 bigint, suburl14x0 bigint, suburl14x1 bigint, suburl15x0 bigint, suburl15x1 bigint, suburl16x0 bigint, suburl16x1 bigint, unique (suburl0x0, suburl0x1, suburl1x0, suburl1x1, suburl2x0, suburl2x1, suburl3x0, suburl3x1, suburl4x0, suburl4x1, suburl5x0, suburl5x1, suburl6x0, suburl6x1, suburl7x0, suburl7x1, suburl8x0, suburl8x1, suburl9x0, suburl9x1, suburl10x0, suburl10x1, suburl11x0, suburl11x1, suburl12x0, suburl12x1, suburl13x0, suburl13x1, suburl14x0, suburl14x1, suburl15x0, suburl15x1, suburl16x0, suburl16x1), primary key (urlid));;
insert into urlsTest values(14534,434884248038388279,3320794158431,0,1220534050083,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
create table clicksTest (clicktime time not null, suburl0x0 bigint, suburl0x1 bigint, suburl1x0 bigint, suburl1x1 bigint, suburl2x0 bigint, suburl2x1 bigint, suburl3x0 bigint, suburl3x1 bigint, suburl4x0 bigint, suburl4x1 bigint, suburl5x0 bigint, suburl5x1 bigint, suburl6x0 bigint, suburl6x1 bigint, suburl7x0 bigint, suburl7x1 bigint, suburl8x0 bigint, suburl8x1 bigint, suburl9x0 bigint, suburl9x1 bigint, suburl10x0 bigint, suburl10x1 bigint, suburl11x0 bigint, suburl11x1 bigint, suburl12x0 bigint, suburl12x1 bigint, suburl13x0 bigint, suburl13x1 bigint, suburl14x0 bigint, suburl14x1 bigint, suburl15x0 bigint, suburl15x1 bigint, suburl16x0 bigint, suburl16x1 bigint);;

insert into clicksTest values('19:42:31.558',434884248038388279,3320794158431,0,1220534050083,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);

select urlid,clicktime from (
	clicksTest join urlsTest on 
	urlsTest.suburl0x0=clicksTest.suburl0x0 and 
	urlsTest.suburl0x1=clicksTest.suburl0x1 and 
	urlsTest.suburl1x0=clicksTest.suburl1x0 and 
	urlsTest.suburl1x1=clicksTest.suburl1x1 and urlsTest.suburl2x0=clicksTest.suburl2x0 and urlsTest.suburl2x1=clicksTest.suburl2x1 and urlsTest.suburl3x0=clicksTest.suburl3x0 and urlsTest.suburl3x1=clicksTest.suburl3x1 and urlsTest.suburl4x0=clicksTest.suburl4x0 and urlsTest.suburl4x1=clicksTest.suburl4x1 and urlsTest.suburl5x0=clicksTest.suburl5x0 and urlsTest.suburl5x1=clicksTest.suburl5x1 and urlsTest.suburl6x0=clicksTest.suburl6x0 and urlsTest.suburl6x1=clicksTest.suburl6x1 and urlsTest.suburl7x0=clicksTest.suburl7x0 and urlsTest.suburl7x1=clicksTest.suburl7x1 and urlsTest.suburl8x0=clicksTest.suburl8x0 and urlsTest.suburl8x1=clicksTest.suburl8x1 and urlsTest.suburl9x0=clicksTest.suburl9x0 and urlsTest.suburl9x1=clicksTest.suburl9x1 and urlsTest.suburl10x0=clicksTest.suburl10x0 and urlsTest.suburl10x1=clicksTest.suburl10x1 and urlsTest.suburl11x0=clicksTest.suburl11x0 and urlsTest.suburl11x1=clicksTest.suburl11x1 and urlsTest.suburl12x0=clicksTest.suburl12x0 and urlsTest.suburl12x1=clicksTest.suburl12x1 and urlsTest.suburl13x0=clicksTest.suburl13x0 and urlsTest.suburl13x1=clicksTest.suburl13x1 and urlsTest.suburl14x0=clicksTest.suburl14x0 and urlsTest.suburl14x1=clicksTest.suburl14x1 and urlsTest.suburl15x0=clicksTest.suburl15x0 and urlsTest.suburl15x1=clicksTest.suburl15x1 and urlsTest.suburl16x0=clicksTest.suburl16x0 and urlsTest.suburl16x1=clicksTest.suburl16x1 );;

select urlid,clicktime from
(clicks left outer join urls on
urls.suburl0x0=clicks.suburl0x0 and
urls.suburl0x1=clicks.suburl0x1 and
urls.suburl1x0=clicks.suburl1x0 and
urls.suburl1x1=clicks.suburl1x1 and
urls.suburl2x0=clicks.suburl2x0 and
urls.suburl2x1=clicks.suburl2x1 and
urls.suburl3x0=clicks.suburl3x0 and
urls.suburl3x1=clicks.suburl3x1 and
urls.suburl4x0=clicks.suburl4x0 and
urls.suburl4x1=clicks.suburl4x1 and
urls.suburl5x0=clicks.suburl5x0 and
urls.suburl5x1=clicks.suburl5x1 and
urls.suburl6x0=clicks.suburl6x0 and
urls.suburl6x1=clicks.suburl6x1 and
urls.suburl7x0=clicks.suburl7x0 and
urls.suburl7x1=clicks.suburl7x1 and
urls.suburl8x0=clicks.suburl8x0 and
urls.suburl8x1=clicks.suburl8x1 and
urls.suburl9x0=clicks.suburl9x0 and
urls.suburl9x1=clicks.suburl9x1 and
urls.suburl10x0=clicks.suburl10x0 and
urls.suburl10x1=clicks.suburl10x1 and
urls.suburl11x0=clicks.suburl11x0 and
urls.suburl11x1=clicks.suburl11x1 and
urls.suburl12x0=clicks.suburl12x0 and
urls.suburl12x1=clicks.suburl12x1 and
urls.suburl13x0=clicks.suburl13x0 and
urls.suburl13x1=clicks.suburl13x1 and
urls.suburl14x0=clicks.suburl14x0 and
urls.suburl14x1=clicks.suburl14x1 and
urls.suburl15x0=clicks.suburl15x0 and
urls.suburl15x1=clicks.suburl15x1 and
urls.suburl16x0=clicks.suburl16x0 and
urls.suburl16x1=clicks.suburl16x1) where
clicktime>'00:00:00' and clicktime<'23:59
:59');
--  ^ is wrong, did gave an error with strange symbols

ROLLBACK;
