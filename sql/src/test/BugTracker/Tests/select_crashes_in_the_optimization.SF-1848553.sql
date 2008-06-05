create table s ( snr int, sname varchar(30) );
create table sp ( snr int, pnr varchar(30) );

select 24;
SELECT DISTINCT S.SNAME
FROM S, SP
GROUP BY S.SNR, S.SNAME, SP.SNR, SP.PNR
HAVING SP.SNR = S.SNR
AND SP.PNR = 'P2';

drop table s;
drop table sp;
