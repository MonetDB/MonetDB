select
  str_to_date(DATE1, '%Y%m%d') as DATE1,
  str_to_date(DATE2, '%Y-%m-%d') as DATE2,
  str_to_date(DATE3, '%Y/%m/%d') as DATE3,
  str_to_date(DATE4, '%Y-%m-%d %H:%M:%S') as DATE4,
  str_to_date(DATE5, '%d-%m-%Y %H:%M') as DATE5,
  str_to_date(DATE6, '%Y %B %d') as DATE6
from (
select
'20181201' as DATE1,
'2018-12-01' as DATE2,
'2018/12/01' as DATE3,
'2018-12-01 06:12:15' as DATE4,
'01-09-2008 06:12' as DATE5,
'2008 May 12' as DATE6
) A;
