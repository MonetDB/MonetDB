select ((select count(*) from hidx where hidx.tblid = 3) +
        (select count(*) from hidx where hidx.tblid = 160) +
        (select count(*) from hidx where hidx.tblid = 317) +
        (select count(*) from hidx where hidx.tblid = 474) +
        (select count(*) from hidx where hidx.tblid = 631) +
        (select count(*) from hidx where hidx.tblid = 788));
