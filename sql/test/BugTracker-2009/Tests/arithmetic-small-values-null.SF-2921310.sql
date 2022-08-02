-- http://bugs.monetdb.org/show_bug.cgi?id=2291

-- yields NULL :/
select ((4.4054292 - 4.40572025343667)^2) + ((52.0903881 - 52.091375762174)^2);

-- correct answer
select cast((4.4054292 - 4.40572025343667)^2 as double) + cast((52.0903881 - 52.091375762174)^2 as double);
