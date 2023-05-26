Pipeline issues we should look into later:

1. This perfectly normal query triggers a MALException:

```
sql>select c_name, count(n_name) from customer, nation where c_nationkey = n_nationkey group by c_name limit 5;
MALException:chkFlow:user.main setLifeSpan nested dataflow blocks not allowed
```

2. The following (accidental) query produces a "nested dataflow blocks" error because the `language.pipeline()` block for the semijoin is not properly closed:

```
sql> select r_name, count(*) from nation, region where n_nationkey = r_regionkey group by r_name order by r_name;
MALException:chkFlow:user.main setLifeSpan nested dataflow blocks not allowed
sql>plan select r_name, count(*) from nation, region where n_nationkey = r_regionkey group by r_name order by r_name;
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| rel                                                                                                                                                                             |
+=================================================================================================================================================================================+
| project (                                                                                                                                                                       |
| | project (                                                                                                                                                                     |
| | | select (                                                                                                                                                                    |
| | | |  start  || group by (                                                                                                                                                     |
| | | | |  start semijoin (                                                                                                                                                       |
| | | | | | table("sys"."region") [ "region"."r_regionkey" NOT NULL UNIQUE NUNIQUES 5.000000 HASHCOL , "region"."r_name" NOT NULL UNIQUE NUNIQUES 5.000000 ] L PARTITION COUNT 5, |
| | | | | | table("sys"."nation") [ "nation"."n_nationkey" NOT NULL UNIQUE NUNIQUES 25.000000 HASHCOL  ] COUNT 25                                                                 |
| | | | | ) [ ("nation"."n_nationkey" NOT NULL UNIQUE NUNIQUES 25.000000 HASHCOL ) = ("region"."r_regionkey" NOT NULL UNIQUE NUNIQUES 5.000000 HASHCOL ) ] L PARTITION COUNT 5    |
| | | | ) [ "region"."r_name" NOT NULL UNIQUE NUNIQUES 5.000000 ] [ "region"."r_name" NOT NULL UNIQUE NUNIQUES 5.000000, "sys"."count"() NOT NULL as "%1"."%1" ] COUNT 5          |
| | | ) [ ("%1"."%1" NOT NULL) != (bigint(18) "0") ] COUNT 5                                                                                                                      |
| | ) [ "region"."r_name" NOT NULL UNIQUE NUNIQUES 5.000000, "%1"."%1" NOT NULL ] COUNT 5                                                                                         |
| ) [ "region"."r_name" NOT NULL UNIQUE NUNIQUES 5.000000, "%1"."%1" NOT NULL ] [ "region"."r_name" ASC NOT NULL UNIQUE NUNIQUES 5.000000 ] COUNT 5                               |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

3. The UNION query in `sql/test/Tests/blob_query.test` fails in `aproject_(str)` (`monetdb5/modules/mal/pp_algebra.c`:2074) because the BAT `g` contains invalid values in its tail:

```
(gdb) p BATprint(THRdata[0], g)
#--------------------------#
# h	t  # name
# void	oid  # type
#--------------------------#
[ 0@0,	13672292666396491197@0  ]
```

4. `sql/test/BugTracker-2013/Tests/aggregates-typing-issues.Bug-3277.test`, query `select cast (prod( prob1 ) as bigint) from mtcars2` returns different values in different `mserver5` sessions when `--forcemito` is used (e.g. by Mtest.py).  Without `--forcemito`, the query gives the correct result. So, probably some errors in the pipeline code for the `prod` aggregate.

5. the `stddev_pop` in the following query triggers a crash:
```
explain select l_discount, stddev_pop(l_discount) from lineitem, supplier where l_suppkey = s_suppkey group by l_discount limit 5;
```
