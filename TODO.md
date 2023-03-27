Pipeline issues we should look into later:

1. This perfectly normal query triggers a MALException:
```
sql>select c_name, count(n_name) from customer, nation where c_nationkey = n_nationkey group by c_name limit 5;
MALException:chkFlow:user.main setLifeSpan nested dataflow blocks not allowed
```
