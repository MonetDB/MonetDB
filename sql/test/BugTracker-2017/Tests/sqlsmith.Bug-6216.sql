select
  ref_11.type_digits as c0
from
  (select
            ref_8.login_id as c0,
            ref_8.login_id as c1,
            ref_8.login_id as c2,
            ref_8.role_id as c3,
            ref_8.role_id as c4,
            45 as c5
          from
            sys.user_role as ref_8
          where ref_8.role_id is NULL) as subq_0
      inner join sys.idxs as ref_9
      on (subq_0.c5 = ref_9.id )
    inner join sys.columns as ref_11
    on (subq_0.c5 = ref_11.id )
where ref_9.table_id is NULL
limit 146;
