select
          subq_0.c0 as c1
        from
          (select
                ref_0.grantor as c0,
                39 as c1
              from
                sys.auths as ref_0
              ) as subq_0
        where subq_0.c1 is NULL;
