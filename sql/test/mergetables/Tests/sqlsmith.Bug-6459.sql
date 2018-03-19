
select  
  subq_0.c0 as c0
from 
  (select  
        ref_2.coord_dimension as c0
      from 
        sys.geometry_columns as ref_2
      where ref_2.f_table_catalog is not NULL) as subq_0
where EXISTS (
  select  
      ref_4.srtext as c0
    from 
      sys.spatial_ref_sys as ref_4
        right join sys.tracelog as ref_5
        on (ref_4.proj4text = ref_5.clk )
    where case when EXISTS (
          select  
              subq_0.c0 as c0
            from 
              sys._tables as ref_6
            where false) then ref_4.auth_srid else ref_4.auth_srid end
         is not NULL);

select
  subq_0.c0 as c0
from
  (select
        ref_2.coord_dimension as c0
      from
        sys.geometry_columns as ref_2
      where ref_2.f_table_catalog is not NULL
      limit 127) as subq_0
where EXISTS (
  select
      ref_4.srtext as c0,
      ref_4.auth_srid as c1,
      37 as c2,
      subq_0.c0 as c3,
      ref_5.minflt as c4
    from
      sys.spatial_ref_sys as ref_4
        right join sys.tracelog as ref_5
        on (ref_4.proj4text = ref_5.clk )
    where case when EXISTS (
          select
              subq_0.c0 as c0,
              ref_4.proj4text as c1,
              ref_5.thread as c2,
              subq_0.c0 as c3,
              ref_6.commit_action as c4
            from
              sys.tables as ref_6
            where false) then ref_4.auth_srid else ref_4.auth_srid end
         is not NULL);

select
  cast(coalesce(cast(coalesce(subq_0.c0,
      subq_0.c0) as clob),
    subq_0.c0) as clob) as c0,
  subq_0.c3 as c1,
  subq_0.c1 as c2,
  subq_0.c2 as c3,
  subq_0.c1 as c4,
  subq_0.c2 as c5,
  subq_0.c1 as c6,
  subq_0.c2 as c7,
  subq_0.c0 as c8,
  subq_0.c2 as c9,
  subq_0.c3 as c10
from
  (select
        ref_0.z as c0,
        ref_0.x as c1,
        case when false then ref_0.x else ref_0.x end
           as c2,
        case when EXISTS (
            select
                ref_0.x as c0,
                ref_1.type as c1,
                ref_0.y as c2,
                ref_0.y as c3,
                44 as c4,
                ref_0.z as c5,
                ref_0.y as c6,
                ref_1.id as c7
              from
                sys.tables as ref_1
              where EXISTS (
                select

                    ref_0.y as c0,
                    ref_2.login_id as c1,
                    ref_2.login_id as c2
                  from
                    sys.user_role as ref_2
                  where ref_0.x is NULL)) then ref_0.y else ref_0.y end
           as c3
      from
        sys.r2 as ref_0
      where ref_0.y is NULL
      limit 72) as subq_0
where case when 86 is NULL then subq_0.c1 else subq_0.c1 end
     is NULL;
