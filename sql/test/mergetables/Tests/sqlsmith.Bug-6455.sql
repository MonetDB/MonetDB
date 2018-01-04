select  
  ref_6.length as c0, 
  case when EXISTS (
      select  
          97 as c0, 
          ref_3.y as c1, 
          ref_16.z as c2, 
          ref_9.x as c3, 
          ref_1.dependency_type_name as c4, 
          ref_16.z as c5, 
          ref_6.name as c6, 
          ref_7.y as c7, 
          ref_18.grantor as c8, 
          subq_0.c4 as c9, 
          5 as c10, 
          ref_3.x as c11, 
          ref_4.index_type_id as c12, 
          ref_27.z as c13, 
          ref_1.dependency_type_name as c14, 
          ref_27.y as c15, 
          ref_17.query as c16, 
          ref_7.x as c17, 
          ref_9.z as c18, 
          ref_3.z as c19, 
          ref_1.dependency_type_name as c20, 
          case when EXISTS (
              select  
                  ref_20.vararg as c0, 
                  subq_0.c4 as c1, 
                  ref_27.y as c2, 
                  ref_9.y as c3, 
                  ref_21.pnext as c4, 
                  ref_6.dim_id as c5, 
                  ref_7.z as c6, 
                  ref_8.table_id as c7
                from 
                  sys.storagemodel as ref_28
                where false) then ref_2.depend_id else ref_2.depend_id end
             as c21, 
          ref_16.x as c22, 
          ref_4.index_type_name as c23, 
          ref_6.name as c24, 
          ref_21.seq as c25, 
          ref_16.z as c26, 
          ref_16.x as c27, 
          ref_14.system as c28, 
          ref_1.dependency_type_name as c29, 
          ref_9.z as c30, 
          ref_4.index_type_name as c31, 
          ref_3.y as c32
        from 
          sys.s1 as ref_27
        where ref_27.y is NULL) then ref_9.x else ref_9.x end
     as c1, 
  ref_14.id as c2, 
  ref_14.id as c3, 
  case when ref_20.language is not NULL then ref_2.id else ref_2.id end
     as c4, 
  ref_20.func as c5, 
  ref_9.x as c6, 
  case when true then ref_18.name else ref_18.name end
     as c7, 
  ref_19.function_type_id as c8
from 
  sys.dependency_types as ref_1
              inner join sys.dependencies as ref_2
              on (ref_1.dependency_type_id = ref_2.depend_type )
            left join sys.r3 as ref_3
            on (ref_2.depend_id = ref_3.x )
          inner join sys.index_types as ref_4
          on (ref_2.depend_type is not NULL)
        right join sys.netcdf_dims as ref_6
          left join sys.r as ref_7
              inner join tmp.keys as ref_8
              on (ref_7.z is not NULL)
            right join sys.r2 as ref_9
            on (ref_7.z is not NULL)
          on (ref_6.file_id = ref_8.id )
        on (ref_9.x is NULL)
      left join sys.schemas as ref_14
        right join sys.r1 as ref_16
              inner join tmp._tables as ref_17
              on (ref_16.y = ref_17.id )
            inner join sys.auths as ref_18
            on (false)
          inner join sys.function_types as ref_19
              inner join sys.functions as ref_20
              on (ref_19.function_type_name = ref_20.name )
            left join bam.export as ref_21
            on (ref_20.id = ref_21.pos )
          on (ref_17.type is NULL)
        on (ref_14.name = ref_17.name )
      on (true)
    right join (select  
          83 as c0, 
          ref_26.y as c1, 
          ref_26.y as c2, 
          90 as c3, 
          ref_26.y as c4, 
          case when true then ref_26.y else ref_26.y end
             as c5
        from 
          sys.r as ref_26
        where false
        limit 59) as subq_0
    on (ref_3.x = subq_0.c0 )
where ref_4.index_type_id is NULL
limit 131;
