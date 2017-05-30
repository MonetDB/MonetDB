select
  ref_0.message as c0
from
  sys.rejects as ref_0,
  lateral (select
		(select name from sys.objects)
		   as c0
	  from
		sys.args as sample_1
		  right join tmp._columns as ref_4
			  inner join sys.part as ref_6
				  inner join sys.types as sample_5
				  on (ref_6.p_comment = sample_5.systemname )
				right join sys.netcdf_vars as ref_7
				on (sample_5.digits = ref_7.var_id )
			  on (ref_4.name = sample_5.systemname )
			right join sys.user_role as sample_8
			  inner join sys.partsupp as ref_10
			  on (sample_8.role_id = ref_10.ps_partkey )
			on (ref_4.name = ref_10.ps_comment )
		  on (sample_1.name = ref_10.ps_comment )
	  ) as subq_0;
