select case privilege_code_name when 'SELECT' then 1 when 'UPDATE' then 60.3281 when 'INSERT' then 0.8415381117315 else 0 end as t1 from privilege_codes order by privilege_code_id;
