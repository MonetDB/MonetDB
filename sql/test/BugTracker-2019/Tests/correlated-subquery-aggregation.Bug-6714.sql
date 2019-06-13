select func_id, (select name from functions f where f.id = func_id) as name, max(number), count(*) from args
group by func_id having count(*) > 8 order by func_id limit 12;
