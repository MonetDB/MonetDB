-- test for Bug 6316
select  role_id,
          cast(coalesce(role_id, login_id) as int) 
from sys.user_role 
limit 145;
