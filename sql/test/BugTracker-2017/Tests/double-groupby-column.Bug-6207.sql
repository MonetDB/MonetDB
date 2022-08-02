select privilege_code_name,privilege_code_name from sys.privilege_codes group by privilege_code_name limit 2;
select privilege_code_name, privilege_code_name from sys.privilege_codes group by privilege_code_name,privilege_code_name limit 2;
select f.privilege_code_name, f.privilege_code_name from sys.privilege_codes AS f group by privilege_code_name,privilege_code_name limit 2;
