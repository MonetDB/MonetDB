select debug;
select current_schema;
select current_user;
select current_role;

declare test_nr int;
declare test_str varchar(1024);
declare test_boolean boolean;

set test_nr = 1;
set test_str = 'help';
set test_boolean = true;

select test_nr;
select test_str;
select test_boolean;

select @debug;
select @current_schema;
select @"current_user";
select @"current_role";

set test_nr = 'help';
set test_str = 1;
set test_boolean = 'help';

select @test_nr;
select @test_str;
select @test_boolean;

