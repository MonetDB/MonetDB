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

set test_nr = 'help'; --error, conversion failed
set test_str = 1; --error, variable no longer in cache
set test_boolean = 'help'; --error, variable no longer in cache

select @test_nr; --error, variable no longer in cache
select @test_str; --error, variable no longer in cache
select @test_boolean; --error, variable no longer in cache

