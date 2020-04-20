select debug;
select current_schema;
select current_user;
select current_role;

declare test_nr int; --error, no variable declaration on the global scope
declare test_str varchar(1024); --error, no variable declaration on the global scope
declare test_boolean boolean; --error, no variable declaration on the global scope

set test_nr = 1;
set test_str = 'help';
set test_boolean = true;

select test_nr;
select test_str;
select test_boolean;

select sys.debug;
select sys."current_schema";
select sys."current_user";
select sys."current_role";

select @debug; --error, @ annotation no longer exists
select @"current_schema"; --error, @ annotation no longer exists
select @"current_user"; --error, @ annotation no longer exists
select @"current_role"; --error, @ annotation no longer exists

set test_nr = 'help'; --error, conversion failed
set test_str = 1;
set test_boolean = 'help';

select sys.test_nr;
select sys.test_str;
select sys.test_boolean;

select @test_nr; --error, @ annotation no longer exists
select @test_str; --error, @ annotation no longer exists
select @test_boolean; --error, @ annotation no longer exists
