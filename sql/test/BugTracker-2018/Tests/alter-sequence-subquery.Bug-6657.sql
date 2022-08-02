create sequence "testme" as integer start with 2;
select get_value_for('sys', 'testme');
alter sequence "testme" restart with (select count(*));
select get_value_for('sys', 'testme');
alter sequence "testme" restart with (select 1 union select 2); --error, should not be possible
select get_value_for('sys', 'testme');
drop sequence "testme";
