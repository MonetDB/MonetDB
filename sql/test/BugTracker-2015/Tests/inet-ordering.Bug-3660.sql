start transaction;

create table "bug"
(
    address     inet
);

insert into "bug" values('1.0.0.11');
insert into "bug" values('2.0.0.10');
insert into "bug" values('3.0.0.9');
insert into "bug" values('4.0.0.8');
insert into "bug" values('5.0.0.7');
insert into "bug" values('6.0.0.6');
insert into "bug" values('7.0.0.5');
insert into "bug" values('8.0.0.4');
insert into "bug" values('9.0.0.3');
insert into "bug" values('10.0.0.2');
insert into "bug" values('11.0.0.1');

select count(*) from bug where address>=inet'4.0.0.0';
select count(*) from bug where address<inet'8.0.0.0';
select count(*) from bug where address>=inet'4.0.0.0' and address<inet'8.0.0.0';

rollback;
