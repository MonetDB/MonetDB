set schema "my_schema";

create table version 
  (name varchar(10)
  ,i int
  )
;

insert into version
  (name
  ,i
  ) 
values 
  ('test1'
  ,1
  )
;

create function insertversion(iname varchar(10)
                             ,ii int
                             ) returns int
begin

  insert into version
    (name
    ,i
    ) 
  values
    (iname
    ,ii
    )
  ;

  return 1;

end;
 
create function updateversion(iname varchar(10)
                             ,ii int
                             ) returns int
begin

  update version
     set i = ii
    where name = iname
  ;

  return 1;

end;
 
create function deleteversion(iname varchar(10)
                             ) returns int
begin

  delete 
    from version
   where name = iname
  ;

  return 1;

end;
 
-- grant right to my_user not to my_user2
GRANT SELECT on table version to my_user;
GRANT INSERT on table version to my_user;
GRANT UPDATE on table version to my_user;
GRANT DELETE on table version to my_user;

GRANT EXECUTE on function insertversion to my_user;
GRANT EXECUTE on function updateversion to my_user;
GRANT EXECUTE on function deleteversion to my_user;

GRANT EXECUTE on function insertversion to my_user2;
GRANT EXECUTE on function updateversion to my_user2;
GRANT EXECUTE on function deleteversion to my_user2;
