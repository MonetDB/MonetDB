create table quser (
    quser_id int not null auto_increment,
    user_name varchar(100) not null,
    uid int not null, 
    emailaddress varchar(255) not null,
    password varchar(64) not null,
    enabled boolean not null default true,
    constraint quser_quser_id_pk primary key ( quser_id ),
    constraint user_name_unq unique ( user_name ) 
);

create table query (
    query_id int not null auto_increment,
    quser_id int null,
    query_text varchar(1024) not null,
    is_example int not null default 0,
    is_refused boolean null,
    aborted boolean null,
    returned_error varchar(100) null,
    num_results int null,
    header blob null,
    result_set blob null,
    constraint query_query_id_pk primary key ( query_id ),
    constraint query_quser_id_fk foreign key ( quser_id ) references quser ( quser_id )
);

create table ssqq_queue (
    queue_id int not null auto_increment,
    query_id int not null,
    queue_add timestamp not null default current_timestamp(),
    queue_number int not null default 0,
    os_version varchar(64) null,
    monetdb_version varchar(64) null,
    start_query timestamp null,
    query_ready timestamp null,
    constraint queue_queue_id_pk primary key ( queue_id ),
    constraint queue_query_id_fk foreign key ( query_id ) references query ( query_id )
);

create function insert_quser(name_user varchar(100), addressemail varchar(256),
                             diu int, password_value varchar(64))
returns integer
begin
    declare id_quser integer;
    set id_quser = -1;

    insert into quser (
        user_name,
        uid,
        emailaddress,
        password )
    values (
        name_user,
        diu,
        addressemail,
        password_value );

    set id_quser = (select max(quser_id)
                    from quser);
    
    return id_quser;
end;

create function insert_query(id_quser1 int, text_query varchar(1024), 
                             example int, refused boolean) 
returns integer 
begin
    declare id_query integer;
    set id_query = -1;

    insert into query (
        quser_id,
        query_text,
        is_example,
        is_refused )
    values (
        id_quser1,
        text_query,
        example,
        refused
    );

    set id_query = (select max(query_id)
                    from query);
  
    return id_query;
end;

create function insert_queue(id_query1 integer, version_os varchar(64),
                             version_monetdb varchar(64))
returns integer
begin
    declare id_queue integer;
    set id_queue = -1;

    insert into ssqq_queue (
        query_id,
        os_version,
        monetdb_version )
    values (
        id_query1,
        version_os,
        version_monetdb );

    set id_queue = (select max(queue_id)
                    from ssqq_queue);

    return id_queue;
end;

create function next_query(version_os varchar(64),
                           version_monetdb varchar(64))
returns integer
begin
    declare id_query integer;
    declare id_queue integer;

    set id_query = null;
    set id_queue = null;

    set id_query = ( select min(t1.query_id)
                     from   query as t1 
                       left outer join ssqq_queue as t2
                         on t1.query_id = t2.query_id
                     where  t1.is_refused = false
                     and    t2.queue_id is null
                     and    t1.num_results is null );

    if id_query is not null then 
        set id_queue = ( select insert_queue ( id_query, 
                                               version_os, 
                                               version_monetdb ));
    end if;

    return id_queue;
end;

create function next_queue()
returns integer
begin
    declare id_queue integer;
    set id_queue = null;

    set id_queue = ( select min(queue_id)
                     from   ssqq_queue
                     where  start_query is null );

    if id_queue is not null then 
        update ssqq_queue
            set start_query = current_timestamp()
        where  queue_id = id_queue;
    end if;

    return id_queue;
end;

create function get_query(id_queue1 int)
returns table( query_id integer,
               query_text varchar(1024) )
begin
    return table ( select t1.query_id,
                          t1.query_text
                   from   query as t1,
                          ssqq_queue as t2
                   where  t1.query_id = t2.query_id
                   and    t2.queue_id = id_queue1
    );
end;

create function set_query_result(id_queue2 integer,
                                 is_aborted boolean,
                                 error_returned varchar(100),
                                 results_num integer,
                                 set_header blob,
                                 set_result blob) 
returns boolean
begin
    declare return_value boolean;
    declare id_query integer;
    set return_value = false;
    set id_query = ( select query_id
                     from   ssqq_queue
                     where  queue_id = id_queue2 );

    update query
        set aborted = is_aborted,
            returned_error = error_returned,
            num_results = results_num,
            header = set_header,
            result_set = set_result
    where query_id = id_query;

    update ssqq_queue
        set query_ready = current_timestamp()
    where queue_id = id_queue2;
 
    if ((not is_aborted) and (error_returned is not null)) then
        set return_value = true;
    end if;

    return return_value;
end;

create function next_queue_id()
returns table (
    id_queue integer
)
begin
    return
        select min(queue_id)
        from   ssqq_queue
        where  start_query is null;
end;

create function next_queue_bug2614()
returns integer
begin
    declare id_next integer;

    set id_next = ( select id_queue from next_queue_id() );

    if id_next is not null then 
        update ssqq_queue
            set start_query = current_timestamp()
        where  queue_id = id_next;
    end if;

    return id_next;
end;

/*
create function show_queue()
returns table (
    queue_id int,
    query_id int,
    user_name varchar(100),
    queue_add timestamp,
    queue_number int,
    start_query timestamp,
    query_ready timestamp
)
begin
    return 
        with a as (
            select e.queue_id,
                   e.query_id,
                   u.user_name,
                   e.queue_add,
                   e.queue_number,
                   e.start_query,
                   e.query_ready
            from   ssqq_queue as e,
                   query as q
                   left join quser as u 
                     on q.quser_id = u.quser_id
            where  e.query_id = q.query_id
        )
        select * 
        from   a     
        order by a.queue_id desc;
end;
*/
/*
create function show_query()
returns table (
begin
    return (

    );
end;
*/
create function show_query_result(id_query int)
returns table (
    query_id int,
    user_name varchar(100),
    emailaddress varchar(255),
    query_text varchar(1024),
    is_example int,
    is_refused boolean,
    aborted boolean,
    returned_error varchar(100),
    num_results int,
    header blob,
    result_set blob,
    queue_add timestamp,
    queue_number int,
    os_version varchar(64),
    monetdb_version varchar(64),
    start_query timestamp,
    query_ready timestamp
)
begin
return (
    select id_query,
           u.user_name,
           u.emailaddress,
           q.query_text,
           q.is_example,
           q.is_refused,
           q.aborted,
           q.returned_error,
           q.num_results,
           q.header,
           q.result_set,
           e.queue_add,
           e.queue_number,
           e.os_version,
           e.monetdb_version,
           e.start_query,
           e.query_ready
    from   query as q
    left join quser as u
      on q.quser_id = u.quser_id
    left join ssqq_queue as e
      on q.query_id = e.query_id
    where  q.query_id = id_query
);
end;
