START TRANSACTION;

--The dump statement should normally have an auto-incremented column representing the creation order.
--But in cases of db objects that can be interdependent, i.e. sys.functions and table-likes, we need access to the underlying sequence of the AUTO_INCREMENT property.
--Because we need to explicitly overwrite the creation order column "o" in those cases. After inserting the dump statements for sys.functions and table-likes,
--we can restart the auto-increment sequence with a sensible value for following dump statements.

CREATE SEQUENCE tmp._auto_increment;
CREATE TEMPORARY TABLE dump_statements(o INT DEFAULT NEXT VALUE FOR tmp._auto_increment, s STRING, PRIMARY KEY (o));

--Because ALTER SEQUENCE statements are not allowed in procedures,
--we have to do a really nasty hack to restart the _auto_increment sequence.

CREATE FUNCTION tmp.restart_sequence(sch STRING, seq STRING, val BIGINT) RETURNS BIGINT EXTERNAL NAME sql."restart";


CREATE PROCEDURE tmp.dump_database(describe BOOLEAN)
BEGIN

    set schema sys;

	INSERT INTO dump_statements(s) VALUES ('START TRANSACTION;');
	INSERT INTO dump_statements(s) VALUES ('SET SCHEMA "sys";');

	INSERT INTO dump_statements(s) --dump_create_roles
		SELECT 'CREATE ROLE ' || sys.dq(name) || ';' FROM sys.auths
        WHERE name NOT IN (SELECT name FROM sys.db_user_info)
        AND grantor <> 0;

	INSERT INTO dump_statements(s) --dump_create_users
		SELECT
        'CREATE USER ' ||  sys.dq(ui.name) ||  ' WITH ENCRYPTED PASSWORD ' ||
            sys.sq(sys.password_hash(ui.name)) ||
        ' NAME ' || sys.sq(ui.fullname) ||  ' SCHEMA sys;'
        FROM sys.db_user_info ui, sys.schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot';

	INSERT INTO dump_statements(s) --dump_create_schemas
        SELECT
            'CREATE SCHEMA ' ||  sys.dq(s.name) || ifthenelse(a.name <> 'sysadmin', ' AUTHORIZATION ' || a.name, ' ') || ';'
        FROM sys.schemas s, sys.auths a
        WHERE s.authorization = a.id AND s.system = FALSE;

    INSERT INTO dump_statements(s) --dump_add_schemas_to_users
	    SELECT
            'ALTER USER ' || sys.dq(ui.name) || ' SET SCHEMA ' || sys.dq(s.name) || ';'
        FROM sys.db_user_info ui, sys.schemas s
        WHERE ui.default_schema = s.id
            AND ui.name <> 'monetdb'
            AND ui.name <> '.snapshot'
            AND s.name <> 'sys';

    INSERT INTO dump_statements(s) --dump_grant_user_priviledges
        SELECT
            'GRANT ' || sys.dq(a2.name) || ' ' || ifthenelse(a1.name = 'public', 'PUBLIC', sys.dq(a1.name)) || ';'
		FROM sys.auths a1, sys.auths a2, sys.user_role ur
		WHERE a1.id = ur.login_id AND a2.id = ur.role_id;

	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_sequences();

	--START OF COMPLICATED DEPENDENCY STUFF:
	--functions and table-likes can be interdependent. They should be inserted in the order of their catalogue id.
	DECLARE offs INT;
	SET offs = (SELECT max(o) FROM dump_statements) - (SELECT min(ids.id) FROM (select id from sys.tables union select id from sys.functions) ids(id));

	INSERT INTO dump_statements SELECT f.o + offs, f.stmt FROM sys.dump_functions() f;
	INSERT INTO dump_statements SELECT t.o + offs, t.stmt FROM sys.dump_tables() t;

	SET offs = (SELECT max(o) + 1 FROM dump_statements);
	DECLARE dummy_result BIGINT; --HACK: otherwise I cannot call restart_sequence.
	SET dummy_result = tmp.restart_sequence('tmp', '_auto_increment', offs);
	--END OF COMPLICATED DEPENDENCY STUFF.

	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_column_defaults();
	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_table_constraint_type();
	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_indices();
	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_foreign_keys();
	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_partition_tables();
	INSERT INTO dump_statements(s) SELECT * from sys.dump_triggers();
	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_comments();

	--We are dumping ALL privileges so we need to erase existing privileges on the receiving side;
	INSERT INTO dump_statements(s) VALUES ('TRUNCATE sys.privileges;');
	INSERT INTO dump_statements(s) SELECT * FROM sys.dump_privileges();

	--TODO User Defined Types? sys.types
	--TODO loaders ,procedures, window and filter sys.functions.
	--TODO dumping table data
	--TODO look into order dependent group_concat
	--TODO ALTER SEQUENCE using RESTART WITH after importing table_data.
	--TODO ADD upgrade code 

    INSERT INTO dump_statements(s) VALUES ('COMMIT;');

END;

CALL tmp.dump_database(TRUE);

SELECT s FROM dump_statements order by o;

ROLLBACK;
