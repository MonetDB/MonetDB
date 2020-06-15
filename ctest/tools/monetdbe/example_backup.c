
#include "monetdb_config.h"
#include <monetdbe.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main(void) 
{
	char* err = NULL;
	char *msg;
	monetdbe_database mdbe;

	if ((msg = monetdbe_open(&mdbe, NULL)) != NULL)
		error(msg);

	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (b bool, t tinyint, s smallint, x integer, l bigint, "
#ifdef HAVE_HGE
		"h hugeint, "
#else
		"h bigint, "
#endif
		"f float, d double, y string)", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (TRUE, 42, 42, 42, 42, 42, 42.42, 42.42, 'Hello'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'World')", NULL, NULL)) != NULL)
		error(err)

	msg = monetdbe_backup(mdbe, "/tmp/backup");

	if ((msg = monetdbe_close(mdbe)) != NULL)
		error(msg);
	return 0;
}
