
#include "monetdb_config.h"
#include "stream.h"
#include "mstring.h"
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include "mapi.h"
#include <monetdb_embedded.h>

extern int dump_database(Mapi mid, stream *toConsole, bool describe, bool useInserts);

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main() 
{
	char* err = NULL;
	Mapi mid = (Mapi)malloc(sizeof(struct MapiStruct));

	if ((mid->msg = monetdb_startup(NULL, 0)) != NULL)
		error(mid->msg);
	if ((mid->msg = monetdb_connect(&mid->conn)) != NULL)
		error(mid->msg);

	if ((err = monetdb_query(mid->conn, "CREATE TABLE test (b bool, t tinyint, s smallint, x integer, l bigint, "
#if HAVE_HGE
		"h hugeint, "
#else
		"h bigint, "
#endif
		"f float, d double, y string)", NULL, NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdb_query(mid->conn, "INSERT INTO test VALUES (TRUE, 42, 42, 42, 42, 42, 42.42, 42.42, 'Hello'), (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'World')", NULL, NULL, NULL)) != NULL)
		error(err)

	/* open file stream */
	stream *fd = open_wastream("/tmp/backup");

	if (dump_database(mid, fd, 0, 0)) {
		if (mid->msg)
			error(mid->msg)
		fprintf(stderr, "database backup failed\n");
	}
	close_stream(fd);

	if ((mid->msg = monetdb_disconnect(mid->conn)) != NULL)
		error(mid->msg);
	if ((mid->msg = monetdb_shutdown()) != NULL)
		error(mid->msg);
}
