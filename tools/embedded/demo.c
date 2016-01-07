#include "monetdb_config.h"
#include "monet_options.h"
#include "sql_scenario.h"
#include "mal.h"
#include "embedded.h"
#include <locale.h>

/*
 configure & install MonetDB as follows:

./configure --prefix=/tmp/embedded-install --enable-embedded \
--disable-fits --disable-geom --disable-rintegration --disable-gsl --disable-netcdf \
--disable-jdbc --disable-merocontrol --disable-odbc --disable-console --disable-microhttpd \
--without-perl --without-python2 --without-python3 --without-rubygem --without-unixodbc \
--without-samtools --without-sphinxclient --without-geos --without-samtools --without-readline \
--enable-debug --enable-silent-rules --disable-assert --disable-strict --disable-int128
make -j clean install

then build this file as follows:

gcc demo.c -I../../monetdb5/mal -I../../gdk -I../../common/stream -I../../common/options \
-I../../ -I ../../sql/backends/monet5 -I../../sql/include -I../../monetdb5/modules/atoms \
-I../../sql/server -I ../../sql/common -I../../sql/storage -I../../clients/mapilib \
-I../../monetdb5/modules/mal -lembedded -lbat -L/tmp/embedded-install/lib -g

*/

int main() {
	char* err = NULL;
	void* conn = NULL;
	res_table* result = NULL;

	// we want to get rid of first argument, this is why we want to inline mal/sql scripts and have fat library
	err = monetdb_startup("/dev/null", "/tmp/embedded-dbfarm", 1);
	if (err != NULL) {
		fprintf(stderr, "Init fail: %s\n", err);
		return -1;
	}
	conn = monetdb_connect();
	err = monetdb_query(conn, "SELECT * FROM tables;", (void**) &result);
	if (err != NULL) {
		fprintf(stderr, "Query fail: %s\n", err);
		return -2;
	}
	fprintf(stderr, "Query result with %i cols and %lu rows\n", result->nr_cols, BATcount(BATdescriptor(result->cols[0].b)));
	monetdb_disconnect(conn);
	monetdb_shutdown();
}
