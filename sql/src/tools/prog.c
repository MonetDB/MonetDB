#include "embeddedclient.h"

int
main(int argc, char **argv)
{
	Mapi mid;
	MapiHdl hdl;
	char *line;

	(void) argc;
	mid = embedded_sql();
	while (*++argv) {
		hdl = mapi_query(mid, *argv);
		do {
			if (mapi_result_error(hdl) != NULL)
				mapi_explain_result(hdl, stderr);
			while ((line = mapi_fetch_line(hdl)) != NULL)
				printf("%s\n", line);
		} while (mapi_next_result(hdl) == 1);
		mapi_close_handle(hdl);
	}
	return 0;
}
