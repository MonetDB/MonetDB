#ifndef SQL_KEYWORD_H 
#define SQL_KEYWORD_H 

/* we need to define these here as the parser header file is generated to late.
 * The numbers get remapped in the scanner. 
 */
#define KW_ALIAS 4000
#define KW_TYPE  4001

typedef struct keyword {
	char *keyword;
	int len;
	int token;
	struct keyword *next;
} keyword;

#define HASH_SIZE 512
#define HASH_MASK (HASH_SIZE-1)

extern int keywords_init_done;
extern keyword *keywords[HASH_SIZE];

extern int keyword_key(char *k, int *l);
sqlcommon_export void keywords_insert(char *k, int token);
sqlcommon_export keyword * find_keyword(char *text);
extern int keyword_exists(char *text);

sqlcommon_export void keyword_init();
sqlcommon_export void keyword_exit();

#endif /* SQL_KEYWORD_H */

