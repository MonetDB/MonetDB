#ifndef QUERY_H
#define QUERY_H

typedef enum sql_query_t {
	Q_END = 0,
	Q_PARSE = 1, 
	Q_RESULT = 2,
	Q_TABLE = 3,
	Q_UPDATE = 4,
	Q_DATA = 5, 
	Q_SCHEMA = 6,
	Q_TRANS = 7,
	Q_DEBUG = 8,
	Q_DEBUGP = 9
} sql_query_t;

#endif /* QUERY_H */
