#ifndef QUERY_H
#define QUERY_H

typedef enum sql_query_t {
	QPARSE = 1, 
	QTABLE = 2,
	QUPDATE = 3,
	QDATA = 4, 
	QHEADER = 5 
} sql_query_t;

#endif /* QUERY_H */
