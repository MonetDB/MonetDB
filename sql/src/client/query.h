#ifndef QUERY_H
#define QUERY_H

typedef enum sql_query_t {
	QEND = 0,
	QPARSE = 1, 
	QRESULT = 2,
	QTABLE = 3,
	QUPDATE = 4,
	QDATA = 5, 
	QCREATE = 6,
	QDEBUG = 7
} sql_query_t;

#endif /* QUERY_H */
