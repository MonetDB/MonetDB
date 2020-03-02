
#define ID(a)				a
#define glue2(a, b)			a ## b
#define glue(a, b, c)		a ## b ## c
#define glue4(a, b, c, d)	a ## b ## c ## d
#define CONCAT2(a, b)		glue2(a, b)
#define CONCAT3(a, b, c)	glue(a, b, c)
#define CONCAT4(a, b, c, d)	glue4(a, b, c, d)
#define CONCAT6(a, b, c, d, e, f)	CONCAT2(glue(a, b, c), glue(d, e, f))
#define _STRINGIFY(ARG)		#ARG
#define STRINGIFY(ARG)		_STRINGIFY(ARG)
