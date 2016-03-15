extern FILE* embedded_stdout;
extern FILE* embedded_stderr;

#define exit(status) ((void) (status))
#undef assert
#define NDEBUG 1
#define assert(ignore) ((void) 0)
#undef stdout
#define stdout embedded_stdout
#undef stderr
#define stderr embedded_stderr

#define srand(seed) ((void) (seed))
extern int embedded_r_rand(void);
#define rand embedded_r_rand
