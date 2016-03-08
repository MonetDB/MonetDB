#undef HAVE_OPENSSL
#undef HAVE_UUID
#undef HAVE_CURL
#undef HAVE_LIBBZ2
#undef HAVE_LIBLZMA
#undef HAVE_FITS
#undef HAVE_GSL
#undef HAVE_LIBXML
#undef HAVE_NETCDF
#undef HAVE_SAMTOOLS

#ifdef HAVE_EMBEDDED
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
#endif

#ifdef HAVE_EMBEDDED_R
#define srand(seed) ((void) (seed))
extern int embedded_r_rand(void);
#define rand embedded_r_rand
#endif
