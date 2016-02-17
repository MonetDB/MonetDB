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
static FILE* embedded_stdout;
static FILE* embedded_stderr;

#define exit(status) {(void) status;}
#define srand(seed) {(void) seed;}
#undef assert
#define assert(status) {(void) (status);}
#undef stdout
#define stdout embedded_stdout
#undef stderr
#define stderr embedded_stderr
#endif
