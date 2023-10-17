#include <stdbool.h>

#define MP__BOOL_START (100)
#define MP__LONG_START (200)
#define MP__STRING_START (300)

typedef enum mparm {
	MP_UNKNOWN,
	MP_IGNORE,

        // bool
        MP_TLS = MP__BOOL_START,
        MP_AUTOCOMMIT,

        // long
        MP_PORT = MP__LONG_START,
        MP_TIMEZONE,
        MP_REPLYSIZE,

        // string
        MP_SOCK = MP__STRING_START,
	MP_SOCKDIR,
        MP_CERT,
        MP_CLIENTKEY,
        MP_CLIENTCERT,
        MP_HOST,
        MP_DATABASE,
        MP_TABLESCHEMA,
        MP_TABLE,
        MP_CERTHASH,
        MP_USER,
        MP_PASSWORD,
        MP_LANGUAGE,
        MP_SCHEMA,
        MP_BINARY,
} mparm;

typedef enum mparm_class {
	MPCLASS_BOOL,
	MPCLASS_LONG,
	MPCLASS_STRING,
} mparm_class;

static inline mparm_class
mparm_classify(mparm parm)
{
	if (parm < MP__LONG_START)
		return MPCLASS_BOOL;
	else if (parm >= MP__STRING_START)
		return MPCLASS_STRING;
	else
		return MPCLASS_LONG;
}


/* returns NULL if not found, pointer to mparm if found */
mparm mparm_parse(const char *name);
const char *mparm_name(mparm parm);
bool mparm_is_core(mparm parm);

typedef struct msettings msettings;

/* NULL means OK. non-NULL is error message. Valid until next call. Do not free. */
typedef const char *msettings_error;

/* returns NULL if could not allocate */
msettings *msettings_create(void);
msettings *msettings_clone(const msettings *mp);
extern const msettings *msettings_default;

/* always returns NULL */
msettings *msettings_destroy(msettings *mp);

/* retrieve and set; call abort() on type error */

const char* msetting_string(const msettings *mp, mparm parm);
msettings_error msetting_set_string(msettings *mp, mparm parm, const char* value)
	__attribute__((__nonnull__(3)));

long msetting_long(const msettings *mp, mparm parm);
msettings_error msetting_set_long(msettings *mp, mparm parm, long value);

bool msetting_bool(const msettings *mp, mparm parm);
msettings_error msetting_set_bool(msettings *mp, mparm parm, bool value);

/* parse into the appropriate type, or format into newly malloc'ed string (NULL means malloc failed) */
msettings_error msetting_parse(msettings *mp, mparm parm, const char *text);
char *msetting_as_string(msettings *mp, mparm parm);

/* store ignored parameter */
msettings_error msetting_set_ignored(msettings *mp, const char *key, const char *value);

/* store named parameter */
msettings_error msetting_set_named(msettings *mp, bool allow_core, const char *key, const char *value);

/* update the msettings from the URL. set *error_buffer to NULL and return true
 * if success, set *error_buffer to malloc'ed error message and return false on failure.
 * if return value is true but *error_buffer is NULL, malloc failed. */
bool msettings_parse_url(msettings *mp, const char *url, char **error_buffer);

/* 1 = true, 0 = false, -1 = could not parse */
int msetting_parse_bool(const char *text);

/* return an error message if the validity rules are not satisfied */
bool msettings_validate(msettings *mp, char **errmsg);


/* virtual parameters */
enum msetting_tls_verify {
	verify_none,
	verify_system,
	verify_cert,
	verify_hash,
};
bool msettings_connect_scan(const msettings *mp);
const char *msettings_connect_sockdir(const msettings *mp);
const char *msettings_connect_unix(const msettings *mp);
const char *msettings_connect_tcp(const msettings *mp);
long msettings_connect_port(const msettings *mp);
const char *msettings_connect_certhash_digits(const msettings *mp);
long msettings_connect_binary(const msettings *mp);
enum msetting_tls_verify msettings_connect_tls_verify(const msettings *mp);

/* automatically incremented each time the corresponding field is updated */
long msettings_user_generation(const msettings *mp);
long msettings_password_generation(const msettings *mp);

/* convenience helpers*/
bool msettings_lang_is_mal(const msettings *mp);
bool msettings_lang_is_sql(const msettings *mp);
bool msettings_lang_is_profiler(const msettings *mp);
