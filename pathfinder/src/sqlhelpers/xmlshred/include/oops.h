#ifndef OOPS_H__
#define OOPS_H__

#include <stdarg.h>

/* Different types of error-messages
 */ 
enum err_t {
      SH_FATAL    /**< exit with failure */
	, SH_WARNING  /**< do not exit */
	, SH_DUMP     /**< exit and write a core dump */
};
typedef enum err_t err_t;

/* Global error-message routine */
void SHoops (err_t err, const char *fmt, ...);

#endif /* OOPS_H__ */
