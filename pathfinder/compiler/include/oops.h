/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file oops.h stack-based error handling
 *
 * $Id$
 */

#ifndef OOPS_H
#define OOPS_H

#include "pathfinder.h"

/**
 * If you add error codes here, remember to update
 * the #oops_msg[] table in file oops.c!
 */
typedef enum {
      OK = 0                        /**< no error */
    , OOPS_OK = 0                   /**< dito */
    , OOPS_FATAL = -1               /**< generic fatal error condition */
    , OOPS_NOTICE = -2              /**< notice (no error) */
    , OOPS_UNKNOWNERROR = -3        /**< Unknown error */
    , OOPS_CMDLINEARGS = -4         /**< misformed command line argument(s) */
    , OOPS_PARSE = -5               /**< parse error */
    , OOPS_OUTOFMEM = -6            /**< memory allocation failure */
    , OOPS_BADNS = -7               /**< bad usage of XML namespaces */
    , OOPS_UNKNOWNVAR = -8          /**< variable(s) out of scope or unknown */
    , OOPS_NESTDEPTH = -9           /**< query nested too deeply */
    , OOPS_NOCONTEXT = -10          /**< illegal reference to context node */
    , OOPS_NOSERVICE = -11          /**< invalid TCP port (privileged?) */
    , OOPS_TAGMISMATCH = -12        /**< XML start/end tags do not match */
    , OOPS_NOTPRETTY = -13          /**< prettyprinting problem */
    , OOPS_APPLYERROR = -14         /**< error in function application */
    , OOPS_FUNCREDEF = -15          /**< function redefined */
    , OOPS_DUPLICATE_KEY = -16      /**< duplicate key in environment */
    , OOPS_TYPENOTDEF = -17         /**< use of undefined type */
    , OOPS_TYPEREDEF = -18          /**< dup type names in one symbol space */
    , OOPS_TYPECHECK = -19          /**< type error */
    , OOPS_SCHEMAIMPORT = -20       /**< XML Schema import */
    , OOPS_WARNING = -100           /**< only warnings below */
    , OOPS_WARN_NOTSUPPORTED = -101 /**< unsupported feature */
    , OOPS_WARN_VARREUSE = -102     /**< variable reuse */
} PFrc_t;


#define PFoops(rc,...) \
    PFoops_ ((rc), __FILE__, __func__, __LINE__, __VA_ARGS__)

void PFoops_ (PFrc_t, 
              const char*, const char*, const int,
              const char *, ...)
    __attribute__ ((format (printf, 5, 6)))
    __attribute__ ((noreturn));

#define PFoops_loc(rc,loc,...) \
    PFoops_loc_ ((rc), (loc), __FILE__, __func__, __LINE__, __VA_ARGS__)

void PFoops_loc_ (PFrc_t, PFloc_t, 
                  const char*, const char*, const int, 
                  const char *, ...)
    __attribute__ ((format (printf, 6, 7)))
    __attribute__ ((noreturn));

void PFlog (const char *, ...)
    __attribute__ ((format (printf, 1, 2)));


void PFinfo (PFrc_t, const char *, ...)
    __attribute__ ((format (printf, 2, 3)));

void PFinfo_loc (PFrc_t, PFloc_t, const char *, ...)
    __attribute__ ((format (printf, 3, 4)));


#endif	/* OOPS_H */

/* vim:set shiftwidth=4 expandtab: */
