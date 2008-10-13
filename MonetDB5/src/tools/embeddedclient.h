#ifndef _EMBEDDEDCLIENT_H_
#define _EMBEDDEDCLIENT_H_

/* avoid using "#ifdef WIN32" so that this file does not need our config.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#ifndef LIBEMBEDDEDMAL
#define embeddedclient_export extern __declspec(dllimport)
#else
#define embeddedclient_export extern __declspec(dllexport)
#endif
#else
#define embeddedclient_export extern
#endif

#include <stdio.h>
#include <stream.h>
#include <mapilib/Mapi.h>
#include <monet_options.h>

#ifdef __cplusplus
extern "C" {
#endif

embeddedclient_export Mapi embedded_mal(opt *set, int len);

#ifdef __cplusplus
}
#endif

#endif /* _EMBEDDEDCLIENT_H_ */
