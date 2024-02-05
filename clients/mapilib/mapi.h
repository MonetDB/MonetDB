/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#ifndef _MAPI_H_INCLUDED
#define _MAPI_H_INCLUDED 1

#include <stdio.h>		/* for FILE * */
#include <stdint.h>		/* for int64_t */
#include <stdbool.h>		/* for bool */

#include "mapi_querytype.h"

#define MAPI_SEEK_SET	0
#define MAPI_SEEK_CUR	1
#define MAPI_SEEK_END	2

typedef int MapiMsg;

#define MOK		0
#define MERROR		(-1)
#define MTIMEOUT	(-2)
#define MMORE		(-3)
#define MSERVER		(-4)
#define MREDIRECT	(-5)

enum mapi_handshake_options_levels {
	MAPI_HANDSHAKE_AUTOCOMMIT = 1,
	MAPI_HANDSHAKE_REPLY_SIZE = 2,
	MAPI_HANDSHAKE_SIZE_HEADER = 3,
	MAPI_HANDSHAKE_COLUMNAR_PROTOCOL = 4,
	MAPI_HANDSHAKE_TIME_ZONE = 5,
	// make sure to insert new option levels before this one.
	// it is the value sent by the server during the initial handshake.
	MAPI_HANDSHAKE_OPTIONS_LEVEL,
};

/*
 * The table field information is extracted from the table headers
 * obtained from the server. This list may be extended in the future.
 * The type of both the 'param' and 'binding'
 * variables refer to their underlying C-type. They are used for
 * automatic type coercion between back-end and application.
 */
typedef struct MapiStruct *Mapi;

typedef struct MapiStatement *MapiHdl;

#ifdef __cplusplus
extern "C" {
#endif

/* avoid using "#ifdef WIN32" so that this file does not need our config.h */
#if defined(_MSC_VER) || defined(__CYGWIN__) || defined(__MINGW32__)
#ifndef LIBMAPI
#define mapi_export extern __declspec(dllimport)
#else
#define mapi_export extern __declspec(dllexport)
#endif
#else
#define mapi_export extern
#endif

#ifndef __GNUC__
/* This feature is available in gcc versions 2.5 and later.  */
# ifndef __attribute__
#  define __attribute__(Spec)	/* empty */
# endif
#endif

/* connection-oriented functions */
mapi_export Mapi mapi_mapi(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname);
mapi_export Mapi mapi_mapiuri(const char *url, const char *user, const char *pass, const char *lang);
mapi_export MapiMsg mapi_destroy(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export Mapi mapi_connect(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname);
mapi_export char **mapi_resolve(const char *host, int port, const char *pattern);
mapi_export MapiMsg mapi_disconnect(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_reconnect(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_ping(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export void mapi_setfilecallback2(
	Mapi mid,
	char *(*getfunc)(void *priv, const char *filename,
			 bool binary, uint64_t offset, size_t *size),
	char *(*putfunc)(void *priv, const char *filename, bool binary,
			 const void *data, size_t size),
	void *priv)
	__attribute__((__nonnull__(1)));
mapi_export void mapi_setfilecallback(
	Mapi mid,
	char *(*getfunc)(void *priv, const char *filename,
			 bool binary, uint64_t offset, size_t *size),
	char *(*putfunc)(void *priv, const char *filename,
			 const void *data, size_t size),
	void *priv)
	__attribute__((__nonnull__(1)));

mapi_export MapiMsg mapi_error(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_error_str(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export void mapi_noexplain(Mapi mid, const char *errorprefix)
	__attribute__((__nonnull__(1)));
mapi_export void mapi_explain(Mapi mid, FILE *fd)
	__attribute__((__nonnull__(1)));
mapi_export void mapi_explain_query(MapiHdl hdl, FILE *fd)
	__attribute__((__nonnull__(1)));
mapi_export void mapi_explain_result(MapiHdl hdl, FILE *fd);
mapi_export void mapi_trace(Mapi mid, bool flag)
	__attribute__((__nonnull__(1)));
#ifdef _STREAM_H_		/* if stream.h was included */
mapi_export stream *mapi_get_from(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export stream *mapi_get_to(Mapi mid)
	__attribute__((__nonnull__(1)));
#endif
mapi_export bool mapi_get_trace(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_get_time_zone(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export bool mapi_get_autocommit(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export bool mapi_get_columnar_protocol(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_log(Mapi mid, const char *nme)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_set_time_zone(Mapi mid, int seconds_east_of_utc)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_setAutocommit(Mapi mid, bool autocommit)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_set_columnar_protocol(Mapi mid, bool columnar_protocol)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_set_size_header(Mapi mid, bool value)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_release_id(Mapi mid, int id)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_result_error(MapiHdl hdl);
mapi_export const char *mapi_result_errorcode(MapiHdl hdl);
mapi_export MapiMsg mapi_next_result(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_needmore(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export bool mapi_more_results(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiHdl mapi_new_handle(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_close_handle(MapiHdl hdl);
mapi_export MapiMsg mapi_bind(MapiHdl hdl, int fnr, char **ptr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_bind_var(MapiHdl hdl, int fnr, int type, void *ptr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_bind_numeric(MapiHdl hdl, int fnr, int scale, int precision, void *ptr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_clear_bindings(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_param_type(MapiHdl hdl, int fnr, int ctype, int sqltype, void *ptr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_param_string(MapiHdl hdl, int fnr, int sqltype, char *ptr, int *sizeptr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_param(MapiHdl hdl, int fnr, char **ptr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_param_numeric(MapiHdl hdl, int fnr, int scale, int precision, void *ptr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_clear_params(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiHdl mapi_prepare(Mapi mid, const char *cmd)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_prepare_handle(MapiHdl hdl, const char *cmd)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_execute(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_fetch_reset(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_finish(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiHdl mapi_query(Mapi mid, const char *cmd)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_query_handle(MapiHdl hdl, const char *cmd)
	__attribute__((__nonnull__(1)));
mapi_export MapiHdl mapi_query_prep(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_query_part(MapiHdl hdl, const char *cmd, size_t size)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_query_done(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_query_abort(MapiHdl hdl, int reason)
	__attribute__((__nonnull__(1)));
mapi_export MapiHdl mapi_send(Mapi mid, const char *cmd)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_read_response(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_cache_limit(Mapi mid, int limit)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_cache_freeup(MapiHdl hdl, int percentage)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_seek_row(MapiHdl hdl, int64_t rowne, int whence)
	__attribute__((__nonnull__(1)));

mapi_export MapiMsg mapi_set_timeout(Mapi mid, unsigned int timeout, bool (*callback)(void *), void *callback_data)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_timeout(Mapi mid, unsigned int time)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_fetch_row(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int64_t mapi_fetch_all_rows(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_get_field_count(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int64_t mapi_get_row_count(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int64_t mapi_get_last_id(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int64_t mapi_rows_affected(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int64_t mapi_get_querytime(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int64_t mapi_get_maloptimizertime(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int64_t mapi_get_sqloptimizertime(MapiHdl hdl)
	__attribute__((__nonnull__(1)));

mapi_export char *mapi_fetch_field(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export size_t mapi_fetch_field_len(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export MapiMsg mapi_store_field(MapiHdl hdl, int fnr, int outtype, void *outparam)
	__attribute__((__nonnull__(1)));
mapi_export char *mapi_fetch_line(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_split_line(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_get_lang(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_get_uri(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_get_dbname(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_get_host(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_get_user(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_get_mapi_version(void);
mapi_export const char *mapi_get_monet_version(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export const char *mapi_get_motd(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export bool mapi_is_connected(Mapi mid)
	__attribute__((__nonnull__(1)));
mapi_export char *mapi_get_table(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export char *mapi_get_name(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export char *mapi_get_type(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_get_len(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_get_digits(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_get_scale(MapiHdl hdl, int fnr)
	__attribute__((__nonnull__(1)));
mapi_export char *mapi_get_query(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_get_querytype(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export int mapi_get_tableid(MapiHdl hdl)
	__attribute__((__nonnull__(1)));
mapi_export char *mapi_quote(const char *msg, int size)
	__attribute__((__nonnull__(1)));
mapi_export char *mapi_unquote(char *msg)
	__attribute__((__nonnull__(1)));
mapi_export MapiHdl mapi_get_active(Mapi mid)
	__attribute__((__nonnull__(1)));
#ifdef _MSC_VER
mapi_export const char *wsaerror(int);
#endif

#ifdef __cplusplus
}
#endif
#endif				/* _MAPI_H_INCLUDED */
