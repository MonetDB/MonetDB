/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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

/* Does your compiler support `__attribute__' extension? */
#if defined(__has_attribute)
#if __has_attribute(__const__)
#define ATTR_CONST			__attribute__((__const__))
#endif
#if __has_attribute(__nonnull__)
#define ATTR_NONNULL(...)		__attribute__((__nonnull__(__VA_ARGS__)))
#endif
#endif

#ifndef ATTR_CONST
#define ATTR_CONST
#endif
#ifndef ATTR_NONNULL
#define ATTR_NONNULL(...)
#endif

/* connection-oriented functions */
mapi_export Mapi mapi_mapi(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname);
mapi_export Mapi mapi_mapiuri(const char *url, const char *user, const char *pass, const char *lang);
mapi_export MapiMsg mapi_destroy(Mapi mid)
	ATTR_NONNULL(1);
mapi_export Mapi mapi_connect(const char *host, int port, const char *username, const char *password, const char *lang, const char *dbname);
mapi_export char **mapi_resolve(const char *host, int port, const char *pattern);
mapi_export MapiMsg mapi_disconnect(Mapi mid)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_reconnect(Mapi mid)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_ping(Mapi mid)
	ATTR_NONNULL(1);
mapi_export void mapi_setfilecallback2(
	Mapi mid,
	char *(*getfunc)(void *priv, const char *filename,
			 bool binary, uint64_t offset, size_t *size),
	char *(*putfunc)(void *priv, const char *filename, bool binary,
			 const void *data, size_t size),
	void *priv)
	ATTR_NONNULL(1);
mapi_export void mapi_setfilecallback(
	Mapi mid,
	char *(*getfunc)(void *priv, const char *filename,
			 bool binary, uint64_t offset, size_t *size),
	char *(*putfunc)(void *priv, const char *filename,
			 const void *data, size_t size),
	void *priv)
	ATTR_NONNULL(1);
mapi_export void mapi_setclientprefix(Mapi mid, const char *prefix);

mapi_export MapiMsg mapi_error(Mapi mid)
	ATTR_NONNULL(1);
mapi_export const char *mapi_error_str(Mapi mid)
	ATTR_NONNULL(1);
mapi_export void mapi_noexplain(Mapi mid, const char *errorprefix)
	ATTR_NONNULL(1);
mapi_export void mapi_explain(Mapi mid, FILE *fd)
	ATTR_NONNULL(1);
mapi_export void mapi_explain_query(MapiHdl hdl, FILE *fd)
	ATTR_NONNULL(1);
mapi_export void mapi_explain_result(MapiHdl hdl, FILE *fd);
mapi_export void mapi_trace(Mapi mid, bool flag)
	ATTR_NONNULL(1);
#ifdef _STREAM_H_		/* if stream.h was included */
mapi_export stream *mapi_get_from(Mapi mid)
	ATTR_NONNULL(1);
mapi_export stream *mapi_get_to(Mapi mid)
	ATTR_NONNULL(1);
#endif
mapi_export bool mapi_get_trace(Mapi mid)
	ATTR_NONNULL(1);
mapi_export int mapi_get_time_zone(Mapi mid)
	ATTR_NONNULL(1);
mapi_export bool mapi_get_autocommit(Mapi mid)
	ATTR_NONNULL(1);
mapi_export bool mapi_get_columnar_protocol(Mapi mid)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_log(Mapi mid, const char *nme)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_set_time_zone(Mapi mid, int seconds_east_of_utc)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_setAutocommit(Mapi mid, bool autocommit)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_set_columnar_protocol(Mapi mid, bool columnar_protocol)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_set_size_header(Mapi mid, bool value)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_release_id(Mapi mid, int id)
	ATTR_NONNULL(1);
mapi_export const char *mapi_result_error(MapiHdl hdl);
mapi_export const char *mapi_result_errorcode(MapiHdl hdl);
mapi_export MapiMsg mapi_next_result(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_needmore(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export bool mapi_more_results(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiHdl mapi_new_handle(Mapi mid)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_close_handle(MapiHdl hdl);
mapi_export MapiMsg mapi_bind(MapiHdl hdl, int fnr, char **ptr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_bind_var(MapiHdl hdl, int fnr, int type, void *ptr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_bind_numeric(MapiHdl hdl, int fnr, int scale, int precision, void *ptr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_clear_bindings(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_param_type(MapiHdl hdl, int fnr, int ctype, int sqltype, void *ptr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_param_string(MapiHdl hdl, int fnr, int sqltype, char *ptr, int *sizeptr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_param(MapiHdl hdl, int fnr, char **ptr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_param_numeric(MapiHdl hdl, int fnr, int scale, int precision, void *ptr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_clear_params(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiHdl mapi_prepare(Mapi mid, const char *cmd)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_prepare_handle(MapiHdl hdl, const char *cmd)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_execute(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_fetch_reset(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_finish(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiHdl mapi_query(Mapi mid, const char *cmd)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_query_handle(MapiHdl hdl, const char *cmd)
	ATTR_NONNULL(1);
mapi_export MapiHdl mapi_query_prep(Mapi mid)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_query_part(MapiHdl hdl, const char *cmd, size_t size)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_query_done(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_query_abort(MapiHdl hdl, int reason)
	ATTR_NONNULL(1);
mapi_export MapiHdl mapi_send(Mapi mid, const char *cmd)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_read_response(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_cache_limit(Mapi mid, int limit)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_cache_freeup(MapiHdl hdl, int percentage)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_seek_row(MapiHdl hdl, int64_t rowne, int whence)
	ATTR_NONNULL(1);

mapi_export MapiMsg mapi_set_rtimeout(Mapi mid, unsigned int timeout, bool (*callback)(void *), void *callback_data)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_set_timeout(Mapi mid, unsigned int timeout, bool (*callback)(void *), void *callback_data)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_timeout(Mapi mid, unsigned int time)
	ATTR_NONNULL(1);
mapi_export int mapi_fetch_row(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int64_t mapi_fetch_all_rows(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int mapi_get_field_count(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int64_t mapi_get_row_count(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int64_t mapi_get_last_id(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int64_t mapi_rows_affected(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int64_t mapi_get_querytime(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int64_t mapi_get_maloptimizertime(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int64_t mapi_get_sqloptimizertime(MapiHdl hdl)
	ATTR_NONNULL(1);

mapi_export char *mapi_fetch_field(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export size_t mapi_fetch_field_len(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export MapiMsg mapi_store_field(MapiHdl hdl, int fnr, int outtype, void *outparam)
	ATTR_NONNULL(1);
mapi_export char *mapi_fetch_line(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int mapi_split_line(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export const char *mapi_get_lang(Mapi mid)
	ATTR_NONNULL(1);
mapi_export const char *mapi_get_uri(Mapi mid)
	ATTR_NONNULL(1);
mapi_export const char *mapi_get_dbname(Mapi mid)
	ATTR_NONNULL(1);
mapi_export const char *mapi_get_host(Mapi mid)
	ATTR_NONNULL(1);
mapi_export const char *mapi_get_user(Mapi mid)
	ATTR_NONNULL(1);
mapi_export const char *mapi_get_mapi_version(void)
	ATTR_CONST;
mapi_export const char *mapi_get_monet_version(Mapi mid)
	ATTR_NONNULL(1);
mapi_export const char *mapi_get_motd(Mapi mid)
	ATTR_NONNULL(1);
mapi_export bool mapi_is_connected(Mapi mid)
	ATTR_NONNULL(1);
mapi_export char *mapi_get_table(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export char *mapi_get_name(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export char *mapi_get_type(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export int mapi_get_len(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export int mapi_get_digits(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export int mapi_get_scale(MapiHdl hdl, int fnr)
	ATTR_NONNULL(1);
mapi_export char *mapi_get_query(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int mapi_get_querytype(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export int mapi_get_tableid(MapiHdl hdl)
	ATTR_NONNULL(1);
mapi_export char *mapi_quote(const char *msg, int size)
	ATTR_NONNULL(1);
mapi_export char *mapi_unquote(char *msg)
	ATTR_NONNULL(1);
mapi_export MapiHdl mapi_get_active(Mapi mid)
	ATTR_NONNULL(1);
#ifdef _MSC_VER
mapi_export const char *wsaerror(int);
#endif

#ifdef __cplusplus
}
#endif
#endif				/* _MAPI_H_INCLUDED */
