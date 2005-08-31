/*
 * Copyright (c) 2004-2005 Sergey Lyubka <valenok@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

/*
 * This file is an interface for embedding the shttpd web
 * server (http://shttpd.sf.net) into an existing application.
 * shttpd.c should be added to the application sources, and -DEMBEDDED
 * must be used when compiling.
 * Flags that may be used at compilation:
 *	-DEMBEDDED      must be used
 *	-DIO_MAX=xxx    maximum request size, and maximum data size
 *                  that can be returned by the callback function
 *	-DMT            use multi-threading. That means, for every new
 *                  connection, dedicated thread is forked.
 *                  callback function then may block, outputting the
 *                  data by means of shttpd_printf() function.
 * Changelog:
 * 1.12             first appeared.
 * 1.13             shttpd_push() added
 * 1.14             __cplusplus guards added
 *                  shttpd_protect_url() added
 * 1.16             added shttpd_template().
 * 1.24             Removed shttpd_set_options(), added parameter to
 *                  shttpd_init()
 */

#ifndef SHTTPD_HEADER_INCLUDED
#define	SHTTPD_HEADER_INCLUDED

#define	SHTTPD_VERSION	"1.26"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/*
 * In order to generate pages dynamically for certain URLs, application
 * must register callback function, which will be called when user
 * requests registered URL.
 * This structure will be passed to a callback function.
 */
struct shttpd_callback_arg {
    struct conn *connection;    /* Opaque structure */
    void        *callback_data; /* User-defined data */
    char        *buf;           /* Buffer to fill */
    size_t      buflen;         /* Buffer length */
};

/*
 * Callback function prototype.
 * The following rules apply (XXX true for non-MT configuration):
 *  o the callback function is called only once for each request.
 *  o it must fill the buffer with data.
 *  o it should not block
 *  o it cannot pass more data then buflen. The buflen value can be changed
 *    at compilation time, by means of -DIO_MAX=buffer_size definition.
 * Return values:
 *  o  <= 0 if no data is copied in the buffer, or
 *  o  number of bytes copied in the buffer
 */
typedef int (*shttpd_callback_t)(struct shttpd_callback_arg *);

/*
 * shttpd_init
 *      initialize shttpd: set default mime types etc.
 *      should be called once, before using any other function.
 *      Configuration file name must be passed.
 *      If NULL is passed, default values will be used.
 *
 * shttpd_fini
 *      dealocate all resources allocated by shttpd.
 *      should be used once, usually before program termination.
 *
 * shttpd_open_port
 *      return opened socket to specified local port. This
 *      socket should be then passed to shttpd_poll().
 *      Multiple listened sockets may be opened.
 *
 * shttpd_register_url
 *      Setup the user-defined function for specified URL.
 *
 * shttpd_protect_url
 *      Associate authorization file with an URL.
 *
 * shtppd_merge_fds
 *      If the external application wants to multiplex IO with
 *      the shttpd, this function
 *      should be used. It populates application's read and
 *      write descritor sets and bumps up max_fd.
 *      Then the application may block in select(), avoiding
 *      cpu-intensive polling. On return from select(),
 *      shttpd_poll() may be used with 0 wait time.
 *
 * shttpd_poll
 *      Verify all connections. If there are requests made to
 *      registered URLs, call appropriate callback function.
 *      This function may wait for data given number of
 *      milliseconds.
 *
 * shttpd_template
 *      This is for generating pages from template files.
 *      The variable argument list is a NULL-terminated
 *      list of keyword/substitution pairs.
 *      Gets HTML template file, substitutes keywords in it.
 *      Put the generated content into IO buffer.
 *      return the number of bytes. The resulted text size
 *      is less than IO_MAX, otherwise it is truncated.
 *      The template must be a text file.
 *
 * shttpd_get_var
 *      Return variable value for given variable name.
 *      This can be used if the request is like
 *      http://my_host/url?var1=value1&var2=value2
 *      Then, shttpd_get_var(arg->connection, "var1") should
 *      return value "value1".
 *
 * shttpd_printf
 *      XXX Available only in multi-threaded configuration.
 *      Do not use this function unless you want to return
 *      very large chunks of data, more then IO_MAX.
 *      This function is the only way to pass data in MT
 *      scenario. Writing directly to supplied buffer
 *      will make no effect.
 *      XXX It is not recommended to use MT configuration.
 */

extern void shttpd_init(const char *config_file);
extern void shttpd_fini(void);
extern int shttpd_open_port(int port);
extern void shttpd_register_url(const char *url, 
                                shttpd_callback_t callback, void *callback_data);
extern void shttpd_protect_url(const char *url, const char *file);
extern void shttpd_merge_fds(fd_set *rset, fd_set *wset,int *maxfd);
extern void shttpd_poll(int sock, unsigned milliseconds);
extern char *shttpd_get_msg(struct conn *);
extern const char *shttpd_get_var(struct conn *, const char *varname);
extern int shttpd_template(struct conn *, const char *headers, const char *file, ...);

#ifdef IO_MAX
#undef IO_MAX
#endif
#define IO_MAX (1024 * 1024) /* Max request size */

#ifdef MT
extern int shttpd_printf(struct conn *, const char *fmt, ...);
extern int shttpd_push(struct conn *, const void *buf, size_t len);
#endif /* MT */

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* SHTTPD_HEADER_INCLUDED */
