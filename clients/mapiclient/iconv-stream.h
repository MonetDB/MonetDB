#include <iconv.h>

struct ic_priv_t {
	stream *s;
	iconv_t cd;
	bool eof:1;
	bool err:1;
	size_t buflen;
	char buffer[BUFSIZ];
};

static ssize_t
ic_read(void *restrict private, void *restrict buf, size_t elmsize, size_t cnt)
{
	struct ic_priv_t *ic = private;
	char *inbuf = ic->buffer;
	size_t inbytesleft = ic->buflen;
	char *outbuf = buf;
	size_t outbytesleft = elmsize * cnt;

	if (outbytesleft == 0)
		return 0;
	while (outbytesleft > 0 && !ic->eof) {
		if (ic->buflen == sizeof(ic->buffer)) {
			/* ridiculously long multibyte sequence, return error */
			fprintf(stderr, "multibyte sequence too long\n");
			return -1;
		}

		switch (mnstr_read(ic->s, ic->buffer + ic->buflen, 1, 1)) {
		case 1:
			/* expected: read one byte */
			ic->buflen++;
			inbytesleft++;
			break;
		case 0:
			/* end of file */
			ic->eof = true;
			if (ic->buflen > 0) {
				/* incomplete input */
				fprintf(stderr, "incomplete input\n");
				return -1;
			}
			if (iconv(ic->cd, NULL, NULL, &outbuf, &outbytesleft) == (size_t) -1) {
				/* some error occurred */
				fprintf(stderr, "iconv reported an error\n");
				return -1;
			}
			goto exit_func;	/* double break */
		default:
			/* error */
			if (mnstr_peek_error(ic->s))
				fprintf(stderr, "%s\n", mnstr_peek_error(ic->s));
			mnstr_clearerr(ic->s);
			return -1;
		}
		if (iconv(ic->cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft) == (size_t) -1) {
			switch (errno) {
			case EILSEQ:
				fprintf(stderr, "invalid multibyte sequence\n");
				return -1;
			case EINVAL:
				/* incomplete multibyte sequence encountered */
				break;
			case E2BIG:
				/* not enough space in output buffer,
				 * return what we have, saving what's in
				 * the buffer */
				goto exit_func;
			default:
				fprintf(stderr, "iconv reported an error\n");
				return -1;
			}
		}
		if (inbytesleft == 0) {
			/* converted complete buffer */
			inbuf = ic->buffer;
			ic->buflen = 0;
		}
	}
  exit_func:
	if (inbuf > ic->buffer)
		memmove(ic->buffer, inbuf, inbytesleft);
	ic->buflen = inbytesleft;
	if (outbytesleft == elmsize * cnt && !mnstr_eof(ic->s)) {
		/* if we're returning data, we must pass on EOF on the
		 * next call (i.e. keep ic->eof set), otherwise we
		 * must clear it so that the next call will cause the
		 * underlying stream to be read again */
		ic->eof = false;
	}
	return (ssize_t) ((elmsize * cnt - outbytesleft) / elmsize);
}

static ssize_t
ic_write(void *restrict private, const void *restrict buf, size_t elmsize, size_t cnt)
{
	struct ic_priv_t *ic = private;
	char *inbuf = (char *) buf;	/* iconv requires non-const */
	size_t inbytesleft = elmsize * cnt;
	char *bf = NULL;

	if (ic == NULL) {
		fprintf(stderr, "stream already ended\n");
		return -1;
	}

	if (ic->err) {
		for (size_t i = 0; i < inbytesleft; i++)
			if (inbuf[i] == '\n') {
				ic->err = false;
				break;
			}
		return -1;
	}

	/* if unconverted data from a previous call remains, add it to
	 * the start of the new data, using temporary space */
	if (ic->buflen > 0) {
		bf = malloc(ic->buflen + inbytesleft);
		if (bf == NULL) {
			/* cannot allocate memory */
			fprintf(stderr, "out of memory\n");
			goto bailout;
		}
		memcpy(bf, ic->buffer, ic->buflen);
		memcpy(bf + ic->buflen, buf, inbytesleft);
		inbuf = bf;
		inbytesleft += ic->buflen;
		ic->buflen = 0;
	}
	while (inbytesleft > 0) {
		char *outbuf = ic->buffer;
		size_t outbytesleft = sizeof(ic->buffer);

		if (iconv(ic->cd, &inbuf, &inbytesleft, &outbuf, &outbytesleft) == (size_t) -1) {
			switch (errno) {
			case EILSEQ:
				/* invalid multibyte sequence encountered */
				fprintf(stderr, "invalid multibyte sequence\n");
				goto bailout;
			case EINVAL:
				/* incomplete multibyte sequence
				 * encountered flush what has been
				 * converted */
				if (outbytesleft < sizeof(ic->buffer) &&
				    mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0) {
					fprintf(stderr, "incomplete multibyte sequence\n");
					goto bailout;
				}
				/* remember what hasn't been converted */
				if (inbytesleft > sizeof(ic->buffer)) {
					/* ridiculously long multibyte
					 * sequence, so return
					 * error */
					fprintf(stderr, "multibyte sequence too long\n");
					goto bailout;
				}
				memcpy(ic->buffer, inbuf, inbytesleft);
				ic->buflen = inbytesleft;
				if (bf)
					free(bf);
				return (ssize_t) cnt;
			case E2BIG:
				/* not enough space in output buffer */
				break;
			default:
				fprintf(stderr, "iconv reported an error\n");
				goto bailout;
			}
		}
		if (mnstr_write(ic->s, ic->buffer, 1, sizeof(ic->buffer) - outbytesleft) < 0) {
			const char *e = mnstr_peek_error(ic->s);
			if (e)
				fprintf(stderr, "%s\n", e);
			mnstr_clearerr(ic->s);
			goto bailout;
		}
	}
	if (bf)
		free(bf);
	return (ssize_t) cnt;

  bailout:
	if (bf)
		free(bf);
	ic->err = true;
	return -1;
}

static void
ic_close(void *private)
{
	struct ic_priv_t *ic = private;
	if (ic->cd != (iconv_t) -1)
		iconv_close(ic->cd);
	ic->cd = (iconv_t) -1;
	mnstr_close(ic->s);
}

static void
ic_destroy(void *private)
{
	ic_close(private);
	mnstr_destroy(((struct ic_priv_t *) private)->s);
	free(private);
}

static stream *
iconv_rstream(stream *restrict ss, const char *restrict charset, const char *restrict name)
{
	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;

	struct ic_priv_t *priv = malloc(sizeof(struct ic_priv_t));
	if (priv == NULL) {
		fprintf(stderr, "Cannot allocate memory\n");
		return NULL;
	}
	*priv = (struct ic_priv_t) {
		.s = ss,
		.cd = iconv_open("utf-8", charset),
	};
	if (priv->cd == (iconv_t) -1) {
		free(priv);
		fprintf(stderr, "Cannot initiate character set translation from %s\n",
				charset);
		return NULL;
	}
	stream *s = callback_stream(priv, ic_read, NULL, ic_close, ic_destroy, name);
	if (s == NULL) {
		fprintf(stderr, "Cannot allocate memory\n");
		iconv_close(priv->cd);
		free(priv);
		return NULL;
	}
	return s;
}

static stream *
iconv_wstream(stream *restrict ss, const char *restrict charset, const char *restrict name)
{
	if (ss == NULL || charset == NULL || name == NULL)
		return NULL;

	struct ic_priv_t *priv = malloc(sizeof(struct ic_priv_t));
	if (priv == NULL) {
		fprintf(stderr, "Cannot allocate memory\n");
		return NULL;
	}
	*priv = (struct ic_priv_t) {
		.s = ss,
		.cd = iconv_open(charset, "utf-8"),
	};
	if (priv->cd == (iconv_t) -1) {
		free(priv);
		fprintf(stderr, "Cannot initiate character set translation from %s\n",
				charset);
		return NULL;
	}
	stream *s = callback_stream(priv, NULL, ic_write, ic_close, ic_destroy, name);
	if (s == NULL) {
		fprintf(stderr, "Cannot allocate memory\n");
		iconv_close(priv->cd);
		free(priv);
		return NULL;
	}
	return s;
}
