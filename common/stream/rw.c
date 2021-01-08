/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "stream.h"
#include "stream_internal.h"



int
mnstr_readChr(stream *restrict s, char *restrict val)
{
	if (s == NULL || val == NULL)
		return -1;
	return (int) s->read(s, (void *) val, sizeof(*val), 1);
}

int
mnstr_writeChr(stream *s, char val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readBte(stream *restrict s, int8_t *restrict val)
{
	if (s == NULL || val == NULL)
		return -1;
	return (int) s->read(s, (void *) val, sizeof(*val), 1);
}

int
mnstr_writeBte(stream *s, int8_t val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	return s->write(s, (void *) &val, sizeof(val), 1) == 1;
}

int
mnstr_readSht(stream *restrict s, int16_t *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = short_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeSht(stream *s, int16_t val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = short_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_readInt(stream *restrict s, int *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = normal_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeInt(stream *s, int val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = normal_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_writeStr(stream *restrict s, const char *restrict val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	return s->write(s, (void *) val, strlen(val), (size_t) 1) == 1;
}

int
mnstr_readStr(stream *restrict s, char *restrict val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	do {
		if (mnstr_readChr(s, val) != 1) {
			return -1;
		}
		val++;
	} while (*(val - 1) != '\0');
	return 1;
}


int
mnstr_readLng(stream *restrict s, int64_t *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = long_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeLng(stream *s, int64_t val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = long_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_writeFlt(stream *s, float val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	assert(s->binary);
	return s->write(s, &val, sizeof(val), 1) == 1;
}

int
mnstr_writeDbl(stream *s, double val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	assert(s->binary);
	return s->write(s, &val, sizeof(val), 1) == 1;
}


#ifdef HAVE_HGE
int
mnstr_readHge(stream *restrict s, hge *restrict val)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	switch (s->read(s, val, sizeof(*val), 1)) {
	case 1:
		if (s->swapbytes)
			*val = huge_int_SWAP(*val);
		return 1;
	case 0:
		return 0;
	default:		/* -1 */
		return -1;
	}
}

int
mnstr_writeHge(stream *s, hge val)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return 0;
	assert(s->binary);
	if (s->swapbytes)
		val = huge_int_SWAP(val);
	return s->write(s, &val, sizeof(val), 1) == 1;
}
#endif

int
mnstr_readBteArray(stream *restrict s, int8_t *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;

	if (s->read(s, (void *) val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errkind == MNSTR_NO__ERROR)
			mnstr_set_error(s, MNSTR_UNEXPECTED_EOF, NULL);
		return 0;
	}

	return 1;
}

int
mnstr_writeBteArray(stream *restrict s, const int8_t *restrict val, size_t cnt)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR || val == NULL)
		return 0;
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readShtArray(stream *restrict s, int16_t *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errkind == MNSTR_NO__ERROR)
			mnstr_set_error(s, MNSTR_UNEXPECTED_EOF, NULL);
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = short_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeShtArray(stream *restrict s, const int16_t *restrict val, size_t cnt)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeSht(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readIntArray(stream *restrict s, int *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errkind == MNSTR_NO__ERROR)
			mnstr_set_error(s, MNSTR_UNEXPECTED_EOF, NULL);
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = normal_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeIntArray(stream *restrict s, const int *restrict val, size_t cnt)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeInt(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

int
mnstr_readLngArray(stream *restrict s, int64_t *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errkind == MNSTR_NO__ERROR)
			mnstr_set_error(s, MNSTR_UNEXPECTED_EOF, NULL);
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = long_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeLngArray(stream *restrict s, const int64_t *restrict val, size_t cnt)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeLng(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}

#ifdef HAVE_HGE
int
mnstr_readHgeArray(stream *restrict s, hge *restrict val, size_t cnt)
{
	if (s == NULL || val == NULL)
		return 0;
	assert(s->binary);
	if (s->read(s, val, sizeof(*val), cnt) < (ssize_t) cnt) {
		if (s->errkind == MNSTR_NO__ERROR)
			mnstr_set_error(s, MNSTR_UNEXPECTED_EOF, NULL);
		return 0;
	}
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++, val++)
			*val = huge_int_SWAP(*val);
	}
	return 1;
}

int
mnstr_writeHgeArray(stream *restrict s, const hge *restrict val, size_t cnt)
{
	if (s == NULL || s->errkind != MNSTR_NO__ERROR || val == NULL)
		return 0;
	assert(s->binary);
	if (s->swapbytes) {
		for (size_t i = 0; i < cnt; i++)
			if (!mnstr_writeHge(s, val[i]))
				return 0;
		return 1;
	}
	return s->write(s, val, sizeof(*val), cnt) == (ssize_t) cnt;
}
#endif

int
mnstr_printf(stream *restrict s, const char *restrict format, ...)
{
	char buf[512], *bf = buf;
	int i = 0;
	size_t bfsz = sizeof(buf);
	va_list ap;

	if (s == NULL || s->errkind != MNSTR_NO__ERROR)
		return -1;

	va_start(ap, format);
	i = vsnprintf(bf, bfsz, format, ap);
	va_end(ap);
	while (i < 0 || (size_t) i >= bfsz) {
		if (i >= 0)	/* glibc 2.1 */
			bfsz = (size_t) i + 1;	/* precisely what is needed */
		else		/* glibc 2.0 */
			bfsz *= 2;	/* twice the old size */
		if (bf != buf)
			free(bf);
		bf = malloc(bfsz);
		if (bf == NULL) {
			mnstr_set_error(s, MNSTR_WRITE_ERROR, "malloc failed");
			return -1;
		}
		va_start(ap, format);
		i = vsnprintf(bf, bfsz, format, ap);
		va_end(ap);
	}
	s->write(s, (void *) bf, (size_t) i, (size_t) 1);
	if (bf != buf)
		free(bf);
	return s->errkind == MNSTR_NO__ERROR ? i : -1;
}

