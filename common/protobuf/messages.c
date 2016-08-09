#include "messages.h"
#ifdef HAVE_LIBSNAPPY
#include <snappy-c.h> // C forever
#endif

#include <stdarg.h>		/* va_alist.. */


Mhapi__Message* message_read(stream *s, compression_method comp) {
	lng len;
	mnstr_readLng(s, &len);
	return message_read_length(s, comp, len);
}

Mhapi__Message* message_read_length(stream *s, compression_method comp, lng len) {
	Mhapi__Message* ret;
	char* read_buf;

	if (len < 1) {
		return NULL;
	}
	read_buf = malloc(len);
	if (!read_buf) {
		return NULL;
	}
	if (mnstr_read(s, read_buf, len, 1) != len) {
		return NULL;
	}
	switch(comp) {
	case COMPRESSION_NONE:
		break;
	case COMPRESSION_SNAPPY:
#ifdef HAVE_LIBSNAPPY
	{
		size_t uncompressed_length;
		char *uncompressed_buf;
		if (!snappy_uncompressed_length(read_buf, len, &uncompressed_length) == SNAPPY_OK) {
			free(read_buf);
			return NULL;
		}
		uncompressed_buf = malloc(uncompressed_length);
		if (!uncompressed_buf) {
			free(read_buf);
			return NULL;
		}
		if (snappy_uncompress(read_buf, len, uncompressed_buf, &uncompressed_length) != SNAPPY_OK) {
			free(read_buf);
			free(uncompressed_buf);
			return NULL;
		}
		free(read_buf);
		read_buf = uncompressed_buf;
		len = uncompressed_length;
	}
#else
		return NULL;
#endif
	}

	ret = (Mhapi__Message*) protobuf_c_message_unpack(&mhapi__message__descriptor, NULL, len, (uint8_t*) read_buf);
	free(read_buf);
	return ret;
}

ssize_t message_write(stream *s, compression_method comp, Mhapi__Message *msg) {
	lng len = protobuf_c_message_get_packed_size((ProtobufCMessage*) msg);
	char* write_buf = malloc(len);
	if (!write_buf) {
		return -1;
	}
	if (protobuf_c_message_pack((ProtobufCMessage*) msg, (uint8_t*) write_buf) != (size_t) len) {
		return -1;
	}
	switch(comp) {
	case COMPRESSION_NONE:
		break;
	case COMPRESSION_SNAPPY:
#ifdef HAVE_LIBSNAPPY
	{
		size_t compressed_length = snappy_max_compressed_length(len);
		char *compressed_buf = malloc(compressed_length);
		if (!compressed_buf) {
			free(write_buf);
			return NULL;
		}
		if (snappy_compress(write_buf, len, compressed_buf, &compressed_length) != SNAPPY_OK) {
			free(write_buf);
			free(compressed_buf);
			return NULL;
		}
		free(write_buf);
		write_buf = compressed_buf;
		len = compressed_length;
	}
#else
		return -1;
#endif
	}

	if (!mnstr_writeLng(s, len) || mnstr_write(s, write_buf, len, 1) != len) {
		return -1;
	}
	return len;
}

void message_send_error(stream *s, protocol_version proto, const char *format, ...) {
	char buf[BUFSIZ], *bf = buf;
	int i = 0;
	va_list ap;

	if (s == NULL) {
		return;
	}
	va_start(ap, format);
	i = vsnprintf(bf, BUFSIZ, format, ap);
	va_end (ap);

	if (proto == prot9) {
		mnstr_printf(s, "!%s\n", bf);
		mnstr_flush(s);
	} else {
		Mhapi__Message msg;
		Mhapi__Error err;

		mhapi__message__init(&msg);
		mhapi__error__init(&err);

		msg.message_case = MHAPI__MESSAGE__MESSAGE_ERROR;
		err.message = bf;
		msg.error = &err;

		if (proto == prot10compressed) {
			message_write(s, COMPRESSION_SNAPPY, &msg);
		} else {
			message_write(s, COMPRESSION_NONE, &msg);
		}
	}
}

// fixme this is mostly redundant

void message_send_warning(stream *s, protocol_version proto, const char *format, ...) {
	char buf[BUFSIZ], *bf = buf;
	int i = 0;
	va_list ap;

	if (s == NULL) {
		return;
	}
	va_start(ap, format);
	i = vsnprintf(bf, BUFSIZ, format, ap);
	va_end (ap);

	if (proto == prot9) {
		mnstr_printf(s, "#%s\n", bf);
		mnstr_flush(s);
	} else {
		Mhapi__Message msg;
		Mhapi__Warning err;

		mhapi__message__init(&msg);
		mhapi__warning__init(&err);

		msg.message_case = MHAPI__MESSAGE__MESSAGE_WARNING;
		err.message = bf;
		msg.warning = &err;

		if (proto == prot10compressed) {
			message_write(s, COMPRESSION_SNAPPY, &msg);
		} else {
			message_write(s, COMPRESSION_NONE, &msg);
		}
	}
}

