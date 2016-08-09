#include "mhapi.pb-c.h"
#include "stream.h"

Mhapi__Message* message_read(stream *s, compression_method comp);
Mhapi__Message* message_read_length(stream *s, compression_method comp, lng length);

ssize_t message_write(stream *s, compression_method comp, Mhapi__Message *msg);
void message_send_error(stream *s, protocol_version proto, const char *format, ...);
void message_send_warning(stream *s, protocol_version proto, const char *format, ...);
