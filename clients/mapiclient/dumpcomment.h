/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2017 MonetDB B.V.
 */

#include "stream.h"

typedef struct comment_buffer comment_buffer;

comment_buffer *comment_buffer_create(void);

stream *comment_appender(comment_buffer *comments);

int append_comment(
        comment_buffer *comments,
        const char *obj_type,
        const char *schema_name,
        const char *outer_name,
        const char *inner_name,
        void *parameter_types,
        const char *remark
);

int write_comment_buffer(stream *out, comment_buffer *comments);
  
void comment_buffer_destroy(comment_buffer *comments);
