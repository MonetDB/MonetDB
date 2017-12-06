/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2017 MonetDB B.V.
 */

#include <assert.h>

#include "dumpcomment.h"

/* temporary */
static void
quoted_print(stream *f, const char *s, const char singleq)
{
	mnstr_write(f, singleq ? "'" : "\"", 1, 1);
	while (*s) {
		switch (*s) {
		case '\\':
			mnstr_write(f, "\\\\", 1, 2);
			break;
		case '"':
			mnstr_write(f, "\"\"", 1, singleq ? 1 : 2);
			break;
		case '\'':
			mnstr_write(f, "''", 1, singleq ? 2 : 1);
			break;
		case '\n':
			mnstr_write(f, "\\n", 1, 2);
			break;
		case '\t':
			mnstr_write(f, "\\t", 1, 2);
			break;
		default:
			if ((0 < *s && *s < 32) || *s == '\177')
				mnstr_printf(f, "\\%03o", *s & 0377);
			else
				mnstr_write(f, s, 1, 1);
			break;
		}
		s++;
	}
	mnstr_write(f, singleq ? "'" : "\"", 1, 1);
}



struct comment_buffer {
        buffer *buf;
        stream *append;
};

comment_buffer*
comment_buffer_create(void)
{
        buffer *buf;
        stream *s;
        comment_buffer *comments;

        buf = buffer_create(4000);
        if (!buf)
        return NULL;

        s = buffer_wastream(buf, "comments_buffer");
        if (s == NULL) {
                buffer_destroy(buf);
                return NULL;
        }

        comments = malloc(sizeof(*comments));
        if (comments == NULL) {
                mnstr_destroy(s);
                buffer_destroy(buf);
                return NULL;
        }

        comments->buf = buf;
        comments->append = s;

        return comments;
}

stream *
comment_appender(comment_buffer *comments)
{
        return comments->append;
}

int
append_comment(
        comment_buffer *comments,
        const char *obj_type,
        const char *schema_name,
        const char *outer_name,
        const char *inner_name,
        void *parameter_types,
        const char *remark
) {
        char *sep = "";

        if (!remark)
                return 0;

        mnstr_printf(comments->append, "COMMENT ON %s ", obj_type);
        if (schema_name) {
                mnstr_printf(comments->append, "%s", sep);
                quoted_print(comments->append, schema_name, 0);
                sep = ".";
        }
        if (outer_name) {
                mnstr_printf(comments->append, "%s", sep);
                quoted_print(comments->append, outer_name, 0);
                sep = ".";
        }
        if (inner_name) {
                mnstr_printf(comments->append, "%s", sep);
                quoted_print(comments->append, inner_name, 0);
                sep = ".";
        }
        (void) parameter_types;

        mnstr_printf(comments->append, " IS ");
        quoted_print(comments->append, remark, 1);
        mnstr_printf(comments->append, ";\n");

        return 0;
}

int
write_comment_buffer(stream *out, comment_buffer *comments)
{
        assert((comments->buf == NULL) == (comments->append == NULL));
        if (comments->buf == NULL)
                return 0;

        if (out) {
                char *text = buffer_get_buf(comments->buf);
                if (text) {
                        mnstr_printf(out, "%s", text);
                        free(text);
                }
        }

        return 0;
}

void
comment_buffer_destroy(comment_buffer *comments)
{
        mnstr_destroy(comments->append);
        buffer_destroy(comments->buf);
        free(comments);
}
