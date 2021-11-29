
#ifndef TMPL_TYPE
#error "only include sql_copyinto_dec_tmpl.h with TMPL_TYPE defined"
#endif
#ifndef TMPL_FUNC_NAME
#error "only include sql_copyinto_dec_tmpl.h with TMPL_FUNC_NAME defined"
#endif
#ifndef TMPL_BULK_FUNC_NAME
#error "only include sql_copyinto_dec_tmpl.h with TMPL_BULK_FUNC_NAME defined"
#endif

static inline void*
TMPL_FUNC_NAME(void *dst, size_t dst_len, const char *s, unsigned int digits, unsigned int scale)
{
	unsigned int integer_digits = digits - scale;
	unsigned int i;
	bool neg = false;
	TMPL_TYPE *r;
	TMPL_TYPE res = 0;

	while(isspace((unsigned char) *s))
		s++;

	if (*s == '-'){
		neg = true;
		s++;
	} else if (*s == '+'){
		s++;
	}
	for (i = 0; *s && *s != '.' && ((res == 0 && *s == '0') || i < integer_digits); s++) {
		if (!isdigit((unsigned char) *s))
			break;
		res *= 10;
		res += (*s-'0');
		if (res)
			i++;
	}
	if (*s == '.') {
		s++;
		while (*s && isdigit((unsigned char) *s) && scale > 0) {
			res *= 10;
			res += *s++ - '0';
			scale--;
		}
	}
	while(*s && isspace((unsigned char) *s))
		s++;
	while (scale > 0) {
		res *= 10;
		scale--;
	}
	if (*s)
		return NULL;
	assert(dst_len >= sizeof(TMPL_TYPE));(void)dst_len;
	r = dst;
	if (neg)
		*r = -res;
	else
		*r = res;
	return (void *) r;

	return NULL;
}


static int
TMPL_BULK_FUNC_NAME(READERtask *task, Column *c, int col, int count, size_t width, unsigned int digits, unsigned int scale)
{
	void *cursor = task->primary.data;
	size_t w = width;
	for (int i = 0; i < count; i++) {
		char *unescaped = NULL;
		int ret = prepare_conversion(task, c, col, i, &cursor, &w, &unescaped);
		if (ret > 0) {
			void *p = TMPL_FUNC_NAME(cursor, w, unescaped, digits, scale);
			if (p == NULL) {
				ret = report_conversion_failed(task, c, i, col + 1, unescaped);
				make_it_nil(c, &cursor, &w);
			}
		}
		if (ret < 0) {
			return -1;
		}
		assert(w == width); // should not have attempted to reallocate!
		cursor = (char*)cursor + width;
	}
	return 0;
}



#undef TMPL_TYPE
#undef TMPL_FUNC_NAME
#undef TMPL_BULK_FUNC_NAME
