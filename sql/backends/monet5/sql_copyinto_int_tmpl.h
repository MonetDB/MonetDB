
#ifndef TMPL_TYPE
#error "only include sql_copyinto_int_tmpl.h with TMPL_TYPE defined"
#endif
#ifndef TMPL_FUNC_NAME
#error "only include sql_copyinto_int_tmpl.h with TMPL_FUNC_NAME defined"
#endif
#ifndef TMPL_BULK_FUNC_NAME
#error "only include sql_copyinto_int_tmpl.h with TMPL_BULK_FUNC_NAME defined"
#endif

static inline ConversionResult
TMPL_FUNC_NAME(const char *s, TMPL_TYPE *dst)
{
	bool pos = true;
	TMPL_TYPE acc = 0;

	while(isspace((unsigned char) *s))
		s++;

	if (*s == '-') {
		pos = false;
		s++;
	} else if (*s == '+') {
		s++;
	}

	while (isdigit((unsigned char) *s)) {
		// int is safe because of promotion rules
		int digit = *s - '0';
		TMPL_TYPE new_acc = 10 * acc + digit;
		if (new_acc < acc) {
			// overflow
			return ConversionFailed;
		}
		acc = new_acc;
		s++;
	}

	if (*s == '.') {
		s++;
		while (*s == '0')
			s++;
	}

	while (isspace((unsigned char) *s))
		s++;

	if (*s != '\0')
		return ConversionFailed;

	if (pos)
		*dst = acc;
	else
		*dst = -acc;

	return ConversionOk;
}


static ConversionResult
TMPL_BULK_FUNC_NAME(READERtask *task, Column *c, int col, int count, size_t width)
{
	TMPL_TYPE *cursor = task->primary.data;
	assert(sizeof(TMPL_TYPE) == width); (void)width;
	ConversionResult res = ConversionOk;
	for (int i = 0; i < count; i++) {
		char *unescaped = NULL;
		res = prepare_conversion(task, c, col, i, &unescaped);
		if (res == ConversionOk) {
			res = TMPL_FUNC_NAME(unescaped, cursor);
			assert(res != ConversionNull);
			if (res != ConversionOk) {
				res = report_conversion_failed(task, c, i, col + 1, unescaped);
			}
		}
		if (res != ConversionOk) {
			set_nil(c, cursor);
		}
		if (res == ConversionFailed) {
			return res;
		}
		cursor++;
	}
	return res;
}



#undef TMPL_TYPE
#undef TMPL_FUNC_NAME
#undef TMPL_BULK_FUNC_NAME
