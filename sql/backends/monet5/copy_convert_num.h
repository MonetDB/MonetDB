

#if !defined(TMPL_TYPE) || !defined(TMPL_SUFFIXED) || !defined TMPL_NIL || !defined(TMPL_MAX)
#error "This file is a template, it cannot be included standalone"
#endif


static TMPL_TYPE
TMPL_SUFFIXED(parse_one_integer) (struct error_handling *errors, int rel_row, const char *value)
{
	bool pos = true;
	TMPL_TYPE acc = 0;
	const char *s = value;

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
		if (unlikely(acc >= (TMPL_MAX / 10))) {
			copy_report_error(errors, rel_row, "overflow: %s", value);
			return TMPL_NIL;
		}
		TMPL_TYPE new_acc = 10 * acc + digit;
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

	if (*s != '\0') {
		copy_report_error(errors, rel_row, "trailing garbage: %s", s);
		acc = TMPL_NIL;
	}

	if (!pos)
		acc = -acc;

	return acc;
}


static TMPL_TYPE
TMPL_SUFFIXED(parse_one_decimal) (struct error_handling *errors, struct decimal_parms *parms, int rel_row, const char *value)
{
	const char *s = value;
	int digits = parms->digits;
	int scale = parms->scale;
	int integer_digits = digits - scale;
	bool neg = false;
	TMPL_TYPE res = 0;

	while(isspace((unsigned char) *s))
		s++;

	if (*s == '-'){
		neg = true;
		s++;
	} else if (*s == '+'){
		s++;
	}

	for (int i = 0; *s && *s != '.' && ((res == 0 && *s == '0') || i < integer_digits); s++) {
		if (!isdigit((unsigned char) *s))
			break;
		res *= 10;
		res += (*s - '0');
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
	if (*s) {
		copy_report_error(errors, rel_row, "trailing garbage: %s", s);
		res = TMPL_NIL;
	}
	if (neg)
		res = -res;
	return res;
}



static str
TMPL_SUFFIXED(parse_many_decimals) (struct error_handling *errors, void *parms_, int count, void *dest_, char *data, int *offsets)
{
	struct decimal_parms *parms = parms_;
	TMPL_TYPE *dest = dest_;

	for (int i = 0; i < count; i++) {
		int offset = offsets[i];
		if (is_int_nil(offset)) {
			dest[i] = int_nil;
			continue;
		}
		dest[i] = TMPL_SUFFIXED(parse_one_decimal)(errors, parms, i, data + offset);
	}

	return MAL_SUCCEED;
}

static str
TMPL_SUFFIXED(parse_many_integers) (struct error_handling *errors, void *parms, int count, void *dest_, char *data, int *offsets)
{
	(void)parms;
	TMPL_TYPE *dest = dest_;

	for (int i = 0; i < count; i++) {
		int offset = offsets[i];
		if (is_int_nil(offset)) {
			dest[i] = int_nil;
			continue;
		}
		dest[i] = TMPL_SUFFIXED(parse_one_integer)(errors, i, data + offset);
	}

	return MAL_SUCCEED;
}


str
TMPL_SUFFIXED(COPYparse_decimal) (
	bat *parsed_bat_id,
	bat *block_bat_id, bat *offsets_bat_id,
	int *digits, int *scale,
	int *dummy)
{
	struct decimal_parms myparms = {
		.digits = *digits,
		.scale = *scale,
	};
	return parse_fixed_width_column(
		parsed_bat_id, "copy.parse_decimal",
		*block_bat_id, *offsets_bat_id,
		TMPL_SUFFIXED(TYPE), TMPL_SUFFIXED(parse_many_decimals), &myparms);
}

str
TMPL_SUFFIXED(COPYparse_integer) (
	bat *parsed_bat_id,
	bat *block_bat_id, bat *offsets_bat_id,
	int *dummy)
{
	return parse_fixed_width_column(
		parsed_bat_id, "copy.parse_integer",
		*block_bat_id, *offsets_bat_id,
		TMPL_SUFFIXED(TYPE), TMPL_SUFFIXED(parse_many_integers), NULL);
}


#undef TMPL_TYPE
#undef TMPL_NIL
#undef TMPL_MAX
#undef TMPL_SUFFIXED
