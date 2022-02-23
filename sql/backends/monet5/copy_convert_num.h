

#if !defined(TMPL_TYPE) || !defined(TMPL_SUFFIXED)
#error "This file is a template, it cannot be included standalone"
#endif



static TMPL_TYPE
TMPL_SUFFIXED(parse_one_decimal) (struct error_handling *errors, struct decimal_parms *parms, int rel_row, const char *s)
{
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
		res = int_nil;
	}
	if (neg)
		res = -res;
	return res;
}



static str
TMPL_SUFFIXED(parse_many_decimal) (struct error_handling *errors, void *parms_, int count, void *dest_, char *data, int *offsets)
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
		TMPL_SUFFIXED(TYPE), TMPL_SUFFIXED(parse_many_decimal), &myparms);
}


#undef TMPL_TYPE
#undef TMPL_SUFFIXED
