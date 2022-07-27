

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
		if (unlikely(acc > ((TMPL_MAX - digit) / 10))) {
			copy_report_error(errors, rel_row, -1, "overflow: %s", value);
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

	if (s == value) {
		copy_report_error(errors, rel_row, -1, "missing integer");
		return TMPL_NIL;
	}
	if (*s != '\0') {
		if (isdigit(*s))
			copy_report_error(errors, rel_row, -1, "unexpected decimal digit '%c' while parsing integer", *s);
		else
			copy_report_error(errors, rel_row, -1, "unexpected character '%c' while parsing integer", *s);
		return TMPL_NIL;
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
		if (isdigit(*s))
			copy_report_error(errors, rel_row, -1, "too many decimal digits while parsing decimal: %s", value);
		else
			copy_report_error(errors, rel_row, -1, "unexpected characters while parsing decimal: %s", s);
		return TMPL_NIL;
	}
	if (neg)
		res = -res;
	return res;
}



static void
TMPL_SUFFIXED(parse_many_decimals) (struct error_handling *errors, void *parms_, int count, void *dest_, char *data, int *offsets)
{
	struct decimal_parms *parms = parms_;
	TMPL_TYPE *dest = dest_;

	for (int i = 0; i < count; i++) {
		int offset = offsets[i];
		if (is_int_nil(offset)) {
			dest[i] = TMPL_NIL;
			continue;
		}
		dest[i] = TMPL_SUFFIXED(parse_one_decimal)(errors, parms, i, data + offset);
	}
}

static void
TMPL_SUFFIXED(parse_many_integers) (struct error_handling *errors, void *parms, int count, void *dest_, char *data, int *offsets)
{
	(void)parms;
	TMPL_TYPE *dest = dest_;

	for (int i = 0; i < count; i++) {
		int offset = offsets[i];
		if (is_int_nil(offset)) {
			dest[i] = TMPL_NIL;
			continue;
		}
		dest[i] = TMPL_SUFFIXED(parse_one_integer)(errors, i, data + offset);
	}
}


str
TMPL_SUFFIXED(COPYparse_decimal) (
	bat *parsed_bat_id,
	bat *block_bat_id, bat *offsets_bat_id,
	int *digits, int *scale,
	TMPL_TYPE *dummy,
	bat *failures_bat, lng *starting_row, int *col_no, str *col_name)
{
	str msg = MAL_SUCCEED;
	(void)dummy;

	struct error_handling errors;
	copy_init_error_handling(&errors, *failures_bat, *starting_row, *col_no, *col_name);

	struct decimal_parms myparms = {
		.digits = *digits,
		.scale = *scale,
	};

	// Does this number of digits fit in the result type?
	// Should be ensured by the rest of the system but let's double check
	// so we do not have to think about overflows while parsing at all.
	double max = pow(10, myparms.digits);
	if (max >= (double)TMPL_MAX)
		bailout("copy.parse_decimal", "result type cannot hold %d digits", myparms.digits);

	msg = parse_fixed_width_column(
		parsed_bat_id, &errors, "copy.parse_decimal",
		*block_bat_id, *offsets_bat_id,
		TMPL_SUFFIXED(TYPE), TMPL_SUFFIXED(parse_many_decimals), &myparms);

end:
	copy_destroy_error_handling(&errors);
	return msg;
}

str
TMPL_SUFFIXED(COPYparse_integer) (
	bat *parsed_bat_id,
	bat *block_bat_id, bat *offsets_bat_id,
	TMPL_TYPE *dummy,
	bat *failures_bat, lng *starting_row, int *col_no, str *col_name)
{
	(void)dummy;

	struct error_handling errors;

	copy_init_error_handling(&errors, *failures_bat, *starting_row, *col_no, *col_name);

	str msg = parse_fixed_width_column(
		parsed_bat_id, &errors, "copy.parse_integer" ,
		*block_bat_id, *offsets_bat_id,
		TMPL_SUFFIXED(TYPE), TMPL_SUFFIXED(parse_many_integers), NULL);

	copy_destroy_error_handling(&errors);
	return msg;
}

str
TMPL_SUFFIXED(COPYscale) (
	bat *result_bat_id,
	bat *values_bat_id, int *factor,
	bat *failures_bat_id, lng *starting_row, int *col_no, str *col_name)
{
	str msg = MAL_SUCCEED;
	const char *operatorname = "copy.scale";
	BAT *values_bat = BATdescriptor(*values_bat_id);
	size_t n = BATcount(values_bat);
	BAT *results_bat = NULL;
	TMPL_TYPE limit = TMPL_MAX / *factor;
	TMPL_TYPE *values;
	TMPL_TYPE *results;

	if (!values_bat)
		bailout(operatorname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	results_bat = COLnew(0, TMPL_SUFFIXED(TYPE), BATcount(values_bat), TRANSIENT);
	if (!results_bat)
		bailout(operatorname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	struct error_handling errors;
	copy_init_error_handling(&errors, *failures_bat_id, *starting_row, *col_no, *col_name);

	values = Tloc(values_bat, 0);
	results = Tloc(results_bat, 0);
	for (size_t i = 0; i < n; i++) {
		TMPL_TYPE val = values[i];
		TMPL_TYPE scaled;
		if (val == TMPL_NIL) {
			scaled = TMPL_NIL;
		} else if (val > limit || val < -limit) {
			copy_report_error(&errors, i, -1, "value too large");
			scaled = TMPL_NIL;
		} else {
			scaled = *factor * val;
		}
		results[i] = scaled;
	}
	BATsetcount(results_bat, n);
	// we don't know anything about the data we just parsed
	results_bat->tkey = false;
	results_bat->tnil = false;
	results_bat->tnonil = false;
	results_bat->tsorted = false;
	results_bat->trevsorted = false;

end:
	if (msg == MAL_SUCCEED)
		msg = copy_check_too_many_errors(&errors, operatorname);
	copy_destroy_error_handling(&errors);
	if (values_bat)
		BBPunfix(values_bat->batCacheid);
	if (results_bat) {
		if (msg == MAL_SUCCEED) {
			BBPkeepref(results_bat);
			*result_bat_id = results_bat->batCacheid;
		} else {
			BBPunfix(results_bat->batCacheid);
		}
	}
	return msg;
}


#undef TMPL_TYPE
#undef TMPL_NIL
#undef TMPL_MAX
#undef TMPL_SUFFIXED
