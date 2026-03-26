

#if !defined(TMPL_TYPE) || !defined(TMPL_SUFFIXED) || !defined TMPL_NIL || !defined(TMPL_MAX)
#error "This file is a template, it cannot be included standalone"
#endif


static TMPL_TYPE
TMPL_SUFFIXED(parse_one_integer) (struct error_handling *errors, BUN rel_row, const char *value, int *nils)
{
	bool pos = true;
	TMPL_TYPE acc = 0;
	const char *s = value;

	if (*s <= ' ')
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
			copy_report_error(errors, (lng) rel_row, -1, "overflow: %s", value);
			return TMPL_NIL;
		}
		TMPL_TYPE new_acc = 10 * acc + digit;
		acc = new_acc;
		s++;
	}

	if (*s == 'E' || *s == 'e') {
		int exp = 0;
		s++;
		while (isdigit((unsigned char) *s)) {
			// int is safe because of promotion rules
			int digit = *s - '0';
			int new_exp = 10 * exp + digit;
			exp = new_exp;
			s++;
		}
		TMPL_TYPE m = 1;
		while (exp > 0) {
			m *= 10;
			exp--;
		}
		acc *= m;
		/* overflow check missing */
	}
	if (*s == '.') {
		s++;
		while (*s == '0')
			s++;
	}

	while(*s && isspace((unsigned char) *s))
		s++;

	if (s == value) {
		copy_report_error(errors, (lng) rel_row, -1, "missing integer");
		(*nils)++;
		return TMPL_NIL;
	}
	if (*s != '\0') {
		if (isdigit(*s))
			copy_report_error(errors, (lng) rel_row, -1, "unexpected decimal digit '%c' while parsing integer", *s);
		else
			copy_report_error(errors, (lng) rel_row, -1, "unexpected character '%c' while parsing integer", *s);
		(*nils)++;
		return TMPL_NIL;
	}

	if (!pos)
		acc = -acc;

	return acc;
}

static TMPL_TYPE
TMPL_SUFFIXED(parse_one_decimal_skip) (struct error_handling *errors, struct decimal_parms *parms, BUN rel_row, const char *value)
{
	const char *s = value;
	int digits = parms->digits;
	int scale = parms->scale;
	int integer_digits = digits - scale;
	bool neg = false;
	TMPL_TYPE res = 0;

	if (*s <= ' ')
		while(isspace((unsigned char) *s))
			s++;

	if (*s == '-'){
		neg = true;
		s++;
	} else if (*s == '+'){
		s++;
	}

	for (int i = 0; *s && (*s != parms->sep) && ((res == 0 && *s == '0') || i < integer_digits); s++) {
		if (*s == parms->skip)
			continue;
		if (!isdigit((unsigned char) *s))
			break;
		res *= 10;
		res += (*s - '0');
		if (res)
			i++;
	}
	if (*s == parms->sep) {
		s++;
		for ( ;*s && (isdigit((unsigned char) *s) || (*s == parms->skip)) && scale > 0; s++) {
			if (*s == parms->skip)
				continue;
			res *= 10;
			res += *s - '0';
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
			copy_report_error(errors, (lng) rel_row, -1, "too many decimal digits while parsing decimal: %s", value);
		else
			copy_report_error(errors, (lng) rel_row, -1, "unexpected characters while parsing decimal: %s", s);
		parms->nils++;
		return TMPL_NIL;
	}
	if (neg)
		res = -res;
	return res;
}



static TMPL_TYPE
TMPL_SUFFIXED(parse_one_decimal) (struct error_handling *errors, struct decimal_parms *parms, BUN rel_row, const char *value)
{
	const char *s = value;
	int digits = parms->digits;
	int scale = parms->scale;
	int integer_digits = digits - scale;
	bool neg = false;
	TMPL_TYPE res = 0;

	if (*s <= ' ')
		while(isspace((unsigned char) *s))
			s++;

	if (*s == '-'){
		neg = true;
		s++;
	} else if (*s == '+'){
		s++;
	}

	for (; *s; s++) {
		if (!isdigit((unsigned char) *s))
			break;
		res *= 10;
		res += (*s - '0');
		integer_digits-=(res)?1:0;
	}
	if (*s == parms->sep) {
		s++;
		for ( ;*s && isdigit((unsigned char)*s) && scale > 0; s++) {
			res *= 10;
			res += *s - '0';
			scale--;
		}
	}
	while(*s && isspace((unsigned char) *s))
		s++;
	while (scale > 0) {
		res *= 10;
		scale--;
	}
	if (*s || integer_digits < 0) {
		if (integer_digits < 0 || isdigit(*s))
			copy_report_error(errors, (lng) rel_row, -1, "too many decimal digits while parsing decimal: %s", value);
		else
			copy_report_error(errors, (lng) rel_row, -1, "unexpected characters while parsing decimal: %s", s);
		parms->nils++;
		return TMPL_NIL;
	}
	if (neg)
		res = -res;
	return res;
}



static void
TMPL_SUFFIXED(parse_many_decimals) (struct error_handling *errors, void *parms_, BUN count, void *dest_, char *data, int *offsets)
{
	struct decimal_parms *parms = parms_;
	TMPL_TYPE *dest = dest_;
	int nils = 0;

	if (parms->skip) {
		for (BUN i = 0; i < count; i++) {
			int offset = offsets[i];
			if (is_int_nil(offset)) {
				dest[i] = TMPL_NIL;
				nils++;
				continue;
			}
			dest[i] = TMPL_SUFFIXED(parse_one_decimal_skip)(errors, parms, i, data + offset);
		}
	} else {
		for (BUN i = 0; i < count; i++) {
			int offset = offsets[i];
			if (is_int_nil(offset)) {
				dest[i] = TMPL_NIL;
				nils++;
				continue;
			}
			dest[i] = TMPL_SUFFIXED(parse_one_decimal)(errors, parms, i, data + offset);
		}
	}
	parms->nils += nils;
}

static void
TMPL_SUFFIXED(parse_many_integers) (struct error_handling *errors, void *parms, BUN count, void *dest_, char *data, int *offsets)
{
	int *Nils = (int*)parms;
	TMPL_TYPE *dest = dest_;
	int nils = 0;

	for (BUN i = 0; i < count; i++) {
		int offset = offsets[i];
		if (is_int_nil(offset)) {
			dest[i] = TMPL_NIL;
			nils++;
			continue;
		}
		dest[i] = TMPL_SUFFIXED(parse_one_integer)(errors, i, data + offset, &nils);
	}
	*Nils += nils;
}


str
TMPL_SUFFIXED(COPYparse_decimal) (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;

	(void)mb;
	bat *parsed_bat_id = getArgReference_bat(stk, pci, 0);
	bat block_bat_id = *getArgReference_bat(stk, pci, 1);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 2);
	bat offsets_bat_id = *getArgReference_bat(stk, pci, 3);
	int digits = *getArgReference_int(stk, pci, 4);
	int scale = *getArgReference_int(stk, pci, 5);
	// arg 6 is a dummy
	bat rows = *getArgReference_bat(stk, pci, 7);
	int col_no = *getArgReference_int(stk, pci, 8);
	str col_name = *getArgReference_str(stk, pci, 9);
	str dec_sep = *getArgReference_str(stk, pci, 10);
	str dec_skip = *getArgReference_str(stk, pci, 11);

	struct error_handling errors;
	copy_init_error_handling(&errors, cntxt, 0, col_no, col_name, rows);

	struct decimal_parms myparms = {
		.nils = 0,
		.digits = digits,
		.scale = scale,
		.sep = strNil(dec_sep) ? '.' : dec_sep[0],
		.skip = strNil(dec_skip) ? '\0' : dec_skip[0],
	};

	// Does this number of digits fit in the result type?
	// Should be ensured by the rest of the system but let's double check
	// so we do not have to think about overflows while parsing at all.
	double max = pow(10, myparms.digits);
	if (max >= (double)TMPL_MAX)
		bailout("copy.parse_decimal", "result type cannot hold %d digits", myparms.digits);

	msg = parse_fixed_width_column(
		parsed_bat_id, &errors, "copy.parse_decimal",
		block_bat_id, p, offsets_bat_id,
		TMPL_SUFFIXED(TYPE), TMPL_SUFFIXED(parse_many_decimals), &myparms);

end:
	if (errors.init)
		copy_destroy_error_handling(&errors);
	return msg;
}

str
TMPL_SUFFIXED(COPYparse_integer) (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	bat *parsed_bat_id = getArgReference_bat(stk, pci, 0);
	bat block_bat_id = *getArgReference_bat(stk, pci, 1);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 2);
	bat offsets_bat_id = *getArgReference_bat(stk, pci, 3);
	// TMPL_TYPE dummy = *getArgReference_TMPL_TYPE(stk, pci, 4);
	bat rows = *getArgReference_bat(stk, pci, 5);
	int col_no = *getArgReference_int(stk, pci, 6);
	str col_name = *getArgReference_str(stk, pci, 7);

	struct error_handling errors;
	errors.init = 0;
	copy_init_error_handling(&errors, cntxt, 0, col_no, col_name, rows);

	int nils = 0;
	str msg = parse_fixed_width_column(
		parsed_bat_id, &errors, "copy.parse_integer" ,
		block_bat_id, p, offsets_bat_id,
		TMPL_SUFFIXED(TYPE), TMPL_SUFFIXED(parse_many_integers), &nils);

	if (errors.init)
		copy_destroy_error_handling(&errors);
	return msg;
}

str
TMPL_SUFFIXED(COPYscale) (Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str msg = MAL_SUCCEED;
	const char *operatorname = "copy.scale";

	(void)mb;
	bat *result_bat_id = getArgReference_bat(stk, pci, 0);
	bat values_bat_id = *getArgReference_bat(stk, pci, 1);
	int factor = *getArgReference_int(stk, pci, 2);
	bat rows = *getArgReference_bat(stk, pci, 3);
	int col_no = *getArgReference_int(stk, pci, 4);
	str col_name = *getArgReference_str(stk, pci, 5);

	BAT *values_bat = BATdescriptor(values_bat_id);
	size_t n = BATcount(values_bat);
	BAT *results_bat = NULL;
	TMPL_TYPE limit = TMPL_MAX / factor;
	TMPL_TYPE *values;
	TMPL_TYPE *results;

	struct error_handling errors;
	errors.init = 0;
	copy_init_error_handling(&errors, cntxt, 0, col_no, col_name, rows);

	if (!values_bat)
		bailout(operatorname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	results_bat = COLnew(0, TMPL_SUFFIXED(TYPE), BATcount(values_bat), TRANSIENT);
	if (!results_bat)
		bailout(operatorname, SQLSTATE(HY013) MAL_MALLOC_FAIL);

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
			scaled = factor * val;
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
	if (errors.init)
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
