/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "formatinput.h"
#include "type_conversion.h"

//! Parse a PyCodeObject from a string, the string is expected to be in the
//! format {@<encoded_function>};, where <encoded_function> is the Marshalled
//! code object
PyObject *PyCodeObject_ParseString(char *string, char **msg);
PyObject *PyCodeObject_ParseString(char *string, char **msg)
{
	size_t length = strlen(string);
	PyObject *code_object, *tuple, *mystr;
	char *code_copy = GDKmalloc(length * sizeof(char));
	char hex[3];
	size_t i, j;
	hex[2] = '\0';
	if (code_copy == NULL) {
		*msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL);
		return NULL;
	}
	// decode hex codes (e.g. \x00) in the string to the actual numeric
	// representation
	for (i = 2, j = 0; i < length - 2; i++) {
		if (string[i] == '\\' && string[i + 1] == '\\')
			i++;
		if (string[i] == '\\' && string[i + 1] == 't') {
			code_copy[j++] = '\t';
			i++;
		} else if (string[i] == '\\' && string[i + 1] == 'n') {
			code_copy[j++] = '\n';
			i++;
		} else if (string[i] == '\\' && string[i + 1] == 'x') {
			hex[0] = string[i + 2];
			hex[1] = string[i + 3];
			code_copy[j++] = (char)strtol(hex, NULL, 16);
			i += 3;
		} else {
			code_copy[j++] = string[i];
		}
	}
	code_copy[j] = '\0';
	tuple = PyTuple_New(1);
	mystr = PyString_FromStringAndSize(
		code_copy,
		j); // use FromStringAndSize because the string is not null-terminated
	PyTuple_SetItem(tuple, 0, mystr);
	code_object = PyObject_CallObject(marshal_loads, tuple);
	Py_DECREF(tuple);
	GDKfree(code_copy);
	if (code_object == NULL) {
		PyErr_Print();
		*msg = createException(MAL, "pyapi.eval",
							   "Failed to marshal.loads() encoded object");
		return NULL;
	}
	*msg = MAL_SUCCEED;
	return code_object;
}

char *FormatCode(char *code, char **args, size_t argcount, size_t tabwidth,
				 PyObject **code_object, char **msg, char **additional_args,
				 size_t additional_argcount)
{
	// Format the python code by fixing the indentation levels
	// We do two passes, first we get the length of the resulting formatted code
	// and then we actually create the resulting code
	size_t i = 0, j = 0, k = 0;
	size_t length = strlen(code);
	size_t size = 0;
	size_t spaces_per_level = 2;

	size_t code_location = 0;
	char *newcode = NULL;

	size_t indentation_count = 0;
	size_t max_indentation = 100;
	// This keeps track of the different indentation levels
	// indentation_levels is a sorted array with how many spaces of indentation
	// that specific array has
	// so indentation_levels[0] = 4 means that the first level (level 0) has 4
	// spaces in the source code
	// after this array is constructed we can count the amount of spaces before
	// a statement and look in this
	// array to immediately find the indentation level of the statement
	size_t *indentation_levels;
	// statements_per_level keeps track of how many statements are at the
	// specified indentation level
	// this is needed to compute the size of the resulting formatted code
	// for every indentation level i, we add statements_per_level[i] * (i + 1) *
	// spaces_per_level spaces
	size_t *statements_per_level;

	size_t initial_spaces = 0;
	size_t statement_size = 0;
	bool seen_statement = false;
	bool multiline_statement = false;
	int multiline_quotes = 0;

	char base_start[] = "def pyfun(";
	char base_end[] = "):\n";
	*msg = NULL;
#ifndef IS_PY3K
	if (code[1] == '@') {
		*code_object = PyCodeObject_ParseString(code, msg);
		return NULL;
	}
#else
	(void)code_object;
#endif

	indentation_levels = (size_t *)GDKzalloc(max_indentation * sizeof(size_t));
	statements_per_level =
		(size_t *)GDKzalloc(max_indentation * sizeof(size_t));
	if (indentation_levels == NULL || statements_per_level == NULL) {
		*msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL);
		goto finally;
	}

	// Base function definition size
	// For every argument, add a comma, and add another entry for the '\0'
	size += strlen(base_start) + strlen(base_end) + argcount + 1;
	for (i = 0; i < argcount; i++) {
		if (args[i] != NULL) {
			size += strlen(args[i]) + 1;
		}
	}
	// Additional parameters
	for (i = 0; i < additional_argcount; i++)
		size += strlen(additional_args[i]) + 1;

	// First remove the "{" at the start and the "};" at the end of the
	// function, this is added when we have a function created through SQL and
	// python doesn't like them
	// We need to be careful to only remove ones at the start/end, otherwise we
	// might invalidate some otherwise valid python code containing them
	for (i = length - 1, j = 0; i > 0; i--) {
		if (code[i] != '\n' && code[i] != ' ' && code[i] != '\t' &&
			code[i] != ';' && code[i] != '}')
			break;
		if (j == 0) {
			if (code[i] == ';') {
				code[i] = ' ';
				j = 1;
			}
		} else if (j == 1) {
			if (code[i] == '}') {
				code[i] = ' ';
				break;
			}
		}
	}
	for (i = 0; i < length; i++) {
		if (code[i] != '\n' && code[i] != ' ' && code[i] != '\t' &&
			code[i] != '{')
			break;
		if (code[i] == '{') {
			code[i] = ' ';
		}
	}
	// We indent using spaces, four spaces per level
	// We also erase empty lines
	for (i = 0; i < length; i++) {
		// handle multiline strings (strings that start with """)
		if (code[i] == '\"') {
			if (!multiline_statement) {
				multiline_quotes++;
				multiline_statement = multiline_quotes == 3;
			} else {
				multiline_quotes--;
				multiline_statement = multiline_quotes != 0;
			}
		} else {
			multiline_quotes = multiline_statement ? 3 : 0;
		}

		if (!seen_statement) {
			// We have not seen a statement on this line yet
			if (code[i] == '\n') {
				// Empty line, skip to the next one
				initial_spaces = 0;
			} else if (code[i] == ' ') {
				initial_spaces++;
			} else if (code[i] == '\t') {
				initial_spaces += tabwidth;
			} else {
				// Statement starts here
				seen_statement = true;
			}
		}
		if (seen_statement) {
			// We have seen a statement on this line, check the indentation
			// level
			statement_size++;

			if (code[i] == '\n' || i == length - 1) {
				// Statement ends here
				bool placed = false;
				size_t level = 0;

				if (multiline_statement) {
					// if we are in a multiline statement, we don't want to mess
					// with the indentation
					size += statement_size;
					initial_spaces = 0;
					statement_size = 0;
					continue;
				}
				// First put the indentation in the indentation table
				if (indentation_count >= max_indentation) {
					// If there is no room in the indentation arrays we will
					// extend them
					// This probably will never happen unless in really extreme
					// code (or if max_indentation is set very low)
					size_t *new_indentation =
						GDKzalloc(2 * max_indentation * sizeof(size_t));
					size_t *new_statements_per_level;
					if (new_indentation == NULL) {
						*msg =
							createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL);
						goto finally;
					}
					new_statements_per_level =
						GDKzalloc(2 * max_indentation * sizeof(size_t));
					if (new_statements_per_level == NULL) {
						*msg =
							createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL);
						goto finally;
					}

					for (i = 0; i < max_indentation; i++) {
						new_indentation[i] = indentation_levels[i];
						new_statements_per_level[i] = statements_per_level[i];
					}
					GDKfree(indentation_levels);
					GDKfree(statements_per_level);
					indentation_levels = new_indentation;
					statements_per_level = new_statements_per_level;
					max_indentation *= 2;
				}

				for (j = 0; j < indentation_count; j++) {
					if (initial_spaces == indentation_levels[j]) {
						// The exact space count is already in the array, so we
						// can stop
						level = j;
						placed = true;
						break;
					}

					if (initial_spaces < indentation_levels[j]) {
						// The indentation level is smaller than this level (but
						// bigger than the previous level)
						// So the indentation level belongs here, so we move
						// every level past this one upward one level
						// and put the indentation level here
						for (k = indentation_count; k > j; k--) {
							indentation_levels[k] = indentation_levels[k - 1];
							statements_per_level[k] =
								statements_per_level[k - 1];
						}
						indentation_count++;
						statements_per_level[j] = 0;
						indentation_levels[j] = initial_spaces;
						level = j;
						placed = true;
						break;
					}
				}
				if (!placed) {
					// The space count is the biggest we have seen, so we add it
					// to the end of the array
					level = indentation_count;
					indentation_levels[indentation_count++] = initial_spaces;
				}
				statements_per_level[level]++;
				size += statement_size;
				seen_statement = false;
				initial_spaces = 0;
				statement_size = 0;
			}
		}
	}
	// Add the amount of spaces we will add to the size
	for (i = 0; i < indentation_count; i++) {
		size += (i + 1) * spaces_per_level * statements_per_level[i];
	}

	// Allocate space for the function
	newcode = GDKzalloc(size);
	if (newcode == NULL) {
		*msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL);
		goto finally;
	}
	initial_spaces = 0;
	seen_statement = false;

	// First print in the function definition and arguments
	for (i = 0; i < strlen(base_start); i++) {
		newcode[code_location++] = base_start[i];
	}
	// Add user-defined parameters
	for (i = 0; i < argcount; i++) {
		if (args[i] != NULL) {
			for (j = 0; j < strlen(args[i]); j++) {
				newcode[code_location++] = args[i][j];
			}
			if (i != argcount - 1 || additional_argcount > 0) {
				newcode[code_location++] = ',';
			}
		}
	}
	// Add additional parameters
	for (i = 0; i < additional_argcount; i++) {
		if (additional_args[i] != NULL) {
			for (j = 0; j < strlen(additional_args[i]); j++) {
				newcode[code_location++] = additional_args[i][j];
			}
			if (i != additional_argcount - 1) {
				newcode[code_location++] = ',';
			}
		}
	}
	for (i = 0; i < strlen(base_end); i++) {
		newcode[code_location++] = base_end[i];
	}

	// Now the second pass, actually construct the code
	for (i = 0; i < length; i++) {
		// handle multiline statements
		if (code[i] == '\"') {
			if (!multiline_statement) {
				multiline_quotes++;
				multiline_statement = multiline_quotes == 3;
			} else {
				multiline_quotes--;
				multiline_statement = multiline_quotes != 0;
			}
		} else {
			multiline_quotes = multiline_statement ? 3 : 0;
		}

		if (!seen_statement) {
			if (multiline_statement)
				seen_statement = true; // if we are in a multiline string, we
									   // simply want to copy everything
									   // (including indentation)
			// We have not seen a statement on this line yet
			else if (code[i] == '\n') {
				// Empty line, skip to the next one
				initial_spaces = 0;
			} else if (code[i] == ' ') {
				initial_spaces++;
			} else if (code[i] == '\t') {
				initial_spaces += tabwidth;
			} else {
				// Look through the indentation_levels array to find the level
				// of the statement
				// from the amount of initial spaces
				bool placed = false;
				size_t level = 0;
				// Statement starts here
				seen_statement = true;
				for (j = 0; j < indentation_count; j++) {
					if (initial_spaces == indentation_levels[j]) {
						level = j;
						placed = true;
						break;
					}
				}
				if (!placed) {
					// This should never happen, because it means the initial
					// spaces was not present in the array
					// When we just did exactly the same loop over the array, we
					// should have encountered this statement
					// This means that something happened to either the
					// indentation_levels array or something happened to the
					// code
					*msg = createException(MAL, "pyapi.eval",
										   "If you see this error something "
										   "went wrong in the code. Sorry.");
					goto finally;
				}
				for (j = 0; j < (level + 1) * spaces_per_level; j++) {
					// Add spaces to the code
					newcode[code_location++] = ' ';
				}
			}
		}
		if (seen_statement) {
			// We have seen a statement on this line, copy it
			newcode[code_location++] = code[i];
			if (code[i] == '\n') {
				// The statement has ended, move on to the next line
				seen_statement = false;
				initial_spaces = 0;
				statement_size = 0;
			}
		}
	}
	newcode[code_location] = '\0';
	if (code_location >= size) {
		// Something went wrong with our size computation, this also should
		// never happen
		*msg = createException(MAL, "pyapi.eval",
							   "If you see this error something went wrong in "
							   "the code (size computation). Sorry.");
		goto finally;
	}
finally:
	GDKfree(indentation_levels);
	GDKfree(statements_per_level);
	return newcode;
}

void _formatinput_init(void) { _import_array(); }
