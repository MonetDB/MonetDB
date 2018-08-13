/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * The error strings are geared at answering the question "what happened".
 * Optional information about "why it happened" is added
 * as local strings in the code base with possibly runtime information.
 * Information on "how to avoid it" is sporadically added using expected values.
 *
 * General considerations on error classes are summarized below:
 * MAL_MALLOC_FAIL
 * An operation allocates space for an object failed.
 * Either the pre-requisites are not satisfied, or the system
 * runs low on resources and can not accomodate the object.
 * For failures to create BATs it sometimes indicates that an
 * intermediate BAT size is too large.
 * OPERATION_FAILED
 * Mostly the module and function name are indicative enough.
 * If possible more is said about the error context,
 * informative references to arguments or variables,
 * provided it is produced by the underlying implementation.
 * GDK_EXCEPTION
 * In general these are generated deep inside the kernel.
 * They are captured by the MAL interpreter.
 * SEMANTIC_*
 * The patterns can be used to defer semantic type errors
 * to runtime.
 * 
 * Getting all exception strings in one place improves consistency
 * and maintenance.
 *
 * At a later stage we could introduce internationalization support, i.e.
 * use a translation table where needed.
 */
#ifndef MAL_ERRORS
#define MAL_ERRORS

#define SQLSTATE(sqlstate)	#sqlstate "!"

#define MANUAL_HELP "See documentation for details"

#define PROGRAM_GENERAL "Program contains errors."
#define PROGRAM_NYI  "Not yet implemented"

#define SYNTAX_GENERAL	"Syntax error detected."
#define SYNTAX_SIGNATURE "Function signature missing."

#define SEMANTIC_GENERAL "Semantic errors detected"
#define SEMANTIC_PROGRAM_ERRORS "Program contains semantic errors."
#define SEMANTIC_SIGNATURE_MISSING "Function signature missing."
#define SEMANTIC_OPERATION_MISSING "Operation not found."
#define SEMANTIC_TYPE_ERROR "Explicit type required"
#define SEMANTIC_TYPE_MISMATCH "Type mismatch"

#define INTERNAL_BAT_ACCESS "Internal error, can not access BAT."
#define INTERNAL_BAT_HEAD "BAT has wrong head type"
#define INTERNAL_OBJ_CREATE "Can not create object"
#define INTERNAL_AUTHORIZATION "authorization BATs not empty"

#define MAL_MALLOC_FAIL	"Could not allocate space"
#define MAL_STACK_FAIL	"Running out of stack space."
#define MAL_CALLDEPTH_FAIL	"Recursive call limit reached."

#define INVCRED_ACCESS_DENIED "access denied for user"
#define INVCRED_INVALID_USER "invalid credentials for user"
#define INVCRED_REMOVE_USER "Can not remove user"
#define INVCRED_WRONG_ID "Undefined client id"

#define RUNTIME_IO_EOF "Attempt to read beyond end-of-file"
#define RUNTIME_FILE_NOT_FOUND "File not found"
#define RUNTIME_UNLINK "File could not be unlinked"
#define RUNTIME_DIR_ERROR "Unable to open directory"
#define RUNTIME_CREATE_ERROR "Unable to create file/directory"
#define RUNTIME_STREAM_FAILED "Could not create stream"
#define RUNTIME_STREAM_WRITE "Could not write to stream"
#define RUNTIME_STREAM_INPUT "Could not read from stream"

#define RUNTIME_LOAD_ERROR "Loading error"
#define RUNTIME_OBJECT_MISSING "Object not found"
#define RUNTIME_SIGNATURE_MISSING "The <module>.<function> not found"
#define RUNTIME_OBJECT_UNDEFINED "Object not found"
#define RUNTIME_UNKNOWN_INSTRUCTION "Instruction type not supported"
#define RUNTIME_QRY_TIMEOUT "Query aborted due to timeout"
#define RUNTIME_SESSION_TIMEOUT "Query aborted due to session timeout"
#define OPERATION_FAILED "operation failed"

#define BOX_CLOSED "Box is not open"

#define SABAOTH_NOT_INITIALIZED "Sabaoth not initialized"
#define SABAOTH_USE_RESTRICTION "Sabaoth was not initialized as active database"

#define SCENARIO_NOT_FOUND "Scenario not initialized"

#define MACRO_SYNTAX_ERROR "RETURN statement is not the last one"
#define MACRO_DUPLICATE "Duplicate macro expansion"
#define MACRO_TOO_DEEP "Too many macro expansions"

#define OPTIMIZER_CYCLE "Too many optimization cycles"

#define ILLARG_NOTNIL " NIL not allowed"
#define ILLARG_CONSTANTS "Constant argument required"

#define ILLEGAL_ARGUMENT "Illegal argument"
#define IDENTIFIER_EXPECTED "Identifier expected"
#define POSITIVE_EXPECTED "Argument must be positive"
#define ARGUMENT_TOO_LARGE "Argument too large"
#define TOO_MANY_BITS "Too many bits"
#define DUPLICATE_DEFINITION "Duplicate definition"
#define RANGE_ERROR "Range error"

#define SERVER_STOPPED "Server stopped"

#define XML_PARSE_ERROR "Document parse error"
#define XML_COMMENT_ERROR "Comment may not contain '--'"
#define XML_PI_ERROR "No processing instruction target specified"
#define XML_VERSION_ERROR "Illegal XML version"
#define XML_STANDALONE_ERROR "Illegal XML standalone value"
#define XML_NOT_WELL_FORMED "Resulting document not well-formed"
#define XML_ATTRIBUTE_ERROR "No attribute name specified"
#define XML_ATTRIBUTE_INVALID "Invalid attribute name"
#define XML_NO_ELEMENT "No element name specified"
#define XML_NO_NAMESPACE "Namespace support not implemented"
#define XML_ILLEGAL_NAMESPACE "Illegal namespace"
#define XML_ILLEGAL_ATTRIBUTE "Illegal attribute"
#define XML_ILLEGAL_CONTENT "Illegal content"

#define GDK_EXCEPTION "GDK reported error."
#define MAL_DEPRECATED "Deprecated MAL operation."

#define TYPE_NOT_SUPPORTED "Type is not supported"
#endif /* MAL_ERRORS */
