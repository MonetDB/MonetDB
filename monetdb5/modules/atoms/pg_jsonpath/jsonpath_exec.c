/*-------------------------------------------------------------------------
 *
 * jsonpath_exec.c
 *	 Routines for SQL/JSON path execution.
 *
 * Jsonpath is executed in the global context stored in JsonPathExecContext,
 * which is passed to almost every function involved into execution.  Entry
 * point for jsonpath execution is executeJsonPath() function, which
 * initializes execution context including initial JsonPathItem and JsonbValue,
 * flags, stack for calculation of @ in filters.
 *
 * The result of jsonpath query execution is enum JsonPathExecResult and
 * if succeeded sequence of JsonbValue, written to JsonValueList *found, which
 * is passed through the jsonpath items.  When found == NULL, we're inside
 * exists-query and we're interested only in whether result is empty.  In this
 * case execution is stopped once first result item is found, and the only
 * execution result is JsonPathExecResult.  The values of JsonPathExecResult
 * are following:
 * - jperOk			-- result sequence is not empty
 * - jperNotFound	-- result sequence is empty
 * - jperError		-- error occurred during execution
 *
 * Jsonpath is executed recursively (see executeItem()) starting form the
 * first path item (which in turn might be, for instance, an arithmetic
 * expression evaluated separately).  On each step single JsonbValue obtained
 * from previous path item is processed.  The result of processing is a
 * sequence of JsonbValue (probably empty), which is passed to the next path
 * item one by one.  When there is no next path item, then JsonbValue is added
 * to the 'found' list.  When found == NULL, then execution functions just
 * return jperOk (see executeNextItem()).
 *
 * Many of jsonpath operations require automatic unwrapping of arrays in lax
 * mode.  So, if input value is array, then corresponding operation is
 * processed not on array itself, but on all of its members one by one.
 * executeItemOptUnwrapTarget() function have 'unwrap' argument, which indicates
 * whether unwrapping of array is needed.  When unwrap == true, each of array
 * members is passed to executeItemOptUnwrapTarget() again but with unwrap == false
 * in order to avoid subsequent array unwrapping.
 *
 * All boolean expressions (predicates) are evaluated by executeBoolItem()
 * function, which returns tri-state JsonPathBool.  When error is occurred
 * during predicate execution, it returns jpbUnknown.  According to standard
 * predicates can be only inside filters.  But we support their usage as
 * jsonpath expression.  This helps us to implement @@ operator.  In this case
 * resulting JsonPathBool is transformed into jsonb bool or null.
 *
 * Arithmetic and boolean expression are evaluated recursively from expression
 * tree top down to the leaves.  Therefore, for binary arithmetic expressions
 * we calculate operands first.  Then we check that results are numeric
 * singleton lists, calculate the result and pass it to the next path item.
 *
 * Copyright (c) 2019-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_exec.c
 *
 *-------------------------------------------------------------------------
 */

#include "jsonpath.h"
#include "postgres_defines_internal.h"

/*
 * Represents "base object" and it's "id" for .keyvalue() evaluation.
 */
typedef struct JsonBaseObjectInfo
{
	// REPLACED sonbContainer *jbc;
	JsonbValue *jbc;
	int			id;
} JsonBaseObjectInfo;

/* Callbacks for executeJsonPath() */
typedef JsonbValue *(*JsonPathGetVarCallback) (void *vars , yyjson_alc* alc, char *varName, int varNameLen,
											   JsonbValue **baseObject, int *baseObjectId);
typedef int (*JsonPathCountVarsCallback) (void *vars);

/*
 * Context of jsonpath execution.
 */
typedef struct JsonPathExecContext
{
	void	   *vars;			/* variables to substitute into jsonpath */
	JsonPathGetVarCallback getVar_;	/* callback to extract a given variable
									 * from 'vars' */
	JsonbValue *root;			/* for $ evaluation */
	JsonbValue *current;		/* for @ evaluation */
	JsonBaseObjectInfo baseObject;	/* "base object" for .keyvalue()
									 * evaluation */
	yyjson_alc 		*alc;
	allocator		*sa;
	yyjson_mut_doc 	*mutable_doc;
	int			lastGeneratedObjectId;	/* "id" counter for .keyvalue()
										 * evaluation */
	int			innermostArraySize; /* for LAST array index evaluation */
	bool		laxMode;		/* true for "lax" mode, false for "strict"
								 * mode */
	bool		ignoreStructuralErrors; /* with "true" structural errors such
										 * as absence of required json item or
										 * unexpected json item type are
										 * ignored */
	bool		throwErrors;	/* with "false" all suppressible errors are
								 * suppressed */
	bool		useTz;
	char* _errmsg;
} JsonPathExecContext;

/* Context for LIKE_REGEX execution. */
typedef struct JsonLikeRegexContext
{
	text	   *regex;
	int			cflags;
} JsonLikeRegexContext;

/* Result of jsonpath predicate evaluation */
typedef enum JsonPathBool
{
	jpbFalse = 0,
	jpbTrue = 1,
	jpbUnknown = 2
} JsonPathBool;

/* Result of jsonpath expression evaluation */
typedef enum JsonPathExecResult
{
	jperOk = 0,
	jperNotFound = 1,
	jperError = 2
} JsonPathExecResult;

#define jperIsError(jper)			((jper) == jperError)

/*
 * List of jsonb values with shortcut for single-value list.
 */
typedef struct JsonValueList
{
	JsonbValue *singleton;
	List	   *list;
} JsonValueList;

typedef struct JsonValueListIterator
{
	JsonbValue *value;
	List	   *list;
	ListCell   *next;
} JsonValueListIterator;

/* Structures for JSON_TABLE execution  */

/*
 * Struct holding the result of jsonpath evaluation, to be used as source row
 * for JsonTableGetValue() which in turn computes the values of individual
 * JSON_TABLE columns.
 */
typedef struct JsonTablePlanRowSource
{
	Datum		value;
	bool		isnull;
} JsonTablePlanRowSource;

/* strict/lax flags is decomposed into four [un]wrap/error flags */
#define jspStrictAbsenceOfErrors(cxt)	(!(cxt)->laxMode)
#define jspAutoUnwrap(cxt)				((cxt)->laxMode)
#define jspAutoWrap(cxt)				((cxt)->laxMode)
#define jspIgnoreStructuralErrors(cxt)	((cxt)->ignoreStructuralErrors)
#define jspThrowErrors(cxt)				((cxt)->throwErrors)

/* Convenience macro: return or throw error depending on context */
#define RETURN_ERROR(throw_error) \
do { \
	if (jspThrowErrors(cxt)) \
		throw_error; \
	return jperError; \
} while (0)

typedef JsonPathBool (*JsonPathPredicateCallback) (JsonPathItem *jsp,
												   JsonbValue *larg,
												   JsonbValue *rarg,
												   void *param);
typedef Numeric (*BinaryArithmFunc) (Numeric num1, Numeric num2, bool *error);

static JsonPathExecResult executeJsonPath(JsonPath *path, void *vars,
										  JsonPathGetVarCallback getVar_,
										  JsonPathCountVarsCallback countVars,
										  Jsonb *json, bool throwErrors,
										  JsonValueList *result, bool useTz, JsonPathExecContext* cxt);
static JsonPathExecResult executeItem(JsonPathExecContext *cxt,
									  JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
static JsonPathExecResult executeItemOptUnwrapTarget(JsonPathExecContext *cxt,
													 JsonPathItem *jsp, JsonbValue *jb,
													 JsonValueList *found, bool unwrap);
static JsonPathExecResult executeItemUnwrapTargetArray(JsonPathExecContext *cxt,
													   JsonPathItem *jsp, JsonbValue *jb,
													   JsonValueList *found, bool unwrapElements);
static JsonPathExecResult executeNextItem(JsonPathExecContext *cxt,
										  JsonPathItem *cur, JsonPathItem *next,
										  JsonbValue *v, JsonValueList *found, bool copy);
static JsonPathExecResult executeItemOptUnwrapResult(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
													 bool unwrap, JsonValueList *found);
static JsonPathExecResult executeItemOptUnwrapResultNoThrow(JsonPathExecContext *cxt, JsonPathItem *jsp,
															JsonbValue *jb, bool unwrap, JsonValueList *found);
static JsonPathBool executeBoolItem(JsonPathExecContext *cxt,
									JsonPathItem *jsp, JsonbValue *jb, bool canHaveNext);
static JsonPathBool executeNestedBoolItem(JsonPathExecContext *cxt,
										  JsonPathItem *jsp, JsonbValue *jb);
static JsonPathExecResult executeAnyItem(JsonPathExecContext *cxt,
										 JsonPathItem *jsp, JsonbValue *jbc, JsonValueList *found,
										 uint32 level, uint32 first, uint32 last,
										 bool ignoreStructuralErrors, bool unwrapNext);
static JsonPathBool executePredicate(JsonPathExecContext *cxt,
									 JsonPathItem *pred, JsonPathItem *larg, JsonPathItem *rarg,
									 JsonbValue *jb, bool unwrapRightArg,
									 JsonPathPredicateCallback exec, void *param);
static JsonPathExecResult executeBinaryArithmExpr(JsonPathExecContext *cxt,
												  JsonPathItem *jsp, JsonbValue *jb,
												  BinaryArithmFunc func, JsonValueList *found);
typedef Numeric (*PGFunction) (Numeric);
static JsonPathExecResult executeUnaryArithmExpr(JsonPathExecContext *cxt,
												 JsonPathItem *jsp, JsonbValue *jb, PGFunction func,
												 JsonValueList *found);
static JsonPathBool executeStartsWith(JsonPathItem *jsp,
									  JsonbValue *whole, JsonbValue *initial, void *param);
static JsonPathBool executeLikeRegex(JsonPathItem *jsp, JsonbValue *str,
									 JsonbValue *rarg, void *param);
static JsonPathExecResult executeNumericItemMethod(JsonPathExecContext *cxt,
												   JsonPathItem *jsp, JsonbValue *jb, bool unwrap, PGFunction func,
												   JsonValueList *found);
static JsonPathExecResult executeKeyValueMethod(JsonPathExecContext *cxt,
												JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
static JsonPathExecResult appendBoolResult(JsonPathExecContext *cxt,
										   JsonPathItem *jsp, JsonValueList *found, JsonPathBool res);
static JsonbValue *GetJsonPathVar(void *cxt , yyjson_alc* alc, char *varName, int varNameLen,
								  JsonbValue **baseObject, int *baseObjectId);
static int	CountJsonPathVars(void *cxt);
static yyjson_mut_val*
JsonItemFromDatum(Datum val, Oid typid, int32 typmod, yyjson_mut_doc* mutable_doc);
static int	JsonbArraySize(JsonbValue *jb);
static JsonPathBool executeComparison(JsonPathItem *cmp, JsonbValue *lv,
									  JsonbValue *rv, void *p);
static JsonPathBool compareItems(JsonPathExecContext *cxt, int32 op, JsonbValue *jb1, JsonbValue *jb2,
								 bool useTz);
static int	compareNumeric(Numeric a, Numeric b);
static JsonPathExecResult getArrayIndex(JsonPathExecContext *cxt,
										JsonPathItem *jsp, JsonbValue *jb, int32 *index);
static JsonBaseObjectInfo setBaseObject(JsonPathExecContext *cxt,
										JsonbValue *jbv, int32 id);
// REPLACED static void JsonValueListClear(JsonValueList *jvl);
static void JsonValueListAppend(JsonPathExecContext *cxt, JsonValueList *jvl, JsonbValue *jbv);
static int	JsonValueListLength(const JsonValueList *jvl);
static bool JsonValueListIsEmpty(JsonValueList *jvl);
static JsonbValue *JsonValueListHead(JsonValueList *jvl);
// REPLACED static List *JsonValueListGetList(JsonValueList *jvl);
static void JsonValueListInitIterator(const JsonValueList *jvl,
									  JsonValueListIterator *it);
static JsonbValue *JsonValueListNext(const JsonValueList *jvl,
									 JsonValueListIterator *it);
// REPLACED static JsonbValue *JsonbInitBinary(JsonbValue *jbv, Jsonb *jb);
static int	JsonbType(JsonbValue *jb);
static bool isObjectOrArray(JsonbValue *jb);
static JsonbValue *getScalar(JsonbValue *scalar, enum jbvType type);
static JsonbValue *wrapItemsInArray(yyjson_alc* alc, const JsonValueList *items);

/********************Execute functions for JsonPath**************************/

/*
 * Interface to jsonpath executor
 *
 * 'path' - jsonpath to be executed
 * 'vars' - variables to be substituted to jsonpath
 * 'getVar_' - callback used by getJsonPathVariable() to extract variables from
 *		'vars'
 * 'countVars' - callback to count the number of jsonpath variables in 'vars'
 * 'json' - target document for jsonpath evaluation
 * 'throwErrors' - whether we should throw suppressible errors
 * 'result' - list to store result items into
 *
 * Returns an error if a recoverable error happens during processing, or NULL
 * on no error.
 *
 * Note, jsonb and jsonpath values should be available and untoasted during
 * work because JsonPathItem, JsonbValue and result item could have pointers
 * into input values.  If caller needs to just check if document matches
 * jsonpath, then it doesn't provide a result arg.  In this case executor
 * works till first positive result and does not check the rest if possible.
 * In other case it tries to find all the satisfied result items.
 */
static JsonPathExecResult
executeJsonPath(JsonPath *path, void *vars, JsonPathGetVarCallback getVar_,
				JsonPathCountVarsCallback countVars,
				Jsonb *json, bool throwErrors, JsonValueList *result,
				bool useTz, JsonPathExecContext* cxt)
{
	JsonPathExecResult res;
	JsonPathItem* jsp;
	JsonbValue*	jbv;

	jsp = path->expr;

	
	jbv = json;

	cxt->vars = vars;
	cxt->getVar_ = getVar_;
	cxt->laxMode = (path->lax);
	cxt->ignoreStructuralErrors = cxt->laxMode;
	cxt->root = cxt->current = jbv;
	cxt->baseObject.jbc = NULL;
	cxt->baseObject.id = 0;
	/* 1 + number of base objects in vars */
	cxt->lastGeneratedObjectId = 1 + countVars(vars);
	cxt->innermostArraySize = -1;
	cxt->throwErrors = throwErrors;
	cxt->useTz = useTz;
	cxt->mutable_doc = yyjson_mut_doc_new(cxt->alc);

	if (jspStrictAbsenceOfErrors(cxt) && !result)
	{
		/*
		 * In strict mode we must get a complete list of values to check that
		 * there are no errors at all.
		 */
		JsonValueList vals = {0};

		res = executeItem(cxt, jsp, jbv, &vals);

		if (jperIsError(res))
			return res;

		return JsonValueListIsEmpty(&vals) ? jperNotFound : jperOk;
	}

	res = executeItem(cxt, jsp, jbv, result);

	Assert(!throwErrors || !jperIsError(res));

	return res;
}

/*
 * Execute jsonpath with automatic unwrapping of current item in lax mode.
 */
static JsonPathExecResult
executeItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
			JsonbValue *jb, JsonValueList *found)
{
	return executeItemOptUnwrapTarget(cxt, jsp, jb, found, jspAutoUnwrap(cxt));
}

/*
 * Main jsonpath executor function: walks on jsonpath structure, finds
 * relevant parts of jsonb and evaluates expressions over them.
 * When 'unwrap' is true current SQL/JSON item is unwrapped if it is an array.
 */
static JsonPathExecResult
executeItemOptUnwrapTarget(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb, JsonValueList *found, bool unwrap)
{
	JsonPathItem* elem;
	JsonPathExecResult res = jperNotFound;
	JsonBaseObjectInfo baseObject;

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	switch (jsp->type)
	{
		case jpiNull:
		case jpiBool:
		case jpiNumeric:
		case jpiString:
		case jpiVariable:
			{
				JsonbValue *v;
				bool		hasNext = (elem = jsp->next) ? true: false;

				if (!hasNext && !found && jsp->type != jpiVariable)
				{
					/*
					 * Skip evaluation, but not for variables.  We must
					 * trigger an error for the missing variable.
					 */
					res = jperOk;
					break;
				}

				baseObject = cxt->baseObject;
				yyjson_mut_val* mut_val = NULL;
				
				switch (jsp->type)
				{
					case jpiNull:
						mut_val = yyjson_mut_null(cxt->mutable_doc);
						break;
					case jpiBool:
						mut_val = yyjson_mut_bool(cxt->mutable_doc, jspGetBool(jsp));
						break;
					case jpiNumeric:
						Numeric num = jspGetNumeric(jsp);
						if (num.type == YYJSON_SUBTYPE_REAL)
							mut_val = yyjson_mut_real(cxt->mutable_doc, num.dnum);
						else
							mut_val = yyjson_mut_int(cxt->mutable_doc, num.lnum);
						break;
					case jpiString:
						mut_val = yyjson_mut_str(cxt->mutable_doc, jspGetString(jsp, NULL));
						break;
					case jpiVariable:
						{
							char	   *varName;
							int			varNameLength;
							JsonbValue*	baseObject;
							int			baseObjectId;
							JsonbValue *v;

							Assert(jsp->type == jpiVariable);
							varName = jspGetString(jsp, &varNameLength);

							if (cxt->vars == NULL ||
								(v = cxt->getVar_(cxt->vars, cxt->alc , varName, varNameLength,
												&baseObject, &baseObjectId)) == NULL) {
								ereport(ERROR,
										(errcode(ERRCODE_UNDEFINED_OBJECT),
										errmsg("could not find jsonpath variable \"%s\"", varName)));
								return jperError;
												}

							if (baseObjectId > 0)
							{
								setBaseObject(cxt, baseObject, baseObjectId);
								mut_val =  yyjson_val_mut_copy(cxt->mutable_doc,  v);
							}
						}
						break;
					default:
						elog(ERROR, "unexpected jsonpath item type");
						return jperError;
				}
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_val, cxt->alc);
				v = yyjson_doc_get_root(doc);

				res = executeNextItem(cxt, jsp, elem,
									  v, found, hasNext);
				cxt->baseObject = baseObject;
			}
			break;

			/* all boolean item types: */
		case jpiAnd:
		case jpiOr:
		case jpiNot:
		case jpiIsUnknown:
		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
		case jpiExists:
		case jpiStartsWith:
		case jpiLikeRegex:
			{
				JsonPathBool st = executeBoolItem(cxt, jsp, jb, true);

				res = appendBoolResult(cxt, jsp, found, st);
				break;
			}

		case jpiAdd:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_add_opt_error, found);

		case jpiSub:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_sub_opt_error, found);

		case jpiMul:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_mul_opt_error, found);

		case jpiDiv:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_div_opt_error, found);

		case jpiMod:
			return executeBinaryArithmExpr(cxt, jsp, jb,
										   numeric_mod_opt_error, found);

		case jpiPlus:
			return executeUnaryArithmExpr(cxt, jsp, jb, NULL, found);

		case jpiMinus:
			return executeUnaryArithmExpr(cxt, jsp, jb, numeric_uminus,
										  found);

		case jpiAnyArray:
			if (JsonbType(jb) == jbvArray)
			{
				bool		hasNext = (elem = jsp->next);

				res = executeItemUnwrapTargetArray(cxt, hasNext ? elem : NULL,
												   jb, found, jspAutoUnwrap(cxt));
			}
			else if (jspAutoWrap(cxt))
				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			else if (!jspIgnoreStructuralErrors(cxt))
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
									  errmsg("jsonpath wildcard array accessor can only be applied to an array"))));
									  
			break;

		case jpiAnyKey:
			if (JsonbType(jb) == jbvObject)
			{
				bool		hasNext = (elem = jsp->next);

				return executeAnyItem
					(cxt, hasNext ? elem : NULL,
					 jb, found, 1, 1, 1,
					 false, jspAutoUnwrap(cxt));
			}
			else if (unwrap && JsonbType(jb) == jbvArray)
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND),
									  errmsg("jsonpath wildcard member accessor can only be applied to an object"))));
			}
			break;

		case jpiIndexArray:
			if (JsonbType(jb) == jbvArray || jspAutoWrap(cxt))
			{
				int			innermostArraySize = cxt->innermostArraySize;
				int			i;
				int			size = JsonbArraySize(jb);
				bool		singleton = size < 0;
				bool		hasNext = (elem = jsp->next);

				if (singleton)
					size = 1;

				cxt->innermostArraySize = size; /* for LAST evaluation */

				for (i = 0; i < jsp->value.array.nelems; i++) 
				{
					JsonPathItem* from;
					JsonPathItem* to;
					int32		index;
					int32		index_from;
					int32		index_to;
					bool		range;

					Assert(jsp->type == jpiIndexArray);
					from = jsp->value.array.elems[i].from;
					if (!jsp->value.array.elems[i].to)
						range = false;
					else {
						to = jsp->value.array.elems[i].to;
						range = true;
					}

					res = getArrayIndex(cxt, from, jb, &index_from);

					if (jperIsError(res))
						break;

					if (range)
					{
						res = getArrayIndex(cxt, to, jb, &index_to);

						if (jperIsError(res))
							break;
					}
					else
						index_to = index_from;

					if (!jspIgnoreStructuralErrors(cxt) &&
						(index_from < 0 ||
						 index_from > index_to ||
						 index_to >= size))
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
											  errmsg("jsonpath array subscript is out of bounds"))));

					if (index_from < 0)
						index_from = 0;

					if (index_to >= size)
						index_to = size - 1;

					res = jperNotFound;

					for (index = index_from; index <= index_to; index++)
					{
						JsonbValue *v;
						bool		copy;

						if (singleton)
						{
							v = jb;
							copy = true;
						}
						else
						{
							v = yyjson_arr_get(jb, index);


							if (v == NULL)
								continue;

							copy = false;
						}

						if (!hasNext && !found)
							return jperOk;

						res = executeNextItem(cxt, jsp, elem, v, found,
											  copy);

						if (jperIsError(res))
							break;

						if (res == jperOk && !found)
							break;
					}

					if (jperIsError(res))
						break;

					if (res == jperOk && !found)
						break;
				}

				cxt->innermostArraySize = innermostArraySize;
			}
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
									  errmsg("jsonpath array accessor can only be applied to an array"))));
			}
			break;

		case jpiAny:
			{
				bool		hasNext = (elem = jsp->next);

				/* first try without any intermediate steps */
				if (jsp->value .anybounds.first == 0)
				{
					bool		savedIgnoreStructuralErrors;

					savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
					cxt->ignoreStructuralErrors = true;
					res = executeNextItem(cxt, jsp, elem,
										  jb, found, true);
					cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;

					if (res == jperOk && !found)
						break;
				}

				res = executeAnyItem
					(cxt, hasNext ? elem : NULL,
						jb, found,
						1,
						jsp->value .anybounds.first,
						jsp->value .anybounds.last,
						true, jspAutoUnwrap(cxt));
				break;
			}

		case jpiKey:
			if (JsonbType(jb) == jbvObject)
			{
				str key = jspGetString(jsp, NULL);
				yyjson_val * v = yyjson_obj_get(jb, key);

				if (v != NULL)
				{
					res = executeNextItem(cxt, jsp, NULL,
										  v, found, false);
				}
				else if (!jspIgnoreStructuralErrors(cxt))
				{
					Assert(found);
					RETURN_ERROR(ereport(ERROR,
							(errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND), \
							 errmsg("JSON object does not contain key \"%s\"", key))));
				}
			}
			else if (unwrap && JsonbType(jb) == jbvArray)
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
			else if (!jspIgnoreStructuralErrors(cxt))
			{
				Assert(found);
				RETURN_ERROR(ereport(ERROR,
									 (errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND),
									  errmsg("jsonpath member accessor can only be applied to an object"))));
			}
			break;

		case jpiCurrent:
			res = executeNextItem(cxt, jsp, NULL, cxt->current,
								  found, true);
			break;

		case jpiRoot:
			jb = cxt->root;
			baseObject = setBaseObject(cxt, jb, 0);
			res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			cxt->baseObject = baseObject;
			break;

		case jpiFilter:
			{
				JsonPathBool st;

				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				elem = jsp->value.arg;
				st = executeNestedBoolItem(cxt, elem, jb);
				if (st != jpbTrue)
					res = jperNotFound;
				else
					res = executeNextItem(cxt, jsp, NULL,
										  jb, found, true);
				break;
			}

		case jpiType:
			{
				yyjson_mut_val * mut_jbv = yyjson_mut_str(cxt->mutable_doc, yyjson_get_type_desc(jb));
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				yyjson_val* jbv = yyjson_doc_get_root(doc);

				res = executeNextItem(cxt, jsp, NULL, jbv,
									  found, false);
			}
			break;

		case jpiSize:
			{
				int			size = JsonbArraySize(jb);

				if (size < 0)
				{
					if (!jspAutoWrap(cxt))
					{
						if (!jspIgnoreStructuralErrors(cxt))
							RETURN_ERROR(ereport(ERROR,
												 (errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND),
												  errmsg("jsonpath item method .%s() can only be applied to an array",
														 jspOperationName(jsp->type)))));
						break;
					}

					size = 1;
				}
				yyjson_mut_val * mut_jbv = yyjson_mut_int(cxt->mutable_doc, size);
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				yyjson_val* jbv = yyjson_doc_get_root(doc);

				res = executeNextItem(cxt, jsp, NULL, jbv, found, false);

			}
			break;

		case jpiAbs:
			return executeNumericItemMethod(cxt, jsp, jb, unwrap, numeric_abs,
											found);
		case jpiDouble:
			{
				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);
				if (JsonbType(jb) == jbvNumeric)
				{
					double val;

					if (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_SINT) {
						lng lval = yyjson_get_int(jb);
						val = (double) lval;
					}
					else {
						assert (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_REAL);
						val = yyjson_get_real(jb);
					}
					if (isinf(val) || isnan(val))
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											  errmsg("NaN or Infinity is not allowed for jsonpath item method .%s()",
													 jspOperationName(jsp->type)))));

					yyjson_mut_val * mut_jbv = yyjson_mut_double(cxt->mutable_doc, val);
					yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
					jb = yyjson_doc_get_root(doc);
					res = jperOk;
				}
				else if (JsonbType(jb) == jbvString)
				{
					/* cast string as double */
					double		val;
					const char *tmp = yyjson_get_str(jb);
					size_t len = sizeof(dbl);
					dbl* pval = &val;

					if (dblFromStr(tmp, &len, &pval, true) < 0)
						RETURN_ERROR(ereport(ERROR,
											(errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											errmsg("%s is not allowed for jsonpath item method .%s()",
													tmp, jspOperationName(jsp->type)))));

					if (isinf(val) || isnan(val))
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											  errmsg("NaN or Infinity is not allowed for jsonpath item method .%s()",
													 jspOperationName(jsp->type)))));

					yyjson_mut_val * mut_jbv = yyjson_mut_double(cxt->mutable_doc, val);
					yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
					jb = yyjson_doc_get_root(doc);
					res = jperOk;
				}

				if (res == jperNotFound)
					RETURN_ERROR(ereport(ERROR,
										 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
										  errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
												 jspOperationName(jsp->type)))));

				res = executeNextItem(cxt, jsp, NULL, jb, found, false);
			}
			break;

		case jpiDatetime:
		case jpiDate:
		case jpiTime:
		case jpiTimeTz:
		case jpiTimestamp:
		case jpiTimestampTz:
			assert (0);
			// TODO: remove datetime functionality
			return jperError;

		case jpiKeyValue:
			if (unwrap && JsonbType(jb) == jbvArray)
				return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

			return executeKeyValueMethod(cxt, jsp, jb, found);

		case jpiLast:
			{
				JsonbValue	tmpjbv;
				JsonbValue *lastjbv;
				int			last;
				bool		hasNext = (elem = jsp->next);

				if (cxt->innermostArraySize < 0) {
					elog(ERROR, "evaluating jsonpath LAST outside of array subscript");
					return jperError;
				}

				if (!hasNext && !found)
				{
					res = jperOk;
					break;
				}

				last = cxt->innermostArraySize - 1;

				(void) tmpjbv;
				yyjson_mut_val * mut_jbv = yyjson_mut_sint(cxt->mutable_doc, last);
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				lastjbv = yyjson_doc_get_root(doc);

				res = executeNextItem(cxt, jsp, elem,
									  lastjbv, found, hasNext);
			}
			break;

		case jpiBigint:
			{
				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				lng		datum;
				if (JsonbType(jb) == jbvNumeric)
				{
					if (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_SINT) {
						datum = yyjson_get_int(jb);
					}
					else {
						assert (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_REAL);
						datum = (lng) yyjson_get_real(jb);
					}
					res = jperOk;
				}
				else if (JsonbType(jb) == jbvString)
				{
					const char* src = yyjson_get_str(jb);
					size_t llen = sizeof(lng);
					lng lval;
					lng* plval = &lval;

					size_t dlen = sizeof(dbl);
					dbl dval;
					dbl* pdval = &dval;

					if (lngFromStr(src, &llen, &plval, false) > 0) {
						datum = lval;
					}
					else if (dblFromStr(src, &dlen, &pdval, false) > 0) {
						datum = (lng) dval;
					}
					else {
						RETURN_ERROR(ereport(ERROR,
											(errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											errmsg("%s is not allowed for jsonpath item method .%s()",
													src, jspOperationName(jsp->type)))));
					}
					res = jperOk;
				}

				if (res == jperNotFound)
					RETURN_ERROR(ereport(ERROR,
										 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
										  errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
												 jspOperationName(jsp->type)))));
				yyjson_mut_val * mut_jbv = yyjson_mut_int(cxt->mutable_doc, datum);
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				jb = yyjson_doc_get_root(doc);
				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			}
			break;

		case jpiBoolean:
			{
				bool		bval;

				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				if (JsonbType(jb) == jbvBool)
				{
					bval = yyjson_get_bool(jb);

					res = jperOk;
				}
				else if (JsonbType(jb) == jbvNumeric)
				{
					if (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_SINT) {
						bval = yyjson_get_int(jb)?1:0;
					}
					else {
						assert (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_REAL);
						bval = yyjson_get_real(jb)?1:0;
					}

					res = jperOk;
				}
				else if (JsonbType(jb) == jbvString)
				{
					res = jperOk;
					const char *tmp = yyjson_get_str(jb);
					size_t len = sizeof(bit);
					bit* pbval = (bit*) &bval;
					if (bitFromStr(tmp, &len, &pbval, true) < 0)
						RETURN_ERROR(ereport(ERROR,
											(errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											errmsg("%s is not allowed for jsonpath item method .%s()",
													tmp, jspOperationName(jsp->type)))));
				}

				if (res == jperNotFound)
					RETURN_ERROR(ereport(ERROR,
										 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
										  errmsg("jsonpath item method .%s() can only be applied to a boolean, string, or numeric value",
												 jspOperationName(jsp->type)))));

				yyjson_mut_val * mut_jbv = yyjson_mut_bool(cxt->mutable_doc, bval);
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				jb = yyjson_doc_get_root(doc);
				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			}
			break;

		case jpiDecimal:
		case jpiNumber:
			{
				dbl			num;
				char	   *numstr = NULL;

				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				if (JsonbType(jb) == jbvNumeric)
				{
					if (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_SINT) {
						num = (dbl) yyjson_get_int(jb);
					}
					else {
						assert (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_REAL);
						num = yyjson_get_real(jb);
					}
					res = jperOk;
				}
				else if (JsonbType(jb) == jbvString)
				{
					numstr = (char*) yyjson_get_str(jb);
					size_t len = sizeof(lng);
					dbl* pnum = &num;
					if (dblFromStr(numstr, &len, &pnum, true) < 0)
						res = jperNotFound;
					else
						res = jperOk;
				}

				if (res == jperNotFound)
					RETURN_ERROR(ereport(ERROR,
										 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
										  errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
												 jspOperationName(jsp->type)))));

				yyjson_mut_val * mut_jbv = yyjson_mut_real(cxt->mutable_doc, num);
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				jb = yyjson_doc_get_root(doc);

				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			}
			break;

		case jpiInteger:
			{
				lng	datum;

				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
														false);

				if (JsonbType(jb) == jbvNumeric)
				{
					if (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_SINT) {
						datum = yyjson_get_int(jb);
					}
					else {
						assert (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_REAL);
						datum = (lng) yyjson_get_real(jb);
					}
					res = jperOk;
				}
				else if (JsonbType(jb) == jbvString)
				{
					const char* tmp = yyjson_get_str(jb);
					size_t len = sizeof(lng);
					lng* pdatum = &datum;

					if (lngFromStr(tmp, &len, &pdatum, true) < 0)
						RETURN_ERROR(ereport(ERROR,
											(errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											errmsg("%s is not allowed for jsonpath item method .%s()",
													tmp, jspOperationName(jsp->type)))));
					res = jperOk;
				}

				if (res == jperNotFound)
					RETURN_ERROR(ereport(ERROR,
										 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
										  errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
												 jspOperationName(jsp->type)))));

				yyjson_mut_val * mut_jbv = yyjson_mut_int(cxt->mutable_doc, datum);
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				jb = yyjson_doc_get_root(doc);

				res = executeNextItem(cxt, jsp, NULL, jb, found, true);
			}
			break;

		case jpiStringFunc:
			{
				char* tmp = NULL;
				bool cleanup_tmp = false;

				if (unwrap && JsonbType(jb) == jbvArray)
					return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

				switch (JsonbType(jb))
				{
					case jbvString:

						tmp = (char*) yyjson_get_str(jb);
						break;
					case jbvNumeric:
						size_t len = 0;
						if (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_SINT) {
							lng val = yyjson_get_int(jb);
							(void) lngToStr(&tmp, &len, &val, true);
						}
						else {
							assert (yyjson_get_subtype(jb) == YYJSON_SUBTYPE_REAL);
							dbl val = yyjson_get_real(jb);
							(void) dblToStr(&tmp, &len, &val, true);
						}
						cleanup_tmp = true;
						break;
					case jbvBool:
						tmp = yyjson_get_bool(jb) ? "true" : "false";
						break;
					case jbvDatetime:
						break;
					case jbvNull:
					case jbvArray:
					case jbvObject:
						RETURN_ERROR(ereport(ERROR,
											 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
											  errmsg("jsonpath item method .%s() can only be applied to a boolean, string, numeric, or datetime value",
													 jspOperationName(jsp->type)))));
						break;
				}

				Assert(tmp != NULL);	/* We must have set tmp above */
				yyjson_mut_val * mut_jbv = yyjson_mut_str(cxt->mutable_doc, tmp);
				yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
				jb = yyjson_doc_get_root(doc);

				res = executeNextItem(cxt, jsp, NULL, jb, found, true);

				if (cleanup_tmp)
					GDKfree(tmp);
			}
			break;

		default:
			elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
			return jperError;
	}

	return res;
}

/*
 * Unwrap current array item and execute jsonpath for each of its elements.
 */
static JsonPathExecResult
executeItemUnwrapTargetArray(JsonPathExecContext *cxt, JsonPathItem *jsp,
							 JsonbValue *jb, JsonValueList *found,
							 bool unwrapElements)
{
	return executeAnyItem
		(cxt, jsp, jb, found, 1, 1, 1,
		 false, unwrapElements);
}

/*
 * Execute next jsonpath item if exists.  Otherwise put "v" to the "found"
 * list if provided.
 */
static JsonPathExecResult
executeNextItem(JsonPathExecContext *cxt,
				JsonPathItem *cur, JsonPathItem *next,
				JsonbValue *v, JsonValueList *found, bool copy)
{
	(void) copy;
	bool		hasNext;

	if (!cur)
		hasNext = next != NULL;
	else if (next)
		hasNext = jspHasNext(cur);
	else
	{
		hasNext = (next = cur->next);
	}

	if (hasNext)
		return executeItem(cxt, next, v, found);

	if (found)
		JsonValueListAppend(cxt, found, v);

	return jperOk;
}

/*
 * Same as executeItem(), but when "unwrap == true" automatically unwraps
 * each array item from the resulting sequence in lax mode.
 */
static JsonPathExecResult
executeItemOptUnwrapResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
						   JsonbValue *jb, bool unwrap,
						   JsonValueList *found)
{
	if (unwrap && jspAutoUnwrap(cxt))
	{
		JsonValueList seq = {0};
		JsonValueListIterator it;
		JsonPathExecResult res = executeItem(cxt, jsp, jb, &seq);
		JsonbValue *item;

		if (jperIsError(res))
			return res;

		JsonValueListInitIterator(&seq, &it);
		while ((item = JsonValueListNext(&seq, &it)))
		{
			if (JsonbType(item) == jbvArray)
				executeItemUnwrapTargetArray(cxt, NULL, item, found, false);
			else
				JsonValueListAppend(cxt, found, item);
		}

		return jperOk;
	}

	return executeItem(cxt, jsp, jb, found);
}

/*
 * Same as executeItemOptUnwrapResult(), but with error suppression.
 */
static JsonPathExecResult
executeItemOptUnwrapResultNoThrow(JsonPathExecContext *cxt,
								  JsonPathItem *jsp,
								  JsonbValue *jb, bool unwrap,
								  JsonValueList *found)
{
	JsonPathExecResult res;
	bool		throwErrors = cxt->throwErrors;

	cxt->throwErrors = false;
	res = executeItemOptUnwrapResult(cxt, jsp, jb, unwrap, found);
	cxt->throwErrors = throwErrors;

	return res;
}

/* Execute boolean-valued jsonpath expression. */
static JsonPathBool
executeBoolItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
				JsonbValue *jb, bool canHaveNext)
{
	JsonPathItem* larg;
	JsonPathItem* rarg;
	JsonPathBool res;
	JsonPathBool res2;

	/* since this function recurses, it could be driven to stack overflow */
	check_stack_depth();

	if (!canHaveNext && jspHasNext(jsp)) {
		elog(ERROR, "boolean jsonpath item cannot have next item");
		assert(0); // cannot happen
	}

	switch (jsp->type)
	{
		case jpiAnd:
			larg = jsp->value.args.left;
			res = executeBoolItem(cxt, larg, jb, false);

			if (res == jpbFalse)
				return jpbFalse;

			/*
			 * SQL/JSON says that we should check second arg in case of
			 * jperError
			 */

			rarg = jsp->value.args.right;
			res2 = executeBoolItem(cxt, rarg, jb, false);

			return res2 == jpbTrue ? res : res2;

		case jpiOr:
			larg = jsp->value.args.left;
			res = executeBoolItem(cxt, larg, jb, false);

			if (res == jpbTrue)
				return jpbTrue;

			rarg = jsp->value.args.right;
			res2 = executeBoolItem(cxt, rarg, jb, false);

			return res2 == jpbFalse ? res : res2;

		case jpiNot:
			larg = jsp->value.arg;

			res = executeBoolItem(cxt, larg, jb, false);

			if (res == jpbUnknown)
				return jpbUnknown;

			return res == jpbTrue ? jpbFalse : jpbTrue;

		case jpiIsUnknown:
			larg = jsp->value.arg;
			res = executeBoolItem(cxt, larg, jb, false);
			return res == jpbUnknown ? jpbTrue : jpbFalse;

		case jpiEqual:
		case jpiNotEqual:
		case jpiLess:
		case jpiGreater:
		case jpiLessOrEqual:
		case jpiGreaterOrEqual:
			larg = jsp->value.args.left;
			rarg = jsp->value.args.right;			
			return executePredicate(cxt, jsp, larg, rarg, jb, true,
									executeComparison, cxt);

		case jpiStartsWith:		/* 'whole STARTS WITH initial' */
			larg = jsp->value.args.left;	/* 'whole' */
			rarg = jsp->value.args.right; /* 'initial' */
			return executePredicate(cxt, jsp, larg, rarg, jb, false,
									executeStartsWith, NULL);

		case jpiLikeRegex:		/* 'expr LIKE_REGEX pattern FLAGS flags' */
			{
				/*
				 * 'expr' is a sequence-returning expression.  'pattern' is a
				 * regex string literal.  SQL/JSON standard requires XQuery
				 * regexes, but we use Postgres regexes here.  'flags' is a
				 * string literal converted to integer flags at compile-time.
				 */
				JsonLikeRegexContext lrcxt = {0};
				larg = jsp->value.like_regex.expr;

				return executePredicate(cxt, jsp, larg, NULL, jb, false,
										executeLikeRegex, &lrcxt);
			}

		case jpiExists:
			larg = jsp->value.arg;
			if (jspStrictAbsenceOfErrors(cxt))
			{
				/*
				 * In strict mode we must get a complete list of values to
				 * check that there are no errors at all.
				 */
				JsonValueList vals = {0};
				JsonPathExecResult res =
					executeItemOptUnwrapResultNoThrow(cxt, larg, jb,
													  false, &vals);

				if (jperIsError(res))
					return jpbUnknown;

				return JsonValueListIsEmpty(&vals) ? jpbFalse : jpbTrue;
			}
			else
			{
				JsonPathExecResult res =
					executeItemOptUnwrapResultNoThrow(cxt, larg, jb,
													  false, NULL);

				if (jperIsError(res))
					return jpbUnknown;

				return res == jperOk ? jpbTrue : jpbFalse;
			}

		default:
			elog(ERROR, "invalid boolean jsonpath item type: %d", jsp->type);
			assert(0); // cannot happen
			return jpbUnknown;
	}
}

/*
 * Execute nested (filters etc.) boolean expression pushing current SQL/JSON
 * item onto the stack.
 */
static JsonPathBool
executeNestedBoolItem(JsonPathExecContext *cxt, JsonPathItem *jsp,
					  JsonbValue *jb)
{
	JsonbValue *prev;
	JsonPathBool res;

	prev = cxt->current;
	cxt->current = jb;
	res = executeBoolItem(cxt, jsp, jb, false);
	cxt->current = prev;

	return res;
}

/*
 * Implementation of several jsonpath nodes:
 *  - jpiAny (.** accessor),
 *  - jpiAnyKey (.* accessor),
 *  - jpiAnyArray ([*] accessor)
 */
static JsonPathExecResult
executeAnyItem(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jbc,
			   JsonValueList *found, uint32 level, uint32 first, uint32 last,
			   bool ignoreStructuralErrors, bool unwrapNext)
{
	JsonPathExecResult res = jperNotFound;

	check_stack_depth();

	if (level > last)
		return res;

	bool is_object = yyjson_is_obj(jbc);
	bool is_array = yyjson_is_arr(jbc);
	assert (!(is_object && is_array));
	yyjson_obj_iter obj_iter;
	yyjson_arr_iter arr_iter;
	if (is_object) obj_iter = yyjson_obj_iter_with(jbc);
	if (is_array)  arr_iter = yyjson_arr_iter_with(jbc);

	/*
	 * Recursively iterate over jsonb objects/arrays
	 */
	while (true)
	{
		yyjson_val* val;
		if (is_object) {
			yyjson_val *key = yyjson_obj_iter_next(&obj_iter);
			if (key == NULL)
				break;
			val = yyjson_obj_iter_get_val(key);
		}
		else if (is_array) {
			val = yyjson_arr_iter_next(&arr_iter);
			if (val == NULL)
				break;
		}

		if (level >= first ||
				(first == PG_UINT32_MAX && last == PG_UINT32_MAX &&
				 isObjectOrArray(val)))	/* leaves only requested */
		{
			/* check expression */
			if (jsp)
			{
				if (ignoreStructuralErrors)
				{
					bool		savedIgnoreStructuralErrors;

					savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
					cxt->ignoreStructuralErrors = true;
					res = executeItemOptUnwrapTarget(cxt, jsp, val , found, unwrapNext);
					cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;
				}
				else
					res = executeItemOptUnwrapTarget(cxt, jsp, val , found, unwrapNext);

				if (jperIsError(res))
					break;

				if (res == jperOk && !found)
					break;
			}
			else if (found)
				JsonValueListAppend(cxt, found, val);
			else
				return jperOk;
		}

		if (level < last && isObjectOrArray(val))
		{
			res = executeAnyItem
				(cxt, jsp, val, found,
					level + 1, first, last,
					ignoreStructuralErrors, unwrapNext);

			if (jperIsError(res))
				break;

			if (res == jperOk && found == NULL)
				break;
		}
	}

	return res;
}

/*
 * Execute unary or binary predicate.
 *
 * Predicates have existence semantics, because their operands are item
 * sequences.  Pairs of items from the left and right operand's sequences are
 * checked.  TRUE returned only if any pair satisfying the condition is found.
 * In strict mode, even if the desired pair has already been found, all pairs
 * still need to be examined to check the absence of errors.  If any error
 * occurs, UNKNOWN (analogous to SQL NULL) is returned.
 */
static JsonPathBool
executePredicate(JsonPathExecContext *cxt, JsonPathItem *pred,
				 JsonPathItem *larg, JsonPathItem *rarg, JsonbValue *jb,
				 bool unwrapRightArg, JsonPathPredicateCallback exec,
				 void *param)
{
	JsonPathExecResult res;
	JsonValueListIterator lseqit;
	JsonValueList lseq = {0};
	JsonValueList rseq = {0};
	JsonbValue *lval;
	bool		error = false;
	bool		found = false;

	/* Left argument is always auto-unwrapped. */
	res = executeItemOptUnwrapResultNoThrow(cxt, larg, jb, true, &lseq);
	if (jperIsError(res))
		return jpbUnknown;

	if (rarg)
	{
		/* Right argument is conditionally auto-unwrapped. */
		res = executeItemOptUnwrapResultNoThrow(cxt, rarg, jb,
												unwrapRightArg, &rseq);
		if (jperIsError(res))
			return jpbUnknown;
	}

	JsonValueListInitIterator(&lseq, &lseqit);
	while ((lval = JsonValueListNext(&lseq, &lseqit)))
	{
		JsonValueListIterator rseqit;
		JsonbValue *rval;
		bool		first = true;

		JsonValueListInitIterator(&rseq, &rseqit);
		if (rarg)
			rval = JsonValueListNext(&rseq, &rseqit);
		else
			rval = NULL;

		/* Loop over right arg sequence or do single pass otherwise */
		while (rarg ? (rval != NULL) : first)
		{
			JsonPathBool res = exec(pred, lval, rval, param);

			if (res == jpbUnknown)
			{
				if (jspStrictAbsenceOfErrors(cxt))
					return jpbUnknown;

				error = true;
			}
			else if (res == jpbTrue)
			{
				if (!jspStrictAbsenceOfErrors(cxt))
					return jpbTrue;

				found = true;
			}

			first = false;
			if (rarg)
				rval = JsonValueListNext(&rseq, &rseqit);
		}
	}

	if (found)					/* possible only in strict mode */
		return jpbTrue;

	if (error)					/* possible only in lax mode */
		return jpbUnknown;

	return jpbFalse;
}

static inline
Numeric yyjson2Numeric(JsonbValue* val) {
	Numeric num = {.type=yyjson_get_subtype(val)};

	switch (num.type) {
		case YYJSON_SUBTYPE_SINT:
			num.lnum = yyjson_get_int(val);
			break;
		case YYJSON_SUBTYPE_REAL:
			num.dnum = yyjson_get_real(val);
			break;
		default:
			assert(0);
	}
	return num;
}

static inline // TODO: replace Numeric with yyjson where possible
JsonbValue* Numeric2yyjson(JsonPathExecContext *cxt, Numeric num) {
	yyjson_mut_val* mut_jbv;

	switch (num.type) {
		case YYJSON_SUBTYPE_SINT:
			mut_jbv = yyjson_mut_int(cxt->mutable_doc, num.lnum);
			break;
		case YYJSON_SUBTYPE_REAL:
			mut_jbv = yyjson_mut_real(cxt->mutable_doc, num.dnum);
			break;
		default:
			assert(0);
	}

	yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
	return yyjson_doc_get_root(doc);
}

/*
 * Execute binary arithmetic expression on singleton numeric operands.
 * Array operands are automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeBinaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
						JsonbValue *jb, BinaryArithmFunc func,
						JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathItem* elem;
	JsonValueList lseq = {0};
	JsonValueList rseq = {0};
	JsonbValue *lval;
	JsonbValue *rval;
	Numeric		res;

	elem = jsp->value.args.left;

	/*
	 * XXX: By standard only operands of multiplicative expressions are
	 * unwrapped.  We extend it to other binary arithmetic expressions too.
	 */
	jper = executeItemOptUnwrapResult(cxt, elem, jb, true, &lseq);
	if (jperIsError(jper))
		return jper;

	elem = jsp->value.args.right;

	jper = executeItemOptUnwrapResult(cxt, elem, jb, true, &rseq);
	if (jperIsError(jper))
		return jper;

	if (JsonValueListLength(&lseq) != 1 ||
		!(lval = getScalar(JsonValueListHead(&lseq), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
							  errmsg("left operand of jsonpath operator %s is not a single numeric value",
									 jspOperationName(jsp->type)))));

	if (JsonValueListLength(&rseq) != 1 ||
		!(rval = getScalar(JsonValueListHead(&rseq), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED),
							  errmsg("right operand of jsonpath operator %s is not a single numeric value",
									 jspOperationName(jsp->type)))));

	if (jspThrowErrors(cxt))
	{
		res = func(yyjson2Numeric(lval), yyjson2Numeric(rval), NULL);
		// TODO: throw error
	}
	else
	{
		bool		error = false;
		res = func(yyjson2Numeric(lval), yyjson2Numeric(rval), &error);

		if (error)
			return jperError;
	}

	if (!(elem = jsp->next) && !found)
		return jperOk; // TODO: weird why do func if not using the result perhaps check when found is empty

	lval = Numeric2yyjson(cxt, res);
	
	return executeNextItem(cxt, jsp, elem, lval, found, false);
}

/*
 * Execute unary arithmetic expression for each numeric item in its operand's
 * sequence.  Array operand is automatically unwrapped in lax mode.
 */
static JsonPathExecResult
executeUnaryArithmExpr(JsonPathExecContext *cxt, JsonPathItem *jsp,
					   JsonbValue *jb, PGFunction func, JsonValueList *found)
{
	JsonPathExecResult jper;
	JsonPathExecResult jper2;
	JsonPathItem* elem;
	JsonValueList seq = {0};
	JsonValueListIterator it;
	JsonbValue *val;
	bool		hasNext;

	elem = jsp->value.arg;
	jper = executeItemOptUnwrapResult(cxt, elem, jb, true, &seq);

	if (jperIsError(jper))
		return jper;

	jper = jperNotFound;

	hasNext = (elem = jsp->next);

	JsonValueListInitIterator(&seq, &it);
	while ((val = JsonValueListNext(&seq, &it)))
	{
		if ((val = getScalar(val, jbvNumeric)))
		{
			if (!found && !hasNext)
				return jperOk;
		}
		else
		{
			if (!found && !hasNext)
				continue;		/* skip non-numerics processing */

			RETURN_ERROR(ereport(ERROR,
								 (errcode(ERRCODE_SQL_JSON_NUMBER_NOT_FOUND),
								  errmsg("operand of unary jsonpath operator %s is not a numeric value",
										 jspOperationName(jsp->type)))));
		}

		if (func)
		{
			Numeric res = func(yyjson2Numeric(val));
			val = Numeric2yyjson(cxt, res);
		}

		jper2 = executeNextItem(cxt, jsp, elem, val, found, false);

		if (jperIsError(jper2))
			return jper2;

		if (jper2 == jperOk)
		{
			if (!found)
				return jperOk;
			jper = jperOk;
		}
	}

	return jper;
}

/*
 * STARTS_WITH predicate callback.
 *
 * Check if the 'whole' string starts from 'initial' string.
 */
static JsonPathBool
executeStartsWith(JsonPathItem *jsp, JsonbValue *whole, JsonbValue *initial,
				  void *param)
{
	(void) jsp; (void) param;
	if (!(whole = getScalar(whole, jbvString)))
		return jpbUnknown;		/* error */

	if (!(initial = getScalar(initial, jbvString)))
		return jpbUnknown;		/* error */

	if (yyjson_get_len(whole) >= yyjson_get_len(initial) &&
		!memcmp(yyjson_get_str(whole) ,
				yyjson_get_str(initial) ,
				yyjson_get_len(initial) ))
		return jpbTrue;

	return jpbFalse;
}

/*
 * LIKE_REGEX predicate callback.
 *
 * Check if the string matches regex pattern.
 */
static JsonPathBool
executeLikeRegex(JsonPathItem *jsp, JsonbValue *str, JsonbValue *rarg,
				 void *param)
{
	JsonLikeRegexContext *cxt = param;
	(void) cxt; (void) jsp;(void) rarg;(void) param;

	if (!(str = getScalar(str, jbvString)))
		return jpbUnknown;

	assert(0); // TODO regex not supported yet
	return jpbFalse;
}

/*
 * Execute numeric item methods (.abs(), .floor(), .ceil()) using the specified
 * user function 'func'.
 */
static JsonPathExecResult
executeNumericItemMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
						 JsonbValue *jb, bool unwrap, PGFunction func,
						 JsonValueList *found)
{
	JsonPathItem* next;
	Numeric	datum;

	if (unwrap && JsonbType(jb) == jbvArray)
		return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);

	if (!(jb = getScalar(jb, jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM),
							  errmsg("jsonpath item method .%s() can only be applied to a numeric value",
									 jspOperationName(jsp->type)))));
	datum = func(yyjson2Numeric(jb));

	if (!(next = jsp->next) && !found)
		return jperOk;

	jb = Numeric2yyjson(cxt, datum);

	return executeNextItem(cxt, jsp, next, jb, found, false);
}

/*
 * Implementation of the .datetime() and related methods.
 *
 * Converts a string into a date/time value. The actual type is determined at
 * run time.
 * If an argument is provided, this argument is used as a template string.
 * Otherwise, the first fitting ISO format is selected.
 *
 * .date(), .time(), .time_tz(), .timestamp(), .timestamp_tz() methods don't
 * have a format, so ISO format is used.  However, except for .date(), they all
 * take an optional time precision.
 */
// TODO: datetime and related maybe later

/*
 * Implementation of .keyvalue() method.
 *
 * .keyvalue() method returns a sequence of object's key-value pairs in the
 * following format: '{ "key": key, "value": value, "id": id }'.
 *
 * "id" field is an object identifier which is constructed from the two parts:
 * base object id and its binary offset in base object's jsonb:
 * id = 10000000000 * base_object_id + obj_offset_in_base_object
 *
 * 10000000000 (10^10) -- is a first round decimal number greater than 2^32
 * (maximal offset in jsonb).  Decimal multiplier is used here to improve the
 * readability of identifiers.
 *
 * Base object is usually a root object of the path: context item '$' or path
 * variable '$var', literals can't produce objects for now.  But if the path
 * contains generated objects (.keyvalue() itself, for example), then they
 * become base object for the subsequent .keyvalue().
 *
 * Id of '$' is 0. Id of '$var' is its ordinal (positive) number in the list
 * of variables (see getJsonPathVariable()).  Ids for generated objects
 * are assigned using global counter JsonPathExecContext.lastGeneratedObjectId.
 */
static JsonPathExecResult
executeKeyValueMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
					  JsonbValue *jb, JsonValueList *found)
{
	JsonPathExecResult res = jperNotFound;
	JsonPathItem* next;
	JsonbValue* jbc;
	JsonbValue*	key;
	JsonbValue*	val;
	bool		hasNext;

	if (!yyjson_is_obj(jb))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND),
							  errmsg("jsonpath item method .%s() can only be applied to an object",
									 jspOperationName(jsp->type)))));

	jbc = jb;

	if (!yyjson_obj_size(jbc))
		return jperNotFound;	/* no key-value pairs */

	hasNext = (next = jsp->next);

	yyjson_mut_val * keystr =  yyjson_mut_strcpy(cxt->mutable_doc, "key");
	yyjson_mut_val * valstr =  yyjson_mut_strcpy(cxt->mutable_doc, "value");
	yyjson_mut_val * idstr =  yyjson_mut_strcpy(cxt->mutable_doc, "id");

	/* construct object id from its base object and offset inside that */
	yyjson_mut_val * id =  yyjson_mut_uint(cxt->mutable_doc, 0 /*TODO either generate proper object id's or remove this from the feature*/);
	yyjson_obj_iter obj_iter = yyjson_obj_iter_with(jbc);
	

	while ((key = yyjson_obj_iter_next(&obj_iter)))
	{
		JsonBaseObjectInfo baseObject;

		res = jperOk;

		if (!hasNext && !found)
			break;

		val = yyjson_obj_iter_get_val(key);

		yyjson_mut_val* mut_obj = yyjson_mut_obj (cxt->mutable_doc);
		yyjson_mut_val* mut_val = yyjson_val_mut_copy(cxt->mutable_doc, val);
		yyjson_mut_val* mut_key = yyjson_val_mut_copy(cxt->mutable_doc, key);

		(void) yyjson_mut_obj_add(mut_obj, keystr, mut_key); // TODO: error handling
		(void) yyjson_mut_obj_add(mut_obj, valstr, mut_val); // TODO: error handling
		(void) yyjson_mut_obj_add(mut_obj, idstr, id); // TODO: error handling

		yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_obj, cxt->alc);
		yyjson_val* obj = yyjson_doc_get_root(doc);
		baseObject = setBaseObject(cxt, obj, cxt->lastGeneratedObjectId++);
		res = executeNextItem(cxt, jsp, next, obj, found, true);

		cxt->baseObject = baseObject;

		if (jperIsError(res))
		{
			break;
		}

		if (res == jperOk && !found)
			break;
	}

	return res;
}

/*
 * Convert boolean execution status 'res' to a boolean JSON item and execute
 * next jsonpath.
 */
static JsonPathExecResult
appendBoolResult(JsonPathExecContext *cxt, JsonPathItem *jsp,
				 JsonValueList *found, JsonPathBool res)
{
	JsonPathItem* next;

	if (!(next = jsp->next) && !found)
		return jperOk;			/* found singleton boolean value */

	yyjson_mut_val * mut_jbv;

	if (res == jpbUnknown)
	{
		 mut_jbv = yyjson_mut_null(cxt->mutable_doc);
	}
	else
	{
		mut_jbv = yyjson_mut_bool(cxt->mutable_doc, true);
	}

	yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, cxt->alc);
	yyjson_val* jbv = yyjson_doc_get_root(doc);

	return executeNextItem(cxt, jsp, next, jbv, found, true);
}


/*
 * Returns the computed value of a JSON path variable with given name.
 */
static JsonbValue *
GetJsonPathVar(void *cxt, yyjson_alc* alc, char *varName, int varNameLen,
			   JsonbValue **baseObject, int *baseObjectId)
{
	JsonPathVariable *var = NULL;
	List	   *vars = cxt;
	ListCell   *lc;
	JsonbValue *result;
	int			id = 1;

	foreach(lc, vars)
	{
		JsonPathVariable *curvar = lfirst(lc);

		if (curvar->namelen == varNameLen &&
			strncmp(curvar->name, varName, varNameLen) == 0)
		{
			var = curvar;
			break;
		}

		id++;
	}

	if (var == NULL)
	{
		*baseObjectId = -1;
		return NULL;
	}

	yyjson_mut_doc* mutable_doc = yyjson_mut_doc_new(alc);
	yyjson_mut_val * mut_jbv;

	if (var->isnull)
	{
		mut_jbv = yyjson_mut_null(mutable_doc);
	}
	else
		mut_jbv = JsonItemFromDatum(var->value, var->typid, var->typmod, mutable_doc);

	yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_jbv, alc);
	result = yyjson_doc_get_root(doc);

	*baseObject = result;
	*baseObjectId = id;

	return result;
}

static int
CountJsonPathVars(void *cxt)
{
	List	   *vars = (List *) cxt;

	return list_length(vars);
}


/*
 * Initialize JsonbValue to pass to jsonpath executor from given
 * datum value of the specified type.
 */
static yyjson_mut_val*
JsonItemFromDatum(Datum val, Oid typid, int32 typmod, yyjson_mut_doc* mutable_doc)
{
	(void) val; (void) typid; (void) typmod; (void) mutable_doc;
	assert(0);
	// TODO should convert GDK/MonetDB type to Numeric
	return NULL;
}


/**************** Support functions for JsonPath execution *****************/

/*
 * Returns the size of an array item, or -1 if item is not an array.
 */
static int
JsonbArraySize(JsonbValue *jb)
{
	if (JsonbType(jb) == jbvArray)
		return yyjson_arr_size(jb);

	return -1;
}

/* Comparison predicate callback. */
static JsonPathBool
executeComparison(JsonPathItem *cmp, JsonbValue *lv, JsonbValue *rv, void *p)
{
	JsonPathExecContext *cxt = (JsonPathExecContext *) p;

	return compareItems(cxt, cmp->type, lv, rv, cxt->useTz);
}

/*
 * Perform per-byte comparison of two strings.
 */
static int
binaryCompareStrings(const char *s1, int len1,
					 const char *s2, int len2)
{
	int			cmp;

	cmp = memcmp(s1, s2, Min(len1, len2));

	if (cmp != 0)
		return cmp;

	if (len1 == len2)
		return 0;

	return len1 < len2 ? -1 : 1;
}

/*
 * Compare two strings in the current server encoding using Unicode codepoint
 * collation.
 */
static int
compareStrings(const char *mbstr1, int mblen1,
			   const char *mbstr2, int mblen2)
{
	return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
}

/*
 * Compare two SQL/JSON items using comparison operation 'op'.
 */
static JsonPathBool
compareItems(JsonPathExecContext *cxt, int32 op, JsonbValue *jb1, JsonbValue *jb2, bool useTz)
{
	int			cmp;
	bool		res;

	if (JsonbType(jb1) != JsonbType(jb2))
	{
		if (JsonbType(jb1) == jbvNull || JsonbType(jb2) == jbvNull)

			/*
			 * Equality and order comparison of nulls to non-nulls returns
			 * always false, but inequality comparison returns true.
			 */
			return op == jpiNotEqual ? jpbTrue : jpbFalse;

		/* Non-null items of different types are not comparable. */
		return jpbUnknown;
	}

	switch (JsonbType(jb1))
	{
		case jbvNull:
			cmp = 0;
			break;
		case jbvBool:
			cmp = yyjson_get_bool(jb1) == yyjson_get_bool(jb2) ? 0 :
				yyjson_get_bool(jb1) ? 1 : -1;
			break;
		case jbvNumeric:
			cmp = compareNumeric(yyjson2Numeric(jb1), yyjson2Numeric(jb2));
			break;
		case jbvString:
			if (op == jpiEqual)
				return yyjson_get_len(jb1) != yyjson_get_len(jb2) ||
					memcmp(yyjson_get_str(jb1) ,
						   yyjson_get_str(jb2) ,
						   yyjson_get_len(jb1) ) ? jpbFalse : jpbTrue;

			cmp = compareStrings(yyjson_get_str(jb1) , yyjson_get_len(jb1) ,
								 yyjson_get_str(jb2) , yyjson_get_len(jb2) );
			break;
		case jbvDatetime:
			{
				(void) useTz;
				assert(0); // TODO not implemented
				return jpbUnknown;
			}
			break;

		case jbvArray:
		case jbvObject:
			return jpbUnknown;	/* non-scalars are not comparable */

		default:
			elog(ERROR, "invalid jsonb value type %d", JsonbType(jb1));
			assert(0); // should not happen
	}

	switch (op)
	{
		case jpiEqual:
			res = (cmp == 0);
			break;
		case jpiNotEqual:
			res = (cmp != 0);
			break;
		case jpiLess:
			res = (cmp < 0);
			break;
		case jpiGreater:
			res = (cmp > 0);
			break;
		case jpiLessOrEqual:
			res = (cmp <= 0);
			break;
		case jpiGreaterOrEqual:
			res = (cmp >= 0);
			break;
		default:
			elog(ERROR, "unrecognized jsonpath operation: %d", op);
			assert(0); // cannot happen
			return jpbUnknown;
	}

	return res ? jpbTrue : jpbFalse;
}

/* Compare two numerics */
static int
compareNumeric(Numeric a, Numeric b)
{
	if (a.type == YYJSON_SUBTYPE_REAL && b.type == YYJSON_SUBTYPE_REAL)
		return a.dnum - b.dnum > 0? 1 : a.dnum - b.lnum < 0 ? -1 : 0;
	if (a.type == YYJSON_SUBTYPE_REAL && b.type == YYJSON_SUBTYPE_SINT)
		return a.dnum - b.lnum > 0? 1 : a.dnum - b.lnum < 0 ? -1 : 0;
	if (a.type == YYJSON_SUBTYPE_SINT && b.type == YYJSON_SUBTYPE_REAL)
		return a.dnum - b.lnum > 0? 1 : a.dnum - b.lnum < 0 ? -1 : 0;
	// else (a.type == YYJSON_SUBTYPE_SINT && b.type == YYJSON_SUBTYPE_SINT)
	return a.lnum - b.lnum;
}

/*
 * Execute array subscript expression and convert resulting numeric item to
 * the integer type with truncation.
 */
static JsonPathExecResult
getArrayIndex(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
			  int32 *index)
{
	JsonbValue *jbv;
	JsonValueList found = {0};
	JsonPathExecResult res = executeItem(cxt, jsp, jb, &found);
	Datum		numeric_index;
	bool		have_error = false;

	if (jperIsError(res))
		return res;

	if (JsonValueListLength(&found) != 1 ||
		!(jbv = getScalar(JsonValueListHead(&found), jbvNumeric)))
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
							  errmsg("jsonpath array subscript is not a single numeric value"))));

	numeric_index = (Datum) yyjson_get_int(jbv);
	*index = (int) numeric_index;

	if (have_error)
		RETURN_ERROR(ereport(ERROR,
							 (errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT),
							  errmsg("jsonpath array subscript is out of integer range"))));

	return jperOk;
}

/* Save base object and its id needed for the execution of .keyvalue(). */
static JsonBaseObjectInfo
setBaseObject(JsonPathExecContext *cxt, JsonbValue *jbv, int32 id)
{
	JsonBaseObjectInfo baseObject = cxt->baseObject;

	cxt->baseObject.jbc = jbv;
	cxt->baseObject.id = id;

	return baseObject;
}


static void
JsonValueListAppend(JsonPathExecContext *cxt, JsonValueList *jvl, JsonbValue *jbv)
{
	if (jvl->singleton)
	{
		#define escontext cxt /*trick to have the macro work*/
		jvl->list = list_make2(jvl->singleton, jbv);
		jvl->singleton = NULL;
	}
	else if (!jvl->list)
		jvl->singleton = jbv;
	else
		jvl->list = lappend(jvl->list, jbv);
}

static int
JsonValueListLength(const JsonValueList *jvl)
{
	return jvl->singleton ? 1 : list_length(jvl->list);
}

static bool
JsonValueListIsEmpty(JsonValueList *jvl)
{
	return !jvl->singleton && (jvl->list == NIL);
}

static JsonbValue *
JsonValueListHead(JsonValueList *jvl)
{
	return jvl->singleton ? jvl->singleton : linitial(jvl->list);
}

static void
JsonValueListInitIterator(const JsonValueList *jvl, JsonValueListIterator *it)
{
	if (jvl->singleton)
	{
		it->value = jvl->singleton;
		it->list = NIL;
		it->next = NULL;
	}
	else if (jvl->list != NIL)
	{
		it->value = (JsonbValue *) linitial(jvl->list);
		it->list = jvl->list;
		it->next = list_second_cell(jvl->list);
	}
	else
	{
		it->value = NULL;
		it->list = NIL;
		it->next = NULL;
	}
}

/*
 * Get the next item from the sequence advancing iterator.
 */
static JsonbValue *
JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it)
{
	(void) jvl;
	JsonbValue *result = it->value;

	if (it->next)
	{
		it->value = lfirst(it->next);
		it->next = lnext(it->list, it->next);
	}
	else
	{
		it->value = NULL;
	}

	return result;
}

/*
 * Returns jbv* type of JsonbValue.
 */
static int
JsonbType(JsonbValue *jb)
{

	yyjson_type tpe = yyjson_get_type(jb);

	switch (tpe)
	{
		case YYJSON_TYPE_NONE:	return -1;
		case YYJSON_TYPE_NULL:	return jbvNull;
		case YYJSON_TYPE_BOOL: 	return jbvBool;
		case YYJSON_TYPE_NUM:	return jbvNumeric;
		case YYJSON_TYPE_STR:	return jbvString;
		case YYJSON_TYPE_ARR:	return jbvArray;
		case YYJSON_TYPE_OBJ:	return jbvObject;
		default: assert(0);		return -2;
	}
}

static bool
isObjectOrArray(JsonbValue *jb) {
	return yyjson_get_type(jb) == YYJSON_TYPE_ARR || yyjson_get_type(jb) == YYJSON_TYPE_OBJ;
}

/* Get scalar of given type or NULL on type mismatch */
static JsonbValue *
getScalar(JsonbValue *scalar, enum jbvType type)
{
	return JsonbType(scalar) == (int) type ? scalar : NULL;
}

/* Construct a JSON array from the item list */
static JsonbValue *
wrapItemsInArray(yyjson_alc* alc, const JsonValueList *items)
{
	JsonValueListIterator it;
	JsonbValue *jbv;
	yyjson_mut_doc* mutable_doc = yyjson_mut_doc_new(alc);

	yyjson_mut_val* mut_arr = yyjson_mut_arr(mutable_doc);

	JsonValueListInitIterator(items, &it);
	while ((jbv = JsonValueListNext(items, &it)))
	{
		yyjson_mut_val* mut_val = yyjson_val_mut_copy(mutable_doc, jbv);
		yyjson_mut_arr_add_val(mut_arr, mut_val);
	}
	yyjson_doc* doc = yyjson_mut_val_imut_copy(mut_arr, alc);
	yyjson_val* arr = yyjson_doc_get_root(doc);
	return arr;
}

/*
 * Executor-callable JSON_EXISTS implementation
 *
 * Returns NULL instead of throwing errors if 'error' is not NULL, setting
 * *error to true.
 */
bool
JsonPathExists(Datum jb, JsonPath *jp, bool *error, List *vars, yyjson_alc* alc, char* errmsg)
{
	JsonPathExecResult res;

	JsonPathExecContext _cxt = {0};
	_cxt.alc = alc;
	_cxt.sa = _cxt.alc->ctx;
	_cxt._errmsg = errmsg;
	JsonPathExecContext *cxt = &_cxt;

	res = executeJsonPath(jp, vars,
						  GetJsonPathVar, CountJsonPathVars,
						  DatumGetJsonbP(jb), !error, NULL, true, cxt);
	if (!jperIsError(res) || errmsg[0])
		return false; // throw exception

	Assert(error || !jperIsError(res));

	if (error && jperIsError(res))
		*error = true;

	return res == jperOk;
}

/*
 * Executor-callable JSON_QUERY implementation
 *
 * Returns NULL instead of throwing errors if 'error' is not NULL, setting
 * *error to true.  *empty is set to true if no match is found.
 */
JsonbValue *
JsonPathQuery(Datum jb, JsonPath *jp, JsonWrapper wrapper, bool *empty,
			  bool *error, List *vars,
			  const char *column_name, yyjson_alc* alc, char* errmsg)
{
	JsonbValue *singleton;
	bool		wrap;
	JsonValueList found = {0};
	JsonPathExecResult res;
	int			count;

	JsonPathExecContext _cxt = {0};
	_cxt.alc = alc;
	_cxt.sa = _cxt.alc->ctx;
	_cxt._errmsg = errmsg;
	JsonPathExecContext* cxt = &_cxt;

	res = executeJsonPath(jp, vars,
						  GetJsonPathVar, CountJsonPathVars,
						  DatumGetJsonbP(jb), !error, &found, true, cxt);
	Assert(error || !jperIsError(res));
	if (error && jperIsError(res))
	{
		*error = true;
		*empty = false;
		return NULL;
	}

	/*
	 * Determine whether to wrap the result in a JSON array or not.
	 *
	 * First, count the number of SQL/JSON items in the returned
	 * JsonValueList. If the list is empty (singleton == NULL), no wrapping is
	 * necessary.
	 *
	 * If the wrapper mode is JSW_NONE or JSW_UNSPEC, wrapping is explicitly
	 * disabled. This enforces a WITHOUT WRAPPER clause, which is also the
	 * default when no WRAPPER clause is specified.
	 *
	 * If the mode is JSW_UNCONDITIONAL, wrapping is enforced regardless of
	 * the number of SQL/JSON items, enforcing a WITH WRAPPER or WITH
	 * UNCONDITIONAL WRAPPER clause.
	 *
	 * For JSW_CONDITIONAL, wrapping occurs only if there is more than one
	 * SQL/JSON item in the list, enforcing a WITH CONDITIONAL WRAPPER clause.
	 */
	count = JsonValueListLength(&found);
	singleton = count > 0 ? JsonValueListHead(&found) : NULL;
	if (singleton == NULL)
		wrap = false;
	else if (wrapper == JSW_NONE || wrapper == JSW_UNSPEC)
		wrap = false;
	else if (wrapper == JSW_UNCONDITIONAL)
		wrap = true;
	else if (wrapper == JSW_CONDITIONAL)
		wrap = count > 1;
	else
	{
		elog(ERROR, "unrecognized json wrapper %d", (int) wrapper);
		return NULL; // TODO I don't think it can happen
		wrap = false;
	}

	if (wrap)
		return wrapItemsInArray(alc, &found); // TODO track the yyjson_doc

	/* No wrapping means only one item is expected. */
	if (count > 1)
	{
		if (error)
		{
			*error = true;
			return NULL;
		}

		if (column_name) {
			ereport(ERROR,
					(errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
					 errmsg("JSON path expression for column \"%s\" should return single item without wrapper",
							column_name),
					 errhint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.")));
			return NULL;
		}
		else {
			ereport(ERROR,
					(errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
					 errmsg("JSON path expression in JSON_QUERY should return single item without wrapper"),
					 errhint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.")));
			return NULL;
		}
	}

	if (singleton)
		return singleton; // TODO track the yyjson_doc

	*empty = true;
	return NULL;
}

/*
 * Executor-callable JSON_VALUE implementation
 *
 * Returns NULL instead of throwing errors if 'error' is not NULL, setting
 * *error to true.  *empty is set to true if no match is found.
 */
JsonbValue *
JsonPathValue(Datum jb, JsonPath *jp, bool *empty, bool *error, List *vars,
			  const char *column_name, yyjson_alc* alc, char* errmsg)
{
	JsonbValue *res;
	JsonValueList found = {0};
	JsonPathExecResult jper PG_USED_FOR_ASSERTS_ONLY;
	int			count;

	JsonPathExecContext _cxt = {0};
	_cxt.alc = alc;
	_cxt.sa = _cxt.alc->ctx;
	_cxt._errmsg = errmsg;
	JsonPathExecContext* cxt = &_cxt;

	jper = executeJsonPath(jp, vars, GetJsonPathVar, CountJsonPathVars,
						   DatumGetJsonbP(jb),
						   !error, &found, true, &_cxt);

	Assert(error || !jperIsError(jper));

	if (error && jperIsError(jper))
	{
		*error = true;
		*empty = false;
		return NULL;
	}

	count = JsonValueListLength(&found);

	*empty = (count == 0);

	if (*empty)
		return NULL;

	/* JSON_VALUE expects to get only singletons. */
	if (count > 1)
	{
		if (error)
		{
			*error = true;
			return NULL;
		}

		if (column_name) {
			ereport(ERROR,
					(errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
					 errmsg("JSON path expression for column \"%s\" should return single scalar item",
							column_name)));
			return NULL;
		}
		else {
			ereport(ERROR,
					(errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM),
					 errmsg("JSON path expression in JSON_VALUE should return single scalar item")));
			return NULL;
		}
	}

	res = JsonValueListHead(&found);

	/* JSON_VALUE expects to get only scalars. */
	if (!IsAJsonbScalar(res))
	{
		if (error)
		{
			*error = true;
			return NULL;
		}

		if (column_name) {
			ereport(ERROR,
					(errcode(ERRCODE_SQL_JSON_SCALAR_REQUIRED),
					 errmsg("JSON path expression for column \"%s\" should return single scalar item",
							column_name)));
			return NULL;
		}
		else {
			ereport(ERROR,
					(errcode(ERRCODE_SQL_JSON_SCALAR_REQUIRED),
					 errmsg("JSON path expression in JSON_VALUE should return single scalar item")));
			return NULL;
		}
	}

	if (JsonbType(res) == jbvNull)
		return NULL;

	return res;
}
