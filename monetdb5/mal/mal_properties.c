/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f mal_properties
 * @a M. Kersten
 * @+ Property Management
 * Properties come in several classes, those linked with the symbol table and
 * those linked with the runtime environment. The former are determined
 * once upon parsing or catalog lookup. The runtime properties have two
 * major subclasses, i.e. reflective and prescriptive. The reflective properties
 * merely provide a fast cache to information aggregated from the target.
 * Prescriptive properties communicate desirable states, leaving it to other
 * system components to reach this state at the cheapest cost possible.
 * This multifacetted world makes it difficult to come up with a concise model
 * for dealing with properties. The approach taken here is an experimental step
 * into this direction.
 *
 * This @sc{mal_properties} module
 * provides a generic scheme to administer property sets and
 * a concise API to manage them.
 * Its design is geared towards support of MAL optimizers, which
 * typically make multiple passes over a program to derive an
 * alternative, better version. Such code-transformations are
 * aided by keeping track of derived information, e.g. the expected
 * size of a temporary result or the alignment property between BATs.
 *
 * Properties capture part of the state of the system in the form of an
 * simple term expression @sc{(name, operator, constant)}.
 * The property model assumes a namespace built around Identifiers.
 * The operator satisfy the syntax rules for MAL operators.
 * Conditional operators are quite common, e.g. the triple (count, <, 1000)
 * can be used to denote a small table.
 *
 * The property bearing objects in the MAL setting are
 * variables (symbol table entries).
 * The direct relationship between instructions and
 * a target variable, make it possible to keep the instruction properties
 * in the corresponding target variable.
 *
 * @emph{Variables properties}
 * The variables can be extended at any time with a property set.
 * Properties have a scope identical to the scope of the corresponding variable.
 * Ommision of the operator and value turns it into a boolean valued property,
 * whose default value is @sc{true}.
 * @verbatim
 * 	b{count=1000,sorted}:= mymodule.action("table");
 * 	name{aligngroup=312} := bbp.take("person_name");
 * 	age{aligngroup=312} := bbp.take("person_age");
 * @end verbatim
 * The example illustrates a mechanism to maintain alignment information.
 * Such a property is helpful for optimizers to pick an efficient algorithm.
 *
 * @emph{MAL function signatures.}
 * A function signature contains a description of
 * the objects it is willing to accept and an indication of the
 * expected result. The arguments can be tagged with properties
 * that 'should be obeyed, or implied' by the actual arguments.
 * It extends the typing scheme used during compilation/optimization.
 * Likewise, the return values can be tagged with properties that 'at least'
 * exist upon function return.
 * @verbatim
 *     function test(b:bat[:oid,:int]{count<1000}):bat[:oid,:int]{sorted}
 *        #code block
 *     end test
 * @end verbatim
 *
 * These properties are informative to optimizers.
 * They can be enforced at runtime using the operation
 * @sc{optimizer.enforceRules()} which injects calls into
 * the program to check them.
 * An assertion error is raised if the property does not hold.
 * The code snippet
 * @verbatim
 * 	z:= user.test(b);
 * @end verbatim
 * is translated into the following code block;
 * @verbatim
 * 	mal.assert(b,"count","<",1000);
 * 	z:= user.test(b);
 * 	mal.assert(z,"sorted");
 * @end verbatim
 *
 * @emph{How to propagate properties?}
 * Property inspection and manipulation is strongly linked with the operators
 * of interest. Optimizers continuously inspect and update the properties, while
 * kernel operators should not be bothered with their existence.
 * Property propagation is strongly linked with the actual operator
 * implementation.  We examine a few recurring cases.
 *
 * V:=W; Both V and W should be type compatible, otherwise the compiler will
 * already complain.(Actually, it requires V.type()==W.type() and ~V.isaConstant())
 * But what happens with all others? What is the property propagation rule for
 * the assignment? Several cases can be distinguished:
 *
 * I)   W has a property P, unknown to V.
 * II)  V has a propery P, unknown to W.
 * III) V has property P, and W has property Q, P and Q are incompatible.
 * IV)  V and W have a property P, but its value disaggrees.
 *
 * case I). If the variable V was not initialized, we can simply copy or share
 * the properties. Copying might be too expensive, while shareing leads
 * to managing the dependencies.
 * case II) It means that V is re-assigned a value, and depending on its type and
 * properties we may have to 'garbage collect/finalize' it first. Alternatively,
 * it could be interpreted as a property that will hold after assignment
 * which is not part of the right-hand side expression.
 * case III) if P and Q are type compatible, it means an update of the P value.
 * Otherwise, it should generates an exception.
 * case IV) this calls for an update of V.P using the value of W.P. How this
 * should be done is property specific.
 *
 * Overall, the policy would be to 'disgard' all knowledge from V first and
 * then copy the properties from W.
 *
 * [Try 1]
 * V:= fcn(A,B,C) and signature fcn(A:int,B:int,C:int):int
 * The signature provides several handles to attach properties.
 * Each formal parameter could come with a list of 'desirable/necessary'
 * properties. Likewise, the return values have a property set.
 * This leads to the extended signature
 * function fcn(A:T@{P1@},....,B:T@{Pn@}): (C:T@{Pk@}...D:T@{Pm@})
 * where each Pi denotes a property set.
 * Properties P1..Pn can be used to select the proper function variant.
 * At its worst, several signatures of fcn() should be inspected at
 * runtime to find one with matching properties. To enable analysis and
 * optimization, however, it should be clear that once the function is finished,
 * the properties Pk..Pm exist.
 *
 * [Try 2]
 * V:= fcn(A,B,C) and signature fcn(A:int,B:int,C:int):int
 * The function is applicable when a (simple conjuntive) predicate over
 * the properties of the actual arguments holds. A side-effect of execution
 * of the function leads to an update of the property set associated with
 * the actual arguments. An example:
 * @example
 * function fcn (A:int,B:bat[int,int],C:int):int
 *         ?@{A.read=true, B.count<1000@}
 *         @{fcn.write:=true;@}
 * @end example
 *
 * [Try 3] Organize property management by the processor involved, e.g.
 * a cost-based optimizer or a access control enforcer.
 * For each optimizer we should then specify the 'symbolic' effect of
 * execution of instructions of interest. This means 'duplication' of
 * the instruction set.
 *
 * Can you drop properties? It seems possible, but since property operations
 * occur before actual execution there is no guarantee that they actually
 * take place.
 *
 * [case: how to handle sort(b:bat@{P@}):bat@{+sorted@} as a means to propagate  ]
 * [actually we need an expression language to indicate the propety set,
 * e.g. sort(b:bat):bat@{sorted,+b@} which first obtains the properties of b and
 * extends it with sorted. A nested structure emerge
 *
 * Is it necessary to construct the property list intersection?
 * Or do we need a user defined function to consolidate property lists?
 * ]
 *
 * Aside, it may be valuable to collect information on e.g. the execution time
 * of functions as a basis for future optimizations. Rather then cluttering the
 * property section, it makes sense to explicitly update this information in
 * a catalog.
 * @+ Properties at the MAL level
 * Aside from routines targeted as changing the MAL blocks, it should
 * be possible to reason about the properties within the language itself.
 * This calls for gaining access and update.
 * For example, the following snippet shows how properties
 * are used in a code block.
 *
 * @example
 * B := bbp.new(int,int);
 * I := properties.has(B,@{hsorted@});
 * J := properties.get(B,@{cost@});
 * print(J);
 *
 * properties.set(B,@{cost@},2315);
 * barrier properties.has(B,@{sorted@});
 * exit;
 * @end example
 * @-
 * These example illustrate that the property manipulations are
 * executed throug patterns, which also accept a stack frame.
 *
 * Sample problem with dropping properties:
 * @example
 *     B := bbp.new(int,int);
 * barrier tst:= randomChoice()
 *     I := properties.drop(B,@{hsorted@});
 * exit	tst;
 * @end example
 *
 * @+ The cost model problem
 * An important issue for property management is to be able to pre-calculate
 * a cost for a MAL block. This calls for an cost model implementation that
 * recognizes instructions of interest, understands and can deal with the
 * dataflow semantics, and
 *
 * For example, selectivity estimations can be based on a histogram associated
 * with a BAT. The code for this could look like
 * @example
 *     B:= new(int,int);
 *     properties.add(B,@{min,max,histogram@});
 *     Z:= select(B,1,100);
 * @end example
 * Addition of a property may trigger its evaluation, provided enough
 * information is available (e.g. catalog). The instruction triggers the
 * calls properties.set(B,@{min@}), properties.set(B,@{max@}), and
 * properties.set(B,@{histogram@})
 * once a property evaluation engine is ran against the code block.
 * After assignment to Z, we have to propagate properties
 * properties.update(B,@{min@}).
 * @+ SQL case
 * To study the use of properties in the complete pipeline SQL-execution
 * we contrive a small SQL problem. The person table is sorted by name,
 * the car table is unsorted.
 * @example
 * create table person(name varchar not null,
 * 		address varchar);
 * create table car(name varchar,
 * 		model varchar,
 * 		milage int not null);
 * select distinct name, model, milage
 * from person, car
 * where car.name= person.name
 *   and milage>60000;
 * @end example
 * @+ Implementation rules
 * Properties can be associated with variables, MAL blocks, and MAL instructions.
 * The property list is initialized upon explicit request only, e.g. by
 * the frontend parser, a box manager, or as a triggered action.
 *
 * Every property should come with a function that accepts a reference to
 * the variable and updates the property record. This function is activated
 * either once or automatically upon each selection.
 *
 * @+ Property ADT implementation
 *
 *
 * addProperty(O,P) adds property P to the list associated with O. If O represents
 * a compound structure, e.g. a BAT, we should indicate the component as well. For
 * example, addProperty(O,P,Ia,...Ib) introduces a property shared by the
 * components Ia..Ib (indicated with an integer index.
 *
 * hasProperty(O,P) is a boolean function that merely checks existence
 * hasnotProperty(O,P) is the dual operation.
 *
 *
 * setProperty(O,P,V) changes the propety value to V. It may raise a
 * PropertyUpdateViolation exception when this can not be realized.
 * Note, the property value itself is changed, not the object referenced.
 *
 * getProperty(O,P) retrieves the current value of a property. This may involve
 * calling a function or running a database query.
 *
 * setPropertyAttribute(O,P,A) changes the behavior of the property. For example,
 * the attribute 'freeze' will result in a call to the underlying function only
 * once and to cache the result for the remainder of the objects life time.
 *
 * @+ Predefined properties
 * The MAL language uses a few defaults, recognizable as properties
 * @multitable @columnfractions .1 .8
 * @item unsafe
 * @tab function has side effects.Default, unsafe=off
 * @item read
 * @tab data can be read but not updated
 * @item append
 * @tab data can be appended
 * @end multitable
 * @-
 */
#include "monetdb_config.h"
#include "mal_properties.h"
#include "mal_type.h"		/* for idcmp() */

static str *properties = NULL;
static int nr_properties = 0;
static int max_properties = 0;

sht
PropertyIndex(str name)
{
	int i=0;
	for (i=0; i<nr_properties; i++) {
		if (strcmp(properties[i], name) == 0)
			return i;
	}
	mal_set_lock(mal_contextLock,"propertyIndex");
	/* small change its allready added */
	for (i=0; i<nr_properties; i++) {
		if (strcmp(properties[i], name) == 0) {
			mal_unset_lock(mal_contextLock,"propertyIndex");
			return i;
		}
	}
	if (i >= max_properties) {
		max_properties += 256;
		properties = GDKrealloc(properties, max_properties * sizeof(str));
	}
	properties[nr_properties] = GDKstrdup(name);
	mal_unset_lock(mal_contextLock,"propertyIndex");
	return nr_properties++;
}

str
PropertyName(sht idx)
{
	if (idx < nr_properties)
		return properties[idx];
	return "None";
}

prop_op_t
PropertyOperator( str s )
{
	if (!s || !*s)
		return op_eq;
	if (*s == '<') {
		if (*(s+1) == '=')
			return op_lte;
		return op_lt;
	} else if (*s == '>') {
		if (*(s+1) == '=')
			return op_gte;
		return op_gt;
	} else if (*s == '=')
		return op_eq;
	else if (*s == '!' && *(s+1) == '=')
		return op_ne;
	return op_eq;
}

str
PropertyOperatorString( prop_op_t op )
{
	switch(op) {
	case op_lt: return "<";
	case op_lte: return "<=";
	case op_eq: return "=";
	case op_gte: return ">=";
	case op_gt: return ">";
	case op_ne: return "!=";
	}
	return "=";
}

