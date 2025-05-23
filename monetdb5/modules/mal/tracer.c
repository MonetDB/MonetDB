/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * GDKtracer exposes routines where an occurring failure should reach the
 * client immediately. For that reason, GDKtracer reports those errors
 * directly to the stream.
 *
 */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

static str
TRACERflush_buffer(void *ret)
{
	(void) ret;
	GDKtracer_flush_buffer();
	return MAL_SUCCEED;
}


static str
TRACERset_component_level(void *ret, const char *const *comp_id, const char *const *lvl_id)
{
	(void) ret;
	if (GDKtracer_set_component_level(*comp_id, *lvl_id) != GDK_SUCCEED)
		throw(MAL, "logging.setcomplevel", ILLEGAL_ARGUMENT);

	return MAL_SUCCEED;
}


static str
TRACERreset_component_level(void *ret, const char *const *comp_id)
{
	(void) ret;
	if (GDKtracer_reset_component_level(*comp_id) != GDK_SUCCEED)
		throw(MAL, "logging.resetcomplevel", ILLEGAL_ARGUMENT "\n");

	return MAL_SUCCEED;
}


static str
TRACERset_layer_level(void *ret, const char *const *layer_id, const char *const *lvl_id)
{
	(void) ret;
	if (GDKtracer_set_layer_level(*layer_id, *lvl_id) != GDK_SUCCEED)
		throw(MAL, "logging.setlayerlevel", ILLEGAL_ARGUMENT "\n");

	return MAL_SUCCEED;
}


static str
TRACERreset_layer_level(void *ret, const char *const *layer_id)
{
	(void) ret;
	if (GDKtracer_reset_layer_level(*layer_id) != GDK_SUCCEED)
		throw(MAL, "logging.resetlayerlevel", ILLEGAL_ARGUMENT "\n");

	return MAL_SUCCEED;
}


static str
TRACERset_flush_level(void *ret, const char *const *lvl_id)
{
	(void) ret;
	if (GDKtracer_set_flush_level(*lvl_id) != GDK_SUCCEED)
		throw(MAL, "logging.setflushlevel", ILLEGAL_ARGUMENT "\n");

	return MAL_SUCCEED;
}


static str
TRACERreset_flush_level(void *ret)
{
	(void) ret;
	if (GDKtracer_reset_flush_level() != GDK_SUCCEED)
		throw(MAL, "logging.resetflushlevel", _OPERATION_FAILED "\n");

	return MAL_SUCCEED;
}


static str
TRACERset_adapter(void *ret, const char *const *adapter_id)
{
	(void) ret;
	if (GDKtracer_set_adapter(*adapter_id) != GDK_SUCCEED)
		throw(MAL, "logging.setadapter", ILLEGAL_ARGUMENT "\n");

	return MAL_SUCCEED;
}


static str
TRACERreset_adapter(void *ret)
{
	(void) ret;
	if (GDKtracer_reset_adapter() != GDK_SUCCEED)
		throw(MAL, "logging.resetadapter", _OPERATION_FAILED "\n");

	return MAL_SUCCEED;
}


static str
TRACERcomp_info(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	BAT *id, *component, *log_level;
	bat *i = getArgReference_bat(stk, pci, 0);
	bat *c = getArgReference_bat(stk, pci, 1);
	bat *l = getArgReference_bat(stk, pci, 2);

	id = COLnew(0, TYPE_int, (BUN) TRC_NAME(COMPONENTS_COUNT), TRANSIENT);
	component = COLnew(0, TYPE_str, (BUN) TRC_NAME(COMPONENTS_COUNT), TRANSIENT);
	log_level = COLnew(0, TYPE_str, (BUN) TRC_NAME(COMPONENTS_COUNT), TRANSIENT);

	if (id == NULL || component == NULL || log_level == NULL) {
		BBPreclaim(id);
		BBPreclaim(component);
		BBPreclaim(log_level);
		throw(MAL, "logging.compinfo", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	// Fill the BATs
	if (GDKtracer_fill_comp_info(id, component, log_level) != GDK_SUCCEED) {
		BBPunfix(id->batCacheid);
		BBPunfix(component->batCacheid);
		BBPunfix(log_level->batCacheid);
		throw(MAL, "logging.compinfo", GDK_EXCEPTION);
	}

	*i = id->batCacheid;
	BBPkeepref(id);
	*c = component->batCacheid;
	BBPkeepref(component);
	*l = log_level->batCacheid;
	BBPkeepref(log_level);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func tracer_init_funcs[] = {
 command("logging", "flush", TRACERflush_buffer, true, "Flush the buffer", args(1,1, arg("",void))),
 command("logging", "setcomplevel", TRACERset_component_level, true, "Sets the log level for a specific component", args(1,3, arg("",void),arg("comp",str),arg("lvl",str))),
 command("logging", "resetcomplevel", TRACERreset_component_level, true, "Resets the log level for a specific component back to the default", args(1,2, arg("",void),arg("comp",str))),
 command("logging", "setlayerlevel", TRACERset_layer_level, true, "Sets the log level for a specific layer", args(1,3, arg("",void),arg("layer",str),arg("lvl",str))),
 command("logging", "resetlayerlevel", TRACERreset_layer_level, true, "Resets the log level for a specific layer back to the default", args(1,2, arg("",void),arg("layer",str))),
 command("logging", "setflushlevel", TRACERset_flush_level, true, "Sets the flush level", args(1,2, arg("",void),arg("lvl",str))),
 command("logging", "resetflushlevel", TRACERreset_flush_level, true, "Resets the flush level back to the default", args(1,1, arg("",void))),
 command("logging", "setadapter", TRACERset_adapter, true, "Sets the adapter", args(1,2, arg("",void),arg("adapter",str))),
 command("logging", "resetadapter", TRACERreset_adapter, true, "Resets the adapter back to the default", args(1,1, arg("",void))),
 pattern("logging", "compinfo", TRACERcomp_info, false, "Returns in the form of a SQL result-set all the components along with their ID\nand the their current logging level being set", args(3,3, batarg("id",int),batarg("component",str),batarg("log_level",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_tracer_mal)
{ mal_module("tracer", NULL, tracer_init_funcs); }
