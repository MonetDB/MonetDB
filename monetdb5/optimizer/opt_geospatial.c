#include "monetdb_config.h"
#include "opt_geospatial.h"
//#include "mal_instruction.h"
//#include "mal_interpreter.h"


int OPTgeospatialImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	int i=0, actions = 0;
	int nextFreeSlot = mb->stop; //up to this there are instructions
	InstrPtr *oldInstrPtr = mb->stmt; //pointer to the first instruction
	int slimit = mb->ssize; //what is this?
	InstrPtr newInstrPtr;
	int newInstrReturnValue;

	(void) pci;
	(void) stk;	
	(void) cntxt;

	//create new mal stack
	if(newMalBlkStmt(mb, slimit) < 0)
		return 0;
	

	//iterate over the instructions and put them back in the stach
	for(i=0; i<nextFreeSlot; i++) {

		//chech the module and function name
		if(getModuleId(oldInstrPtr[i]) && !strcasecmp(getModuleId(oldInstrPtr[i]),"batgeom")) 	{
			if(strcasecmp(getFunctionId(oldInstrPtr[i]), "contains") == 0) {

				//create the new instruction
				newInstrPtr = newStmt(mb, "batgeom", "MBRfilter");
				//create the return variable of the new instruction
				newInstrReturnValue = newVariable(mb, GDKstrdup("result"), newBatType(TYPE_oid, getArgType(mb,oldInstrPtr[i],2)));
				//set the return and input arguments of the new instruction
				setReturnArgument(newInstrPtr, newInstrReturnValue);
				newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],1));
				newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],2));
			
				//replace the second argument of the contains function with the results of the new instruction (the filtered results) 
				delArgument(oldInstrPtr[i], 2);
				pushInstruction(mb, oldInstrPtr[i]);
				setArgument(mb, oldInstrPtr[i], 2, newInstrReturnValue);
				
				actions++;
			}
		} else //put all other instructions back
			pushInstruction(mb, oldInstrPtr[i]);
	}
	
	GDKfree(oldInstrPtr);
	return actions;
}
