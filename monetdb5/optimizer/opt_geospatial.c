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
	int aBATreturnId, bBATreturnId;

	(void) pci;
	(void) stk;	
	(void) cntxt;

	//create new mal stack
	if(newMalBlkStmt(mb, slimit) < 0)
		return 0;
	

	//iterate over the instructions
	for(i=0; i<nextFreeSlot; i++) {

		//chech the module and function name
		if(getModuleId(oldInstrPtr[i]) && !strcasecmp(getModuleId(oldInstrPtr[i]),"batgeom")) 	{
			if(strcasecmp(getFunctionId(oldInstrPtr[i]), "contains") == 0) {

				//create the new instruction
				newInstrPtr = newStmt(mb, "batgeom", "MBRfilter");
				//create the return variables of the new instruction
				aBATreturnId = newVariable(mb, GDKstrdup("aBAT_filtered"), newBatType(TYPE_oid, getArgType(mb,oldInstrPtr[i],1)));
				bBATreturnId = newVariable(mb, GDKstrdup("bBAT_filtered"), newBatType(TYPE_oid, getArgType(mb,oldInstrPtr[i],2)));
				//set the return and input arguments of the new instruction
				setReturnArgument(newInstrPtr, aBATreturnId); //set the first return argument
				newInstrPtr = pushReturn(mb, newInstrPtr, bBATreturnId); //push a second return argument
				newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],1));
				newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],2));

				//replace the arguments of the contains function with the results of the new instructions (the filtered results) 
				delArgument(oldInstrPtr[i], 1);
				delArgument(oldInstrPtr[i], 2);
				pushInstruction(mb, oldInstrPtr[i]);
				setArgument(mb, oldInstrPtr[i], 1, aBATreturnId);
				setArgument(mb, oldInstrPtr[i], 2, bBATreturnId);
				
				actions+=3; //three changes
			} else //put back all other instructions from batgeom
				pushInstruction(mb, oldInstrPtr[i]);
		} else //put all other instructions back
			pushInstruction(mb, oldInstrPtr[i]);
	}
	
	GDKfree(oldInstrPtr);
	return actions;
}
