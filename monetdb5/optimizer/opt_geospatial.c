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
//	int aBATreturnId, bBATreturnId;
	int returnBATId;

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
				int inputBAT = 0;
	
				//create the new instruction
				newInstrPtr = newStmt(mb, "batgeom", "ContainsFilter");
				//create the return variable of the new instruction (it should be of the same type with the input variable (BAT) of the old instruction)	
				if(isaBatType(getArgType(mb,oldInstrPtr[i],1))) {
					inputBAT = 1;
				} else if(isaBatType(getArgType(mb,oldInstrPtr[i],2))) {
					inputBAT=2;
				}
				
				returnBATId = newVariable(mb, GDKstrdup("BATfiltered"), getArgType(mb,oldInstrPtr[i],inputBAT));
				
				//set the return and input arguments of the new instruction
				setReturnArgument(newInstrPtr, returnBATId);
				//newInstrPtr = pushReturn(mb, newInstrPtr, bBATreturnId); //push a second return argument
				newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],1));
				newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],2));

			
				//replace the BAT arguments of the contains function with the BAT of the new instruction (the filtered BAT) 
				delArgument(oldInstrPtr[i], inputBAT);
				pushInstruction(mb, oldInstrPtr[i]);
				setArgument(mb, oldInstrPtr[i], inputBAT, returnBATId);
				
				actions+=3; //three changes
			} else //put back all other instructions from batgeom
				pushInstruction(mb, oldInstrPtr[i]);
		} else //put all other instructions back
			pushInstruction(mb, oldInstrPtr[i]);
	}
	
	GDKfree(oldInstrPtr);
	return actions;
}
