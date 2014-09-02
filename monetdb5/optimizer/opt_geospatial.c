#include "monetdb_config.h"
#include "opt_geospatial.h"
//#include "mal_instruction.h"
//#include "mal_interpreter.h"


int OPTgeospatialImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	int i=0, actions = 0;
	int nextFreeSlot = mb->stop; //up to this there are instructions
	InstrPtr *oldInstrPtr = mb->stmt; //pointer to the first instruction
	int slimit = mb->ssize; //what is
	
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
				if(oldInstrPtr[i]->argc == 5) {
					//replace the instruction with two new ones
					InstrPtr filterInstrPtr, containsInstrPtr;
					bat filteredOIDsBAT_id;
					
					//create and put in the MAL plan the new instructions
					filterInstrPtr = newStmt(mb, "batgeom", "Filter");
					containsInstrPtr = newStmt(mb, "batgeom", "Contains");

					//set the return argument of the filter
					filteredOIDsBAT_id = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
					setReturnArgument(filterInstrPtr, filteredOIDsBAT_id);
					//set the input arguments of the filter
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],1));
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],2));
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],3));

					//set the arguments of the contains function
					setReturnArgument(containsInstrPtr, getArg(oldInstrPtr[i],0));
					containsInstrPtr = pushArgument(mb, containsInstrPtr, getArg(oldInstrPtr[i],1));
					containsInstrPtr = pushArgument(mb, containsInstrPtr, getArg(oldInstrPtr[i],2));
					containsInstrPtr = pushArgument(mb, containsInstrPtr, getArg(oldInstrPtr[i],3));
					containsInstrPtr = pushArgument(mb, containsInstrPtr, filteredOIDsBAT_id);
					containsInstrPtr = pushArgument(mb, containsInstrPtr, getArg(oldInstrPtr[i],4));
				} else {
					int inputBAT = 0;
					InstrPtr newInstrPtr;
					int returnBATId;

					if(isaBatType(getArgType(mb,oldInstrPtr[i],1)))
						inputBAT = 1;
				
					if(isaBatType(getArgType(mb,oldInstrPtr[i],2))) {
						if(inputBAT == 1) {
							//no Filtering when both inputs are BATs because it cannot be parallelised)
							pushInstruction(mb, oldInstrPtr[i]);
							continue;
						}
						inputBAT=2;
					}	

					//create the new instruction
					newInstrPtr = newStmt(mb, "batgeom", "Filter");
			
					//create the return variable of the new instruction (it should be of the same type with the input variable (BAT) of the old instruction)	
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
				
					actions+=2; //two changes
				}
			} else //put back all other instructions from batgeom
				pushInstruction(mb, oldInstrPtr[i]);
		} else //put all other instructions back
			pushInstruction(mb, oldInstrPtr[i]);
	}
	
	GDKfree(oldInstrPtr);
	return actions;
}
