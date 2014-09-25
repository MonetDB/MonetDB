#include "monetdb_config.h"
#include "opt_geospatial.h"

static void createFilterInstruction(MalBlkPtr mb, InstrPtr *oldInstrPtr, int intructionNum, int filterFirstArgument) {
	InstrPtr filterInstrPtr, projectInstrPtr, projectXInstrPtr, projectYInstrPtr;
	int filterReturnId, subselectReturnId, projectXReturnId, projectYReturnId;

	//create and put in the MAL plan the new instructions
	filterInstrPtr = newStmt(mb, "batgeom", "Filter");
	projectXInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				
	projectYInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				
	pushInstruction(mb, oldInstrPtr[intructionNum]);
	pushInstruction(mb, oldInstrPtr[intructionNum+1]);
	projectInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				

	//make new return variables
	filterReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
	projectXReturnId = newVariable(mb, GDKstrdup("xBATfiltered"), getArgType(mb,oldInstrPtr[intructionNum],2));
	projectYReturnId = newVariable(mb, GDKstrdup("yBATfiltered"), getArgType(mb,oldInstrPtr[intructionNum],3));
	subselectReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));

	//set the arguments for filter
	setReturnArgument(filterInstrPtr, filterReturnId);
	filterInstrPtr = pushArgument(mb, filterInstrPtr, filterFirstArgument);
	filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[intructionNum],2));
	filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[intructionNum],3));

	//set the arguments for the x project
	setReturnArgument(projectXInstrPtr, projectXReturnId);
	projectXInstrPtr = pushArgument(mb, projectXInstrPtr, filterReturnId);
	projectXInstrPtr = pushArgument(mb, projectXInstrPtr, getArg(oldInstrPtr[intructionNum], 2));
					
	//set the arguments for the y project
	setReturnArgument(projectYInstrPtr, projectYReturnId);
	projectYInstrPtr = pushArgument(mb, projectYInstrPtr, filterReturnId);
	projectYInstrPtr = pushArgument(mb, projectYInstrPtr, getArg(oldInstrPtr[intructionNum], 3));

	//set the arguments of the spatial function
	delArgument(oldInstrPtr[intructionNum], 2);
	setArgument(mb, oldInstrPtr[intructionNum], 2, projectXReturnId);
	delArgument(oldInstrPtr[intructionNum], 3);
	setArgument(mb, oldInstrPtr[intructionNum], 3, projectYReturnId);

	//the new subselect does not use candidates
	setReturnArgument(projectInstrPtr, getArg(oldInstrPtr[intructionNum+1],0)); //get the variable before changing it
	setReturnArgument(oldInstrPtr[intructionNum+1], subselectReturnId);
	delArgument(oldInstrPtr[intructionNum+1], 2);
					
	//add a new function that gets the oids of the original BAT that qualified the spatial function
	projectInstrPtr = pushArgument(mb, projectInstrPtr, subselectReturnId);
	projectInstrPtr = pushArgument(mb, projectInstrPtr, filterReturnId);

}


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
			if(strcasecmp(getFunctionId(oldInstrPtr[i]), "contains1") == 0)  {
				if(oldInstrPtr[i]->argc == 5) {
					//call all necessary intructions for the filter and the evaluation of the spatial relation	
					createFilterInstruction(mb, oldInstrPtr, i, getArg(oldInstrPtr[i],1));

					//skip the algebra.subselect command
					i++;

					actions += 5;
				} else {
					int inputBAT = 0;
					InstrPtr newInstrPtr;
					int returnBATId;

					if(isaBatType(getArgType(mb,oldInstrPtr[i],1)))
						inputBAT = 1;
				
					if(isaBatType(getArgType(mb,oldInstrPtr[i],2))) {
						if(inputBAT == 1) {
							//no Filtering (for now) when both inputs are BATs
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
					newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],1));
					newInstrPtr = pushArgument(mb, newInstrPtr, getArg(oldInstrPtr[i],2));
			
					//replace the BAT arguments of the contains function with the BAT of the new instruction (the filtered BAT) 
					delArgument(oldInstrPtr[i], inputBAT);
					pushInstruction(mb, oldInstrPtr[i]);
					setArgument(mb, oldInstrPtr[i], inputBAT, returnBATId);

					actions +=2;
				}
			} else if(strcasecmp(getFunctionId(oldInstrPtr[i]), "distance") == 0 && strcasecmp(getFunctionId(oldInstrPtr[i+1]), "thetasubselect") == 0) {
				//I should check the theta comparison. In case it is > OR >= then there should be no filtering
				if(oldInstrPtr[i]->argc == 5) {
					InstrPtr bufferInstrPtr;
					int bufferReturnId;
					
					//create a buffer that will be used for the filter
					bufferInstrPtr = newStmt(mb, "geom", "Buffer");
					//make new return variables
					bufferReturnId = newTmpVariable(mb, getArgType(mb, oldInstrPtr[i], 1));
					//set the arguments
					setReturnArgument(bufferInstrPtr, bufferReturnId);
					bufferInstrPtr = pushArgument(mb, bufferInstrPtr, getArg(oldInstrPtr[i], 1)); //a TYPE_wkb would be usufull
					bufferInstrPtr = pushArgument(mb, bufferInstrPtr, getArg(oldInstrPtr[i+1], 3));
				
					//call all necessary intructions for the filter and the evaluation of the spatial relation	
					createFilterInstruction(mb, oldInstrPtr, i, bufferReturnId);

					//skip the algebra.thetasubselect command
					i++;

					actions += 5;
				} else {
					int inputBAT = 0, geomArg =0;
					InstrPtr bufferInstrPtr, filterInstrPtr;
					int bufferReturnId, filterReturnId;
				
					if(isaBatType(getArgType(mb,oldInstrPtr[i],1))) {
						inputBAT = 1;
						geomArg = 2;
					}
				
					if(isaBatType(getArgType(mb,oldInstrPtr[i],2))) {
						if(inputBAT != 0) {
							//no Filtering (for now) when both inputs are BATs
							pushInstruction(mb, oldInstrPtr[i]);
							continue;
						}
						inputBAT = 2;
						geomArg = 1;
					}	

					//create two new instruction
					bufferInstrPtr = newStmt(mb, "geom", "Buffer");
					filterInstrPtr = newStmt(mb, "batgeom", "Filter");
				
					//make new return variables
					bufferReturnId = newTmpVariable(mb, getArgType(mb, oldInstrPtr[i], geomArg)); //Buffer returns a geometry
					filterReturnId = newTmpVariable(mb, getArgType(mb, oldInstrPtr[i], inputBAT)); //Filter returns a BAT

					//set the arguments for the Buffer
					setReturnArgument(bufferInstrPtr, bufferReturnId);
					bufferInstrPtr = pushArgument(mb, bufferInstrPtr, getArg(oldInstrPtr[i], geomArg));
					bufferInstrPtr = pushArgument(mb, bufferInstrPtr, getArg(oldInstrPtr[i+1], 3));
				
					//set the arguments for the Filter
					setReturnArgument(filterInstrPtr, filterReturnId);
					filterInstrPtr = pushArgument(mb, filterInstrPtr, bufferReturnId);
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i], inputBAT));
	
					//replace the BAT arguments of the contains function with the BAT of the new instruction (the filtered BAT) 
					delArgument(oldInstrPtr[i], inputBAT);
					pushInstruction(mb, oldInstrPtr[i]);
					setArgument(mb, oldInstrPtr[i], inputBAT, filterReturnId);
				
					actions+=3;
				}
			} else //put back all other instructions from batgeom
				pushInstruction(mb, oldInstrPtr[i]);
		} else //put all other instructions back
			pushInstruction(mb, oldInstrPtr[i]);
	}
	
	GDKfree(oldInstrPtr);
	return actions;
}
