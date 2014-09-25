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
fprintf(stderr, "%s\n", getFunctionId(oldInstrPtr[i]));
			if(strcasecmp(getFunctionId(oldInstrPtr[i]), "contains1") == 0)  {
				if(oldInstrPtr[i]->argc == 5) {
					//replace the instruction with two new ones
					InstrPtr filterInstrPtr, /*fcnInstrPtr,*/ projectInstrPtr, projectXInstrPtr, projectYInstrPtr;
					int filterReturnId, subselectReturnId, projectXReturnId, projectYReturnId;
					//create and put in the MAL plan the new instructions
					filterInstrPtr = newStmt(mb, "batgeom", "Filter");
					projectXInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				
					projectYInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				
					//fcnInstrPtr = newStmt(mb, "batgeom", "Contains1");
					pushInstruction(mb, oldInstrPtr[i]);
					pushInstruction(mb, oldInstrPtr[i+1]);
					projectInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				

					//set the return argument of the filter
					filterReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
					setReturnArgument(filterInstrPtr, filterReturnId);
					//set the input arguments of the filter
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],1));
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],2));
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],3));

					//set the arguments for the X, Y projections
					projectXReturnId = newVariable(mb, GDKstrdup("xBATfiltered"), getArgType(mb,oldInstrPtr[i],2));
					setReturnArgument(projectXInstrPtr, projectXReturnId);
					projectXInstrPtr = pushArgument(mb, projectXInstrPtr, getArg(oldInstrPtr[i], 2));
					projectXInstrPtr = pushArgument(mb, projectXInstrPtr, filterReturnId);

					projectYReturnId = newVariable(mb, GDKstrdup("yBATfiltered"), getArgType(mb,oldInstrPtr[i],3));
					setReturnArgument(projectYInstrPtr, projectYReturnId);
					projectYInstrPtr = pushArgument(mb, projectYInstrPtr, getArg(oldInstrPtr[i], 3));
					projectYInstrPtr = pushArgument(mb, projectYInstrPtr, filterReturnId);

					////set the arguments of the contains function
					//setReturnArgument(fcnInstrPtr, getArg(oldInstrPtr[i],0));
					//fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],1));
					//fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],2));
					//fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],3));
					//fcnInstrPtr = pushArgument(mb, fcnInstrPtr, filterReturnId);
					//fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],4));
					//replace the x, y BATs with the filtered version of them
					delArgument(oldInstrPtr[i], 2);
					setArgument(mb, oldInstrPtr[i], 2, projectXReturnId);
					delArgument(oldInstrPtr[i], 3);
					setArgument(mb, oldInstrPtr[i], 3, projectYReturnId);

					//the new subselect does not use candidates
					subselectReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
					setReturnArgument(projectInstrPtr, getArg(oldInstrPtr[i+1],0)); //get the variable before changing it
					setReturnArgument(oldInstrPtr[i+1], subselectReturnId);
					delArgument(oldInstrPtr[i+1], 2);
					
					//add a new function that gets the oids of the original BAT that qualified the distance subselect
					projectInstrPtr = pushArgument(mb, projectInstrPtr, subselectReturnId);
					projectInstrPtr = pushArgument(mb, projectInstrPtr, filterReturnId);
					
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
					//replace the instruction with the new ones
					InstrPtr bufferInstrPtr, filterInstrPtr, fcnInstrPtr, projectInstrPtr;
					int bufferReturnId, filterReturnId, subselectReturnId;
					
					//create and put in the MAL plan the new instructions
					bufferInstrPtr = newStmt(mb, "geom", "Buffer");
					filterInstrPtr = newStmt(mb, "batgeom", "Filter");
					fcnInstrPtr = newStmt(mb, "batgeom", "Distance");
					pushInstruction(mb, oldInstrPtr[i+1]);
					projectInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				
		
					//make new return variables
					bufferReturnId = newTmpVariable(mb, getArgType(mb, oldInstrPtr[i], 1));
					filterReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
					subselectReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));

					//set the arguments for the Buffer
					setReturnArgument(bufferInstrPtr, bufferReturnId);
					bufferInstrPtr = pushArgument(mb, bufferInstrPtr, getArg(oldInstrPtr[i], 1)); //a TYPE_wkb would be usufull
					bufferInstrPtr = pushArgument(mb, bufferInstrPtr, getArg(oldInstrPtr[i+1], 3));

					//set the arguments for the Filter
					setReturnArgument(filterInstrPtr, filterReturnId);
					filterInstrPtr = pushArgument(mb, filterInstrPtr, bufferReturnId);
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],2));
					filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[i],3));

					//set the arguments of the distance function
					setReturnArgument(fcnInstrPtr, getArg(oldInstrPtr[i],0));
					fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],1));
					fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],2));
					fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],3));
					fcnInstrPtr = pushArgument(mb, fcnInstrPtr, filterReturnId);
					fcnInstrPtr = pushArgument(mb, fcnInstrPtr, getArg(oldInstrPtr[i],4));
					
					//the new subselect does not use candidates
					setReturnArgument(projectInstrPtr, getArg(oldInstrPtr[i+1],0)); //get the variable before changing it
					setReturnArgument(oldInstrPtr[i+1], subselectReturnId);
					delArgument(oldInstrPtr[i+1], 2);
					
					//add a new function that gets the oids of the original BAT that qualified the distance subselect
					projectInstrPtr = pushArgument(mb, projectInstrPtr, subselectReturnId);
					projectInstrPtr = pushArgument(mb, projectInstrPtr, filterReturnId);
					
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
