#include "monetdb_config.h"
#include "opt_geospatial.h"


typedef struct {
	int first;
	int second;
} arguments;

static arguments subselectInputs[5];
static int spatials;

static arguments projectInputs[5];
//static int subselectFirstInput[5];
//static int subselectSecondInput[5];
//static int foundItems = 0;
static int j = 0;
//static int subselectsReturnArgument[5];
//static int projectReturnArgument[5];
static int subselects =0;

static void createFilterInstruction(MalBlkPtr mb, InstrPtr *oldInstrPtr, int instructionNum, int filterFirstArgument) {
	InstrPtr filterInstrPtr, projectXInstrPtr, projectYInstrPtr;//, subselectInstrPtr, projectInstrPtr;
	int filterReturnId, projectXReturnId, projectYReturnId;//, subselectReturnId;

	//create and put in the MAL plan the new instructions
	filterInstrPtr = newStmt(mb, "batgeom", "Filter");
	projectXInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				
	projectYInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				
	pushInstruction(mb, oldInstrPtr[instructionNum]);
//	//pushInstruction(mb, oldInstrPtr[instructionNum+1]);				
//	subselectInstrPtr = newStmt(mb, "algebra", "subselect");				
//	projectInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				

	//make new return variables
	filterReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
	projectXReturnId = newTmpVariable(mb, getArgType(mb,oldInstrPtr[instructionNum],2));
	projectYReturnId = newTmpVariable(mb, getArgType(mb,oldInstrPtr[instructionNum],3));
//	subselectReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));

	//set the arguments for filter
	setReturnArgument(filterInstrPtr, filterReturnId);
	filterInstrPtr = pushArgument(mb, filterInstrPtr, filterFirstArgument);
	filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[instructionNum],2));
	filterInstrPtr = pushArgument(mb, filterInstrPtr, getArg(oldInstrPtr[instructionNum],3));

	//set the arguments for the x project
	setReturnArgument(projectXInstrPtr, projectXReturnId);
	projectXInstrPtr = pushArgument(mb, projectXInstrPtr, filterReturnId);
	projectXInstrPtr = pushArgument(mb, projectXInstrPtr, getArg(oldInstrPtr[instructionNum], 2));
					
	//set the arguments for the y project
	setReturnArgument(projectYInstrPtr, projectYReturnId);
	projectYInstrPtr = pushArgument(mb, projectYInstrPtr, filterReturnId);
	projectYInstrPtr = pushArgument(mb, projectYInstrPtr, getArg(oldInstrPtr[instructionNum], 3));

	//set the arguments of the spatial function
	delArgument(oldInstrPtr[instructionNum], 2);
	setArgument(mb, oldInstrPtr[instructionNum], 2, projectXReturnId);
	delArgument(oldInstrPtr[instructionNum], 3);
	setArgument(mb, oldInstrPtr[instructionNum], 3, projectYReturnId);
	
	//store the return variable of the spatial function
	subselectInputs[spatials].first = getArg(oldInstrPtr[instructionNum],0);
	subselectInputs[spatials].second = filterReturnId;
//fprintf(stderr, "%d -> SpatialReturnId: %d, FilterReturnId: %d\n", foundItems, subselectFirstInput[foundItems], subselectSecondInput[foundItems]);
	spatials++;

/*	//the new subselect does not use candidates
	setReturnArgument(projectInstrPtr, getArg(oldInstrPtr[instructionNum+1],0)); //get the variable before changing it
	setReturnArgument(oldInstrPtr[instructionNum+1], subselectReturnId);
	if(oldInstrPtr[instructionNum+1]->argc == 8)
		delArgument(oldInstrPtr[instructionNum+1], 2);
				
	//add a new function that gets the oids of the original BAT that qualified the spatial function
	projectInstrPtr = pushArgument(mb, projectInstrPtr, subselectReturnId);
	projectInstrPtr = pushArgument(mb, projectInstrPtr, filterReturnId);
*/
}

static void fixSubselect(MalBlkPtr mb, InstrPtr *oldInstrPtr, int instructionNum, int filterReturnId) {
	InstrPtr projectInstrPtr;
	int subselectReturnId, projectReturnId;
	int k=0;
	projectInstrPtr = newStmt(mb, "algebra", "leftfetchjoin");				


	//get the return variable of this subselect
	projectInputs[subselects].first = getArg(oldInstrPtr[instructionNum],0);
	//unless the subselect involves results from other subselect the return variable of 
	//it will be the return variable of the projection
	projectReturnId = projectInputs[subselects].first;

	//create the new subselect command that does not use any candidates
	subselectReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
	setReturnArgument(oldInstrPtr[instructionNum], subselectReturnId);
	if(oldInstrPtr[instructionNum]->argc == 8) {
//		fprintf(stderr, "secondArg %d\n", getArg(oldInstrPtr[instructionNum], 2));

		//check if the second argument to this subselect is something coming from another subselect
		for(k = 0; k<subselects; k++) {
			if(getArg(oldInstrPtr[instructionNum], 2) == projectInputs[k].first) {
				InstrPtr joinInstrPtr = newStmt(mb, "algebra", "subjoin");				
				InstrPtr extraProjectInstrPtr = newStmt(mb, "algebra", "leftfetchjoin"); //to get the oids from the original BAT that correspond to the oids tha satisfy the join				
				int joinReturnId1 = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
				int joinReturnId2 = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
				projectReturnId = newTmpVariable(mb, newBatType(TYPE_oid, TYPE_oid));
			
			
				setReturnArgument(joinInstrPtr, joinReturnId1);
				joinInstrPtr = pushArgument(mb, joinInstrPtr, joinReturnId2);
				joinInstrPtr = pushArgument(mb, joinInstrPtr, projectReturnId); //the first BAT is the result of the projection of this subselect
				joinInstrPtr = pushArgument(mb, joinInstrPtr, projectInputs[k].first); //the second BAT is the results of the projection of the other subselect
				joinInstrPtr = pushNil(mb, joinInstrPtr, TYPE_bat);
				joinInstrPtr = pushNil(mb, joinInstrPtr, TYPE_bat);
				joinInstrPtr = pushBit(mb, joinInstrPtr, 0); //do not match null values
				joinInstrPtr = pushNil(mb, joinInstrPtr, TYPE_lng); //I do not have an estimation of the size
				joinInstrPtr->retc=2;	

				setReturnArgument(extraProjectInstrPtr, projectInputs[subselects].first);
				extraProjectInstrPtr = pushArgument(mb, extraProjectInstrPtr, joinReturnId1); //the result of the join
				extraProjectInstrPtr = pushArgument(mb, extraProjectInstrPtr, projectReturnId); //the BAT used for the join computation

			}
		}
		delArgument(oldInstrPtr[instructionNum], 2);

	}

						
	//add a new function that gets the oids of the original BAT that qualified the spatial function
	setReturnArgument(projectInstrPtr, projectReturnId);
	projectInstrPtr = pushArgument(mb, projectInstrPtr, subselectReturnId);
	projectInstrPtr = pushArgument(mb, projectInstrPtr, filterReturnId);
	
	subselects++;
}

static int getSubselectSecondInput(int currentSubselectFirstInput) {
	int k=0;

	for(k=0; k<spatials ; k++)
		if(currentSubselectFirstInput == subselectInputs[k].first)
				return subselectInputs[k].second;
	return -1;
}

int OPTgeospatialImplementation(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	int i=0, actions = 0;
	int nextFreeSlot = mb->stop; //up to this there are instructions
	InstrPtr *oldInstrPtr = mb->stmt; //pointer to the first instruction
	int slimit = mb->ssize; //what is

	spatials = 0;
	subselects = 0;

	(void) pci;
	(void) stk;	
	(void) cntxt;

	//create new mal stack
	if(newMalBlkStmt(mb, slimit) < 0)
		return 0;
	

	//iterate over the instructions
	for(i=0; i<nextFreeSlot; i++) {	
		//chech the module and function name
		if(getModuleId(oldInstrPtr[i]) && !idcmp(getModuleId(oldInstrPtr[i]),"algebra") && !idcmp(getFunctionId(oldInstrPtr[i]), "subselect")) {
			int filterReturnId = 0;
			pushInstruction(mb, oldInstrPtr[i]);

			if((filterReturnId = getSubselectSecondInput(getArg(oldInstrPtr[i], 1))) > 0)
				fixSubselect(mb, oldInstrPtr, i, filterReturnId);
		} else if(getModuleId(oldInstrPtr[i]) && !strcasecmp(getModuleId(oldInstrPtr[i]),"batgeom")) 	{
			if((strcasecmp(getFunctionId(oldInstrPtr[i]), "contains1") == 0) || (strcasecmp(getFunctionId(oldInstrPtr[i]), "contains2") == 0))  {
				if(oldInstrPtr[i]->argc == 5) {
					//call all necessary intructions for the filter and the evaluation of the spatial relation	
					createFilterInstruction(mb, oldInstrPtr, i, getArg(oldInstrPtr[i],1));

					////skip the algebra.subselect command
					//i++;

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
			} else if((strcasecmp(getFunctionId(oldInstrPtr[i]), "distance1") == 0 || strcasecmp(getFunctionId(oldInstrPtr[i]), "distance2") == 0)  
					&& strcasecmp(getFunctionId(oldInstrPtr[i+1]), "thetasubselect") == 0) {
				//the filter does not make sense if comparison is > OR >= 
			//	if(strcmp(getArg(oldInstrPtr[i+1],4), ">")==0 ||  strcmp(getArg(oldInstrPtr[i+1],4),">=")==0) {
			//		pushInstruction(mb, oldInstrPtr[i]);
			//	} else 
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

					////skip the algebra.thetasubselect command
					//i++;

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
			} else {//put back all other instructions from batgeom 
				pushInstruction(mb, oldInstrPtr[i]);
			}
		} else { //put all other instructions back
			pushInstruction(mb, oldInstrPtr[i]);
			
			//I need to track the first input to the subselect
			for(j=0; j<spatials ; j++) {
				if(getArg(oldInstrPtr[i],1) == subselectInputs[j].first) {
					subselectInputs[j].first = getArg(oldInstrPtr[i], 0);
					break;
				}
			}
		}
	}
	
	GDKfree(oldInstrPtr);
	return actions;
}
