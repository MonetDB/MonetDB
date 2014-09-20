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
 *                * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (c)2014 author Martin Kersten
 * Local dictionary encoding based on 8-bits.
 * Index value zero is not used to easy detection of filler
 * The dictionary size is correlated with the number of entries covered
 * Dictionaries bring back the size to 1 byte, which leads to limited type use and compression gains
 * Floating points are not expected to be replicated 
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_dictionary.h"

void
MOSadvance_dictionary(Client cntxt, MOStask task)
{
	(void) cntxt;

	task->start += MOSgetCnt(task->blk);
	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(sht)+ sizeof(bte) * MOSgetCnt(task->blk),sht)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(int)+ sizeof(bte) * MOSgetCnt(task->blk),int)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(lng)+ sizeof(bte) * MOSgetCnt(task->blk),lng)); break;
	case TYPE_oid: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(oid)+ sizeof(bte) * MOSgetCnt(task->blk),oid)); break;
	case TYPE_wrd: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(wrd)+ sizeof(bte) * MOSgetCnt(task->blk),wrd)); break;
	case TYPE_flt: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(flt)+ sizeof(bte) * MOSgetCnt(task->blk),flt)); break;
	case TYPE_dbl: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(dbl)+ sizeof(bte) * MOSgetCnt(task->blk),dbl)); break;
#ifdef HAVE_HGE
	case TYPE_hge: task->blk = (MosaicBlk)( ((char*)task->blk) + wordaligned(2* MosaicBlkSize + task->dictsize * sizeof(hge)+ sizeof(bte) * MOSgetCnt(task->blk),hge)); break;
#endif
	}
}

/* Beware, the dump routines use the compressed part of the task */
void
MOSdump_dictionary(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->blk;
	int i;
	int *size;
	void *val = (void*)(((char*) blk) + MosaicBlkSize);

	size = (int*) (((char*)blk) + MosaicBlkSize);
	mnstr_printf(cntxt->fdout,"#dict " BUNFMT" ", MOSgetCnt(blk));
	switch(task->type){
	case TYPE_sht:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"sht [%d] %hd",i, ((sht*) val)[i]); break;
	case TYPE_int:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"int [%d] %d",i, ((int*) val)[i]); break;
	case  TYPE_oid:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"oid [%d] "OIDFMT, i, ((oid*) val)[i]); break;
	case  TYPE_lng:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"lng [%d] "LLFMT, i, ((lng*) val)[i]); break;
#ifdef HAVE_HGE
	case  TYPE_hge:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"hge [%d] %.40g", i, (dbl) ((hge*) val)[i]); break;
#endif
	case  TYPE_wrd:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"wrd [%d] "SZFMT, i, ((wrd*) val)[i]); break;
	case TYPE_flt:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"flt [%d] %f",i, ((flt*) val)[i]); break;
	case TYPE_dbl:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"dbl [%d] %g",i, ((dbl*) val)[i]); break;
	}
	mnstr_printf(cntxt->fdout,"\n");
}

void
MOSskip_dictionary(Client cntxt, MOStask task)
{
	MOSadvance_dictionary(cntxt, task);
	if ( MOSgetTag(task->blk) == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define estimateDict(TPE)\
{	TPE *val = (TPE*)task->src;\
	TPE *dict = (TPE*)GDKzalloc(sizeof(TPE) * task->dictsize);\
	if( dict== NULL)\
		return 1.0;\
	for(i =0; i<task->elm; i++, val++){\
		for(j= 0; j< size; j++)\
			if( dict[j] == *val) {cnt++;break;}\
		if ( j == size){\
			if ( size == task->dictsize)\
				break;\
			dict[j] = *val;\
			size++;\
			cnt++;\
		}\
	}\
	GDKfree(dict);\
	if ( i > MOSlimit() ) i = MOSlimit();\
	if(i) factor = (flt) ((int)i * sizeof(int)) / wordaligned(2* MosaicBlkSize + size * sizeof(TPE)+ sizeof(bte) * MOSgetCnt(task->blk),TPE);\
}

// calculate the expected reduction using DICT in terms of elements compressed
flt
MOSestimate_dictionary(Client cntxt, MOStask task)
{	BUN i = -1;
	int cnt =0,j;
	int size=0;
	flt factor= 1.0;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: estimateDict(sht); break;
	case TYPE_lng: estimateDict(lng); break;
	case TYPE_oid: estimateDict(oid); break;
	case TYPE_wrd: estimateDict(wrd); break;
	case TYPE_flt: estimateDict(flt); break;
	case TYPE_dbl: estimateDict(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: estimateDict(hge); break;
#endif
	case TYPE_int:
		{	int *val = (int*)task->src;
			int *dict = (int*)GDKzalloc(sizeof(int) * task->dictsize);
			if( dict== NULL)
				return 1.0;

			for(i =0; i<task->elm; i++, val++){
				for(j= 0; j< size; j++)
					if( dict[j] == *val) {cnt++;break;}
				if ( j == size){
					if ( size == task->dictsize)
						break;
					dict[j] = *val;
					size++;
					cnt++;
				}
			}
			GDKfree(dict);
			if ( i > MOSlimit() ) i = MOSlimit();
			if(i) factor = (flt) ((int)i * sizeof(int)) / wordaligned(2* MosaicBlkSize + size * sizeof(int)+ sizeof(bte) * MOSgetCnt(task->blk),int);
		}
	}
#ifdef _DEBUG_MOSAIC_
	mnstr_printf(cntxt->fdout,"#estimate dict "BUNFMT" elm %4.2f factor\n", i, factor);
#endif
	return factor; 
}

// insert a series of values into the compressor block using dictionary
#define DICTcompress(TPE)\
{	TPE *val = (TPE*)task->src;\
	TPE *dict = (TPE*)((char*)task->blk+ 2 * MosaicBlkSize);\
	BUN limit = task->elm > MOSlimit()? MOSlimit(): task->elm;\
	task->dst = ((char*) dict)+ sizeof(TPE)*task->dictsize;\
	for(i =0; i<limit; i++, val++){\
		for(j= 0; j< *size; j++)\
			if( dict[j] == *val) {\
				MOSincCnt(blk,1);\
				*task->dst++ = (char) j;\
				break;\
			}\
		if ( j == *size){\
			if ( *size == task->dictsize){\
				task->dst += wordaligned(MOSgetCnt(blk) %2,TPE);\
				break;\
			}\
			MOSincCnt(blk,1);\
			dict[j] = *val;\
			*size = *size+1;\
			*task->dst++ = (char) j;\
		}\
	}\
	task->src = (char*) val;\
}

void
MOScompress_dictionary(Client cntxt, MOStask task)
{
	BUN i ;
	int j;
	int *size;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	size = (int*) (((char*)blk) + MosaicBlkSize);
	*size = 0;
	MOSsetTag(blk,MOSAIC_DICT);

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: DICTcompress(sht); break;
	case TYPE_lng: DICTcompress(lng); break;
	case TYPE_oid: DICTcompress(oid); break;
	case TYPE_wrd: DICTcompress(wrd); break;
	case TYPE_flt: DICTcompress(flt); break;
	case TYPE_dbl: DICTcompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTcompress(hge); break;
#endif
	case TYPE_int:
		{	int *val = (int*)task->src;
			int *dict = (int*)((char*)task->blk+ 2 * MosaicBlkSize);
			BUN limit = task->elm > MOSlimit()? MOSlimit(): task->elm;
			task->dst = ((char*) dict)+ sizeof(int)*task->dictsize;
			for(i =0; i<limit; i++, val++){
				for(j= 0; j< *size; j++)
					if( dict[j] == *val) {
						MOSincCnt(blk,1);
						*task->dst++ = (char) j;
						break;
					}
				if ( j == *size){
					if ( *size == task->dictsize){
						// align on word boundary
						task->dst += wordaligned(MOSgetCnt(blk) %2,int);
						break;
					}
					MOSincCnt(blk,1);
					dict[j] = *val;
					*size = *size+1;
					*task->dst++ = (char) j;
				}
			}
			task->src = (char*) val;
		}
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_dictionary(cntxt, task);
#endif
}

// the inverse operator, extend the src
#define DICTdecompress(TPE)\
{	TPE *dict =(TPE*)((char*)task->blk+ 2 * MosaicBlkSize);\
	bte *idx = (bte*)(((char*)dict) + task->dictsize * sizeof(TPE));\
	BUN lim = MOSgetCnt(blk);\
	for(i = 0; i < lim; i++,idx++)\
		((TPE*)task->src)[i] = dict[ (bte)*idx];\
	task->src += i * sizeof(TPE);\
}

void
MOSdecompress_dictionary(Client cntxt, MOStask task)
{
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
	//case TYPE_bte: CASE_bit: no compression achievable
	case TYPE_sht: DICTdecompress(sht); break;
	case TYPE_lng: DICTdecompress(lng); break;
	case TYPE_oid: DICTdecompress(oid); break;
	case TYPE_wrd: DICTdecompress(wrd); break;
	case TYPE_flt: DICTdecompress(flt); break;
	case TYPE_dbl: DICTdecompress(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: DICTdecompress(hge); break;
#endif
	case TYPE_int:
		{	int *dict =(int*)((char*)task->blk+ 2 * MosaicBlkSize);
			bte *idx = (bte*)(((char*)dict) + task->dictsize * sizeof(int));
			BUN lim = MOSgetCnt(blk);
			for(i = 0; i < lim; i++,idx++)
				((int*)task->src)[i] = dict[ (bte)*idx];
			task->src += i * sizeof(int);
		}
	}
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define subselect_dictionary(TPE) {\
 	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize );\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(TPE));\
	if( !*anti){\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				cmp  =  ((*hi && dict[*idx] <= * (TPE*)hgh ) || (!*hi && dict[*idx] < *(TPE*)hgh ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				cmp  =  ((*li && dict[*idx] >= * (TPE*)low ) || (!*li && dict[*idx] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				cmp  =  ((*hi && dict[*idx] <= * (TPE*)hgh ) || (!*hi && dict[*idx] < *(TPE*)hgh )) &&\
						((*li && dict[*idx] >= * (TPE*)low ) || (!*li && dict[*idx] > *(TPE*)low ));\
				if (cmp )\
					*o++ = (oid) first;\
			}\
		}\
	} else {\
		if( *(TPE*) low == TPE##_nil && *(TPE*) hgh == TPE##_nil){\
			/* nothing is matching */\
		} else\
		if( *(TPE*) low == TPE##_nil ){\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				cmp  =  ((*hi && dict[*idx] <= * (TPE*)hgh ) || (!*hi && dict[*idx] < *(TPE*)hgh ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else\
		if( *(TPE*) hgh == TPE##_nil ){\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				cmp  =  ((*li && dict[*idx] >= * (TPE*)low ) || (!*li && dict[*idx] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		} else{\
			for( ; first < last; first++, idx++){\
				MOSskipit();\
				cmp  =  ((*hi && dict[*idx] <= * (TPE*)hgh ) || (!*hi && dict[*idx] < *(TPE*)hgh )) &&\
						((*li && dict[*idx] >= * (TPE*)low ) || (!*li && dict[*idx] > *(TPE*)low ));\
				if ( !cmp )\
					*o++ = (oid) first;\
			}\
		}\
	}\
}

str
MOSsubselect_dictionary(Client cntxt,  MOStask task, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	BUN first,last;
	int cmp;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_dictionary(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: subselect_dictionary(sht); break;
	case TYPE_lng: subselect_dictionary(lng); break;
	case TYPE_oid: subselect_dictionary(oid); break;
	case TYPE_wrd: subselect_dictionary(wrd); break;
	case TYPE_flt: subselect_dictionary(flt); break;
	case TYPE_dbl: subselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: subselect_dictionary(hge); break;
#endif
	case TYPE_int:
	// Expanded MOSselect_dictionary for debugging
	{ 	int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
		bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(int));

		if( !*anti){
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				for( ; first < last; first++){
					MOSskipit();
					*o++ = (oid) first;
				}
			} else
			if( *(int*) low == int_nil ){
				for( ; first < last; first++, idx++){
					MOSskipit();
					cmp  =  ((*hi && dict[*idx] <= * (int*)hgh ) || (!*hi && dict[*idx] < *(int*)hgh ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for( ; first < last; first++, idx++){
					MOSskipit();
					cmp  =  ((*li && dict[*idx] >= * (int*)low ) || (!*li && dict[*idx] > *(int*)low ));
					if (cmp )
						*o++ = (oid) first;
				}
			} else{
				for( ; first < last; first++, idx++){
					MOSskipit();
					cmp  =  ((*hi && dict[*idx] <= * (int*)hgh ) || (!*hi && dict[*idx] < *(int*)hgh )) &&
							((*li && dict[*idx] >= * (int*)low ) || (!*li && dict[*idx] > *(int*)low ));
					if (cmp )
						*o++ = (oid) first;
				}
			}
		} else {
			if( *(int*) low == int_nil && *(int*) hgh == int_nil){
				/* nothing is matching */
			} else
			if( *(int*) low == int_nil ){
				for( ; first < last; first++, idx++){
					MOSskipit();
					cmp  =  ((*hi && dict[*idx] <= * (int*)hgh ) || (!*hi && dict[*idx] < *(int*)hgh ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else
			if( *(int*) hgh == int_nil ){
				for( ; first < last; first++, idx++){
					MOSskipit();
					cmp  =  ((*li && dict[*idx] >= * (int*)low ) || (!*li && dict[*idx] > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			} else{
				for( ; first < last; first++, idx++){
					MOSskipit();
					cmp  =  ((*hi && dict[*idx] <= * (int*)hgh ) || (!*hi && dict[*idx] < *(int*)hgh )) &&
							((*li && dict[*idx] >= * (int*)low ) || (!*li && dict[*idx] > *(int*)low ));
					if ( !cmp )
						*o++ = (oid) first;
				}
			}
		}
	}
	break;
	default:
		if( task->type == TYPE_date)
			subselect_dictionary(date);
		if( task->type == TYPE_daytime)
			subselect_dictionary(daytime);
		if( task->type == TYPE_timestamp)
		{ 	lng *dict= (lng*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(lng));
			int lownil = timestamp_isnil(*(timestamp*)low);
			int hghnil = timestamp_isnil(*(timestamp*)hgh);

			if( !*anti){
				if( lownil && hghnil){
					for( ; first < last; first++){
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( lownil){
					for( ; first < last; first++, idx++){
						MOSskipit();
						cmp  =  ((*hi && dict[*idx] <= * (lng*)hgh ) || (!*hi && dict[*idx] < *(lng*)hgh ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for( ; first < last; first++, idx++){
						MOSskipit();
						cmp  =  ((*li && dict[*idx] >= * (lng*)low ) || (!*li && dict[*idx] > *(lng*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, idx++){
						MOSskipit();
						cmp  =  ((*hi && dict[*idx] <= * (lng*)hgh ) || (!*hi && dict[*idx] < *(lng*)hgh )) &&
								((*li && dict[*idx] >= * (lng*)low ) || (!*li && dict[*idx] > *(lng*)low ));
						if (cmp )
							*o++ = (oid) first;
					}
				}
			} else {
				if( lownil && hghnil){
					/* nothing is matching */
				} else
				if( lownil){
					for( ; first < last; first++, idx++){
						MOSskipit();
						cmp  =  ((*hi && dict[*idx] <= * (lng*)hgh ) || (!*hi && dict[*idx] < *(lng*)hgh ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else
				if( hghnil){
					for( ; first < last; first++, idx++){
						MOSskipit();
						cmp  =  ((*li && dict[*idx] >= * (lng*)low ) || (!*li && dict[*idx] > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				} else{
					for( ; first < last; first++, idx++){
						MOSskipit();
						cmp  =  ((*hi && dict[*idx] <= * (lng*)hgh ) || (!*hi && dict[*idx] < *(lng*)hgh )) &&
								((*li && dict[*idx] >= * (lng*)low ) || (!*li && dict[*idx] > *(lng*)low ));
						if ( !cmp )
							*o++ = (oid) first;
					}
				}
			}
		}
	}
	MOSskip_dictionary(cntxt,task);
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_dictionary(TPE)\
{ 	TPE low,hgh, *v;\
	low= hgh = TPE##_nil;\
	if ( strcmp(oper,"<") == 0){\
		hgh= *(TPE*) val;\
		hgh = PREVVALUE##TPE(hgh);\
	} else\
	if ( strcmp(oper,"<=") == 0){\
		hgh= *(TPE*) val;\
	} else\
	if ( strcmp(oper,">") == 0){\
		low = *(TPE*) val;\
		low = NEXTVALUE##TPE(low);\
	} else\
	if ( strcmp(oper,">=") == 0){\
		low = *(TPE*) val;\
	} else\
	if ( strcmp(oper,"!=") == 0){\
		hgh= low= *(TPE*) val;\
		anti++;\
	} else\
	if ( strcmp(oper,"==") == 0){\
		hgh= low= *(TPE*) val;\
	} \
	v = (TPE*) (((char*)task->blk) + MosaicBlkSize);\
	for( ; first < last; first++, v++){\
		if( (low == TPE##_nil || * v >= low) && (* v <= hgh || hgh == TPE##_nil) ){\
			if ( !anti) {\
				MOSskipit();\
				*o++ = (oid) first;\
			}\
		} else\
		if( anti){\
			MOSskipit();\
			*o++ = (oid) first;\
		}\
	}\
} 

str
MOSthetasubselect_dictionary(Client cntxt,  MOStask task, void *val, str oper)
{
	oid *o;
	int anti=0;
	BUN first,last;
	(void) cntxt;
	
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	if (task->cl && *task->cl > last){
		MOSskip_dictionary(cntxt,task);
		return MAL_SUCCEED;
	}
	o = task->lb;

	switch(task->type){
	case TYPE_sht: thetasubselect_dictionary(sht); break;
	case TYPE_lng: thetasubselect_dictionary(lng); break;
	case TYPE_oid: thetasubselect_dictionary(oid); break;
	case TYPE_wrd: thetasubselect_dictionary(wrd); break;
	case TYPE_flt: thetasubselect_dictionary(flt); break;
	case TYPE_dbl: thetasubselect_dictionary(dbl); break;
#ifdef HAVE_HGE
	case TYPE_hge: thetasubselect_dictionary(hge); break;
#endif
	case TYPE_int:
		{ 	int low,hgh;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(int));
			low= hgh = int_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *(int*) val;
				hgh = PREVVALUEint(hgh);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(int*) val;
			} else
			if ( strcmp(oper,">") == 0){
				low = *(int*) val;
				low = NEXTVALUEint(low);
			} else
			if ( strcmp(oper,">=") == 0){
				low = *(int*) val;
			} else
			if ( strcmp(oper,"!=") == 0){
				hgh= low= *(int*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(int*) val;
			} 
			for( ; first < last; first++, idx++){
				MOSskipit();
				if( (low == int_nil || dict[*idx] >= low) && (dict[*idx] <= hgh || hgh == int_nil) ){
					if ( !anti) {
						*o++ = (oid) first;
					}
				} else
				if( anti){
					*o++ = (oid) first;
				}
			}
		} 
		break;
	default:
		if( task->type == TYPE_timestamp){
		{ 	lng *dict= (lng*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(lng));
			lng low,hgh;

			low= hgh = int_nil;
			if ( strcmp(oper,"<") == 0){
				hgh= *(lng*) val;
				hgh = PREVVALUElng(hgh);
			} else
			if ( strcmp(oper,"<=") == 0){
				hgh= *(lng*) val;
			} else
			if ( strcmp(oper,">") == 0){
				low = *(lng*) val;
				low = NEXTVALUElng(low);
			} else
			if ( strcmp(oper,">=") == 0){
				low = *(lng*) val;
			} else
			if ( strcmp(oper,"!=") == 0){
				hgh= low= *(lng*) val;
				anti++;
			} else
			if ( strcmp(oper,"==") == 0){
				hgh= low= *(lng*) val;
			} 
			for( ; first < last; first++, idx++){
				MOSskipit();
				if( (low == int_nil || dict[*idx] >= low) && (dict[*idx] <= hgh || hgh == int_nil) ){
					if ( !anti) {
						*o++ = (oid) first;
					}
				} else
				if( anti){
					*o++ = (oid) first;
				}
			}
		} 
		}
	}
	MOSskip_dictionary(cntxt,task);
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_dictionary(TPE)\
{	TPE *v;\
	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize );\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(TPE));\
	v= (TPE*) task->src;\
	for(; first < last; first++, idx++){\
		MOSskipit();\
		*v++ = dict[*idx];\
		task->n--;\
	}\
	task->src = (char*) v;\
}

str
MOSleftfetchjoin_dictionary(Client cntxt,  MOStask task)
{
	BUN first,last;
	(void) cntxt;
	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(task->type){
		case TYPE_sht: leftfetchjoin_dictionary(sht); break;
		case TYPE_lng: leftfetchjoin_dictionary(lng); break;
		case TYPE_oid: leftfetchjoin_dictionary(oid); break;
		case TYPE_wrd: leftfetchjoin_dictionary(wrd); break;
		case TYPE_flt: leftfetchjoin_dictionary(flt); break;
		case TYPE_dbl: leftfetchjoin_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: leftfetchjoin_dictionary(hge); break;
#endif
		case TYPE_int:
		{	int *v;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(int));
			v= (int*) task->src;
			for(; first < last; first++, idx++){
				MOSskipit();
				*v++ = dict[*idx];
				task->n--;
			}
			task->src = (char*) v;
		}
		break;
		default:
			if( task->type == TYPE_timestamp){
				leftfetchjoin_dictionary(timestamp); 
			}
	}
	MOSskip_dictionary(cntxt,task);
	return MAL_SUCCEED;
}

#define join_dictionary(TPE)\
{	TPE  *w;\
	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize );\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(int));\
	for(oo= (oid) first; first < last; first++, idx++, oo++){\
		w = (TPE*) task->src;\
		for(n = task->elm, o = 0; n -- > 0; w++,o++)\
		if ( *w == dict[*idx]){\
			BUNappend(task->lbat, &oo, FALSE);\
			BUNappend(task->rbat, &o, FALSE);\
		}\
	}\
}

str
MOSjoin_dictionary(Client cntxt,  MOStask task)
{
	BUN n,first,last;
	oid o, oo;
	(void) cntxt;

	// set the oid range covered and advance scan range
	first = task->start;
	last = first + MOSgetCnt(task->blk);

	switch(task->type){
		case TYPE_sht: join_dictionary(sht); break;
		case TYPE_lng: join_dictionary(lng); break;
		case TYPE_oid: join_dictionary(oid); break;
		case TYPE_wrd: join_dictionary(wrd); break;
		case TYPE_flt: join_dictionary(flt); break;
		case TYPE_dbl: join_dictionary(dbl); break;
#ifdef HAVE_HGE
		case TYPE_hge: join_dictionary(hge); break;
#endif
		case TYPE_int:
		{	int  *w;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(int));
			for(oo= (oid) first; first < last; first++, idx++, oo++){
				w = (int*) task->src;
				for(n = task->elm, o = 0; n -- > 0; w++,o++)
				if ( *w == dict[*idx]){
					BUNappend(task->lbat, &oo, FALSE);
					BUNappend(task->rbat, &o, FALSE);
				}
			}
		}
		break;
		default:
			if( task->type == TYPE_timestamp)
			{	timestamp  *w;
				timestamp *dict= (timestamp*) (((char*) task->blk) + 2 * MosaicBlkSize );
				bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + task->dictsize * sizeof(timestamp));
				for(oo= (oid) first; first < last; first++, idx++, oo++){
					w = (timestamp*) task->src;
					for(n = task->elm, o = 0; n -- > 0; w++,o++)
					if ( w->days == dict[*idx].days && w->msecs == dict[*idx].msecs){
						BUNappend(task->lbat, &oo, FALSE);
						BUNappend(task->rbat, &o, FALSE);
					}
				}
			}
	}
	MOSskip_dictionary(cntxt,task);
	return MAL_SUCCEED;
}
