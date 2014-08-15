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

#define DICTSIZE 2

/* Beware, the dump routines use the compressed part of the task */
static void
MOSdump_dict(Client cntxt, MOStask task)
{
	MosaicBlk blk= task->blk;
	int i;
	lng *size;
	void *val = (void*)(((char*) blk) + MosaicBlkSize);

	size = (lng*) (((char*)blk) + MosaicBlkSize);
	mnstr_printf(cntxt->fdout,"#rle "LLFMT" ", (lng)(blk->cnt));
	switch(task->type){
	case TYPE_sht:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"sht [%d] %d",i, ((int*) val)[i]); break;
	case TYPE_int:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"int [%d] %d",i, ((int*) val)[i]); break;
	case  TYPE_lng:
		for(i=0; i< *size; i++)
		mnstr_printf(cntxt->fdout,"int [%d] "LLFMT, i, ((lng*) val)[i]); break;
	default:
		if( task->type == TYPE_timestamp){
		}
	}
	mnstr_printf(cntxt->fdout,"\n");
}

static void
MOSadvance_dict(MOStask task)
{
	switch(ATOMstorage(task->type)){
	case TYPE_sht: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + wordaligned(DICTSIZE * sizeof(sht)+ sizeof(bte) * task->blk->cnt)); break;
	case TYPE_int: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + wordaligned(DICTSIZE * sizeof(int)+ sizeof(int) * task->blk->cnt)); break;
	case TYPE_lng: task->blk = (MosaicBlk)( ((char*)task->blk) + 2* MosaicBlkSize + wordaligned(DICTSIZE * sizeof(lng)+ sizeof(lng) * task->blk->cnt)); 
	}
}

static void
MOSskip_dict(MOStask task)
{
	MOSadvance_dict(task);
	if ( task->blk->tag == MOSAIC_EOL)
		task->blk = 0; // ENDOFLIST
}

#define estimateDict(TPE)\
{	TPE val = *(TPE*)task->src;\
	TPE *dict = (TPE*)((char*)task->dst + 2 * MosaicBlkSize);\
	for(i =0; i<task->elm; i++, val++){\
		for(j= 0; j< *size; j++)\
			if( dict[j] == val) {cnt++;break;}\
		if ( j == *size){\
			if ( *size == DICTSIZE)\
				break;\
			dict[j] = val;\
			*size= *size+1;\
			cnt++;\
		}\
	}\
}

// calculate the expected reduction using DICT in terms of elements compressed
static lng
MOSestimate_dict(Client cntxt, MOStask task)
{	BUN i = -1;
	int cnt =0,j;
	lng *size;
	(void) cntxt;

	// use the dst to avoid overwriting noneblocked
	size = (lng*) (((char*)task->dst) + MosaicBlkSize);
	*size = 0;
	switch(ATOMstorage(task->type)){
	case TYPE_sht: estimateDict(sht); break;
	case TYPE_lng: estimateDict(lng); break;
	case TYPE_int:
		{	int val = *(int*)task->src;
			int *dict = (int*)((char*)task->dst + 2 * MosaicBlkSize);
			for(i =0; i<task->elm; i++, val++){
				for(j= 0; j< *size; j++)
					if( dict[j] == val) {cnt++;break;}
				if ( j == *size){
					if ( *size == DICTSIZE)
						break;
					dict[j] = val;
					*size= *size+1;
					cnt++;
				}
			}
		}
	}
	return cnt >DICTSIZE? cnt-DICTSIZE:0;
}

// insert a series of values into the compressor block using rle.
#define DICTcompress(TPE)\
{	TPE *val = (TPE*)task->src;\
	TPE *dict = (TPE*)((char*)task->blk+ 2 * MosaicBlkSize);\
	task->dst = ((char*) dict)+ sizeof(TPE)*DICTSIZE;\
	for(i =0; i<task->elm; i++, val++){\
		for(j= 0; j< *size; j++)\
			if( dict[j] == *val) {\
				blk->cnt++;\
				*task->dst++ = (char) j;\
				break;\
			}\
		if ( j == *size){\
			if ( *size == DICTSIZE)\
				break;\
			dict[j] = *val;\
			*size = *size+1;\
			*task->dst++ = (char) j;\
			blk->cnt++;\
		}\
	}\
	task->src = (char*) val;\
}

static void
MOScompress_dict(Client cntxt, MOStask task)
{
	BUN i ;
	int j;
	lng *size;
	MosaicBlk blk = task->blk;

	(void) cntxt;
	size = (lng*) (((char*)blk) + MosaicBlkSize);
	*size = 0;
	blk->tag = MOSAIC_DICT;
	blk->cnt =  0;
	task->time[MOSAIC_DICT] = GDKusec();

	switch(ATOMstorage(task->type)){
	case TYPE_sht: DICTcompress(sht); break;
	case TYPE_int: DICTcompress(int); break;
	case TYPE_lng:
		{	lng *val = (lng*)task->src;
			lng *dict = (lng*)((char*)task->blk+ 2 * MosaicBlkSize);
			task->dst = ((char*) dict)+ sizeof(lng)*DICTSIZE;
			for(i =0; i<task->elm; i++, val++){
				for(j= 0; j< *size; j++)
					if( dict[j] == *val) {
						blk->cnt++;
						*task->dst++ = (char) j;
						break;
					}
				if ( j == *size){
					if ( *size == DICTSIZE){
						// align on word boundary
						task->dst += wordaligned(blk->cnt %2);
						break;
					}
					dict[j] = *val;
					*size = *size+1;
					*task->dst++ = (char) j;
					blk->cnt++;
				}
			}
			task->src = (char*) val;
		}
		break;
	default:
		assert(0);
	}
#ifdef _DEBUG_MOSAIC_
	MOSdump_dict(cntxt, task);
#endif
	task->time[MOSAIC_DICT] = GDKusec() - task->time[MOSAIC_DICT];
}

// the inverse operator, extend the src
#define DICTdecompress(TPE)\
{	bte *idx = (bte*)(compressed + DICTSIZE * sizeof(TPE));\
	TPE *dict = (TPE*) compressed;\
	for(i = 0; i < (BUN) blk->cnt; i++,idx++)\
		((TPE*)task->src)[i] = dict[ (bte)*idx];\
	task->src += i * sizeof(TPE);\
}

static void
MOSdecompress_dict(Client cntxt, MOStask task)
{
	MosaicBlk blk =  ((MosaicBlk) task->blk);
	BUN i;
	lng clk = GDKusec();
	char *compressed;
	(void) cntxt;

	compressed = (char*) blk + 2 * MosaicBlkSize;
	switch(ATOMstorage(task->type)){
	case TYPE_sht: DICTdecompress(sht); break;
	case TYPE_lng: DICTdecompress(lng); break;
	case TYPE_int:
		{	bte *idx = (bte*)(compressed + DICTSIZE * sizeof(int));
			int *dict = (int*) compressed;
			for(i = 0; i < (BUN) blk->cnt; i++,idx++)
				((int*)task->src)[i] = dict[ (bte)*idx];
			task->src += i * sizeof(int);
		}
		break;
	default:
		assert(0);
	}
	task->time[MOSAIC_DICT] = GDKusec() - clk;
}

// perform relational algebra operators over non-compressed chunks
// They are bound by an oid range and possibly a candidate list

#define subselect_dict(TPE) {\
 	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize );\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + DICTSIZE * sizeof(TPE));\
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

static str
MOSsubselect_dict(Client cntxt,  MOStask task, lng first, lng last, void *low, void *hgh, bit *li, bit *hi, bit *anti)
{
	oid *o;
	int cmp;
	(void) cntxt;
	(void) low;
	(void) hgh;
	(void) li;
	(void) hi;
	(void) anti;

	if ( first + task->blk->cnt > last)
		last = task->blk->cnt;
	o = task->lb;

	switch(task->type){
	case TYPE_sht: subselect_dict(sht); break;
	case TYPE_lng: subselect_dict(lng); break;
	case TYPE_int:
	// Expanded MOSselect_dict for debugging
	{ 	int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
		bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + DICTSIZE * sizeof(int));

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
		if( task->type == TYPE_timestamp){
			assert(0);
			//MOSselect_dict(timestamp);
		}
	}
	task->lb = o;
	return MAL_SUCCEED;
}

#define thetasubselect_dict(TPE)\
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

static str
MOSthetasubselect_dict(Client cntxt,  MOStask task, BUN first, BUN last, void *val, str oper)
{
	oid *o;
	int anti=0;
	(void) cntxt;
	
	if ( first + task->blk->cnt > last)
		last = task->blk->cnt;
	o = task->lb;

	switch(ATOMstorage(task->type)){
	case TYPE_sht: thetasubselect_dict(sht); break;
	case TYPE_lng: thetasubselect_dict(lng); break;
	case TYPE_int:
		{ 	int low,hgh;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + DICTSIZE * sizeof(int));
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
				if( (low == int_nil || dict[*idx] >= low) && (dict[*idx] <= hgh || hgh == int_nil) ){
					if ( !anti) {
						MOSskipit();
						*o++ = (oid) first;
					}
				} else
				if( anti){
					MOSskipit();
					*o++ = (oid) first;
				}
			}
		} 
		break;
	}
	task->lb =o;
	return MAL_SUCCEED;
}

#define leftfetchjoin_dict(TPE)\
{	TPE *v;\
	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize );\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + DICTSIZE * sizeof(TPE));\
	v= (TPE*) task->src;\
	for(; first < last; first++, idx++){\
		MOSskipit();\
		*v++ = dict[*idx];\
		task->n--;\
	}\
	task->src = (char*) v;\
}

static str
MOSleftfetchjoin_dict(Client cntxt,  MOStask task, BUN first, BUN last)
{
	(void) cntxt;

	switch(ATOMstorage(task->type)){
		case TYPE_sht: leftfetchjoin_dict(sht); break;
		case TYPE_lng: leftfetchjoin_dict(lng); break;
		case TYPE_int:
		{	int *v;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + DICTSIZE * sizeof(int));
			v= (int*) task->src;
			for(; first < last; first++, idx++){
				MOSskipit();
				*v++ = dict[*idx];
				task->n--;
			}
			task->src = (char*) v;
		}
	}
	return MAL_SUCCEED;
}

#define join_dict(TPE)\
{	TPE  *w;\
	TPE *dict= (TPE*) (((char*) task->blk) + 2 * MosaicBlkSize );\
	bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + DICTSIZE * sizeof(int));\
	for(oo= (oid) first; first < last; first++, idx++, oo++){\
		w = (TPE*) task->src;\
		for(n = task->elm, o = 0; n -- > 0; w++,o++)\
		if ( *w == dict[*idx]){\
			BUNappend(task->lbat, &oo, FALSE);\
			BUNappend(task->rbat, &o, FALSE);\
		}\
	}\
}

static str
MOSjoin_dict(Client cntxt,  MOStask task, BUN first, BUN last)
{
	BUN n;
	oid o, oo;
	(void) cntxt;

	switch(ATOMstorage(task->type)){
		case TYPE_sht: join_dict(sht); break;
		case TYPE_lng: join_dict(lng); break;
		case TYPE_int:
		{	int  *w;
			int *dict= (int*) (((char*) task->blk) + 2 * MosaicBlkSize );
			bte *idx = (bte*) (((char*) task->blk) + 2 * MosaicBlkSize + DICTSIZE * sizeof(int));
			for(oo= (oid) first; first < last; first++, idx++, oo++){
				w = (int*) task->src;
				for(n = task->elm, o = 0; n -- > 0; w++,o++)
				if ( *w == dict[*idx]){
					BUNappend(task->lbat, &oo, FALSE);
					BUNappend(task->rbat, &o, FALSE);
				}
			}
		}
	}
	return MAL_SUCCEED;
}
