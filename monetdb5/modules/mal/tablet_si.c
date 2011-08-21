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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f tablet_si
 * @a Niels Nes, Martin Kersten, Stratos Idreos
 *
 * @- Parallel bulk load for SQL
 * The COPY INTO command for SQL is heavily CPU bound, which means
 * that ideally we would like to exploit the multi-cores to do that
 * work in parallel.
 * Complicating factors are the initial record offset, the
 * possible variable length of the input, and the original sort order
 * that should preferrable be maintained.
 *
 * The code below consists of a file reader, which breaks up the
 * file into distinct lines/fields. Then multiple parallel threads
 * can grab them, convert the value and update the underlying tables.
 *
 * The threads get a reference to a private copy of the READERtask.
 * It includes a list of columns they should handle. This is a basis
 * to distributed cheap and expensive columns over threads.
 *
 * A double buffering scheme might improve CPU and IO concurrent work.
 * Readers and writers now overlap.
 * Also the buffer size of the block stream might be a little small for
 * this task (1MB). It has been increased to 8MB, which indeed improved.
 *
 * The work divider allocates subtasks to threads based on the
 * observed time spending so far.
 */
#include "monetdb_config.h"
#include "tablet.h"
#include "algebra.h"
#include "histogram.h"

#include <string.h>
#include <ctype.h>
#include <gdk_posix.h>

#define _SLICE_TABLET_
/*#define _DEBUG_SLICER_*/
/*#define _DEBUG_BATS_*/
#define _SLICER_ 2

#ifdef _SLICE_TABLET_
bit firstLoad=TRUE;
oid keys;
#endif

typedef struct{
	int next;
	int limit;
	lng *time, wtime;	/* time per col + time per thread */
	int rounds; 	/* how often did we divide the work */
	MT_Id tid;
	MT_Sema sema;	/* threads wait for work , negative next implies exit*/
	MT_Sema reply;	/* let reader continue */
	Tablet *as;
	char *errbuf;
	char *separator;
	size_t seplen;
	char quote;
	int *cols;	/* columns to handle */
	char ***fields;
} READERtask;

/*
 * @-
 * The parsing of the individual values is straightforward. If the value represents
 * the null-replacement string then we grab the underlying nil.
 * If the string starts with the quote identified from SQL, we locate the tail
 * and interpret the body.
 */
#ifdef _SLICE_TABLET_
static inline ptr*
get_val(Column * fmt, char *s, char quote, str *err, int c)
{
	char buf[BUFSIZ];
	char *e, *t;
	ptr *adt;;

	/* include testing on the terminating null byte !! */
	if (fmt->nullstr && strncasecmp(s, fmt->nullstr, fmt->null_length+1) == 0){
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout,"nil value '%s' (%d) found in :%s\n",fmt->nullstr,fmt->nillen,(s?s:""));
#endif
		adt = fmt->nildata;
		fmt->c[0]->T->nonil = 0;
	} else if ( quote && *s == quote ) {
		s++;	/* find the last quote */
		for ( t = e = s; *t ; t++)
			if ( *t == quote) e = t;
		*e = 0;
		adt = fmt->frstr(fmt, fmt->adt, s, e, 0);
	} else {
		for( e=s; *e; e++)
			;
		adt = fmt->frstr(fmt, fmt->adt, s, e, 0);
	}

	if (!adt) {
		char *val;
		val = *s ? GDKstrdup(s) : GDKstrdup("");
		if ( *err == NULL){
			snprintf(buf,BUFSIZ, "value '%s' from line " BUNFMT
				" field %d not inserted, expecting type %s\n", val, BATcount(fmt->c[0])+1, c+1, fmt->type);
			*err= GDKstrdup(buf);
		}
		GDKfree(val);
		/* replace it with a nil */
		adt = fmt->nildata;
		fmt->c[0]->T->nonil = 0;
	}
	/* key maybe NULL but thats not a problem, as long as we have void */
	if (fmt->raw){
		mnstr_write(fmt->raw,adt,ATOMsize(fmt->adt),1);
	}

	return adt;
}

void
replaceVal(BAT * b, oid position, ValPtr val)
{
	switch( val->vtype){
		case TYPE_bte: *(bte*)Tloc(b,BUNfirst(b)+position) = val->val.btval; break;
                case TYPE_sht: *(sht*)Tloc(b,BUNfirst(b)+position) = val->val.shval; break;
                case TYPE_int: *(int*)Tloc(b,BUNfirst(b)+position) = val->val.ival;  break;
                case TYPE_lng: *(lng*)Tloc(b,BUNfirst(b)+position) = val->val.lval;  break;
                case TYPE_dbl: *(dbl*)Tloc(b,BUNfirst(b)+position) = val->val.dval;  break;
                case TYPE_flt: *(flt*)Tloc(b,BUNfirst(b)+position) = val->val.fval;  break;
	}
}

void
replaceWithNull(BAT * b, oid position)
{
	switch( b->ttype){
		case TYPE_bte: *(bte*)Tloc(b,BUNfirst(b)+position) =  *(bte*)ATOMnilptr(b->ttype); break;
                case TYPE_sht: *(sht*)Tloc(b,BUNfirst(b)+position) =  *(sht*)ATOMnilptr(b->ttype); break;
                case TYPE_int: *(int*)Tloc(b,BUNfirst(b)+position) =  *(int*)ATOMnilptr(b->ttype); break;
                case TYPE_lng: *(lng*)Tloc(b,BUNfirst(b)+position) =  *(lng*)ATOMnilptr(b->ttype); break;
                case TYPE_dbl: *(dbl*)Tloc(b,BUNfirst(b)+position) =  *(dbl*)ATOMnilptr(b->ttype); break;
                case TYPE_flt: *(flt*)Tloc(b,BUNfirst(b)+position) =  *(flt*)ATOMnilptr(b->ttype); break;
	}
}

void
suffleVal(BAT *b1, oid pos1, BAT *b2, oid pos2)
{
	switch( b1->ttype){
		case TYPE_bte:{
			bte temp = *(bte*)Tloc(b1,BUNfirst(b1)+pos1);
			*(bte*)Tloc(b1,BUNfirst(b1)+pos1) = *(bte*)Tloc(b2,BUNfirst(b2)+pos2);
			*(bte*)Tloc(b2,BUNfirst(b2)+pos2) = temp;
			break;}
                case TYPE_sht:{
			sht temp = *(sht*)Tloc(b1,BUNfirst(b1)+pos1);
			*(sht*)Tloc(b1,BUNfirst(b1)+pos1) = *(sht*)Tloc(b2,BUNfirst(b2)+pos2);
			*(sht*)Tloc(b2,BUNfirst(b2)+pos2) = temp;
			break;}
                case TYPE_int:{
			int temp = *(int*)Tloc(b1,BUNfirst(b1)+pos1);
			*(int*)Tloc(b1,BUNfirst(b1)+pos1) = *(int*)Tloc(b2,BUNfirst(b2)+pos2);
			*(int*)Tloc(b2,BUNfirst(b2)+pos2) = temp;
			break;}
                case TYPE_lng:{
			lng temp = *(lng*)Tloc(b1,BUNfirst(b1)+pos1);
			*(lng*)Tloc(b1,BUNfirst(b1)+pos1) = *(lng*)Tloc(b2,BUNfirst(b2)+pos2);
			*(lng*)Tloc(b2,BUNfirst(b2)+pos2) = temp;
			break;}
                case TYPE_dbl:{
			dbl temp = *(dbl*)Tloc(b1,BUNfirst(b1)+pos1);
			*(dbl*)Tloc(b1,BUNfirst(b1)+pos1) = *(dbl*)Tloc(b2,BUNfirst(b2)+pos2);
			*(dbl*)Tloc(b2,BUNfirst(b2)+pos2) = temp;
			break;}
                case TYPE_flt:{
			flt temp = *(flt*)Tloc(b1,BUNfirst(b1)+pos1);
			*(flt*)Tloc(b1,BUNfirst(b1)+pos1) = *(flt*)Tloc(b2,BUNfirst(b2)+pos2);
			*(flt*)Tloc(b2,BUNfirst(b2)+pos2) = temp;
			break;}
	}
}

static int
Slice(READERtask *task, Column * fmt, ptr key, str *err, int col)
{
	/*naive : fill big, fill small, then rearrange with every new value */
	char buf[BUFSIZ];
	BAT *BigSlice = NULL; 	/*toy BAT to play with the algos without worying for SQL for now*/
	Histogram h1 = NULL;
	Histogram h2 = NULL;
	int i, optimistic=0;
	ptr *adt;
	BATiter bsi;
	BAT *sample, *sortedSample;
	ValPtr vmin, vmax;
	int sampleSize;
	dbl distance, curDistance, tempDistance;

	/* Tunable params */
	int optimisticStep=0;
	int bins=10;
	dbl acceptableDistance = (dbl)bins;
	oid SmallSliceSize  = task->next/5; /* Use a 20-80 percent for now */
	bit balance = FALSE; /*Try to ballance in the end if needed */

	/*sample to get min-max*/
	sampleSize=SmallSliceSize>100?100:SmallSliceSize; /*sample up to 100 values*/
	sample = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,sampleSize);
	for ( i = 0; i< sampleSize ; i++){
		adt=get_val(fmt, task->fields[col][i], task->quote, err, col);
		bunfastins(sample, key, adt);
	}
	sample->tsorted = FALSE;
	sortedSample = BATtsort(sample);
	bsi = bat_iterator(sortedSample);
	vmin= (ValPtr) GDKzalloc(sizeof(ValRecord));
	vmax= (ValPtr) GDKzalloc(sizeof(ValRecord));
	VALset(vmin, fmt->c[0]->ttype, BUNtail(bsi,0));
	VALset(vmax, fmt->c[0]->ttype, BUNtail(bsi,sampleSize-1));

	#ifdef _DEBUG_SLICER_
		printf("min %d \n", vmin->val.ival);
		printf("max %d \n", vmax->val.ival);
 	#endif

	for ( i = 0; i< task->next ; i++){
		ValPtr newValue = (ValPtr) GDKzalloc(sizeof(ValRecord));
		adt=get_val(fmt, task->fields[col][i], task->quote, err, col);
		VALset(newValue, fmt->c[0]->ttype, adt);

		/*fill small first*/
		if (BATcount(fmt->c[0]) < SmallSliceSize){
			if (h1 == NULL)
				h1 = HSTnew(bins, vmin, vmax);
			bunfastins(fmt->c[0], key, adt);
			HSTincrement(h1, newValue);
			continue;
		}

		/*equally fill big*/
		if (BigSlice == NULL)
			BigSlice = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,2*SmallSliceSize);

		if (BATcount(BigSlice) < SmallSliceSize){
			if (h2 == NULL)
				h2 = HSTnew(bins, vmin, vmax);
			bunfastins(BigSlice, key, adt);
			HSTincrement(h2, newValue);
			continue;
		}

		/*be optimistic; throw a few tuples in the big slice*/
		if (optimistic > 0){
			#ifdef _DEBUG_SLICER_
				printf("be optimistic\n");
			#endif
			bunfastins(BigSlice, key, adt);
			HSTincrement(h2, newValue);
			optimistic--;
			continue;
		}

		#ifdef _DEBUG_SLICER_
			printf("ready to insert new value %d \n",newValue->val.ival);
			BATprint(fmt->c[0]);
			HSTprintf(h1);
			BATprint(BigSlice);
			HSTprintf(h2);
		#endif

		/*choose target BAT in order to balance the histogram distance*/
		distance = HSTbinrelativeWhatIf(h1,h2,newValue,&curDistance);
		if (distance==0){
			bunfastins(BigSlice, key, adt);
			HSTincrement(h2, newValue);
		}
		else if (distance <= HSTbinrelativeWhatIf(h2,h1,newValue,&tempDistance)){
			bunfastins(BigSlice, key, adt);
			HSTincrement(h2, newValue);
		}else{
			int j;
			bit moved = FALSE;
			BATiter bi;
			int sizeSmall = BATcount(fmt->c[0]);
			ValPtr curVal;

			HSTincrement(h1, newValue);

			/* pick a tuple to move to the big one */
			bi = bat_iterator(fmt->c[0]);
			curVal= (ValPtr) GDKzalloc(sizeof(ValRecord));
			for (j=0; j<sizeSmall; j++){
				VALset(curVal, fmt->c[0]->ttype, BUNtail(bi,j));
				distance = HSTbinrelativeWhatIfMove(h1,h2,curVal,&curDistance);
				if (distance==0){
					#ifdef _DEBUG_SLICER_
						printf("replace %d\n", j);
					#endif
					replaceVal(fmt->c[0], j, newValue);
					HSTdecrement(h1,curVal);
					bunfastins(BigSlice, key,&curVal->val.ival);
					HSTincrement(h2,curVal);
					moved = TRUE;
					break;
				}
			}
			/*could not find a tuple to move*/
			if (!moved){

				#ifdef _DEBUG_SLICER_
					printf(" could not find a tuple to move: add to big\n");
				#endif

				bunfastins(BigSlice, key, adt);
				HSTincrement(h2, newValue);
				HSTdecrement(h1, newValue);
			}

		}

		if(curDistance < acceptableDistance && (oid)(task->next-i) > SmallSliceSize)
			optimistic = optimisticStep;
	}
	printf("prebalance result state \n");
	#ifdef _DEBUG_SLICER_
		BATprint(fmt->c[0]);
		BATprint(BigSlice);
	#endif
	HSTprintf(h1);
	HSTprintf(h2);

	if (!balance) return 0;

	if ((curDistance=HSTbinrelative(h1,h2)) > acceptableDistance){
		BATiter bi, bb;
		int j, k=0, sizeSmall = BATcount(fmt->c[0]), sizeBig = BATcount(BigSlice);
		ValPtr curVal, newValue;
		bit moved = TRUE;

		/* pick a tuple to move to the big one */
		bi = bat_iterator(fmt->c[0]);
		bb = bat_iterator(BigSlice);
		curVal= (ValPtr) GDKzalloc(sizeof(ValRecord));
		newValue= (ValPtr) GDKzalloc(sizeof(ValRecord));
		while(curDistance > acceptableDistance && moved==TRUE){
			for (j=0; j<sizeSmall; j++){
				VALset(curVal, fmt->c[0]->ttype, BUNtail(bi,j));
				distance = HSTbinrelativeWhatIfMove(h1,h2,curVal,&curDistance);
				if (distance==0){
					HSTdecrement(h1,curVal);
					HSTincrement(h2,curVal);
					moved = FALSE;
					for (k=0; k<sizeBig; k++){
						VALset(newValue, fmt->c[0]->ttype, BUNtail(bb,k));
						distance = HSTbinrelativeWhatIfMove(h2,h1,newValue,&curDistance);
						if (distance==0){
							suffleVal(fmt->c[0], j, BigSlice, k);
							HSTdecrement(h2,newValue);
							HSTincrement(h1,newValue);
							moved = TRUE;
							break;
						}
					}
					if (moved)
						break;
					else{
						HSTdecrement(h2,curVal);
						HSTincrement(h1,curVal);
					}
				}
			}
		}

	}

	printf("result state \n");
	#ifdef _DEBUG_SLICER_
		BATprint(fmt->c[0]);
		BATprint(BigSlice);
	#endif
	HSTprintf(h1);
	HSTprintf(h2);

	return 0;

bunins_failed:
	if (*err == NULL) {
		snprintf(buf,BUFSIZ, "parsing error from line " BUNFMT " field %d not inserted\n", BATcount(fmt->c[0])+1, col+1);
		*err= GDKstrdup(buf);
	}
	return -1;
}

static int
Slice2(READERtask *task, Column * fmt, ptr key, str *err, int col)
{
	/*naive : fill big, fill small, then rearrange with every new value */
	char buf[BUFSIZ];
	int i, j,optimistic=0;
	ptr *adt;
	BATiter bsi;
	BAT *sample, *sortedSample;
	ValPtr vmin, vmax;
	int sampleSize;
	dbl distance, curDistance, tempDistance;
	oid curSmallSize=0;
	ValPtr newValue = (ValPtr) GDKzalloc(sizeof(ValRecord));
	ValPtr curVal= (ValPtr) GDKzalloc(sizeof(ValRecord));

	/* Tunable params */
	int optimisticStep=500000;
	int bins=10;
	dbl acceptableDistance = 0.1;
	oid SmallSliceSize  = task->next/5; /* Use a 20-80 percent for now */
	bit balance = FALSE; /*Try to ballance in the end if needed */

	(void) j;

	if (firstLoad){
		fmt->c[1] = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,4*SmallSliceSize);

		for(i=0; i<bins; i++)
			fmt->bin[i] = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,SmallSliceSize);

		/*sample to get min-max*/
		sampleSize=SmallSliceSize>100?100:SmallSliceSize; /*sample up to 100 values*/
		sample = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,sampleSize);
		for ( i = 0; i< sampleSize ; i++){
			adt=get_val(fmt, task->fields[col][i], task->quote, err, col);
			bunfastins(sample, key, adt);
		}
		sample->tsorted = FALSE;
		sortedSample = BATtsort(sample);
		bsi = bat_iterator(sortedSample);
		vmin= (ValPtr) GDKzalloc(sizeof(ValRecord));
		vmax= (ValPtr) GDKzalloc(sizeof(ValRecord));
		VALset(vmin, fmt->c[0]->ttype, BUNtail(bsi,0));
		VALset(vmax, fmt->c[0]->ttype, BUNtail(bsi,sampleSize-1));

		#ifdef _DEBUG_SLICER_
			printf("min %d \n", vmin->val.ival);
			printf("max %d \n", vmax->val.ival);
		#endif

		for(i=0; i<2; i++)
			fmt->histogram[i] = HSTnew(bins, vmin, vmax);

		firstLoad=FALSE;
	}

	for ( i = 0; i< task->next ; i++){
		adt=get_val(fmt, task->fields[col][i], task->quote, err, col);
		VALset(newValue, fmt->c[0]->ttype, adt);

		/*fill small first*/
		if (curSmallSize < SmallSliceSize){
			int index=0;
			index = HSTincrement(fmt->histogram[0], newValue);
			bunfastins(fmt->bin[index], key, adt);
			curSmallSize++;
			continue;
		}

		/*equally fill big*/
		if (BATcount(fmt->c[1]) < SmallSliceSize){
			bunfastins(fmt->c[1], key, adt);
			HSTincrement(fmt->histogram[1], newValue);
			continue;
		}

		/*be optimistic; throw a few tuples in the big slice*/
		if (optimistic > 0){
			#ifdef _DEBUG_SLICER_
				printf("be optimistic\n");
			#endif
			bunfastins(fmt->c[1], key, adt);
			HSTincrement(fmt->histogram[1], newValue);
			optimistic--;
			continue;
		}

		#ifdef _DEBUG_BATS_
			printf("ready to insert new value %d \n",newValue->val.ival);
			for (j=0;j<bins;j++){
				printf("bin %d\n",j);
				BATprint(fmt->bin[j]);
			}
			BATprint(fmt->c[1]);
			HSTprintf(fmt->histogram[0]);
			HSTprintf(fmt->histogram[1]);
		#endif

		/*choose target BAT in order to balance the histogram distance*/
		distance = HSTeuclidianNormWhatIf(fmt->histogram[0],fmt->histogram[1],newValue,&curDistance);
		if (distance==0){
			bunfastins(fmt->c[1], key, adt);
			HSTincrement(fmt->histogram[1], newValue);
		}
		else if (distance <= HSTeuclidianNormWhatIf(fmt->histogram[1],fmt->histogram[0],newValue,&tempDistance)){
			bunfastins(fmt->c[1], key, adt);
			HSTincrement(fmt->histogram[1], newValue);
		}else{
			int smallindex, moveindex;
			BATiter bi;

			smallindex = HSTincrement(fmt->histogram[0], newValue);

			moveindex = HSTwhichbinNorm(fmt->histogram[0],fmt->histogram[1]);

			if (moveindex == smallindex || moveindex==-1){
				#ifdef _DEBUG_SLICER_
					printf(" could not find a tuple to move: add to big\n");
				#endif

				bunfastins(fmt->c[1], key, adt);
				HSTincrement(fmt->histogram[1], newValue);
				HSTdecrement(fmt->histogram[0], newValue);
			}else{
				bi = bat_iterator(fmt->bin[moveindex]);
				VALset(curVal, fmt->c[0]->ttype, BUNtail(bi,BATcount(fmt->bin[moveindex])-1));
				#ifdef _DEBUG_SLICER_
					printf("move first tuple from bin %d, new count %d\n", moveindex,(int)BATcount(fmt->bin[moveindex]));
				#endif
				BATsetcount(fmt->bin[moveindex], BUNlast(fmt->bin[moveindex])-1);
				bunfastins(fmt->bin[smallindex], key,&newValue->val.ival);
				HSTdecrement(fmt->histogram[0],curVal);
				bunfastins(fmt->c[1], key,&curVal->val.ival);
				HSTincrement(fmt->histogram[1],curVal);
			}
		}

		if(optimisticStep >0 && curDistance < acceptableDistance && (oid)(task->next-i) > SmallSliceSize)
			optimistic = optimisticStep;
	}
	printf("prebalance result state \n");
	#ifdef _DEBUG_BATS_
		for (j=0;j<bins;j++){
			printf("bin %d\n",j);
			BATprint(fmt->bin[j]);
		}
		BATprint(fmt->c[1]);
	#endif
	printf("Distance: %f \n",HSTeuclidianNorm(fmt->histogram[0],fmt->histogram[1]));
	HSTprintf(fmt->histogram[0]);
	HSTprintf(fmt->histogram[1]);

	if (!balance){
		/*
		for(j=0;j<bins;j++)
			BATappend(fmt->c[0],fmt->bin[j],TRUE);
		printf(" %d  %d \n",(int)BATcount(fmt->c[0]),(int)BATcount(fmt->c[1]));
		*/
		return 0;
	}

	if ((curDistance=HSTeuclidianNorm(fmt->histogram[0],fmt->histogram[1])) > acceptableDistance){
		BATiter bi, bb;
		int k=0, moveindex, sizeBig = BATcount(fmt->c[1]);
		ValPtr curVal, newValue;
		bit moved = TRUE;
		dbl oldDistance = curDistance+1;

		/* pick a tuple to move to the big one */
		bb = bat_iterator(fmt->c[1]);
		curVal= (ValPtr) GDKzalloc(sizeof(ValRecord));
		newValue= (ValPtr) GDKzalloc(sizeof(ValRecord));
		while(curDistance > acceptableDistance && moved==TRUE && oldDistance>curDistance){
			oldDistance=curDistance;
			moved = FALSE;
			if ((moveindex = HSTwhichbinNorm(fmt->histogram[0],fmt->histogram[1]))==-1)
				break;
			bi = bat_iterator(fmt->bin[moveindex]);
			VALset(curVal, fmt->c[0]->ttype, BUNtail(bi,BATcount(fmt->bin[moveindex])-1));
			HSTdecrement(fmt->histogram[0],curVal);
			HSTincrement(fmt->histogram[1],curVal);
			for (k=0; k<sizeBig; k++){
				VALset(newValue, fmt->c[0]->ttype, BUNtail(bb,k));
				distance = HSTeuclidianNormWhatIfMove(fmt->histogram[1],fmt->histogram[0],newValue,&curDistance);
				if (distance==0){
					BATsetcount(fmt->bin[moveindex], BUNlast(fmt->bin[moveindex])-1);
					bunfastins(fmt->bin[HSTgetIndex(fmt->histogram[0],newValue)], key,&newValue->val.ival);
					bunfastins(fmt->c[1], key,&curVal->val.ival);
					HSTdecrement(fmt->histogram[1],newValue);
					HSTincrement(fmt->histogram[0],newValue);
					moved = TRUE;
					break;
				}
			}
			if(!moved){
				HSTdecrement(fmt->histogram[1],curVal);
				HSTincrement(fmt->histogram[0],curVal);
				break;
			}
		}
	}

	/*
	for(j=0;j<bins;j++)
		BATappend(fmt->c[0],fmt->bin[j],TRUE);
	*/

	printf("\nresult state \n");
	#ifdef _DEBUG_BATS_
		for (j=0;j<bins;j++){
			printf("bin %d\n",j);
			BATprint(fmt->bin[j]);
		}
		BATprint(fmt->c[1]);
	#endif
	printf("Distance: %f \n",HSTeuclidianNorm(fmt->histogram[0],fmt->histogram[1]));
	HSTprintf(fmt->histogram[0]);
	HSTprintf(fmt->histogram[1]);

	return 0;

bunins_failed:
	if (*err == NULL) {
		snprintf(buf,BUFSIZ, "parsing error from line " BUNFMT " field %d not inserted\n", BATcount(fmt->c[0])+1, col+1);
		*err= GDKstrdup(buf);
	}
	return -1;
}

static int
Slice3(READERtask *task)
{
	char buf[BUFSIZ];
	int i, j, optimistic=0;
	BATiter bsi;
	BAT *sample, *sortedSample;
	ValPtr vmin, vmax;
	int sampleSize;
	dbl distance, distance2,curDistance, tempDistance, totalDistance;
	oid curSmallSize=0,curBigSize=0;
	str *err=0;
	ptr key=NULL;
	int a, attributes =task->as->nr_attrs;
	ValPtr newValue[attributes];
	ptr *adt[attributes];
	ValPtr curVal=(ValPtr) GDKzalloc(sizeof(ValRecord));
	bit needToMove;

	/* Tunable params */
	int optimisticStep=0;
	int bins=10;
	dbl acceptableDistance = 0.1;
	oid SmallSliceSize  = task->next/5; /* Use a 20-80 percent for now */
	/*bit balance = FALSE; Try to ballance in the end if needed */

	(void) j;

	for(a=0;a<attributes;a++)
		 newValue[a]=(ValPtr) GDKzalloc(sizeof(ValRecord));

	if (firstLoad){
		for(a=0;a<attributes;a++){
			Column *fmt;
			int col;
			if (!(task->cols[a])) continue;

			col = task->cols[a]-1;
			fmt = &task->as->format[col];
			fmt->c[1] = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,4*SmallSliceSize);

			for(i=0; i<bins; i++)
				fmt->bin[i] = BATnew(TYPE_oid, fmt->c[0]->ttype,SmallSliceSize);

			/*sample to get min-max*/
			sampleSize=SmallSliceSize>100?100:SmallSliceSize; /*sample up to 100 values*/
			sample = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,sampleSize);
			for ( i = 0; i< sampleSize ; i++){
				adt[0]=get_val(fmt, task->fields[col][i], task->quote, err, col);
				bunfastins(sample, key, adt[0]);
			}

			sample->tsorted = FALSE;
			sortedSample = BATtsort(sample);
			bsi = bat_iterator(sortedSample);
			vmin= (ValPtr) GDKzalloc(sizeof(ValRecord));
			vmax= (ValPtr) GDKzalloc(sizeof(ValRecord));
			VALset(vmin, fmt->c[0]->ttype, BUNtail(bsi,0));
			VALset(vmax, fmt->c[0]->ttype, BUNtail(bsi,sampleSize-1));

			#ifdef _DEBUG_SLICER_
				printf("min %d \n", vmin->val.ival);
				printf("max %d \n", vmax->val.ival);
			#endif
			for(i=0; i<2; i++)
				fmt->histogram[i] = HSTnew(bins, vmin, vmax);
		}
		keys=0;
		firstLoad=FALSE;
	}




	for ( i = 0; i< task->next ; i++){
		keys++;
		printf("%d\n",i);
		needToMove=FALSE;
		for(a=0;a<attributes;a++){
			Column *fmt;
			int col;
			if (!(task->cols[a])) continue;
			col = task->cols[a]-1;
                        fmt = &task->as->format[task->cols[a]-1];

			adt[a]=get_val(fmt, task->fields[col][i], task->quote, err, col);
			VALset(newValue[a], fmt->c[0]->ttype, adt[a]);
		}


		/*fill small first*/
		if (curSmallSize < SmallSliceSize){
			for(a=0;a<attributes;a++){
				int index;
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];

				index = HSTincrement(fmt->histogram[0], newValue[a]);
				bunfastins(fmt->bin[index], &keys, adt[a]);
			}
			curSmallSize++;
			continue;
		}

		/*equally fill big*/
		if (curBigSize < SmallSliceSize){
			for(a=0;a<attributes;a++){
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];
				bunfastins(fmt->c[1], key, adt[a]);
				HSTincrement(fmt->histogram[1], newValue[a]);
			}
			curBigSize++;
			continue;
		}

		/*be optimistic; throw a few tuples in the big slice*/
		if (optimistic > 0){
			#ifdef _DEBUG_SLICER_
				printf("be optimistic\n");
			#endif
			for(a=0;a<attributes;a++){
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];
				bunfastins(fmt->c[1], key, adt[a]);
				HSTincrement(fmt->histogram[1], newValue[a]);
			}
			optimistic--;
			continue;
		}

		#ifdef _DEBUG_SLICER_
			#ifdef _DEBUG_BATS_
			for(a=0;a<attributes;a++){
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];

				printf("Attr: %d :ready to insert new value %d \n",a,newValue[a]->val.ival);
				for (j=0;j<bins;j++){
					printf("bin %d\n",j);
					BATprint(fmt->bin[j]);
				}
				BATprint(fmt->c[1]);
				HSTprintf(fmt->histogram[0]);
				HSTprintf(fmt->histogram[1]);
			}
			#endif
		#endif

		/*choose target BAT in order to balance the histogram distance*/
		distance=totalDistance=0;
		for(a=0;a<attributes;a++){
			Column *fmt;
			if (!(task->cols[a])) continue;
			fmt = &task->as->format[task->cols[a]-1];

			distance += HSTeuclidianNormWhatIf(fmt->histogram[0],fmt->histogram[1],newValue[a],&curDistance);
			totalDistance+=curDistance;
		}

		if (distance==0){
			#ifdef _DEBUG_SLICER_
				printf("put into big\n");
			#endif
			for(a=0;a<attributes;a++){
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];

				bunfastins(fmt->c[1], key, adt[a]);
				HSTincrement(fmt->histogram[1], newValue[a]);
			}
		}
		else{
			distance2=0;
			for(a=0;a<attributes;a++){
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];

				distance2 += HSTeuclidianNormWhatIf(fmt->histogram[1],fmt->histogram[0],newValue[a],&tempDistance);
			}

			if (distance <= distance2){
				#ifdef _DEBUG_SLICER_
					printf("big is the best of two\n");
				#endif
				for(a=0;a<attributes;a++){
					Column *fmt;
					if (!(task->cols[a])) continue;
					fmt = &task->as->format[task->cols[a]-1];

					bunfastins(fmt->c[1], key, adt[a]);
					HSTincrement(fmt->histogram[1], newValue[a]);
				}
			}else
				needToMove=TRUE;
		}


		if(needToMove){
			int smallindex[attributes], moveindex[attributes];
			BATiter bi;
			BAT *candidates=NULL, *temp;
			bit foundTupleToMove=FALSE,intersected=FALSE;

			#ifdef _DEBUG_SLICER_
				printf("put to small and pick one tuple to move to big\n");
			#endif

			for(a=0;a<attributes;a++){
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];

				smallindex[a] = HSTincrement(fmt->histogram[0], newValue[a]);
			}

			for(a=0;a<attributes;a++){
				Column *fmt;
				if (!(task->cols[a])) continue;
				fmt = &task->as->format[task->cols[a]-1];

				moveindex[a] = HSTwhichbinNorm(fmt->histogram[0],fmt->histogram[1]);

				if (smallindex[a]==moveindex[a] || moveindex[a]==-1){
					moveindex[a]=-1;
					continue;
				}

				if (candidates==NULL){
					candidates = fmt->bin[moveindex[a]];
					continue;
				}

				temp=BATkintersect(candidates,fmt->bin[moveindex[a]]);
				if (BATcount(temp)>0){
					candidates=temp;
					intersected=TRUE;
				}
				else
					moveindex[a]=-1;
			}

			if (candidates!=NULL){
				if (BATcount(candidates)>0){
					oid c=0;
					bit notnil=FALSE;
					bi = bat_iterator(candidates);
					if (!intersected)
						for(c=0;c<BATcount(candidates);c++){
							switch( candidates->ttype){
								case TYPE_bte: if( *(bte*)BUNtail(bi,c) != *(bte*)ATOMnilptr(candidates->ttype) ) notnil=TRUE;break;
								case TYPE_sht: if( *(sht*)BUNtail(bi,c) != *(sht*)ATOMnilptr(candidates->ttype) ) notnil=TRUE;break;
								case TYPE_int: if( *(int*)BUNtail(bi,c) != *(int*)ATOMnilptr(candidates->ttype) ) notnil=TRUE;break;
								case TYPE_lng: if( *(lng*)BUNtail(bi,c) != *(lng*)ATOMnilptr(candidates->ttype) ) notnil=TRUE;break;
								case TYPE_dbl: if( *(dbl*)BUNtail(bi,c) != *(dbl*)ATOMnilptr(candidates->ttype) ) notnil=TRUE;break;
								case TYPE_flt: if( *(flt*)BUNtail(bi,c) != *(flt*)ATOMnilptr(candidates->ttype) ) notnil=TRUE;break;
							}
							if (notnil) break;
						}
					else
						notnil=TRUE;

					if (notnil){
						foundTupleToMove=TRUE;
						candidates=BATslice(candidates,c,c+1);
						#ifdef _DEBUG_SLICER_
							printf("tuple to move is %d\n",(int)c);
							BATprint(candidates);
						#endif
					}
				}
			}


			if (!foundTupleToMove){
				#ifdef _DEBUG_SLICER_
					printf(" could not find a tuple to move: add to big\n");
				#endif

				for(a=0;a<attributes;a++){
					Column *fmt;
					if (!(task->cols[a])) continue;
					fmt = &task->as->format[task->cols[a]-1];

					bunfastins(fmt->c[1], key, adt[a]);
					HSTincrement(fmt->histogram[1], newValue[a]);
					HSTdecrement(fmt->histogram[0], newValue[a]);
				}
			}else{
				#ifdef _DEBUG_SLICER_
					printf("ready to swap\n");
				#endif
				for(a=0;a<attributes;a++){
					Column *fmt;
					oid pos[attributes];
					if (!(task->cols[a])) continue;
					fmt = &task->as->format[task->cols[a]-1];

					if (moveindex[a]>-1){
						pos[a] = *(oid*)Tloc(BATkintersect(BATmark(fmt->bin[moveindex[a]],0),candidates),0);
					}
					else{
						int b;
						for(b=0;b<bins;b++){
							BAT *temp=BATkintersect(BATmark(fmt->bin[b],0),candidates);
							if (BATcount(temp)>0){
								pos[a]=*(oid*)Tloc(temp,0);
								moveindex[a]=b;
								break;
							}
						}
					}

					bi = bat_iterator(fmt->bin[moveindex[a]]);
					VALset(curVal, fmt->c[0]->ttype, BUNtail(bi,pos[a]));

					replaceWithNull(fmt->bin[moveindex[a]],pos[a]);

					bunfastins(fmt->bin[smallindex[a]], &keys,&newValue[a]->val.ival);
					keys++;
					HSTdecrement(fmt->histogram[0],curVal);
					bunfastins(fmt->c[1], key,&curVal->val.ival);
					HSTincrement(fmt->histogram[1],curVal);

					#ifdef _DEBUG_SLICER_
						printf("attr %d: move tuple from bin %d pos %d\n", a,moveindex[a],(int)pos[a]);
					#endif
				}
			}
		}

		if(optimisticStep >0 && totalDistance < acceptableDistance && (oid)(task->next-i) > SmallSliceSize)
			optimistic = optimisticStep;
	}
	printf("prebalance result state \n");
	#ifdef _DEBUG_BATS_
		for(a=0;a<attributes;a++){
			Column *fmt;
			if (!(task->cols[a])) continue;
			fmt = &task->as->format[task->cols[a]-1];
			for (j=0;j<bins;j++){
				printf("bin %d\n",j);
				BATprint(fmt->bin[j]);
			}
			BATprint(fmt->c[1]);
		}
	#endif
	totalDistance=0;
	for(a=0;a<attributes;a++){
		Column *fmt;
		if (!(task->cols[a])) continue;
		fmt = &task->as->format[task->cols[a]-1];
		curDistance=HSTeuclidianNorm(fmt->histogram[0],fmt->histogram[1]);
		totalDistance+=curDistance;
		printf("Attr %d: Distance: %f \n",a,curDistance);
		HSTprintf(fmt->histogram[0]);
		HSTprintf(fmt->histogram[1]);
	}
	printf("Total Distance: %f \n",totalDistance);

	/*
	for(j=0;j<bins;j++)
		BATappend(fmt->c[0],fmt->bin[j],TRUE);
	printf(" %d  %d \n",(int)BATcount(fmt->c[0]),(int)BATcount(fmt->c[1]));
	*/

	return 0;

bunins_failed:
	if (*err == NULL) {
		snprintf(buf,BUFSIZ, "parsing error \n");
		*err= GDKstrdup(buf);
	}
	return -1;
}

#endif

static inline int
SQLinsert_val(Column * fmt, char *s, char quote, ptr key, str *err, int c)
{
	ptr *adt;
	char buf[BUFSIZ];
	char *e, *t;
	int ret = 0;

	/* include testing on the terminating null byte !! */
	if (fmt->nullstr && strncasecmp(s, fmt->nullstr, fmt->null_length+1) == 0){
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout,"nil value '%s' (%d) found in :%s\n",fmt->nullstr,fmt->nillen,(s?s:""));
#endif
		adt = fmt->nildata;
		fmt->c[0]->T->nonil = 0;
	} else if ( quote && *s == quote ) {
		s++;	/* find the last quote */
		for ( t = e = s; *t ; t++)
			if ( *t == quote) e = t;
		*e = 0;
		adt = fmt->frstr(fmt, fmt->adt, s, e, 0);
	} else {
		for( e=s; *e; e++)
			;
		adt = fmt->frstr(fmt, fmt->adt, s, e, 0);
	}

	if (!adt) {
		char *val;
		val = *s ? GDKstrdup(s) : GDKstrdup("");
		if ( *err == NULL){
			snprintf(buf,BUFSIZ, "value '%s' from line " BUNFMT
				" field %d not inserted, expecting type %s\n", val, BATcount(fmt->c[0])+1, c+1, fmt->type);
			*err= GDKstrdup(buf);
		}
		GDKfree(val);
		/* replace it with a nil */
		adt = fmt->nildata;
		fmt->c[0]->T->nonil = 0;
		ret = -1;
	}
	/* key maybe NULL but thats not a problem, as long as we have void */
	if (fmt->raw){
		mnstr_write(fmt->raw,adt,ATOMsize(fmt->adt),1);
	} else {
		bunfastins(fmt->c[0], key, adt);
	}
	return ret;
bunins_failed:
	if (*err == NULL) {
		snprintf(buf,BUFSIZ, "parsing error from line " BUNFMT " field %d not inserted\n", BATcount(fmt->c[0])+1, c+1);
		*err= GDKstrdup(buf);
	}
	return -1;
}

static int
SQLworker_column(READERtask *task, int col)
{
	int i;
	Column *fmt = task->as->format;
	str err = 0;

	if ( BATcapacity(fmt[col].c[0]) < BATcount(fmt[col].c[0]) + task->next ) {
		if ( (fmt[col].c[0] =  BATextend(fmt[col].c[0], BATgrows(fmt[col].c[0]) + task->next)) == NULL){
			/* watch out for concurrent threads */
			mal_set_lock(mal_contextLock,"tablet insert value");
			if (task->as->error == NULL)
				task->as->error = GDKstrdup("Failed to extend the BAT, perhaps disk full");
			mal_unset_lock(mal_contextLock,"tablet insert value");
			mnstr_printf(GDKout,"Failed to extend the BAT, perhaps disk full");
			return -1;
		}
	}

	#ifdef _SLICE_TABLET_
		(void) i;
		if (_SLICER_ ==1){
			if (Slice(task, &fmt[col], NULL, &err, col)){
				assert(err != NULL);
				if (!task->as->tryall){
					/* watch out for concurrent threads */
					mal_set_lock(mal_contextLock,"tablet insert value");
					if (task->as->error == NULL)
						task->as->error = err;	/* restore for upper layers */
					mal_unset_lock(mal_contextLock,"tablet insert value");
					return -1;
				}
				BUNins(task->as->complaints, NULL, err, TRUE);
			}
		}else
		if (_SLICER_ ==2){
			if (Slice2(task, &fmt[col], NULL, &err, col)){
				assert(err != NULL);
				if (!task->as->tryall){
					/* watch out for concurrent threads */
					mal_set_lock(mal_contextLock,"tablet insert value");
					if (task->as->error == NULL)
						task->as->error = err;	/* restore for upper layers */
					mal_unset_lock(mal_contextLock,"tablet insert value");
					return -1;
				}
				BUNins(task->as->complaints, NULL, err, TRUE);
			}
		}

	#endif

	#ifndef _SLICE_TABLET_
	for ( i = 0; i< task->next ; i++){
		if (SQLinsert_val(&fmt[col], task->fields[col][i], task->quote, NULL, &err, col)) {
			assert(err != NULL);
			if (!task->as->tryall){
				/* watch out for concurrent threads */
				mal_set_lock(mal_contextLock,"tablet insert value");
				if (task->as->error == NULL)
					task->as->error = err;	/* restore for upper layers */
				mal_unset_lock(mal_contextLock,"tablet insert value");
				return -1;
			}
			BUNins(task->as->complaints, NULL, err, TRUE);
		}
	}
	#endif

	if (err) {
		/* watch out for concurrent threads */
		mal_set_lock(mal_contextLock,"tablet insert value");
		if (task->as->error == NULL)
			task->as->error = err;	/* restore for upper layers */
		mal_unset_lock(mal_contextLock,"tablet insert value");
	}
	return err ? -1 : 0;
}

static void
SQLworker(void *arg)
{
	READERtask *task = (READERtask *) arg;
	unsigned int i, cnt=0;
	lng t0;

	/* where to leave errors */
	THRset_errbuf(THRget(THRgettid()), task->errbuf);
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout,"SQLworker started\n");
#endif
	while(task->next >= 0 ){
		MT_sema_down(&task->sema,"SQLworker");
		if ( task->next < 0 ){
#ifdef _DEBUG_TABLET_
			mnstr_printf(GDKout,"SQLworker terminated\n");
#endif
			MT_sema_up(&task->reply,"SQLworker");
			return;
		}

		#ifdef _SLICE_TABLET_
			if (task->as->nr_attrs>1)
				Slice3(task);
			else
				for ( i= 0; i < task->as->nr_attrs && task->as->error == NULL; i++)
					if ( task->cols[i]) {
						t0 = GDKusec();
						SQLworker_column(task,task->cols[i]-1);
						t0 = GDKusec() -t0;
						task->time[i] += t0;
						task->wtime += t0;
					}
		#endif
		#ifndef _SLICE_TABLET_
		for ( i= 0; i < task->as->nr_attrs && task->as->error == NULL; i++)
			if ( task->cols[i]) {
				t0 = GDKusec();
				SQLworker_column(task,task->cols[i]-1);
				t0 = GDKusec() -t0;
				task->time[i] += t0;
				task->wtime += t0;
			}
		#endif
		cnt += task->next;

		MT_sema_up(&task->reply,"SQLworker");
	}
	MT_sema_up(&task->reply,"SQLworker");
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout,"SQLworker exits\n");
#endif
	mnstr_printf(GDKout,"##SQLworker exits %s\n",task->as->error);
}
/*
 * @-
 * The line is broken into pieces directly on their field separators. It assumes that we have
 * the record in the cache already, so we can do most work quickly.
 * Furthermore, it assume a uniform (SQL) pattern, without whitespace skipping, but with quote and separator.
 */

static str
SQLload_error(READERtask *task, Tablet *as, int idx, str csep, str rsep)
{
	str line;
	size_t sz =0, rseplen=strlen(rsep), cseplen = strlen(csep);
	unsigned int i;
	for ( i = 0; i < as->nr_attrs; i++)
		if (task->fields[i][idx])
			sz += strlen(task->fields[i][idx]) + cseplen;
		else sz += cseplen;

	line = (str) GDKzalloc(sz + rseplen + 1);
	for ( i = 0; i < as->nr_attrs; i++){
		if (task->fields[i][idx])
			strcat(line,task->fields[i][idx]);
		if ( i < as->nr_attrs -1)
			strcat(line,csep);
	}
	strcat(line,rsep);
	return line;
}

static int
SQLload_file_line(READERtask *task, Tablet * as, char *line, str csep, str rsep)
{
	Column *fmt = as->format;
	BUN i;
	char *start;
	char errmsg[BUFSIZ];
	char ch = *task->separator;

	for (i = 0; i < as->nr_attrs ; i++) {
		task->fields[i][task->next] = start = line;
		/* recognize fields starting with a quote, keep them */
		if ( task->quote && *line == task->quote ){
			line = tablet_skip_string(line + 1, task->quote);
			if (!line) {
				str errline = SQLload_error(task,as,task->next,csep,rsep);
				snprintf(errmsg,BUFSIZ, "End of string (%c) missing "
					"in \"%s\" at line " BUNFMT " field "BUNFMT"\n", task->quote, errline, BATcount(as->format->c[0]) + task->next +1, i);
				GDKerror(errmsg);
				as->error = GDKstrdup(errmsg);
				GDKfree(errline);
				*start = 0; /* to avoid handling an incomplete quoted field */
				for ( ; i< as->nr_attrs ; i++)
					task->fields[i][task->next] = start ;
				if (as->tryall)
					BUNins(as->complaints, NULL, as->error, TRUE);
				return -1;
			}
		}

		/* skip single character separators fast. */
		if ( task->seplen == 1){
			for ( ; *line ; line++)
				if ( *line == ch)  {
					*line = 0;
					line ++;
					goto endoffield;
				}
		} else
			for ( ; *line ; line++)
				if ( *line == ch && (task->seplen == 1 || strncmp(line,task->separator,task->seplen) == 0) ){
					*line = 0;
					line += task->seplen;
					goto endoffield;
				}
		if ( i < as->nr_attrs-1)  {
			snprintf(errmsg,BUFSIZ, "missing separator '%s' line " BUNFMT " field " BUNFMT "\n",
					 fmt->sep, BATcount(fmt->c[0]) + 1 + task->next, i);
			GDKerror(errmsg);
			as->error = GDKstrdup(errmsg);
			if (!as->tryall)
				return -1;
			BUNins(as->complaints, NULL, as->error, TRUE);
			break;
		}
	  endoffield:;
	}
	return 0;
}

static void
SQLworkdivider(READERtask *task, READERtask *ptask, int nr_attrs, int threads)
{
	int i, j, mi;
	lng *loc,t;

	/* after a few rounds we stick to the work assignment */
	if ( task->rounds > 8 )
		return;
	/* simple round robin the first time */
	if ( task->rounds++ == 0){
		for ( i=j=0; i < nr_attrs; i++, j++)
			ptask[ j % threads].cols[i] = task->cols[i];
		return;
	}
	loc = (lng*) GDKzalloc(sizeof(lng) * threads);
	/* use of load directives */
	for ( i=0; i < nr_attrs; i++)
		for ( j=0; j < threads; j++)
			ptask[j].cols[i] = 0;

	/* sort the attributes based on their total time cost */
	for ( i=0; i<nr_attrs; i++)
		for ( j=i+1; j<nr_attrs; j++)
			if ( task->time[i] < task->time[j]) {
				mi = task->cols[i];
				t  = task->time[i];
				task->cols[i] = task->cols[j];
				task->cols[j] = mi;
				task->time[i] = task->time[j];
				task->time[j] = t;
			}

	/* now allocate the work to the threads */
	for ( i=0; i < nr_attrs; i++, j++){
		mi = 0;
		for ( j=1; j< threads; j++)
			if ( loc[j] <  loc[mi]) mi = j;

		ptask[mi].cols[i] = task->cols[i];
		loc[mi] += task->time[i];
	}
	GDKfree(loc);
}

BUN
SQLload_file(Client cntxt, Tablet * as, bstream *b, stream *out, char *csep, char *rsep, char quote, lng skip, lng maxrow)
{
	char *s, *e, *end;
	int ateof = 0;
	char q = 0;					/* remember quote status */
	BUN cnt = 0;
	int res = 0;				/* < 0: error, > 0: success, == 0: continue processing */
	int j;
	BUN i;
	size_t rseplen;
	READERtask *task= (READERtask*) GDKzalloc(sizeof(READERtask));
	READERtask ptask[128];
	int threads= GDKnr_threads < MT_check_nr_cores()? GDKnr_threads: MT_check_nr_cores();
	lng t0, total=0, startTime=GDKusec();
	int askmore = 0;
	int vmtrim = GDK_vm_trim;
	/* trimming process should not be active during this process. */
	/* on sf10 experiments it should a slowdown of a factor 2 on */
	/* large tables. Instead rely on madvise */
	GDK_vm_trim = 0;
	assert(GDKnr_threads <128);


	assert(rsep);
	assert(csep);
	assert(maxrow < 0 || maxrow <= (lng) BUN_MAX);
	rseplen = strlen(rsep);
	task->fields =  (char***) GDKzalloc(as->nr_attrs * sizeof(char*));
	task->cols =  (int*) GDKzalloc(as->nr_attrs * sizeof(int));
	task->time =  (lng*) GDKzalloc(as->nr_attrs * sizeof(lng));
	task->as= as;
	task->quote = quote;
	task->separator = csep;
	task->seplen = strlen(csep);
	task->errbuf =  cntxt->errbuf;

	as->error = NULL;

	/* there is no point in creating more threads than we have columns */
	if (as->nr_attrs < (BUN) threads)
		threads = (int) as->nr_attrs;

	for( i=0; i< as->nr_attrs; i++) {
		task->fields[i] = GDKzalloc(sizeof(char*) * 10000);
		task->cols[i] = (int) (i+1);	/* to distinguish non initialized later with zero */
		/* advice memory manager on expected use of results */
		BATmadvise(as->format[i].c[0], BUF_SEQUENTIAL,BUF_SEQUENTIAL,BUF_SEQUENTIAL,BUF_SEQUENTIAL);
	}
	task->limit = 10000;
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout,"Prepare copy work for %d threads col '%s' rec '%s' quot '%c'\n",threads,csep,rsep,quote);
#endif
	for( j= 0; j < threads ; j++){
		ptask[j]= *task;
		ptask[j].cols =  (int*) GDKzalloc(as->nr_attrs * sizeof(int));
		MT_sema_init(&ptask[j].sema,0,"sqlworker");
		MT_sema_init(&ptask[j].reply,0,"sqlworker");
		MT_create_thread(&ptask[j].tid, SQLworker, (void*) &ptask[j], MT_THR_DETACHED);
	}

#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout,"parallel bulk load "LLFMT " - " LLFMT"\n",skip, maxrow);
	mnstr_printf(GDKout,"csep '%s' rsep '%s'\n",csep,rsep);
#endif

	while (!ateof && (b->pos < b->len || !b->eof) && cnt != (BUN) maxrow && res == 0) {
		t0 = GDKusec();

		if (b->pos >= b->len || askmore)
			ateof = tablet_read_more(b, out, b->size - (b->len - b->pos)) == EOF;
		askmore = 0;

#ifdef _DEBUG_TABLET_
	   	mnstr_printf(GDKout,"read pos=" SZFMT " len=" SZFMT " size=" SZFMT " eof=%d \n", b->pos, b->len, b->size, b->eof);
#endif

		/* now we fill the copy buffer with pointers to the record */
		/* skipping tuples as needed */
		task->next = 0;

		end = b->buf + b->len;
		s = b->buf + b->pos;
		*end = '\0';	/* this is safe, as the stream ensures an extra byte */
		/* Note that we rescan from the start of a record (the last
		   partial buffer from the previous iteration), even if in the
		   previous iteration we have already established that there
		   is no record separator in the first, perhaps significant,
		   part of the buffer.  This is because if the record
		   separator is longer than one byte, it is too complex
		   (i.e. would require more state) to be sure what the state
		   of the quote status is when we back off a few bytes from
		   where the last scan ended (we need to back off some since
		   we could be in the middle of the record separator).  If
		   this is too costly, we have to rethink the matter. */
		e = s;
		q = 0;
		while (s < end && (maxrow < 0 || cnt < (BUN) maxrow) ) {
			/* alloc space in the task buffer */
			if ( task->next == task->limit ){
				for( i=0; i< as->nr_attrs; i++)
					task->fields[i] = GDKrealloc(task->fields[i], sizeof(char*) * (task->limit + 10000));
				task->limit += 10000;
			}
			/* tokenize the record completely
			   the format of the input should comply to the following grammar rule
			    [ [[quote][[esc]char]*[quote]csep]*rsep]*
				where quote is a single user defined character
				within the quoted fields a character may be escaped with a backslash
				The user should supply the correct number of fields.
				The algorithm performance a double scan to simplify rolling back
				when the record received is incomplete.
			*/
			if ( quote == 0) {
				for ( ; *e ; e++)
					if ( *e == *rsep && (rseplen == 1 || strncmp(e,rsep,rseplen) == 0) )
						break;
			} else {
				for ( ; *e ; e++) {
					if (*e == q)
						q = 0;
					else if (*e == quote)
						q = *e;
					else if (*e == '\\') {
						if (e[1])
							e++;
					} else if (q == 0 && *e == *rsep && (rseplen == 1 || strncmp(e, rsep, rseplen) == 0))
						break;
				}
			}

			if ((e != NULL && *e != 0) || ateof) {
				/* found a complete record, or an incomplete one at the end of the file */
				if ( --skip < 0 ) {
					task->fields[0][task->next] = s;
					*e = '\0';
					if ( SQLload_file_line(task, as, s, csep,rsep) < 0) {
#ifdef _DEBUG_TABLET_
						mnstr_printf(GDKout,"line failed:\n");
#endif
						res = -1;
						if (ateof) {
							/* we've consumed the data, even if we couldn't process it */
							b->pos = b->len;
						}
						break;
					}
					cnt++;
					if ( skip < 0)
						task->next++;
				}
				s = e + rseplen;
				e = s;
				b->pos = (size_t) (s - b->buf);
			} else {
				/* no (unquoted) record separator found, read more data so that we can scan more */
				askmore = 1;
				break;
			}
		}
		t0 = GDKusec() - t0;
		total += t0;
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout,"fill the BATs %d  "BUNFMT" cap " BUNFMT"\n", task->next, cnt, BATcapacity(as->format[0].c[0]));
#endif
		if ( task->next ){
			SQLworkdivider(task, ptask, (int) as->nr_attrs, threads);

			/* activate the workers */
			for( j= 0; j < threads ; j++) {
				ptask[j].next= task->next;
				ptask[j].fields= task->fields;
				ptask[j].limit = task->limit;
				MT_sema_up(&ptask[j].sema,"SQLworker");
			}
			/* await their completion */
			for ( j=0; j< threads; j++)
				MT_sema_down(&ptask[j].reply,"sqlreader");
		}
	}

	/* close down the workers */
#ifdef _DEBUG_TABLET_
	mnstr_printf(GDKout,"Close the workers\n");
#endif
	for( j= 0; j < threads ; j++){
		ptask[j].next = -1;
		MT_sema_up(&ptask[j].sema,"SQLworker");
	}
	if (GDKdebug & GRPalgorithms) {
		if (cnt < (BUN) maxrow   && maxrow > 0)
			/* providing a precise count is not always easy, instead consider maxrow as an upperbound */
			mnstr_printf(GDKout,"#SQLload_file: read error, tuples missing (after loading " BUNFMT " records)\n", BATcount(as->format[0].c[0]));
		mnstr_printf(GDKout,"# COPY reader time " LLFMT "\n#",total);
		for( i=0; i< as->nr_attrs; i++)
				mnstr_printf(GDKout,LLFMT " ", task->time[i]);
		mnstr_printf(GDKout,"\n");
		for ( j=0; j< threads; j++)
			mnstr_printf(GDKout,"# COPY thread time " LLFMT "\n",ptask[j].wtime);
	}

	/* wait for their death */
	for ( j=0; j< threads; j++)
		MT_sema_down(&ptask[j].reply,"sqlreader");
#ifdef _DEBUG_TABLET_
		mnstr_printf(GDKout,"Found " BUNFMT" tuples\n",cnt);
#endif
	for( i=0; i< as->nr_attrs; i++)
		GDKfree(task->fields[i]);
	GDKfree(task->fields);
	GDKfree(task->time);
	GDKfree(task);

	/* restore system setting */
	GDK_vm_trim = vmtrim;
	printf("Total Load Time: " LLFMT " \n",GDKusec()-startTime);
	return res < 0 ? BUN_NONE : cnt;
}

#undef _DEBUG_TABLET_
