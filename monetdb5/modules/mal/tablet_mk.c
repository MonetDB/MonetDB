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

#define _SLICE_TABLET_ MK

/* All the params below should eventually be dynamically defined per column
   For simplicity in the initial stages keep them global and fixed */
#ifdef _SLICE_TABLET_
oid SmallSliceSize  = 2; /* Assume this is known */
BAT *BigSlice = NULL; 	/*toy BAT to play with the algos without worying for SQL for now*/
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
#define _SLICE_TABLET_MK
#ifdef _SLICE_TABLET_MK
Histogram h1 = NULL;
Histogram h2 = NULL;


Tablet *SLICEinit(Tablet *as, int slices, Histogram h[])
{
	Tablet *body;
	int len= sizeof(Tablet) + as->nr_attrs * sizeof(Column);
	BUN i;

	assert(SLICES >= slices);
	for ( i=0; i< as->nr_attrs; i++)
		body->columns[i].c[1] = BATnew(TYPE_void, as->columns[i].adt,0);
	for ( i = 0; i< slices; i++)
		/* init histogram */;
	return body;
}
#endif

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
replaceVal(BAT * b, int position, ValPtr val)
{
	switch( val->vtype){
		case TYPE_bte: *(bte*)Tloc(b,position) = val->val.btval;
                case TYPE_sht: *(sht*)Tloc(b,position) = val->val.shval;
                case TYPE_int: *(int*)Tloc(b,position) = val->val.ival;
                case TYPE_lng: *(lng*)Tloc(b,position) = val->val.lval;
                case TYPE_dbl: *(dbl*)Tloc(b,position) = val->val.dval;
                case TYPE_flt: *(flt*)Tloc(b,position) = val->val.fval;
	}
}

#ifdef _SLICE_TABLET_
static int
Slice(READERtask *task, Column * fmt, ptr key, str *err, int col)
{
	/*naive : fill big, fill small, then rearrange with every new value */
	char buf[BUFSIZ];
	int i;
	ptr *adt;
	BATiter bsi;
	BAT *sample, *sortedSample;
	ValPtr vmin, vmax;
	/*toy values*/
	int sampleSize=2;
	int bins=2;
	int distance =0;

	/*sample to get min-max*/
	sample = BATnew(fmt->c[0]->htype, fmt->c[0]->ttype,sampleSize);
	for ( i = 0; i< sampleSize ; i++){
		adt=get_val(&fmt[col], task->fields[col][i], task->quote, err, col);
		bunfastins(sample, key, adt);
	}
	sortedSample = BATtsort(sample);
	bsi = bat_iterator(sortedSample);
	BATprint(sample);
	BATprint(sortedSample);
	vmin= (ValPtr) GDKzalloc(sizeof(ValRecord));
	vmax= (ValPtr) GDKzalloc(sizeof(ValRecord));
	VALset(vmin, fmt->c[0]->ttype, BUNtail(bsi,0));
	VALset(vmax, fmt->c[0]->ttype, BUNtail(bsi,sampleSize-1));

	printf("min %d \n", vmin->val.ival);
	printf("max %d \n", vmax->val.ival);

	for ( i = 0; i< task->next ; i++){
		ValPtr newValue = (ValPtr) GDKzalloc(sizeof(ValRecord));
		adt=get_val(&fmt[col], task->fields[col][i], task->quote, err, col);
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

		/*choose target BAT in order to balance the histogram distance*/
		distance = HSTeuclidianWhatIf(h1,h2,newValue);
		if (distance==0){
			printf(" add to big\n");
			bunfastins(BigSlice, key, adt);
			HSTincrement(h2, newValue);
		}else{
			int j;
			bit moved = FALSE;
			BATiter bi;
			ValPtr curVal;
			int sizeSmall = BATcount(fmt->c[0]);
			printf("add to small\n");
			//bunfastins(fmt->c[0], key, adt);
			HSTincrement(h1, newValue);

			/* pick a tuple to move to the big one */
			bi = bat_iterator(fmt->c[0]);
			curVal= (ValPtr) GDKzalloc(sizeof(ValRecord));
			for (j=0; j<sizeSmall; j++){
				VALset(curVal, fmt->c[0]->ttype, BUNtail(bi,j));
				distance = HSTeuclidianWhatIfMove(h1,h2,curVal);
				if (distance==0){
					moved = TRUE;
					printf("found val to replace %d %d with %d\n",j,curVal->val.ival, newValue->val.ival);
					replaceVal(fmt->c[0], j, newValue);
					HSTprintf(h1);
					HSTdecrement(h1,curVal);
					HSTprintf(h1);
					bunfastins(BigSlice, key,&curVal->val.ival);
					HSTincrement(h2,curVal);
					BATprint(fmt->c[0]);
					BATprint(BigSlice);
					break;
				}
			}
			/*could not find a tuple to move*/
			if (!moved){
				printf(" could not find a tuple to move: add to big\n");
				bunfastins(BigSlice, key, adt);
				HSTincrement(h2, newValue);
				HSTdecrement(h1, newValue);
			}

		}
	}

	printf("result state \n");
	BATprint(fmt->c[0]);
	HSTprintf(h1);
	BATprint(BigSlice);
	HSTprintf(h2);

	return 0;

bunins_failed:
	if (*err == NULL) {
		snprintf(buf,BUFSIZ, "parsing error from line " BUNFMT " field %d not inserted\n", BATcount(fmt->c[0])+1, col+1);
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
		Slice(task, &fmt[col], NULL, &err, col);
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

		for ( i= 0; i < task->as->nr_attrs && task->as->error == NULL; i++)
			if ( task->cols[i]) {
				t0 = GDKusec();
				SQLworker_column(task,task->cols[i]-1);
				t0 = GDKusec() -t0;
				task->time[i] += t0;
				task->wtime += t0;
			}
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
	lng t0, total=0;
	int askmore = 0;
	int vmtrim = GDK_vm_trim;
	Histogram histogram[SLICES];
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

#ifdef _SLICE_TABLET_MK
		SLICEinit(as,2, histogram);
#endif

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
	return res < 0 ? BUN_NONE : cnt;
}

#undef _DEBUG_TABLET_
