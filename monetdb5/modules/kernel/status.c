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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
*/

/*
 * author M.L. Kersten, P. Boncz, N.Nes
 * System state information
 * This document introduces a series of bats  and operations that provide access
 * to information stored within the Monet Version 5 internal data structures.
 * In all cases, pseudo BAT operation returns a transient BAT that
 * should be garbage collected after being used.
 *
 * The main performance drain would be to use a pseudo BAT directly to
 * successively access it components. This can be avoided by first assigning
 * the pseudo BAT to a variable.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include <stdarg.h>
#include <time.h>
#include "mal_exception.h"
#include "status.h"
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

#ifdef HAVE_SYS_RESOURCE_H
# include <sys/resource.h>
#endif

static void
pseudo(int *ret, BAT *b, str X1,str X2) {
	char buf[BUFSIZ];
	snprintf(buf,BUFSIZ,"%s_%s", X1,X2);
	if (BBPindex(buf) <= 0)
		BATname(b,buf);
	BATroles(b,X1,X2);
	BATmode(b,TRANSIENT);
	BATfakeCommit(b);
	*ret = b->batCacheid;
	BBPkeepref(*ret);
}

str
SYSgetmem_cursize(lng *num)
{
	*num = GDKmem_cursize();
	return MAL_SUCCEED;
}

str
SYSgetmem_maxsize(lng *num)
{
	*num = GDK_mem_maxsize;
	return MAL_SUCCEED;
}

str
SYSsetmem_maxsize(int *ret, lng *num)
{
	size_t sze = 0;
	*ret = 0;
	if (*num < 0)
		throw(ILLARG, "status.mem_maxsize", "new size must not be < 0");
#if SIZEOF_SIZE_T == SIZEOF_INT
	{
		lng size_t_max = 2 * (lng)INT_MAX;
		if (*num > size_t_max)
			throw(ILLARG, "status.mem_maxsize", "new size must not be > " LLFMT, size_t_max);
	}
#endif
	if (sze < GDK_mem_bigsize)
		GDK_mem_bigsize = MAX(32768, sze);
	GDK_mem_maxsize = MAX(GDK_mem_bigsize, sze);
	return MAL_SUCCEED;
}

str
SYSgetvm_cursize(lng *num)
{
	*num = GDKvm_cursize();
	return MAL_SUCCEED;
}

str
SYSgetvm_maxsize(lng *num)
{
	*num = GDK_vm_maxsize;
	return MAL_SUCCEED;
}

str
SYSsetvm_maxsize(lng *num)
{
	GDK_vm_maxsize = (size_t) *num;
	return MAL_SUCCEED;
}

/*
 * Performance
 * To obtain a good impression of the Monet performance we need timing information.
 * The most detailed information is best obtained with the system profiler.
 *
 * However, the direct approach is to enable the user to read the timers maintained
 * internally. This is done with the CPU, IO, MEMORY, and BBP command which
 * displays the elapsed time in seconds, user- and system-cpu time in milliseconds
 * since its last invocation and the amount of space in use.  The process
 * identifier is used to differentiate among the possible processes.
 *
 * Note that in multi threaded mode the routine prints the elapsed
 * time since the beginning of each process.
 */
#ifdef HAVE_TIMES
static time_t clk = 0;
static struct tms state;
#endif

str
SYScpuStatistics(int *ret)
{
	int i;
	BAT *b;
#ifdef HAVE_TIMES
	struct tms newst;
# ifndef HZ
	static int HZ;

	if (HZ == 0) {
#  if defined(HAVE_SYSCONF) && defined(_SC_CLK_TCK)
		HZ = sysconf(_SC_CLK_TCK);
#  else
		HZ = CLK_TCK;
#  endif
	}
# endif
#endif

	b = BATnew(TYPE_str, TYPE_int, 32);
	if (b == 0)
		throw(MAL, "status.cpuStatistics", MAL_MALLOC_FAIL);
#ifdef HAVE_TIMES
	if (clk == 0) {
		clk = time(0);
		times(&state);
	}
	times(&newst);
	/* store counters, ignore errors */
	i = (int) (time(0) - clk);
	b = BUNins(b, "elapsed", &i, FALSE);
	i = newst.tms_utime * 1000 / HZ;
	b = BUNins(b, "user", &i, FALSE);
	i = (newst.tms_utime - state.tms_utime) * 1000 / HZ;
	b = BUNins(b, "elapuser", &i, FALSE);
	i = newst.tms_stime * 1000 / HZ;
	b = BUNins(b, "system", &i, FALSE);
	i = (newst.tms_stime - state.tms_stime) * 1000 / HZ;
	b = BUNins(b, "elapsystem", &i, FALSE);

	state = newst;
#else
	i = int_nil;
	b = BUNins(b, "elapsed", &i, FALSE);
	b = BUNins(b, "user", &i, FALSE);
	b = BUNins(b, "elapuser", &i, FALSE);
	b = BUNins(b, "system", &i, FALSE);
	b = BUNins(b, "elapsystem", &i, FALSE);
#endif
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"gdk","cpu");
	return MAL_SUCCEED;
}

static char *memincr = NULL;
str
SYSmemStatistics(int *ret)
{
	struct Mallinfo m;
	BAT *b;
	wrd i;

	m = MT_mallinfo();

	b = BATnew(TYPE_str, TYPE_wrd, 32);
	if (b == 0)
		throw(MAL, "status.memStatistics", MAL_MALLOC_FAIL);

	/* store counters, ignore errors */
	if (memincr == NULL)
		memincr = MT_heapbase;

	i = (wrd) (MT_heapcur() - memincr);

	memincr = MT_heapcur();
	b = BUNins(b, "memincr", &i, FALSE);
	i = (wrd) m.arena;
	b = BUNins(b, "arena", &i, FALSE);
	i = (wrd) m.ordblks;
	b = BUNins(b, "ordblks", &i, FALSE);
	i = (wrd) m.smblks;
	b = BUNins(b, "smblks", &i, FALSE);
	i = (wrd) m.hblkhd;
	b = BUNins(b, "hblkhd", &i, FALSE);
	i = (wrd) m.hblks;
	b = BUNins(b, "hblks", &i, FALSE);
	i = (wrd) m.usmblks;
	b = BUNins(b, "usmblks", &i, FALSE);
	i = (wrd) m.fsmblks;
	b = BUNins(b, "fsmblks", &i, FALSE);
	i = (wrd) m.uordblks;
	b = BUNins(b, "uordblks", &i, FALSE);
	i = (wrd) m.fordblks;
	b = BUNins(b, "fordblks", &i, FALSE);
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"gdk","mem");
	return MAL_SUCCEED;
}

#define heap(X1,X2,X3,X4)\
	if (X1) {\
		sz = HEAPmemsize(X2);\
		if (sz > *minsize) {\
			sprintf(buf, X4"/%s", s);\
			BUNins(bn, buf, &sz, FALSE);\
		}\
		X3 += sz; tot += sz;\
	}
#define heapvm(X1,X2,X3,X4)\
	if (X1) {\
		sz = HEAPvmsize(X2);\
		if (sz > *minsize) {\
			sprintf(buf, X4"/%s", s);\
			BUNins(bn, buf, &sz, FALSE);\
		}\
		X3 += sz; tot += sz;\
	}

str
SYSmem_usage(int *ret, lng *minsize)
{
	lng hbuns = 0, tbuns = 0, hhsh = 0, thsh = 0, hind = 0, tind = 0, head = 0, tail = 0, tot = 0, n = 0, sz;
	BAT *bn = BATnew(TYPE_str, TYPE_lng, 2 * BBPsize);
	struct Mallinfo m;
	char buf[1024];
	bat i;

	if (bn == NULL)
		throw(MAL, "status.memUsage", MAL_MALLOC_FAIL);
	BBPlock("SYSmem_usage");
	for (i = 1; i < BBPsize; i++) {
		BAT *b = BBP_cache(i);
		str s;

		if (!BBPvalid(i))
			continue;

		s = BBPname(i);
		sz = 0;
		if (BBP_desc(i))
			sz += sizeof(BATstore);
		if (BBP_logical(i))
			n += strLen(BBP_logical(i));
		if (BBP_logical(-i))
			n += strLen(BBP_logical(-i));
		if (BBP_physical(i))
			n += strLen(BBP_physical(i));
		if (b)
			sz += sizeof(BAT);	/* mirror */

		if (sz > *minsize) {
			sprintf(buf, "desc/%s", s);
			BUNins(bn, buf, &sz, FALSE);
		}
		tot += (lng) sz;

		if (b == NULL || isVIEW(b)) {
			continue;
		}
		heap(1,&b->H->heap,hbuns,"hbuns");
		heap(1,&b->T->heap,tbuns,"tbuns");
		heap(b->H->hash,b->H->hash->heap,hhsh,"hhsh");
		heap(b->T->hash,b->T->hash->heap,thsh,"thsh");
		heap(b->H->vheap,b->H->vheap,head,"head");
		heap(b->T->vheap,b->T->vheap,tail,"tail");
	}
	/* totals per category */
	BUNins(bn, "_tot/hbuns", &hbuns, FALSE);
	BUNins(bn, "_tot/tbuns", &tbuns, FALSE);
	BUNins(bn, "_tot/head", &head, FALSE);
	BUNins(bn, "_tot/tail", &tail, FALSE);
	BUNins(bn, "_tot/hhsh", &hhsh, FALSE);
	BUNins(bn, "_tot/thsh", &thsh, FALSE);
	BUNins(bn, "_tot/hind", &hind, FALSE);
	BUNins(bn, "_tot/tind", &tind, FALSE);

	/* special area 1: BBP rec */
	sz = BBPlimit * sizeof(BBPrec) + n;
	BUNins(bn, "_tot/bbp", &sz, FALSE);
	tot += sz;

	/* this concludes all major traceable Monet memory usages */
	tot += sz;
	BUNins(bn, "_tot/found", &tot, FALSE);

	/* now look at what the global statistics report (to see if it coincides) */

	/* how much *used* bytes in heap? */
	m = MT_mallinfo();
	sz = (size_t) m.usmblks + (size_t) m.uordblks + (size_t) m.hblkhd;
	BUNins(bn, "_tot/malloc", &sz, FALSE);

	/* measure actual heap size, includes wasted fragmented space and anon mmap space used by malloc() */
	sz = GDKmem_inuse();
	BUNins(bn, "_tot/heap", &sz, FALSE);

	tot = GDKmem_cursize();

	/* allocated swap area memory that is not plain malloc() */
	sz = MAX(0, sz - tot);
	BUNins(bn, "_tot/valloc", &sz, FALSE);

	/* swap-area memory is in either GDKvmalloc or heap */
	BUNins(bn, "_tot/swapmem", &tot, FALSE);

	BBPunlock("SYSmem_usage");
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
SYSvm_usage(int *ret, lng *minsize)
{
	lng hbuns = 0, tbuns = 0, hhsh = 0, thsh = 0, hind = 0, tind = 0, head = 0, tail = 0, tot = 0, sz;
	BAT *bn = BATnew(TYPE_str, TYPE_lng, 2 * BBPsize);
	char buf[1024];
	bat i;

	if (bn == NULL)
		throw(MAL, "status.vmStatistics", MAL_MALLOC_FAIL);
	BBPlock("SYSvm_usage");
	for (i = 1; i < BBPsize; i++) {
		BAT *b;
		str s;

		if (!BBPvalid(i))
			continue;

		s = BBPname(i);
 		b = BBP_cache(i);
		if (b == NULL || isVIEW(b)) {
			continue;
		}
		heapvm(1,&b->H->heap,hbuns,"hbuns");
		heapvm(1,&b->T->heap,tbuns,"tbuns");
		heapvm(b->H->hash,b->H->hash->heap,hhsh,"hshh");
		heapvm(b->T->hash,b->T->hash->heap,thsh,"thsh");
		heapvm(b->H->vheap,b->H->vheap,head,"head");
		heapvm(b->T->vheap,b->T->vheap,tail,"tail");
	}
	/* totals per category */
	BUNins(bn, "_tot/hbuns", &hbuns, FALSE);
	BUNins(bn, "_tot/tbuns", &tbuns, FALSE);
	BUNins(bn, "_tot/head", &head, FALSE);
	BUNins(bn, "_tot/tail", &tail, FALSE);
	BUNins(bn, "_tot/hhsh", &hhsh, FALSE);
	BUNins(bn, "_tot/thsh", &thsh, FALSE);
	BUNins(bn, "_tot/hind", &hind, FALSE);
	BUNins(bn, "_tot/tind", &tind, FALSE);

	/* special area 1: BBP rec */
	sz = BBPlimit * sizeof(BBPrec);
	BUNins(bn, "_tot/bbp", &sz, FALSE);
	tot += sz;


	/* this concludes all major traceable Monet virtual memory usages */
	tot += sz;
	BUNins(bn, "_tot/found", &tot, FALSE);

	/* all VM is either GDKmmap or GDKvmalloc (possibly redirected GDKmalloc), *plus* the heap */
	sz = GDKvm_cursize();
	BUNins(bn, "_tot/vm", &sz, FALSE);

	BBPunlock("SYSvm_usage");
	if (!(bn->batDirty&2)) bn = BATsetaccess(bn, BAT_READ);
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

/*
 * Additional information on the process utilization is given by
 * the ioStatistics command. The following information is obtained.
 *
 * @T
 * \begin{tabular}{| l| l|}\hline
 * maxrss     &the maximum resident set size utilized (in kilobytes).\\
 * minflt     &the number of page faults serviced without any I/O\\
 * 	 &activity; here I/O activity is avoided by "reclaiming" a\\
 *
 * 	 &activity; here I/O activity is avoided by "reclaiming" a\\
 * 	 &page frame from the list of pages awaiting reallocation.\\
 * majflt     &the number of page faults serviced that required I/O\\
 * 	 &activity.\\
 * nswap      &the number of times a process was "swapped" out of main\\
 * 	 &memory\\
 * inblock    &the number of times the file system had to perform input.\\
 * oublock    &the number of times the file system had to perform output.\\
 * nvcsw      &the number of times a context switch resulted due to a\\
 * 	 &process voluntarily giving up the processor before its\\
 * 	 &time slice was completed (usually to await availability of\\
 * 	 &a resource).\\
 * nivcsw     &the number of times a context switch resulted due to a\\
 * 	 &higher priority process becoming runnable or because the\\
 * 	 &current process exceeded its time slice.\\
 * \end{tabular}
 *
 * The resource statistics are collected in a BAT. It can then
 * be queried. A default listing is produced by the command usagecmd.
 * (which should be moved to Monet)
 *
 * The BAT grows. It should be compacted.
 */
str
SYSioStatistics(int *ret)
{
#ifndef NATIVE_WIN32
	struct rusage ru;
#endif
	int i;
	BAT *b;

#ifndef NATIVE_WIN32
	getrusage(RUSAGE_SELF, &ru);
#endif
	b = BATnew(TYPE_str, TYPE_int, 32);
	if (b == 0)
		throw(MAL, "status.ioStatistics", MAL_MALLOC_FAIL);

#ifndef NATIVE_WIN32
	/* store counters, ignore errors */
	i = ru.ru_maxrss;
	BUNins(b, "maxrss", &i, FALSE);
	i = ru.ru_minflt;
	BUNins(b, "minflt", &i, FALSE);
	i = ru.ru_majflt;
	BUNins(b, "majflt", &i, FALSE);
	i = ru.ru_nswap;
	BUNins(b, "nswap", &i, FALSE);
	i = ru.ru_inblock;
	BUNins(b, "inblock", &i, FALSE);
	i = ru.ru_oublock;
	BUNins(b, "oublock", &i, FALSE);
	i = ru.ru_nvcsw;
	BUNins(b, "nvcsw", &i, FALSE);
	i = ru.ru_nivcsw;
	BUNins(b, "ninvcsw", &i, FALSE);
#else
	i = int_nil;
	BUNins(b, "maxrss", &i, FALSE);
	BUNins(b, "minflt", &i, FALSE);
	BUNins(b, "majflt", &i, FALSE);
	BUNins(b, "nswap", &i, FALSE);
	BUNins(b, "inblock", &i, FALSE);
	BUNins(b, "oublock", &i, FALSE);
	BUNins(b, "nvcsw", &i, FALSE);
	BUNins(b, "ninvcsw", &i, FALSE);
#endif

	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"gdk","io");
	return MAL_SUCCEED;
}

str
SYSgdkEnv(int *ret)
{
	int pbat = 0;
	int pdisk = 0;
	int pheat = 0;
	bat i;
	int tmp = 0, per = 0;
	BAT *b;

	b = BATnew(TYPE_str, TYPE_int, 32);
	if (b == 0)
		throw(MAL, "status.batStatistics", MAL_MALLOC_FAIL);

	for (i = 1; i < BBPsize; i++) {
		if (BBPvalid(i)) {
			pbat++;
			if (BBP_cache(i)) {
				pheat += BBP_lastused(i);
				if (BBP_cache(i)->batPersistence == PERSISTENT)
					per++;
				else
					tmp++;
			} else {
				pdisk++;
			}
		}
	}
	b = BUNins(b, "bats", &pbat, FALSE);
	b = BUNins(b, "tmpbats", &tmp, FALSE);
	b = BUNins(b, "perbats", &per, FALSE);
	b = BUNins(b, "ondisk", &pdisk, FALSE);
	b = BUNins(b, "todisk", &BBPout, FALSE);
	b = BUNins(b, "fromdisk", &BBPin, FALSE);
	if (!(b->batDirty & 2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"gdk","env");
	return MAL_SUCCEED;
}

str
SYSgdkThread(int *ret)
{
	BAT *b;
	int i;

	b = BATnew(TYPE_int, TYPE_str, THREADS);
	if (b == 0)
		throw(MAL, "status.getThreads", MAL_MALLOC_FAIL);

	for (i = 0; i < THREADS; i++) {
		if (GDKthreads[i].pid)
			BUNins(b, &GDKthreads[i].tid, GDKthreads[i].name, FALSE);
	}
	if (!(b->batDirty&2)) b = BATsetaccess(b, BAT_READ);
	pseudo(ret,b,"gdk","thread");
	return MAL_SUCCEED;
}
