/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
pseudo(bat *ret, bat *ret2, BAT *bn, BAT *b) {
	BATmode(bn,TRANSIENT);
	BATmode(b,TRANSIENT);
	BATfakeCommit(b);
	BATfakeCommit(bn);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	*ret2 = b->batCacheid;
	BBPkeepref(*ret2);
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
SYSsetmem_maxsize(void *ret, const lng *num)
{
	size_t sze = 0;
	(void) ret;
	if (*num < 0)
		throw(ILLARG, "status.mem_maxsize", "new size must not be < 0");
#if SIZEOF_SIZE_T == SIZEOF_INT
	{
		lng size_t_max = 2 * (lng)INT_MAX;
		if (*num > size_t_max)
			throw(ILLARG, "status.mem_maxsize", "new size must not be > " LLFMT, size_t_max);
	}
#endif
	GDK_mem_maxsize = sze;
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
SYSsetvm_maxsize(void *ret, const lng *num)
{
	(void) ret;
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
SYScpuStatistics(bat *ret, bat *ret2)
{
	int i;
	BAT *b, *bn;
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

	bn = COLnew(0, TYPE_str, 32, TRANSIENT);
	b = COLnew(0, TYPE_int, 32, TRANSIENT);
	if (b == 0 || bn == 0){
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.cpuStatistics", MAL_MALLOC_FAIL);
	}
#ifdef HAVE_TIMES
	if (clk == 0) {
		clk = time(0);
		times(&state);
	}
	times(&newst);
	/* store counters, ignore errors */
	i = (int) (time(0) - clk);
	BUNappend(bn, "elapsed", FALSE);
	BUNappend(b, &i, FALSE);
	i = newst.tms_utime * 1000 / HZ;
	BUNappend(bn, "user", FALSE);
	BUNappend(b, &i, FALSE);
	i = (newst.tms_utime - state.tms_utime) * 1000 / HZ;
	BUNappend(bn, "elapuser", FALSE);
	BUNappend(b, &i, FALSE);
	i = newst.tms_stime * 1000 / HZ;
	BUNappend(bn, "system", FALSE);
	BUNappend(b, &i, FALSE);
	i = (newst.tms_stime - state.tms_stime) * 1000 / HZ;
	BUNappend(bn, "elapsystem", FALSE);
	BUNappend(b, &i, FALSE);

	state = newst;
#else
	i = int_nil;
	BUNappend(bn, "elapsed", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "user", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "elapuser", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "system", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "elapsystem", FALSE);
	BUNappend(b, &i, FALSE);
#endif
	pseudo(ret,ret2,bn,b);
	return MAL_SUCCEED;
}

static size_t memincr;
str
SYSmemStatistics(bat *ret, bat *ret2)
{
	struct Mallinfo m;
	BAT *b, *bn;
	lng i;

	m = MT_mallinfo();

	bn = COLnew(0,TYPE_str, 32, TRANSIENT);
	b = COLnew(0, TYPE_lng, 32, TRANSIENT);
	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.memStatistics", MAL_MALLOC_FAIL);
	}

	/* store counters, ignore errors */
	i = (lng) (GDKmem_cursize() - memincr);
	memincr = GDKmem_cursize();
	BUNappend(bn, "memincr", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.arena;
	BUNappend(bn, "arena", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.ordblks;
	BUNappend(bn, "ordblks", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.smblks;
	BUNappend(bn, "smblks", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.hblkhd;
	BUNappend(bn, "hblkhd", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.hblks;
	BUNappend(bn, "hblks", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.usmblks;
	BUNappend(bn, "usmblks", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.fsmblks;
	BUNappend(bn, "fsmblks", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.uordblks;
	BUNappend(bn, "uordblks", FALSE);
	BUNappend(b, &i, FALSE);
	i = (lng) m.fordblks;
	BUNappend(bn, "fordblks", FALSE);
	BUNappend(b, &i, FALSE);
	pseudo(ret,ret2,bn,b);
	return MAL_SUCCEED;
}

#define heap(X1,X2,X3,X4)\
	if (X1) {\
		sz = HEAPmemsize(X2);\
		if (sz > *minsize) {\
			sprintf(buf, X4"/%s", s);\
			BUNappend(bn, buf, FALSE);\
			BUNappend(b, &sz, FALSE);\
		}\
		X3 += sz; tot += sz;\
	}
#define heapvm(X1,X2,X3,X4)\
	if (X1) {\
		sz = HEAPvmsize(X2);\
		if (sz > *minsize) {\
			sprintf(buf, X4"/%s", s);\
			BUNappend(bn, buf, FALSE);\
			BUNappend(b, &sz, FALSE);\
		}\
		X3 += sz; tot += sz;\
	}

str
SYSmem_usage(bat *ret, bat *ret2, const lng *minsize)
{
	lng hbuns = 0, tbuns = 0, hhsh = 0, thsh = 0, hind = 0, tind = 0, head = 0, tail = 0, tot = 0, n = 0, sz;
	BAT *bn = COLnew(0, TYPE_str, 2 * getBBPsize(), TRANSIENT);
	BAT *b = COLnew(0, TYPE_lng, 2 * getBBPsize(), TRANSIENT);
	struct Mallinfo m;
	char buf[1024];
	bat i;

	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.memUsage", MAL_MALLOC_FAIL);
	}
	BBPlock();
	for (i = 1; i < getBBPsize(); i++) {
		BAT *c = BBPquickdesc(i,0);
		str s;

		if( c == NULL  || !BBPvalid(i))
			continue;

		s = BBPname(i);
		sz = 0;
		if (BBP_desc(i))
			sz += sizeof(BAT);
		if (BBP_logical(i))
			n += strLen(BBP_logical(i));
		if (BBP_physical(i))
			n += strLen(BBP_physical(i));

		if (sz > *minsize) {
			sprintf(buf, "desc/%s", s);
			BUNappend(bn, buf, FALSE);
			BUNappend(b, &sz, FALSE);
		}
		tot += (lng) sz;

		if (c == NULL || isVIEW(c)) {
			continue;
		}
		heap(1,&c->theap,tbuns,"tbuns");
		heap(c->thash && c->thash != (Hash *) 1,c->thash->heap,thsh,"thsh");
		heap(c->tvheap,c->tvheap,tail,"tail");
	}
	/* totals per category */
	BUNappend(bn, "_tot/hbuns", FALSE);
	BUNappend(b, &hbuns, FALSE);
	BUNappend(bn, "_tot/tbuns", FALSE);
	BUNappend(b, &tbuns, FALSE);
	BUNappend(bn, "_tot/head", FALSE);
	BUNappend(b, &head, FALSE);
	BUNappend(bn, "_tot/tail", FALSE);
	BUNappend(b, &tail, FALSE);
	BUNappend(bn, "_tot/hhsh", FALSE);
	BUNappend(b, &hhsh, FALSE);
	BUNappend(bn, "_tot/thsh", FALSE);
	BUNappend(b, &thsh, FALSE);
	BUNappend(bn, "_tot/hind", FALSE);
	BUNappend(b, &hind, FALSE);
	BUNappend(bn, "_tot/tind", FALSE);
	BUNappend(b, &tind, FALSE);

	/* special area 1: BBP rec */
	sz = BBPlimit * sizeof(BBPrec) + n;
	BUNappend(bn, "_tot/bbp", FALSE);
	BUNappend(b, &sz, FALSE);
	tot += sz;

	/* this concludes all major traceable Monet memory usages */
	tot += sz;
	BUNappend(bn, "_tot/found", FALSE);
	BUNappend(b, &tot, FALSE);

	/* now look at what the global statistics report (to see if it coincides) */

	/* how much *used* bytes in heap? */
	m = MT_mallinfo();
	sz = (size_t) m.usmblks + (size_t) m.uordblks + (size_t) m.hblkhd;
	BUNappend(bn, "_tot/malloc", FALSE);
	BUNappend(b, &sz, FALSE);

	/* measure actual heap size, includes wasted fragmented space and anon mmap space used by malloc() */
	sz = GDKmem_cursize();
	BUNappend(bn, "_tot/heap", FALSE);
	BUNappend(b, &sz, FALSE);

	tot = GDKmem_cursize();

	/* allocated swap area memory that is not plain malloc() */
	sz = MAX(0, sz - tot);
	BUNappend(bn, "_tot/valloc", FALSE);
	BUNappend(b, &sz, FALSE);

	/* swap-area memory is in either GDKvmalloc or heap */
	BUNappend(bn, "_tot/swapmem", FALSE);
	BUNappend(b, &tot, FALSE);

	BBPunlock();
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	*ret2 = b->batCacheid;
	BBPkeepref(b->batCacheid);

	return MAL_SUCCEED;
}

str
SYSvm_usage(bat *ret, bat *ret2, const lng *minsize)
{
	lng hbuns = 0, tbuns = 0, hhsh = 0, thsh = 0, hind = 0, tind = 0, head = 0, tail = 0, tot = 0, sz;
	BAT *bn = COLnew(0, TYPE_str, 2 * getBBPsize(), TRANSIENT);
	BAT *b = COLnew(0, TYPE_lng, 2 * getBBPsize(), TRANSIENT);
	char buf[1024];
	bat i;

	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.vmStatistics", MAL_MALLOC_FAIL);
	}
	BBPlock();
	for (i = 1; i < getBBPsize(); i++) {
		BAT *c;
		str s;

		if (!BBPvalid(i))
			continue;

		s = BBPname(i);
 		c = BBP_cache(i);
		if (c == NULL || isVIEW(c)) {
			continue;
		}
		heapvm(1,&c->theap,tbuns,"tcuns");
		heapvm(c->thash && c->thash != (Hash *) 1,c->thash->heap,thsh,"thsh");
		heapvm(c->tvheap,c->tvheap,tail,"tail");
	}
	/* totals per category */
	BUNappend(bn, "_tot/hbuns", FALSE);
	BUNappend(b, &hbuns, FALSE);
	BUNappend(bn, "_tot/tbuns", FALSE);
	BUNappend(b, &tbuns, FALSE);
	BUNappend(bn, "_tot/head", FALSE);
	BUNappend(b, &head, FALSE);
	BUNappend(bn, "_tot/tail", FALSE);
	BUNappend(b, &tail, FALSE);
	BUNappend(bn, "_tot/hhsh", FALSE);
	BUNappend(b, &hhsh, FALSE);
	BUNappend(bn, "_tot/thsh", FALSE);
	BUNappend(b, &thsh, FALSE);
	BUNappend(bn, "_tot/hind", FALSE);
	BUNappend(b, &hind, FALSE);
	BUNappend(bn, "_tot/tind", FALSE);
	BUNappend(b, &tind, FALSE);

	/* special area 1: BBP rec */
	sz = BBPlimit * sizeof(BBPrec);
	BUNappend(bn, "_tot/bbp", FALSE);
	BUNappend(b, &sz, FALSE);
	tot += sz;


	/* this concludes all major traceable Monet virtual memory usages */
	tot += sz;
	BUNappend(bn, "_tot/found", FALSE);
	BUNappend(b, &tot, FALSE);

	/* all VM is either GDKmmap or GDKvmalloc (possibly redirected GDKmalloc), *plus* the heap */
	sz = GDKvm_cursize();
	BUNappend(bn, "_tot/vm", FALSE);
	BUNappend(b, &sz, FALSE);

	BBPunlock();
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	*ret2 = b->batCacheid;
	BBPkeepref(b->batCacheid);
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
SYSioStatistics(bat *ret, bat *ret2)
{
#ifndef NATIVE_WIN32
	struct rusage ru;
#endif
	int i;
	BAT *b, *bn;

#ifndef NATIVE_WIN32
	getrusage(RUSAGE_SELF, &ru);
#endif
	bn = COLnew(0, TYPE_str, 32, TRANSIENT);
	b = COLnew(0, TYPE_int, 32, TRANSIENT);
	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.ioStatistics", MAL_MALLOC_FAIL);
	}

#ifndef NATIVE_WIN32
	/* store counters, ignore errors */
	i = ru.ru_maxrss;
	BUNappend(bn, "maxrss", FALSE);
	BUNappend(b, &i, FALSE);
	i = ru.ru_minflt;
	BUNappend(bn, "minflt", FALSE);
	BUNappend(b, &i, FALSE);
	i = ru.ru_majflt;
	BUNappend(bn, "majflt", FALSE);
	BUNappend(b, &i, FALSE);
	i = ru.ru_nswap;
	BUNappend(bn, "nswap", FALSE);
	BUNappend(b, &i, FALSE);
	i = ru.ru_inblock;
	BUNappend(bn, "inblock", FALSE);
	BUNappend(b, &i, FALSE);
	i = ru.ru_oublock;
	BUNappend(bn, "oublock", FALSE);
	BUNappend(b, &i, FALSE);
	i = ru.ru_nvcsw;
	BUNappend(bn, "nvcsw", FALSE);
	BUNappend(b, &i, FALSE);
	i = ru.ru_nivcsw;
	BUNappend(bn, "ninvcsw", FALSE);
	BUNappend(b, &i, FALSE);
#else
	i = int_nil;
	BUNappend(bn, "maxrss", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "minflt", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "majflt", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "nswap", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "inblock", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "oublock", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "nvcsw", FALSE);
	BUNappend(b, &i, FALSE);
	BUNappend(bn, "ninvcsw", FALSE);
	BUNappend(b, &i, FALSE);
#endif

	pseudo(ret,ret2,bn,b);
	return MAL_SUCCEED;
}

str
SYSgdkEnv(bat *ret, bat *ret2)
{
	int pbat = 0;
	int pdisk = 0;
	bat i;
	int tmp = 0, per = 0;
	BAT *b,*bn;

	bn = COLnew(0, TYPE_str, 32, TRANSIENT);
	b = COLnew(0, TYPE_int, 32, TRANSIENT);
	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.batStatistics", MAL_MALLOC_FAIL);
	}

	for (i = 1; i < getBBPsize(); i++) {
		if (BBPvalid(i)) {
			pbat++;
			if (BBP_cache(i)) {
				if (BBP_cache(i)->batPersistence == PERSISTENT)
					per++;
				else
					tmp++;
			} else {
				pdisk++;
			}
		}
	}
	BUNappend(bn, "bats", FALSE);
	BUNappend(b, &pbat, FALSE);
	BUNappend(bn, "tmpbats", FALSE);
	BUNappend(b, &tmp, FALSE);
	BUNappend(bn, "perbats", FALSE);
	BUNappend(b, &per, FALSE);
	BUNappend(bn, "ondisk", FALSE);
	BUNappend(b, &pdisk, FALSE);
	BUNappend(bn, "todisk", FALSE);
	BUNappend(b, &BBPout, FALSE);
	BUNappend(bn, "fromdisk", FALSE);
	BUNappend(b, &BBPin, FALSE);
	pseudo(ret,ret2, bn,b);
	return MAL_SUCCEED;
}

str
SYSgdkThread(bat *ret, bat *ret2)
{
	BAT *b, *bn;
	int i;

	bn = COLnew(0,TYPE_int, THREADS, TRANSIENT);
	b = COLnew(0, TYPE_str, THREADS, TRANSIENT);
	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.getThreads", MAL_MALLOC_FAIL);
	}

	for (i = 0; i < THREADS; i++) {
		if (GDKthreads[i].pid){
			BUNappend(bn, &GDKthreads[i].tid, FALSE);
			BUNappend(b, GDKthreads[i].name? GDKthreads[i].name:"", FALSE);
		}
	}
	pseudo(ret,ret2,bn,b);
	return MAL_SUCCEED;
}
