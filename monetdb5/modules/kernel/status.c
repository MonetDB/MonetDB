/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include <time.h>
#include "mal_exception.h"
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif

#ifdef HAVE_SYS_TIMES_H
# include <sys/times.h>
#endif

#ifdef HAVE_SYS_RESOURCE_H
# include <sys/resource.h>
#endif

static int
pseudo(bat *ret, bat *ret2, BAT *bn, BAT *b) {
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	*ret2 = b->batCacheid;
	BBPkeepref(*ret2);
	return 0;
}

static str
SYSgetmem_cursize(lng *num)
{
	*num = GDKmem_cursize();
	return MAL_SUCCEED;
}

static str
SYSgetmem_maxsize(lng *num)
{
	*num = GDK_mem_maxsize;
	return MAL_SUCCEED;
}

static str
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

static str
SYSgetvm_cursize(lng *num)
{
	*num = GDKvm_cursize();
	return MAL_SUCCEED;
}

static str
SYSgetvm_maxsize(lng *num)
{
	*num = GDK_vm_maxsize;
	return MAL_SUCCEED;
}

static str
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

static str
SYScpuStatistics(bat *ret, bat *ret2)
{
	lng i;
	BAT *b, *bn;
#ifdef HAVE_TIMES
	struct tms newst;
# ifndef HZ
	static int HZ = 0;

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
	b = COLnew(0, TYPE_lng, 32, TRANSIENT);
	if (b == 0 || bn == 0){
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.cpuStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
#ifdef HAVE_TIMES
	if (clk == 0) {
		clk = time(0);
		times(&state);
	}
	times(&newst);
	/* store counters, ignore errors */
	i = (lng) (time(0) - clk);
	if (BUNappend(bn, "elapsed", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = newst.tms_utime * 1000 / HZ;
	if (BUNappend(bn, "user", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = (newst.tms_utime - state.tms_utime) * 1000 / HZ;
	if (BUNappend(bn, "elapuser", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = newst.tms_stime * 1000 / HZ;
	if (BUNappend(bn, "system", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = (newst.tms_stime - state.tms_stime) * 1000 / HZ;
	if (BUNappend(bn, "elapsystem", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;

	state = newst;
#else
	i = lng_nil;
	if (BUNappend(bn, "elapsed", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "user", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "elapuser", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "system", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "elapsystem", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
#endif
	if (pseudo(ret,ret2,bn,b))
		goto bailout;
	return MAL_SUCCEED;
  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "status.cpuStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static size_t memincr;
static str
SYSmemStatistics(bat *ret, bat *ret2)
{
	BAT *b, *bn;
	lng i;

	bn = COLnew(0,TYPE_str, 32, TRANSIENT);
	b = COLnew(0, TYPE_lng, 32, TRANSIENT);
	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.memStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* store counters, ignore errors */
	i = (lng) (GDKmem_cursize() - memincr);
	memincr = GDKmem_cursize();
	if (BUNappend(bn, "memincr", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	if (pseudo(ret,ret2,bn,b))
		goto bailout;
	return MAL_SUCCEED;
  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "status.memStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

#define heap(X1,X2,X3,X4)									\
	if (X1) {												\
		sz = HEAPmemsize(X2);								\
		if (sz > *minsize) {								\
			sprintf(buf, X4"/%s", s);						\
			if (BUNappend(bn, buf, false) != GDK_SUCCEED ||	\
				BUNappend(b, &sz, false) != GDK_SUCCEED)	\
				goto bailout;								\
		}													\
		X3 += sz; tot += sz;								\
	}
#define heapvm(X1,X2,X3,X4)									\
	if (X1) {												\
		sz = HEAPvmsize(X2);								\
		if (sz > *minsize) {								\
			sprintf(buf, X4"/%s", s);						\
			if (BUNappend(bn, buf, false) != GDK_SUCCEED ||	\
				BUNappend(b, &sz, false) != GDK_SUCCEED)	\
				goto bailout;								\
		}													\
		X3 += sz; tot += sz;								\
	}

static str
SYSmem_usage(bat *ret, bat *ret2, const lng *minsize)
{
	lng hbuns = 0, tbuns = 0, hhsh = 0, thsh = 0, hind = 0, tind = 0, head = 0, tail = 0, tot = 0, n = 0, sz;
	BAT *bn = COLnew(0, TYPE_str, 2 * getBBPsize(), TRANSIENT);
	BAT *b = COLnew(0, TYPE_lng, 2 * getBBPsize(), TRANSIENT);
	char buf[1024];
	bat i;

	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.memUsage", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	BBPlock();
	for (i = 1; i < getBBPsize(); i++) {
		BAT *c = BBPquickdesc(i, false);
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
			if (BUNappend(bn, buf, false) != GDK_SUCCEED ||
				BUNappend(b, &sz, false) != GDK_SUCCEED)
				goto bailout;
		}
		tot += (lng) sz;

		if (c == NULL || isVIEW(c)) {
			continue;
		}
		heap(1,c->theap,tbuns,"tbuns");
		heap(c->thash && c->thash != (Hash *) 1,&c->thash->heaplink,thsh,"thshl");
		heap(c->thash && c->thash != (Hash *) 1,&c->thash->heapbckt,thsh,"thshb");
		heap(c->tvheap,c->tvheap,tail,"tail");
	}
	/* totals per category */
	if (BUNappend(bn, "_tot/hbuns", false) != GDK_SUCCEED ||
		BUNappend(b, &hbuns, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/tbuns", false) != GDK_SUCCEED ||
		BUNappend(b, &tbuns, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/head", false) != GDK_SUCCEED ||
		BUNappend(b, &head, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/tail", false) != GDK_SUCCEED ||
		BUNappend(b, &tail, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/hhsh", false) != GDK_SUCCEED ||
		BUNappend(b, &hhsh, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/thsh", false) != GDK_SUCCEED ||
		BUNappend(b, &thsh, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/hind", false) != GDK_SUCCEED ||
		BUNappend(b, &hind, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/tind", false) != GDK_SUCCEED ||
		BUNappend(b, &tind, false) != GDK_SUCCEED)
		goto bailout;

	/* special area 1: BBP rec */
	sz = BBPlimit * sizeof(BBPrec) + n;
	if (BUNappend(bn, "_tot/bbp", false) != GDK_SUCCEED ||
		BUNappend(b, &sz, false) != GDK_SUCCEED)
		goto bailout;
	tot += sz;

	/* this concludes all major traceable Monet memory usages */
	tot += sz;
	if (BUNappend(bn, "_tot/found", false) != GDK_SUCCEED ||
		BUNappend(b, &tot, false) != GDK_SUCCEED)
		goto bailout;

	/* now look at what the global statistics report (to see if it coincides) */

	/* measure actual heap size, includes wasted fragmented space and anon mmap space used by malloc() */
	sz = GDKmem_cursize();
	if (BUNappend(bn, "_tot/heap", false) != GDK_SUCCEED ||
		BUNappend(b, &sz, false) != GDK_SUCCEED)
		goto bailout;

	tot = GDKmem_cursize();

	/* allocated swap area memory that is not plain malloc() */
	sz = MAX(0, sz - tot);
	if (BUNappend(bn, "_tot/valloc", false) != GDK_SUCCEED ||
		BUNappend(b, &sz, false) != GDK_SUCCEED)
		goto bailout;

	/* swap-area memory is in either GDKvmalloc or heap */
	if (BUNappend(bn, "_tot/swapmem", false) != GDK_SUCCEED ||
		BUNappend(b, &tot, false) != GDK_SUCCEED)
		goto bailout;

	BBPunlock();
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	*ret2 = b->batCacheid;
	BBPkeepref(b->batCacheid);

	return MAL_SUCCEED;

  bailout:
	BBPunlock();
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "status.memUsage", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
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
		throw(MAL, "status.vmStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		heapvm(1,c->theap,tbuns,"tcuns");
		heapvm(c->thash && c->thash != (Hash *) 1,&c->thash->heaplink,thsh,"thshl");
		heapvm(c->thash && c->thash != (Hash *) 1,&c->thash->heapbckt,thsh,"thshb");
		heapvm(c->tvheap,c->tvheap,tail,"tail");
	}
	/* totals per category */
	if (BUNappend(bn, "_tot/hbuns", false) != GDK_SUCCEED ||
		BUNappend(b, &hbuns, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/tbuns", false) != GDK_SUCCEED ||
		BUNappend(b, &tbuns, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/head", false) != GDK_SUCCEED ||
		BUNappend(b, &head, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/tail", false) != GDK_SUCCEED ||
		BUNappend(b, &tail, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/hhsh", false) != GDK_SUCCEED ||
		BUNappend(b, &hhsh, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/thsh", false) != GDK_SUCCEED ||
		BUNappend(b, &thsh, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/hind", false) != GDK_SUCCEED ||
		BUNappend(b, &hind, false) != GDK_SUCCEED ||
		BUNappend(bn, "_tot/tind", false) != GDK_SUCCEED ||
		BUNappend(b, &tind, false) != GDK_SUCCEED)
		goto bailout;

	/* special area 1: BBP rec */
	sz = BBPlimit * sizeof(BBPrec);
	if (BUNappend(bn, "_tot/bbp", false) != GDK_SUCCEED ||
		BUNappend(b, &sz, false) != GDK_SUCCEED)
		goto bailout;
	tot += sz;


	/* this concludes all major traceable Monet virtual memory usages */
	tot += sz;
	if (BUNappend(bn, "_tot/found", false) != GDK_SUCCEED ||
		BUNappend(b, &tot, false) != GDK_SUCCEED)
		goto bailout;

	/* all VM is either GDKmmap or GDKvmalloc (possibly redirected GDKmalloc), *plus* the heap */
	sz = GDKvm_cursize();
	if (BUNappend(bn, "_tot/vm", false) != GDK_SUCCEED ||
		BUNappend(b, &sz, false) != GDK_SUCCEED)
		goto bailout;

	BBPunlock();
	*ret = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	*ret2 = b->batCacheid;
	BBPkeepref(b->batCacheid);
	return MAL_SUCCEED;

  bailout:
	BBPunlock();
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "status.vmStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
static str
SYSioStatistics(bat *ret, bat *ret2)
{
#ifdef HAVE_SYS_RESOURCE_H
	struct rusage ru;
#endif
	lng i;
	BAT *b, *bn;

#ifdef HAVE_SYS_RESOURCE_H
	getrusage(RUSAGE_SELF, &ru);
#endif
	bn = COLnew(0, TYPE_str, 32, TRANSIENT);
	b = COLnew(0, TYPE_lng, 32, TRANSIENT);
	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.ioStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

#ifdef HAVE_SYS_RESOURCE_H
	/* store counters, ignore errors */
	i = ru.ru_maxrss;
	if (BUNappend(bn, "maxrss", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = ru.ru_minflt;
	if (BUNappend(bn, "minflt", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = ru.ru_majflt;
	if (BUNappend(bn, "majflt", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = ru.ru_nswap;
	if (BUNappend(bn, "nswap", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = ru.ru_inblock;
	if (BUNappend(bn, "inblock", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = ru.ru_oublock;
	if (BUNappend(bn, "oublock", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = ru.ru_nvcsw;
	if (BUNappend(bn, "nvcsw", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
	i = ru.ru_nivcsw;
	if (BUNappend(bn, "ninvcsw", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
#else
	i = lng_nil;
	if (BUNappend(bn, "maxrss", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "minflt", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "majflt", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "nswap", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "inblock", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "oublock", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "nvcsw", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED ||
		BUNappend(bn, "ninvcsw", false) != GDK_SUCCEED ||
		BUNappend(b, &i, false) != GDK_SUCCEED)
		goto bailout;
#endif

	if (pseudo(ret,ret2,bn,b))
		goto bailout;
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "status.ioStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
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
		throw(MAL, "status.batStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i = 1; i < getBBPsize(); i++) {
		if (BBPvalid(i)) {
			pbat++;
			if (BBP_cache(i)) {
				if (BBP_cache(i)->batTransient)
					tmp++;
				else
					per++;
			} else {
				pdisk++;
			}
		}
	}
	if (BUNappend(bn, "bats", false) != GDK_SUCCEED ||
		BUNappend(b, &pbat, false) != GDK_SUCCEED ||
		BUNappend(bn, "tmpbats", false) != GDK_SUCCEED ||
		BUNappend(b, &tmp, false) != GDK_SUCCEED ||
		BUNappend(bn, "perbats", false) != GDK_SUCCEED ||
		BUNappend(b, &per, false) != GDK_SUCCEED ||
		BUNappend(bn, "ondisk", false) != GDK_SUCCEED ||
		BUNappend(b, &pdisk, false) != GDK_SUCCEED ||
		pseudo(ret,ret2, bn,b)) {
		BBPunfix(b->batCacheid);
		BBPunfix(bn->batCacheid);
		throw(MAL, "status.batStatistics", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return MAL_SUCCEED;
}

static str
SYSgdkThread(bat *ret, bat *ret2)
{
	BAT *b, *bn;
	int i;
	Thread thr;

	bn = COLnew(0,TYPE_int, THREADS, TRANSIENT);
	b = COLnew(0, TYPE_str, THREADS, TRANSIENT);
	if (b == 0 || bn == 0) {
		if ( b) BBPunfix(b->batCacheid);
		if ( bn) BBPunfix(bn->batCacheid);
		throw(MAL, "status.getThreads", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (i = 1; i <= THREADS; i++) {
		thr = THRget(i);
		if (ATOMIC_GET(&thr->pid)){
			if (BUNappend(bn, &thr->tid, false) != GDK_SUCCEED ||
				BUNappend(b, thr->name, false) != GDK_SUCCEED)
				goto bailout;
		}
	}
	if (pseudo(ret,ret2,bn,b))
		goto bailout;
	return MAL_SUCCEED;

  bailout:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "status.getThreads", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

#include "mel.h"
mel_func status_init_funcs[] = {
 command("status", "cpuStatistics", SYScpuStatistics, false, "Global cpu usage information", args(2,2, batarg("",str),batarg("",lng))),
 command("status", "memStatistics", SYSmemStatistics, false, "Global memory usage information", args(2,2, batarg("",str),batarg("",lng))),
 command("status", "ioStatistics", SYSioStatistics, false, "Global IO activity information", args(2,2, batarg("",str),batarg("",lng))),
 command("status", "vmStatistics", SYSvm_usage, false, "Get a split-up of how much virtual memory blocks are in use", args(2,3, batarg("",str),batarg("",lng),arg("minsize",lng))),
 command("status", "memUsage", SYSmem_usage, false, "Get a split-up of how much memory blocks are in use", args(2,3, batarg("",str),batarg("",lng),arg("minsize",lng))),
 command("status", "batStatistics", SYSgdkEnv, false, "Show distribution of bats by kind", args(2,2, batarg("",str),batarg("",str))),
 command("status", "getThreads", SYSgdkThread, false, "Produce overview of active threads", args(2,2, batarg("",int),batarg("",str))),
 command("status", "mem_cursize", SYSgetmem_cursize, false, "The amount of physical swapspace in KB that is currently in use", args(1,1, arg("",lng))),
 command("status", "mem_maxsize", SYSgetmem_maxsize, false, "The maximum usable amount of physical swapspace in KB (target only)", args(1,1, arg("",lng))),
 command("status", "mem_maxsize", SYSsetmem_maxsize, false, "Set the maximum usable amount of physical swapspace in KB", args(1,2, arg("",void),arg("v",lng))),
 command("status", "vm_cursize", SYSgetvm_cursize, false, "The amount of logical VM space in KB that is currently in use", args(1,1, arg("",lng))),
 command("status", "vm_maxsize", SYSgetvm_maxsize, false, "The maximum usable amount of logical VM space in KB (target only)", args(1,1, arg("",lng))),
 command("status", "vm_maxsize", SYSsetvm_maxsize, false, "Set the maximum usable amount of physical swapspace in KB", args(1,2, arg("",void),arg("v",lng))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_status_mal)
{ mal_module("status", NULL, status_init_funcs); }
