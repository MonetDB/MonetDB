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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * (co)  Martin L. Kersten 
 * This module provide a lightweight map-reduce scheduler for multicore systems.
 * A limited number of workers are initialized upfront, which take the tasks
 * from a central queue. The header of these task descriptors should comply
 * with the MRtask structure.
 *
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_mapreduce.h"

typedef struct {
	MT_Sema *sema;			/* micro scheduler handle */
	void (*cmd) (void *);		/* the function to be executed */
	void *arg;			/* the arguments of the function */
} MRtask;

/* each entry in the queue contains a list of tasks */
typedef struct MRQUEUE {
	MRtask *tasks;
	int index;		/* next available task */
	int size;		/* number of tasks */
} MRqueue;

static MRqueue *mrqueue;
static int mrqsize = -1;	/* size of queue */
static int mrqlast = -1;
/* it's a shared resource, so we need locks */
static MT_Lock mrqlock MT_LOCK_INITIALIZER("mrqlock");
static MT_Sema mrqsema;		/* threads wait on empty queues */


static void MRworker(void *);

/* There is just a single queue for the workers */
static void
MRqueueCreate(int sz)
{
	int i;
	MT_Id tid;

#ifdef NEED_MT_LOCK_INIT
	MT_lock_init(&mrqlock, "mrqlock");
#endif
	MT_lock_set(&mrqlock, "MRqueueCreate");
	MT_sema_init(&mrqsema, 0, "mrqsema");
	if (mrqueue) {
		MT_lock_unset(&mrqlock, "MRqueueCreate");
		GDKerror("One map-reduce queue allowed");
		return;
	}
	sz *= 2;
	mrqueue = (MRqueue *) GDKzalloc(sizeof(MRqueue) * sz);
	if (mrqueue == 0) {
		MT_lock_unset(&mrqlock, "MRqueueCreate");
		GDKerror("Could not create the map-reduce queue");
		return;
	}
	mrqsize = sz;
	mrqlast = 0;
	/* create a worker thread for each core as specified as system parameter */
	for (i = 0; i < GDKnr_threads; i++)
		MT_create_thread(&tid, MRworker, (void *) 0, MT_THR_DETACHED);
	MT_lock_unset(&mrqlock, "MRqueueCreate");
}

static void
MRenqueue(int taskcnt, MRtask *tasks)
{
	assert(taskcnt > 0);
	MT_lock_set(&mrqlock, "MRenqueue");
	if (mrqlast == mrqsize) {
		mrqsize <<= 1;
		mrqueue = (MRqueue *) GDKrealloc(mrqueue, sizeof(MRqueue) * mrqsize);
		if (mrqueue == 0) {
			MT_lock_unset(&mrqlock, "MRenqueue");
			GDKerror("Could not enlarge the map-reduce queue");
			return;
		}
	}
	mrqueue[mrqlast].index = 0;
	mrqueue[mrqlast].tasks = tasks;
	mrqueue[mrqlast].size = taskcnt;
	mrqlast++;
	MT_lock_unset(&mrqlock, "MRenqueue");
	/* a task list is added for consumption */
	while (taskcnt-- > 0)
		MT_sema_up(&mrqsema, "MRenqueue");
}

static MRtask *
MRdequeue(void)
{
	MRtask *r = NULL;
	int idx;

	MT_sema_down(&mrqsema, "MRdequeue");
	assert(mrqlast);
	MT_lock_set(&mrqlock, "MRdequeue");
	if (mrqlast > 0) {
		idx = mrqueue[mrqlast - 1].index;
		r = &mrqueue[mrqlast - 1].tasks[idx++];
		if (mrqueue[mrqlast - 1].size == idx)
			mrqlast--;
		else
			mrqueue[mrqlast - 1].index = idx;
	}
	MT_lock_unset(&mrqlock, "MRdequeue");
	assert(r);
	return r;
}

static void
MRworker(void *arg)
{
	MRtask *task;
	(void) arg;
	do {
		task = MRdequeue();
		(task->cmd) (task->arg);
		MT_sema_up(task->sema, "MRworker");
	} while (1);
}

/* schedule the tasks and return when all are done */
void
MRschedule(int taskcnt, void **arg, void (*cmd) (void *p))
{
	int i;
	MT_Sema sema;
	MRtask *task = GDKmalloc(taskcnt * sizeof(MRtask));

	if (mrqueue == 0)
		MRqueueCreate(1024);

	MT_sema_init(&sema, 0, "sema");
	for (i = 0; i < taskcnt; i++) {
		task[i].sema = &sema;
		task[i].cmd = cmd;
		task[i].arg = arg ? arg[i] : NULL;
	}
	MRenqueue(taskcnt, task);
	/* waiting for all report result */
	for (i = 0; i < taskcnt; i++)
		MT_sema_down(&sema, "MRschedule");
	MT_sema_destroy(&sema);
	GDKfree(task);
}
