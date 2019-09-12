/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * 2014-2016 author Martin Kersten
 * The header block contains the mapping from OIDs to chunks
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"

// add the chunk to the index to facilitate 'fast' OID-based access
void
MOSupdateHeader(MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;

    hdr->blks[MOSgetTag(task->blk)]++;
    hdr->elms[MOSgetTag(task->blk)] += MOSgetCnt(task->blk);

	if( hdr->top < MOSAICINDEX-1 ){
		if( hdr->top == 0){
			hdr->top++;
		}
		hdr->top++;
	}
}

void
MOSinitHeader(MOStask task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	int i;
	for(i=0; i < MOSAIC_METHODS; i++){
		hdr->elms[i] = hdr->blks[i] = METHOD_NOT_AVAILABLE;
		task->range[i]=0;
		task->factor[i]=0.0;
	}
	hdr->ratio = 0;
	hdr->version = MOSAIC_VERSION;
	hdr->top = 0;
	hdr->checksum.sumlng = 0;
	hdr->checksum2.sumlng = 0;

	for(i=0; i < 256; i++){
		hdr->dictfreq[i]=0;
	}
}

// position the task on the mosaic blk to be scanned
void
MOSinitializeScan(MOStask task, BAT* /*compressed*/ b)
{
	task->blk = (MosaicBlk) (((char*)task->hdr) + MosaicHdrSize);

	task->start = 0;
	task->stop = b->batCount;
}
