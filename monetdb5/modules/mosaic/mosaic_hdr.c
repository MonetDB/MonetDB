/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 *  authors Martin Kersten, Aris Koning
 * The header block contains the mapping from OIDs to chunks, which should become
 * the basis for processing mitosis over a mosaic file.
 */

#include "monetdb_config.h"
#include "mosaic.h"
#include "mosaic_hdr.h"

// add the chunk to the index to facilitate 'fast' OID-based access
void
MOSupdateHeader(MOStask* task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;

    hdr->blks[MOSgetTag(task->blk)]++;
    hdr->elms[MOSgetTag(task->blk)] += MOSgetCnt(task->blk);

	if( hdr->top < METHOD_NOT_AVAILABLE -1 ){
		if( hdr->top == 0){
			hdr->top++;
		}
		hdr->top++;
	}
}

void
MOSinitHeader(MOStask* task)
{
	MosaicHdr hdr = (MosaicHdr) task->hdr;
	int i;
	for(i=0; i < MOSAIC_METHODS; i++){
		hdr->elms[i] = hdr->blks[i] = METHOD_NOT_AVAILABLE;
	}
	hdr->ratio = 0;
	hdr->version = MOSAIC_VERSION;
	hdr->top = 0;
	hdr->bits_dict = 0;
	hdr->pos_dict = 0;
	hdr->length_dict = 0;
	hdr->bits_dict256 = 0;
	hdr->pos_dict256 = 0;
	hdr->length_dict256 = 0;

	task->bsrc->tmosaic->free = MosaicHdrSize;
}

// position the task on the mosaic blk to be scanned
void
MOSinitializeScan(MOStask* task, BAT* /*compressed*/ b)
{
	task->blk = (MosaicBlk) (((char*)task->hdr) + MosaicHdrSize);

	task->start = 0;
	task->stop = b->batCount;
}
