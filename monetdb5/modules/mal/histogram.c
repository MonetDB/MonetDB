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
 * @f histogram
 * @a Martin Kersten, Stratos Idreos
 * @v 1
 * @+ Histogram
 * This support module is geared at handling small equi-bin histograms.
 */
/*
 * @-
 */
#include "monetdb_config.h"
#include "histogram.h"

Histogram
HSTnew(int bins, ValPtr minval, ValPtr maxval)
{
	Histogram h;
	int i=0;

	assert( minval && maxval);
	assert( minval->vtype == maxval->vtype);
	assert( bins > 0);

	h = (Histogram) GDKzalloc(sizeof(*h) + sizeof(dbl) * 2 * bins);
	h->low = *minval;
	h->hgh = h->inc = *maxval;
	h->bins = bins;
	h->total = 0;
	for ( i = 0; i < 2*h->bins;  i++)
		h->cnt[i]=0;

	switch( minval->vtype){
		case TYPE_bte:
			h->inc.val.btval = (maxval->val.btval - minval->val.btval)/ bins + 1;
			assert(h->inc.val.btval);
			break;
		case TYPE_sht:
			h->inc.val.shval = (maxval->val.shval - minval->val.shval)/ bins + 1;
			assert(h->inc.val.shval);
			break;
		case TYPE_int:
			h->inc.val.ival = (maxval->val.ival - minval->val.ival)/ bins + 1;
			assert(h->inc.val.ival);
			break;
		case TYPE_lng:
			h->inc.val.lval = (maxval->val.lval - minval->val.lval)/ bins + 1;
			assert(h->inc.val.lval);
			break;
		case TYPE_dbl:
			h->inc.val.dval = (maxval->val.dval - minval->val.dval)/ bins + 1;
			assert(h->inc.val.dval);
			break;
		case TYPE_flt:
			h->inc.val.fval = (maxval->val.fval - minval->val.fval)/ bins + 1;
			assert(h->inc.val.fval);
	}
	return h;
}

/*get the bin we would increment if we would add this value*/
int
HSTgetIndex(Histogram h, ValPtr val){
	int index =0;
	switch( val->vtype){
		case TYPE_bte:
			if (val->val.btval > h->hgh.val.btval)
				index= (h->hgh.val.btval - h->low.val.btval) / h->inc.val.btval;
			else
				index= (val->val.btval - h->low.val.btval) / h->inc.val.btval;
			break;
		case TYPE_sht:
			if (val->val.shval > h->hgh.val.shval)
				index= (h->hgh.val.shval - h->low.val.shval) / h->inc.val.shval;
			else
				index= (val->val.shval - h->low.val.shval) / h->inc.val.shval;
			break;
		case TYPE_int:
			if (val->val.ival > h->hgh.val.ival)
				index= (h->hgh.val.ival - h->low.val.ival) / h->inc.val.ival;
			else
				index= (val->val.ival - h->low.val.ival) / h->inc.val.ival;
			break;
		case TYPE_lng:
			if (val->val.lval > h->hgh.val.lval)
				index= (int) ((h->hgh.val.lval - h->low.val.lval) / h->inc.val.lval);
			else
				index= (int) ((val->val.lval - h->low.val.lval) / h->inc.val.lval);
			break;
		case TYPE_dbl:
			if (val->val.dval > h->hgh.val.dval)
				index= (int) ((h->hgh.val.dval - h->low.val.dval) / h->inc.val.dval);
			else
				index= (int) ((val->val.dval - h->low.val.dval) / h->inc.val.dval);
			break;
		case TYPE_flt:
			if (val->val.fval > h->hgh.val.fval)
				index= (int) ((h->hgh.val.fval - h->low.val.fval) / h->inc.val.fval);
			else
				index= (int) ((val->val.fval - h->low.val.fval) / h->inc.val.fval);
			break;
	}
	return index;
}

int
HSTincrement(Histogram h, ValPtr val){
	int index = HSTgetIndex(h,val);
	h->cnt[index]++;
	h->total++;
	h->cnt[h->bins+index]=h->cnt[index]/h->total;
	return index;
}

void
HSTdecrement(Histogram h, ValPtr val){
	int index = HSTgetIndex(h,val);
	h->cnt[index]--;
	h->total--;
	h->cnt[h->bins+index]=h->cnt[index]/h->total;
}

dbl
HSTeuclidian(Histogram h1, Histogram h2){
	dbl sum= 0;
	int i;
	assert(h1->bins == h2->bins);
	for ( i = 0; i < h1->bins; i++)
		sum += (h1->cnt[i] - h2->cnt[i]) * (h1->cnt[i] - h2->cnt[i]);
	return sqrt((double) sum);
}


/* if 1 then adding this value to h2 increases the distance between h1-h2*/
int
HSTeuclidianWhatIf(Histogram h1, Histogram h2, ValPtr val){
	dbl sum= 0, sumWhatIf=0, cur;
	dbl distance, distanceWhatIf;
	int i, affectedBin;
	assert(h1->bins == h2->bins);

	affectedBin = HSTgetIndex(h2,val);

	for ( i = 0; i < h1->bins; i++){
		cur = (h1->cnt[i] - h2->cnt[i]) * (h1->cnt[i] - h2->cnt[i]);
		sum += cur;
		if (i==affectedBin)
			sumWhatIf += (h1->cnt[i] - (h2->cnt[i]+1)) * (h1->cnt[i] - (h2->cnt[i]+1));
		else
			sumWhatIf += cur;
	}

	distance = sqrt((double) sum);
	distanceWhatIf = sqrt((double) sumWhatIf);
	if (distanceWhatIf > distance) return 1;
	return 0;
}

/* if 1 then moving this value to h2 increases the distance between h1-h2*/
int
HSTeuclidianWhatIfMove(Histogram h1, Histogram h2, ValPtr val){
	dbl sum= 0, sumWhatIf=0, cur;
	dbl distance, distanceWhatIf;
	int i, affectedBin;
	assert(h1->bins == h2->bins);

	affectedBin = HSTgetIndex(h1,val);

	for ( i = 0; i < h1->bins; i++){
		cur = (h1->cnt[i] - h2->cnt[i]) * (h1->cnt[i] - h2->cnt[i]);
		sum += cur;
		if (i==affectedBin)
			sumWhatIf += ((h1->cnt[i]-1) - (h2->cnt[i]+1)) * ((h1->cnt[i]-1) - (h2->cnt[i]+1));
		else
			sumWhatIf += cur;
	}

	distance = sqrt((double) sum);
	distanceWhatIf = sqrt((double) sumWhatIf);
	if (distanceWhatIf > distance) return 1;
	return 0;
}

dbl
HSTeuclidianNorm(Histogram h1, Histogram h2){
	dbl sum= 0;
	int i;
	assert(h1->bins == h2->bins);
	for ( i = h1->bins; i < 2*h1->bins; i++)
		sum += (h1->cnt[i] - h2->cnt[i]) * (h1->cnt[i] - h2->cnt[i]);
	return sqrt((double) sum);
}

dbl
HSTeuclidianNormWhatIf(Histogram h1, Histogram h2, ValPtr val, dbl *whatIfDistance){
	dbl sum= 0, sumWhatIf=0, cur;
	dbl distance, distanceWhatIf;
	int i, affectedBin;
	assert(h1->bins == h2->bins);

	affectedBin = HSTgetIndex(h2,val)+h2->bins;

	for ( i = h1->bins; i < 2*h1->bins; i++){
		cur = (h1->cnt[i] - h2->cnt[i]) * (h1->cnt[i] - h2->cnt[i]);
		sum += cur;
		if (i==affectedBin)
			sumWhatIf += (h1->cnt[i] - (((h2->cnt[i-h1->bins]+1))/(h2->total+1))) * (h1->cnt[i] - (((h2->cnt[i-h1->bins]+1))/(h2->total+1)));
		else
			sumWhatIf += cur;
	}

	distance = sqrt((double) sum);
	distanceWhatIf = sqrt((double) sumWhatIf);
	*whatIfDistance = distanceWhatIf;
	if (distanceWhatIf > distance) return (distanceWhatIf - distance);
	return 0;
}

/* if 1 then moving this value to h2 increases the distance between h1-h2*/
int
HSTeuclidianNormWhatIfMove(Histogram h1, Histogram h2, ValPtr val, dbl *whatIfDistance){
	dbl sum= 0, sumWhatIf=0, cur;
	dbl distance, distanceWhatIf;
	int i, affectedBin;
	assert(h1->bins == h2->bins);

	affectedBin = HSTgetIndex(h1,val)+h2->bins;

	for ( i = h1->bins; i < 2*h1->bins; i++){
		cur = (h1->cnt[i] - h2->cnt[i]) * (h1->cnt[i] - h2->cnt[i]);
		sum += cur;
		if (i==affectedBin)
			sumWhatIf += ((((h1->cnt[i-h1->bins]-1))/(h1->total-1)) - (((h2->cnt[i-h1->bins]+1))/(h2->total+1))) * ((((h1->cnt[i-h1->bins]-1))/(h1->total-1)) - (((h2->cnt[i-h1->bins]+1))/(h2->total+1)));
		else
			sumWhatIf += cur;
	}

	distance = sqrt((double) sum);
	distanceWhatIf = sqrt((double) sumWhatIf);
	*whatIfDistance = distanceWhatIf;
	if (distanceWhatIf > distance) return 1;
	return 0;
}


dbl
HSTcityblock(Histogram h1, Histogram h2){
	dbl sum=0;
	int i;
	assert(h1->bins == h2->bins);
	for ( i = 0; i < h1->bins; i++)
		sum += ABS(h1->cnt[i] - h2->cnt[i]);
	return (dbl) sum;
}

dbl
HSTchebyshev(Histogram h1, Histogram h2){
	dbl max;
	lng i;
	assert(h1->bins == h2->bins);
	max = ABS(h1->cnt[0] - h2->cnt[0]);
	for ( i = 0; i < h1->bins; i++)
		if ( ABS(h1->cnt[i] - h2->cnt[i]) > max)
			max = ABS(h1->cnt[i] - h2->cnt[i]);
	return (dbl) max;
}


dbl
HSTbinrelative(Histogram h1, Histogram h2){
	lng i;
	dbl totalDiff=0, avgDiff, cur;
	dbl distance=0;
	assert(h1->bins == h2->bins);

	for ( i = 0; i < h1->bins; i++)
		totalDiff += ABS(h1->cnt[i] - h2->cnt[i]);
	avgDiff = totalDiff/h1->bins;

	for ( i = 0; i < h1->bins; i++){
		cur = ABS(avgDiff - ABS(h1->cnt[i] - h2->cnt[i]));
		distance += cur*cur;
	}
	return distance;
}

dbl
HSTbinrelativeWhatIf(Histogram h1, Histogram h2, ValPtr val, dbl *whatIfDistance){
	lng i;
	dbl totalDiff=0, avgDiff, cur, avgDiffWhatIf, totalDiffWhatIf=0, distance=0, distanceWhatIf=0;
	int affectedBin;
	assert(h1->bins == h2->bins);

	affectedBin = HSTgetIndex(h1,val);

	for ( i = 0; i < h1->bins; i++){
		cur = ABS(h1->cnt[i] - h2->cnt[i]);
		totalDiff += cur;
		if (i==affectedBin)
			totalDiffWhatIf += ABS(h1->cnt[i] - (h2->cnt[i]+1));
		else
			totalDiffWhatIf += cur;
	}
	avgDiff = totalDiff/h1->bins;
	avgDiffWhatIf = totalDiffWhatIf/h1->bins;

	for ( i = 0; i < h1->bins; i++){
		cur = ABS(avgDiff - ABS(h1->cnt[i] - h2->cnt[i]));
		distance += cur*cur;

		if (i==affectedBin)
			cur = ABS(avgDiffWhatIf - ABS(h1->cnt[i] - (h2->cnt[i]+1)));
		else
			cur= ABS(avgDiffWhatIf - ABS(h1->cnt[i] - h2->cnt[i]));
		distanceWhatIf += cur*cur;
	}

	*whatIfDistance = distanceWhatIf;
	if (distanceWhatIf > distance) return (distanceWhatIf - distance);
	return 0;
}

int
HSTbinrelativeWhatIfMove(Histogram h1, Histogram h2, ValPtr val,dbl *whatIfDistance){
	lng i;
	dbl totalDiff=0, avgDiff, cur, avgDiffWhatIf, totalDiffWhatIf=0, distance=0, distanceWhatIf=0;
	int affectedBin;
	assert(h1->bins == h2->bins);

	affectedBin = HSTgetIndex(h1,val);

	for ( i = 0; i < h1->bins; i++){
		cur = ABS(h1->cnt[i] - h2->cnt[i]);
		totalDiff += cur;
		if (i==affectedBin)
			totalDiffWhatIf += ABS((h1->cnt[i]-1) - (h2->cnt[i]+1));
		else
			totalDiffWhatIf += cur;
	}
	avgDiff = totalDiff/h1->bins;
	avgDiffWhatIf = totalDiffWhatIf/h1->bins;

	for ( i = 0; i < h1->bins; i++){
		cur = ABS(avgDiff - ABS(h1->cnt[i] - h2->cnt[i]));
		distance += cur*cur;

		if (i==affectedBin)
			cur = ABS(avgDiffWhatIf - ABS((h1->cnt[i]-1) - (h2->cnt[i]+1)));
		else
			cur = ABS(avgDiffWhatIf - ABS(h1->cnt[i] - h2->cnt[i]));
		distanceWhatIf += cur*cur;
	}

	*whatIfDistance = distanceWhatIf;
	if (distanceWhatIf > distance) return 1;
	return 0;
}

int
HSTwhichbinNorm(Histogram h1, Histogram h2){
	dbl cur, max=0;
	int i, whichbin =-1;
	assert(h1->bins == h2->bins);

	for ( i = h1->bins; i < 2*h1->bins; i++){
		if (h1->cnt[i-h1->bins]==0) continue;
	 	cur = h1->cnt[i] - h2->cnt[i];
		if (cur <= 0)
			continue;
		if (cur > max){
			max=cur;
			whichbin = i;
		}
	}

	return whichbin==-1?-1:whichbin-h1->bins;
}

int
HSTwhichbin(Histogram h1, Histogram h2){
	dbl totalDiff=0, avgDiff, cur, max=0, curbinDiff;
	int i, whichbin =-1;
	assert(h1->bins == h2->bins);

	for ( i = 0; i < h1->bins; i++)
		totalDiff += ABS(h1->cnt[i] - h2->cnt[i]);
	avgDiff = totalDiff/h1->bins;

	for ( i = 0; i < h1->bins; i++){
	 	curbinDiff = h1->cnt[i] - h2->cnt[i];
		if (curbinDiff < 0)
			continue;
		cur = ABS(avgDiff - curbinDiff);
		if (cur > max){
			max=cur;
			whichbin = i;
		}
	}

	return whichbin;
}

/*
 * @-
 * The next series determine the effect on the distance when a single
 * component in the histogram h1 is incremented/ decremented.
 */
dbl
HSTeuclidianDelta(Histogram h1, Histogram h2, dbl distance, int idx, int cnt){
	dbl inc;
	dbl sum = distance * distance;
	inc = (h1->cnt[idx] + cnt - h2->cnt[idx]);
	return sqrt(sum - (h1->cnt[idx]- h2->cnt[idx])+ inc * inc);
}

dbl
HSTcityblockDelta(Histogram h1, Histogram h2, dbl d, int idx, int cnt){
	return d - ABS(h1->cnt[idx] - h2->cnt[idx]) + ABS(h1->cnt[idx] + cnt - h2->cnt[idx]);
}

dbl
HSTchebyshevDelta(Histogram h1, Histogram h2, dbl distance, int idx, int cnt){
	lng max;
	int i;

	(void) distance;
	assert(h1->bins == h2->bins);
	max = ABS(h1->cnt[0] + (idx == 0? cnt: 0) - h2->cnt[0]);
	for ( i = 1; i < h1->bins; i++)
		if ( ABS(h1->cnt[i] + (idx == i? cnt: 0) - h2->cnt[i]) > max)
			max = ABS(h1->cnt[i] + (idx == i? cnt: 0) - h2->cnt[i]);
	return (dbl) max;
}

void
HSTprint(stream *s, Histogram h){
	int i;

	for ( i = 0; i < h->bins;  i++)
		mnstr_printf(s, "[%d] " LLFMT "\n", i, h->cnt[i]);
	mnstr_printf(s, "\n");
}

void
HSTprintf(Histogram h){
	int i;

	printf("Total count: " LLFMT "\n",(lng)h->total);
	for ( i = 0; i < h->bins;  i++)
		printf("[%d] " LLFMT " ~ %f \n", i, (lng)h->cnt[i],h->cnt[i+h->bins]);
	printf("\n");
}
