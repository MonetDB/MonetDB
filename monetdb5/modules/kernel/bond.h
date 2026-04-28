
typedef struct bond_collection {
	BAT **dims;        /* array of dimension BATs (not owned, just referenced) */
	BOND_ELEM_T **dimsp;
	int ndims;         /* number of dimensions */
	int bsz;			   /* block size */
	BUN nvecs;         /* number of vectors (rows) */
	dbl *dim_means;    /* mean value per dimension (for dimension ordering) */
	oid *candidates;
	oid *tcand;
	oid *tc;
	dbl *dists;
	dbl *tdist;
	dbl *td;
	dbl kth_upper;		/* kth upper bound */
} bond_collection;

static bond_collection*
bond_create(allocator *ma, BAT **dim_bats, int ndims, int k)
{
	if (dim_bats && ndims > 0) {
		bond_collection *bc = ma_alloc(ma, sizeof(bond_collection));
		*bc = (bond_collection) {
			.ndims = ndims,
			.kth_upper = DBL_MAX,
		};
		if (bc) {
			bc->bsz = VS;
			bc->candidates = ma_alloc(ma, k * sizeof(oid));
			bc->dists = ma_alloc(ma, k * sizeof(dbl));

			bc->tcand = ma_alloc(ma, k * sizeof(oid));
			bc->tdist = ma_alloc(ma, k * sizeof(dbl));
			bc->tc = ma_alloc(ma, bc->bsz * sizeof(oid));
			bc->td = ma_alloc(ma, bc->bsz * sizeof(dbl));
			bc->dims = ma_alloc(ma, ndims * sizeof(BAT *));
			bc->dimsp = ma_alloc(ma, ndims * sizeof(BOND_ELEM_T *));
			bc->dim_means = ma_alloc(ma, ndims * sizeof(dbl));
			bc->nvecs = BATcount(dim_bats[0]);
			for (int d = 0; d < ndims; d++) {
				BAT *b = dim_bats[d];
				bc->dims[d] = b;
				bc->dimsp[d] = (BOND_ELEM_T*)Tloc(b,0);
			}
			return bc;
		}
	}
	return NULL;
}

static dbl
bond_upper_bound(bond_collection *bc, const BOND_ELEM_T *query_vals, BUN k)
{
	oid *cands = bc->candidates, *tc = bc->tc;
	dbl *dists = bc->dists, *td = bc->td;

	// calc full distance for the sample across all dimensions
	dbl res = 0;
	for (int i = 0; i < bc->bsz; i++) {
		tc[i] = i;
		td[i] = 0;
	}

	for (int d = 0; d < bc->ndims; d++) {
		const BOND_ELEM_T *vals = (const BOND_ELEM_T *) bc->dimsp[d];
		BOND_ELEM_T q = query_vals[d];
		dbl sum = 0;

		for (int i = 0; i < bc->bsz; i++) {
			dbl diff = vals[tc[i]] - q;
			dbl m = diff * diff;
			td[i] += m;
			sum += m;
		}
		bc->dim_means[d] += sum;
	}
	res = topn(tc, td, bc->tcand, bc->tdist, bc->bsz, k);
	for(BUN i = 0; i < k; i++) {
		cands[i] = bc->tcand[i];
		dists[i] = bc->tdist[i];
	}
	for (int d = 0; d < bc->ndims; d++)
		bc->dim_means[d] /= bc->bsz;
	return res;
}

static char*
bond_search_fast(bond_collection *bc, const BOND_ELEM_T *query_vals,
			BUN k, int *dim_order, BAT *cands,
		   	BAT **oid_result, BAT **dist_result)
{
	if (cands || !bc || !query_vals || k == 0 || !oid_result || !dist_result || !dim_order)
		throw(MAL, "vss.bond_search", "invalid arguments");

	//lng T0 = GDKusec();
	//lng Tinit = 0, Tdists = 0, Tprune = 0, Ttopn = 0;
	BUN ncands = bc->nvecs, pruned = 0;
	oid *tc = bc->tc;
	dbl *td = bc->td;
	for (BUN z = bc->bsz; z < ncands; z+=bc->bsz) {
		BUN end = z+bc->bsz > ncands?ncands:z+bc->bsz, j, i = 0;
		// initialize, all vectors are candidates

		//lng T0 = GDKusec();
		for (j = z, i = 0; j<end; j++, i++) {
			tc[i] = i;
			td[i] = 0;
		}
		//Tinit += GDKusec() - T0;

		BUN sz = i, cur_pruned = 0;
		int step = 2, mask = step - 1, d = 0;
		for (d = 0; d < bc->ndims && (cur_pruned*4) < sz; d++) {
			/* partial dists */
			int o = dim_order[d];
			BOND_ELEM_T qd = query_vals[o];
			const BOND_ELEM_T *col = (const BOND_ELEM_T *) (bc->dimsp[o]+z);

	//		lng T0 = GDKusec();
			for (i=0; i < sz; i++) {
				dbl diff = col[i] - qd;
				td[i] += diff * diff;
				cur_pruned += (td[i] > bc->kth_upper);
			}
	//		Tdists += GDKusec() - T0;
			//printf("partial dists chunk " BUNFMT " " BUNFMT " d=%zu t=" LLFMT "\n", j, sz, (size_t)d,  GDKusec() - T0);
		}
		for (; d < bc->ndims && sz; d++) {
			/* partial dists */
			int o = dim_order[d];
			BOND_ELEM_T qd = query_vals[o];
			const BOND_ELEM_T *col = (const BOND_ELEM_T *) (bc->dimsp[o]+z);

	//		lng T0 = GDKusec();
			for (i=0; i < sz; i++) {
				dbl diff = col[tc[i]] - qd;
				td[i] += diff * diff;
			}
	//		Tdists += GDKusec() - T0;
			//printf("partial dists chunk " BUNFMT " " BUNFMT " d=%zu t=" LLFMT "\n", j, sz, (size_t)d,  GDKusec() - T0);
			if ((d&mask) != mask)
				continue;
			step *= 2;
			mask = step - 1;

			/* prune */
	//		T0 = GDKusec();
			BUN write_pos = 0;
			for (BUN read_pos = 0; read_pos < sz; read_pos++) {
				if (td[read_pos] <= bc->kth_upper) {
					tc[write_pos] = tc[read_pos];
					td[write_pos] = td[read_pos];
					write_pos++;
				}
			}
			pruned += sz - write_pos;
			sz = write_pos;
	//		Tprune += GDKusec() - T0;
			//printf("prune " BUNFMT " d=%zu " "t=" LLFMT "\n",  sz, (size_t)d,  GDKusec() - T0);
		}

	//	T0 = GDKusec();
		for(BUN i = 0; i<sz; i++)
			tc[i] += z;
		bc->kth_upper = topn_merge(bc->candidates, bc->dists, bc->tc, bc->td, sz, k);
	//	Ttopn += GDKusec() - T0;
		//printf("topn chnkd " BUNFMT " t=" LLFMT " %F\n", j, GDKusec() - T0, bc->kth_upper);
	}
	(void)pruned;
	//printf("vectors t= " LLFMT " init=" LLFMT " dists=" LLFMT " prune " LLFMT " topn " LLFMT " upper %F, pruned " BUNFMT " \n",
	//		GDKusec() - T0, Tinit, Tdists, Tprune, Ttopn, bc->kth_upper, pruned);
	//printf("vectors t= " LLFMT " upper %F, pruned " BUNFMT " \n", GDKusec() - T0, bc->kth_upper, pruned);

	//T0 = GDKusec();
	oid base = bc->dims[0]->hseqbase;
	BAT *koids = COLnew(0, TYPE_oid, k, TRANSIENT);
    BAT *kdist = COLnew(0, TYPE_dbl, k, TRANSIENT);
	// Final top k
	{
		BUN write_pos = 0;
		for (; write_pos < k; write_pos++) {
				*(oid*) Tloc(koids, write_pos) = bc->candidates[write_pos] + base;
				*(dbl*) Tloc(kdist, write_pos) = bc->dists[write_pos];
		}
		k = write_pos;
	}
	BATsetcount(koids, k);
    koids->tsorted = false;
    koids->trevsorted = false;
	BATsetcount(kdist, k);
	kdist->tsorted = false;
    kdist->trevsorted = false;
    //kdist->tnonil = true;
    kdist->tkey = false;
	//printf("final top k " LLFMT "\n", GDKusec() - T0);

	//BATprint(GDKstdout, koids);
	//BATprint(GDKstdout, kdist);

	*oid_result = koids;
	*dist_result = kdist;
	return MAL_SUCCEED;
}

static str
BONDknn(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;

	lng k_lng = *getArgReference_lng(stk, pci, 2);
	if (k_lng <= 0)
		throw(MAL, "vss.knn", ILLEGAL_ARGUMENT ": k must be positive");
	int k = (int) k_lng;
	int qtype = getArgType(mb, pci, pci->argc-1);

	if (qtype != BOND_btype)
		throw(MAL, "vss.knn", ILLEGAL_ARGUMENT ": query values should be reals");
	/* Calculate number of dimensions:
	 * pci->argc = 2 (returns) + 1 (k) + ndims (BATs) + ndims (query vals) */
	int nargs = pci->argc - 3;	/* args after k */
	if (nargs <= 0 || (nargs & 1) != 0)
		throw(MAL, "vss.knn", ILLEGAL_ARGUMENT ": expected equal number of dimension BATs and query values");
	int ndims = nargs / 2;

	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);

	//lng T0 = GDKusec(), T1 = 0;
	/* Get dimension BATs */
	BAT **dim_bats = ma_alloc(ta, ndims * sizeof(BAT *));
	BOND_ELEM_T *query_vals = ma_alloc(ta, ndims * sizeof(BOND_ELEM_T));
	if (!dim_bats || !query_vals) {
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}

	for (int i = 0; i < ndims; i++) {
		BAT *b = BATdescriptor(*getArgReference_bat(stk, pci, 3 + i));
		if (!b) {
			for (int j = 0; j < i; j++)
				BBPreclaim(dim_bats[j]);
			ma_close(&ta_state);
			throw(MAL, "vss.knn", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		dim_bats[i] = b;
		int qarg = 3 + ndims + i;
		query_vals[i] = *(BOND_ELEM_T*)getArgReference(stk, pci, qarg);
	}

	/* Create BOND collection and search */
	bond_collection *bc = bond_create(ta, dim_bats, ndims, k);
	//lng T1 = GDKusec();
	//printf("creation " LLFMT "\n", T1 - T0);
	//T0 = T1;

	if (!bc) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}

	bc->kth_upper = bond_upper_bound(bc, query_vals, k);
	//T1 = GDKusec();
	//printf("upperbound " LLFMT " %F\n", T1 - T0, bc->kth_upper);
	//T0 = T1;

	int *dim_order = bond_dim_order(ta, bc->ndims, bc->dim_means);
	if (!dim_order) {
		for (int i = 0; i < ndims; i++)
			BBPreclaim(dim_bats[i]);
		ma_close(&ta_state);
		throw(MAL, "vss.knn", MAL_MALLOC_FAIL);
	}
	//T1 = GDKusec();
	//printf("create upperbound order " LLFMT "\n", T1 - T0);
	//T0 = T1;

	BAT *oid_result = NULL, *dist_result = NULL;
	char *rc = bond_search_fast(bc, query_vals, (BUN) k, dim_order, NULL, &oid_result, &dist_result);
	//T1 = GDKusec();
	//printf("search " LLFMT "\n", T1 - T0);
	//T0 = T1;

	for (int i = 0; i < ndims; i++)
		BBPreclaim(dim_bats[i]);

	ma_close(&ta_state);
	if (rc != MAL_SUCCEED)
		return rc;

	*getArgReference_bat(stk, pci, 0) = oid_result->batCacheid;
	*getArgReference_bat(stk, pci, 1) = dist_result->batCacheid;
	BBPkeepref(oid_result);
	BBPkeepref(dist_result);
	return MAL_SUCCEED;
}
