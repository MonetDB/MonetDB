/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * (author) R Cijvat
 * The code in this file represents the heart of the bam loader
 * functionality. It uses the functionality provided by the BAM
 * wrapper and the DB Interface, which can be found in respectively
 * bam_wrapper.[h|c] and bam_db_interface.[h|c].
 */

#include "monetdb_config.h"

#include "bam_wrapper.h"
#include "bam_db_interface.h"
#include "bam_globals.h"
#include "bam_loader.h"


/* Macro that checks whether or not a filename ends with either .sam or .bam
 *(case insensitive) */
#define IS_SAMORBAM(filename, len) \
	((filename[len-4] == '.') && \
	 (filename[len-3] == 'b' || filename[len-3] == 'B' || \
		 filename[len-3] == 's' || filename[len-4] == 's') && \
	 (filename[len-2] == 'a' || filename[len-2] == 'A') && \
	 (filename[len-1] == 'm' || filename[len-1] == 'M'))

/* Given a filename that ends on either .sam or .bam, this macro checks if
 * it ends on .bam */
#define IS_BAM(samorbam_filename, len) \
	(samorbam_filename[len-3] == 'b' || samorbam_filename[len-3] == 'B')



typedef struct reader_thread_data {
	sht thread_id;
	/* BAM wrappers of all BAM files that have to be processed */
	bam_wrapper *bws;

	/* Used by threads when they are finding out on which
	 * bam_wrapper to work */
	MT_Lock *reader_lock;
	str reader_lock_ref;
	int *cur_file;
	int nr_files;

	/* Error message or MAL_SUCCEED indicating thread return value */
	str msg;

	/* Flag indicating failure of one of the other threads */
	bit *failure;
} reader_thread_data;

static reader_thread_data *
create_reader_thread_data(bam_wrapper * bws, int nr_files, sht nr_threads)
{
	reader_thread_data *d =
		(reader_thread_data *) GDKmalloc(nr_threads *
						 sizeof(reader_thread_data));
	MT_Lock *reader_lock = (MT_Lock *) GDKmalloc(sizeof(MT_Lock));
	int *cur_file = (int *) GDKmalloc(sizeof(int));
	str reader_lock_ref = "bamReaderLock";
	bit *failure = (bit *) GDKmalloc(sizeof(bit));

	sht i;

	assert(nr_threads > 0);

	if (d == NULL || reader_lock == NULL || cur_file == NULL
		|| failure == NULL) {
		GDKfree(d);
		GDKfree(reader_lock);
		GDKfree(cur_file);
		GDKfree(failure);
		return NULL;
	}

	MT_lock_init(reader_lock, reader_lock_ref);
	*cur_file = -1;
	*failure = FALSE;

	for (i = 0; i < nr_threads; ++i) {
		d[i].thread_id = i;
		d[i].bws = bws;
		d[i].reader_lock = reader_lock;
		d[i].reader_lock_ref = reader_lock_ref;
		d[i].cur_file = cur_file;
		d[i].nr_files = nr_files;
		d[i].msg = MAL_SUCCEED;
		d[i].failure = failure;
	}
	return d;
}

static void
destroy_reader_thread_data(reader_thread_data * d)
{
	/* We can safely access d[0], since the bam_loader function
	 * made sure that nr_threads >= 1 and nr_threads is exactly
	 * the size of the array */
	MT_lock_destroy(d[0].reader_lock);
	GDKfree(d[0].reader_lock);
	GDKfree(d[0].cur_file);
	GDKfree(d[0].failure);
	GDKfree(d);
}


static void
run_process_bam_alignments(void *d)
{
	reader_thread_data *data = (reader_thread_data *) d;
	bam_wrapper *bw;

	TO_LOG("<Thread %d> Starting on next file...\n", data->thread_id);
	/* First, find out on which bam wrapper we have to work */
	MT_lock_set(data->reader_lock);
	if (*data->cur_file == data->nr_files - 1) {
		/* The last file is already (being) processed, this
		 * thread is done */
		MT_lock_unset(data->reader_lock);
		TO_LOG("<Thread %d> No files left to work on; thread done\n",
			   data->thread_id);
		return;
	}
	(*data->cur_file) += 1;
	bw = &data->bws[*data->cur_file];
	MT_lock_unset(data->reader_lock);
	TO_LOG("<Thread %d> Processing alignments of file '%s' (file id "
		   LLFMT ")...\n", data->thread_id, bw->file_location,
		   bw->file_id);

	if ((data->msg =
		 process_alignments(bw, data->failure)) != MAL_SUCCEED) {
		TO_LOG("<Thread %d> Error while processing alignments of file '%s' (file id " LLFMT ") (%s)\n", data->thread_id, bw->file_location, bw->file_id, data->msg);
		REUSE_EXCEPTION(data->msg, MAL, "run_process_bam_alignments",
				"Error while processing alignments of file '%s' (file id "
				LLFMT "): %s", bw->file_location, bw->file_id,
				data->msg);
		return;
	}
	if (*data->failure) {
		/* process_bam_alignments returned because another
		 * thread failed and not because this thread failed */
		TO_LOG("<Thread %d> Exit due to failure in other thread\n",
			   data->thread_id);
		return;
	}

	TO_LOG("<Thread %d> All alignments in file '%s' (file id " LLFMT
		   ") processed!\n", data->thread_id, bw->file_location,
		   bw->file_id);
	run_process_bam_alignments(d);
}



/**
 * Generic function that loads a given list of BAM files according to
 * the values of the other parameters.  It starts with creating the
 * bam schema and the header tables if these do not exist yet.  Then
 * it initializes bam_wrapper structs for all bam files and reads the
 * headers for all BAM files.  If the pairwise storage schema has to
 * be used, It then creates a thread for every file that reads all
 * alignments for this file.
 *
 */
static str
bam_loader(Client cntxt, MalBlkPtr mb, str * filenames, int nr_files,
	   sht dbschema, sht nr_threads)
{
	bam_wrapper *bws = NULL;
	MT_Id *reader_threads = NULL;
	reader_thread_data *r_thread_data = NULL;
	mvc *m = NULL;
	sql_schema *s = NULL;
	sql_table *files_table = NULL;
	lng cur_file_id;
	char buf_threads_msg[4096] = "There were reader threads that contained errors:\n";
	int threads_msg_len = strlen(buf_threads_msg);
	int i, errnr;
	str msg = MAL_SUCCEED;

	TO_LOG("<bam_loader>: Loader started for %d BAM file%s...\n",
		   nr_files, (nr_files != 1 ? "s" : ""));

	/* Check sanity of input */
	if (dbschema != 0 && dbschema != 1) {
		msg = createException(MAL, "bam_loader",
					  SQLSTATE(BA000) "Wrong value for dbschema: '%d' (0=straightforward storage schema, 1=pairwise storage schema)",
					  dbschema);
		goto cleanup;
	}
	if (nr_threads <= 0) {
		nr_threads = 1;
	} else if(nr_threads > 4) {
		nr_threads = 4;
	}

	/* Get SQL context */
	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED) {
		/* Here, and in multiple other locations in this code,
		 * new message is stored in tmp var, since the old msg
		 * needs to be freed after construction of the new
		 * msg */
		REUSE_EXCEPTION(msg, MAL, "bam_loader",
				"Could not retrieve SQLContext: %s", msg);
		goto cleanup;
	}

	/* Start with binding bam schema and the files table */
	if ((msg =
		 bind_bam_schema(m, &s)) != MAL_SUCCEED)
		goto cleanup;
	if((msg = 
		 bind_table(m, s, "files", &files_table)) != MAL_SUCCEED)
		goto cleanup;

	/* Get next file id from files table */
	TO_LOG("<bam_loader> Retrieving next file id...\n");
	if ((msg = next_file_id(m, files_table, &cur_file_id)) != MAL_SUCCEED) {
		goto cleanup;
	}

	/* Init bam_wrapper structs */
	if ((bws =
		 (bam_wrapper *) GDKmalloc(nr_files * sizeof(bam_wrapper))) ==
		NULL) {
		msg = createException(MAL, "bam_loader", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	/* Enables cleanup to check which bam_wrappers to clear */
	memset(bws, 0, nr_files * sizeof(bam_wrapper));

	for (i = 0; i < nr_files; ++i) {
		int fln = strlen(filenames[i]);
		TO_LOG("<bam_loader> Initializing BAM wrapper for file '%s'...\n", filenames[i]);
		if ((msg =
			 init_bam_wrapper(bws + i, (IS_BAM(filenames[i], fln) ? BAM : SAM),
					  filenames[i], cur_file_id++, dbschema)) != MAL_SUCCEED) {
			goto cleanup;
		}
	}

	/* Parse all headers */
	for (i = 0; i < nr_files; ++i) {
		TO_LOG("<bam_loader> Parsing header for file '%s'...\n",
			   filenames[i]);
		if ((msg = process_header(bws + i)) != MAL_SUCCEED) {
			goto cleanup;
		}
	}

	/* If we have to load the BAM data into the pairwise storage
	 * schema, make sure that all input BAM files are sorted on
	 * QNAME */
	if (dbschema == 1) {
		for (i = 0; i < nr_files; ++i) {
			TO_LOG("<bam_loader> Checking sortedness for BAM file '%s'...\n", filenames[i]);
			if (bws[i].ord != ORDERING_QUERYNAME) {
				msg = createException(MAL, "bam_loader",
							  SQLSTATE(BA000) "Only BAM files that are sorted on queryname can be inserted into the pairwise storage schema; "
							  "BAM file '%s' has ordering '%s'",
							  bws[i].file_location,
							  ordering_str(bws[i].
								   ord));
				goto cleanup;
			}
		}
	}

	/* Create alignment storage */
	for (i = 0; i < nr_files; ++i) {
		TO_LOG("<bam_loader> Creating alignment tables for file '%s'...\n", filenames[i]);
		if ((dbschema == 0
			 && (msg = create_alignment_storage_0(cntxt,
								  "bam.create_storage_0",
								  bws + i)) != MAL_SUCCEED)
			|| (dbschema == 1
				&& (msg = create_alignment_storage_1(cntxt,
								  "bam.create_storage_1",
								  bws + i)) != MAL_SUCCEED)) {
			goto cleanup;
		}
	}


	/* Now create threads to read alignment data of different files */
	TO_LOG("<bam_loader> Creating reader threads...\n");
	if ((reader_threads =
		 (MT_Id *) GDKmalloc(nr_threads * sizeof(MT_Id))) == NULL) {
		msg = createException(MAL, "bam_loader", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	if ((r_thread_data =
		 create_reader_thread_data(bws, nr_files, nr_threads)) == NULL) {
		msg = createException(MAL, "bam_loader", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	for (i = 0; i < nr_threads; ++i) {
		if ((errnr =
			 MT_create_thread(&reader_threads[i],
					  run_process_bam_alignments,
					  &r_thread_data[i],
					  MT_THR_JOINABLE)) != 0) {
			msg = createException(MAL, "bam_loader",
						  SQLSTATE(BA000) "Could not create thread to process alignments (errnr %d)",
						  errnr);
			goto cleanup;
		}
	}

	TO_LOG("<bam_loader> Waiting for reader threads to finish...\n");
	/* Wait until all threads finish and collect their
	 * messages. Though it is not very likely, it could be the
	 * case that more than 1 thread generates an error message (not
	 * likely because threads exit once they notice that another
	 * thread has failed).  Therefore, we collect all error
	 * messages in one big error string
	 */
	for (i = 0; i < nr_threads; ++i) {
		if ((errnr = MT_join_thread(reader_threads[i])) != 0) {
			msg = createException(MAL, "bam_loader",
						  SQLSTATE(BA000) "Could not join alignment processing thread (errnr %d)",
						  errnr);
			goto cleanup;
		}
		/* Thread finished ok, append its error message, if any */
		if (r_thread_data[i].msg != MAL_SUCCEED) {
			int step;

			if (msg == MAL_SUCCEED) {
				/* First encountered thread error,
				 * indicate this by pointing to error
				 * buf */
				msg = buf_threads_msg;
			}
			/* snprintf returns -1 on failure; since we
			 * don't want to fail when snprintf fails, we
			 * use MAX to make sure we don't add a
			 * negative amount to threads_msg_len */
			step = snprintf(msg + threads_msg_len,
					4096 - threads_msg_len, "* %s\n",
					r_thread_data[i].msg);
			threads_msg_len += MAX(0, step);
			GDKfree(r_thread_data[i].msg);
		}
	}

	/* Fail if any thread has failed */
	if (msg != MAL_SUCCEED) {
		/* Do not use REUSE_EXCEPTION here, since msg was not
		 * malloced. Instead, just copy buffer contents to
		 * malloced buffer */
		msg = GDKstrdup(msg);
		goto cleanup;
	}

	TO_LOG("<bam_loader> Copying data into DB...\n");
	/* All threads finished succesfully, copy all data into DB */
	for (i = 0; i < nr_files; ++i) {
		if ((msg = copy_into_db(cntxt, bws + i)) != MAL_SUCCEED) {
			goto cleanup;
		}
	}

	  cleanup:
	if (bws) {
		for (i = 0; i < nr_files; ++i) {
			if (bws + i)
				clear_bam_wrapper(bws + i);
		}
		GDKfree(bws);
	}
	if (reader_threads)
		GDKfree(reader_threads);
	if (r_thread_data)
		destroy_reader_thread_data(r_thread_data);

	if (msg != MAL_SUCCEED) {
		TO_LOG("<bam_loader> Error on processing BAM files: %s\n",
			   msg);
	}

	TO_LOG("<bam_loader>: Loader finished processing %d BAM file%s...\n",
		   nr_files, (nr_files != 1 ? "s" : ""));
	return msg;
}


/**
 * Gathers all BAM files in the given repository and calls bam_loader for these files
 */
str
bam_loader_repos(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* arg 1: path to bam file repository */
	str bam_repos = *getArgReference_str(stk, pci, pci->retc);
	/* arg 2: dbschema to use */
	sht dbschema = *getArgReference_sht(stk, pci, pci->retc + 1);
	/* For now, we hard code the number of threads, since the SQL
	   level should not bother with this */

	sht nr_threads = GDKnr_threads;

	str *filenames = NULL;
	int nr_files = 0;

	DIR *d = NULL;
	struct dirent *direntry = NULL;
	char path[4096];

	int i, filecount = 0;
	str msg = MAL_SUCCEED;

	if ((d = opendir(bam_repos)) == NULL) {
		if (errno == ENOENT) {
			msg = createException(MAL, "bam_loader_repos",
						  SQLSTATE(BA000) "Could not find directory '%s'",
						  bam_repos);
		} else {
			msg = createException(MAL, "bam_loader_repos",
						  SQLSTATE(BA000) "Could not open directory '%s'",
						  bam_repos);
		}
		goto cleanup;
	}

	/* First, count number of SAM/BAM files */
	while ((direntry = readdir(d)) != NULL) {
		int len = strlen(direntry->d_name);

		if (IS_SAMORBAM(direntry->d_name, len))
			++filecount;
	}

	if (filecount == 0) {
		msg = createException(MAL, "bam_loader_repos",
					  SQLSTATE(BA000) "No SAM or BAM files found in directory '%s'",
					  bam_repos);
		goto cleanup;
	}

	/* Now malloc enough memory for filenames array */
	if ((filenames = (str *) GDKmalloc(filecount * sizeof(str))) == NULL) {
		msg = createException(MAL, "bam_loader_repos",
					  SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto cleanup;
	}

	/* Enables cleanup to check individual filenames */
	memset(filenames, 0, filecount * sizeof(str));

	rewinddir(d);

	/* Now loop through dir; we add a slash in between the given
	 * bam_repos and the filename. This could lead to double
	 * slashes in a path, but as far as I know, this is no problem
	 * on all OS's */
	while ((direntry = readdir(d)) != NULL) {
		/* Check if d_name has the .sam or .bam extension (case
		 * insensitive) */
		int len = strlen(direntry->d_name);
		if (IS_SAMORBAM(direntry->d_name, len)) {
			/* This is a SAM or BAM file, construct its path and
			 * add that to the files array */
			if (snprintf
				(path, 4096, "%s/%s", bam_repos,
				 direntry->d_name) < 0) {
				msg = createException(MAL, "bam_loader_repos",
							  SQLSTATE(BA000) "Could not construct filepath for SAM/BAM file '%s'",
							  direntry->d_name);
				goto cleanup;
			}
			if (nr_files >= filecount) {
				/* This should not happen. If it does,
				 * it might be the case that a BAM
				 * file was added to the directory in
				 * the meantime */
				msg = createException(MAL, "bam_loader_repos",
							  SQLSTATE(BA000) "An error occurred while reading directory '%s'",
							  bam_repos);
				goto cleanup;
			}
			filenames[nr_files++] = GDKstrdup(path);
		}
	}

	/* And call the bam loader */
	msg = bam_loader(cntxt, mb, filenames, nr_files, dbschema,
			 nr_threads);

	  cleanup:
	if (filenames) {
		for (i = 0; i < nr_files; ++i) {
			if (filenames[i])
				GDKfree(filenames[i]);
		}
		GDKfree(filenames);
	}
	if (d)
		closedir(d);

	(void) stk;
	(void) pci;

	return msg;
}



/**
 * Gathers all BAM files that are listed in the given file and calls
 * bam_loader for these files
 */
str
bam_loader_files(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* arg 1: path to file containing bam file names separated by \n */
	str bam_files = *getArgReference_str(stk, pci, pci->retc);
	/* arg 2: dbschema to use */
	sht dbschema = *getArgReference_sht(stk, pci, pci->retc + 1);
	/* For now, we hard code the number of threads, since the SQL
	   level should not bother with this */
	sht nr_threads = GDKnr_threads;

	str *filenames = NULL;
	int nr_files = 0;

	FILE *f = NULL;

	int cur;
	int line_size;
	str line = NULL;
	size_t line_buf_size = 0;
	int nr_lines = 1;

	str msg = MAL_SUCCEED;
	int i;

	/* Open the file */
	f = fopen(bam_files, "r");
	if (f == NULL) {
		msg = createException(MAL, "bam_loader_files",
					  SQLSTATE(BA000) "Error on opening file list '%s': %s",
					  bam_files, strerror(errno));
		goto cleanup;
	}

	/* Start with counting the number of lines in the file (is nr
	 * of newlines + 1) */
	while ((cur = fgetc(f)) != EOF) {
		if (cur == '\n')
			++nr_lines;
	}

	if (ferror(f)) {
		msg = createException(MAL, "bam_loader_files",
					  SQLSTATE(BA000) "Error while processing file '%s'",
					  bam_files);
		goto cleanup;
	}

	/* Now we can malloc the filenames array */
	if ((filenames = (str *) GDKmalloc(nr_lines * sizeof(str))) == NULL) {
		msg = createException(MAL, "bam_loader_files",
					  SQLSTATE(HY001) MAL_MALLOC_FAIL);
		goto cleanup;
	}
	/* Enables cleanup to check individual files */
	memset(filenames, 0, nr_lines * sizeof(str));

	/* Reset file pointer to beginning of file */
	rewind(f);

	/* Read BAM filenames into files array */
	while ((line_size =
		getline(&line, &line_buf_size, f)) >= 0) {

		if (line_size == 0 || line[0] == '\n' || line[0] == '\r' || line[0] == '#') {
			/* Skip empty lines or lines that start with a hash */
			free(line);
			line = NULL;
			/* We need to reset buf size to 0, since getline will have updated it */
			line_buf_size = 0;
			continue;
		}
		if (line[line_size - 1] == '\n') {
			/* Newline character is not part of the filepath */
			line[line_size - 1] = '\0';
		}
		if(line_size > 1 && line[line_size - 2] == '\r') {
			/* Return character is also not a part of the filepath */
			line[line_size - 2] = '\0';
		}
		filenames[nr_files++] = line;
		line = NULL;
		/* We need to reset it to 0, since getline will have updated this val */
		line_buf_size = 0;
	}

	if (ferror(f)) {
		msg = createException(MAL, "bam_loader_files",
					  SQLSTATE(BA000) "Error while processing file '%s'",
					  bam_files);
		goto cleanup;
	}

	if (nr_files == 0) {
		msg = createException(MAL, "bam_loader_files",
					  SQLSTATE(BA000) "No valid entries found in file '%s'",
					  bam_files);
		goto cleanup;
	}

	/* And call the bam loader */
	msg = bam_loader(cntxt, mb, filenames, nr_files, dbschema,
			 nr_threads);

	  cleanup:
	if(line) {
		//if line still contains anything, this is something that was put there
		//by getline on the last, failed attempt to read a line
		free(line);
	}

	if (filenames) {
		for (i = 0; i < nr_files; ++i) {
			if (filenames[i]) {
				free(filenames[i]);
			}
		}
		GDKfree(filenames);
	}
	if (f)
		fclose(f);


	(void) stk;
	(void) pci;

	return msg;
}


/**
 * Calls the bam loader for a single file
 */
str
bam_loader_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	/* arg 1: path to bam stream */
	str bam_file = *getArgReference_str(stk, pci, pci->retc);
	/* arg 2: dbschema to use */
	sht dbschema = *getArgReference_sht(stk, pci, pci->retc + 1);

	(void) stk;
	(void) pci;

	return bam_loader(cntxt, mb, &bam_file, 1, dbschema, 1);
}


str
bam_drop_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	lng file_id = *getArgReference_lng(stk, pci, pci->retc);
	sht dbschema = *getArgReference_sht(stk, pci, pci->retc + 1);

	str msg;

	msg = drop_file(cntxt, "bam.drop_file", file_id, dbschema);
	if (msg != MAL_SUCCEED) {
		str msg2 = createException(MAL, "bam_drop_file",
			  SQLSTATE(BA000) "Error when dropping file with file id '" LLFMT
			  "': %s\n", file_id, msg);
		freeException(msg);
		return msg2;
	}

	(void) stk;
	(void) pci;
	(void) mb;

	return MAL_SUCCEED;
}
