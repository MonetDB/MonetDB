#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "monetdb_config.h"

#include "registrar.h"
#include "mtime.h"

#include "sql_mvc.h"
#include "sql.h"

/*
 * keeps BAT and other properties of columns of a table.
 */
typedef struct {
	bat *column_bats; /* keeps bats of the columns: lower array */
	str *column_names; /* names of columns that are kept in the higher array */
	str *column_types_strs; /* type strings of columns */
} temp_subcontainer;

/*
 * keeps (some) tables of a schema.
 */
typedef struct {
	str schema_name; /* schema or vault name */
	temp_subcontainer *tables_columns; /* keeps tables: higher array */
	str *table_names; /* names of tables that are kept in the higher array */
	int *num_columns; /* number of columns in each table in the higher array */
	sht num_tables;
} temp_container;

lng get_line_num(str filename);
lng get_file_paths(str repo_path, str** ret_file_paths);
str mseed_create_temp_container(temp_container* ret_tc);
str mseed_create_temp_container_with_data_tables(temp_container* ret_tc);
str mseed_register(str file_path, temp_container* ret_tc);
str mseed_register_and_mount(str file_path, temp_container* ret_tc);
int concatenate_strs(str* words_to_concat, int num_words_to_concat, str* ret_concatenated);
str prepare_insertion(Client cntxt, temp_container* tc);
str insert_into_vault(Client cntxt, MalBlkPtr mb, temp_container* tc);
str SQLstatementIntern(Client c, str *expr, str nme, int execute, bit output);
str register_clean_up(temp_container* tc);

/*
 * returns number of lines in a file.
 *
 * WARNING: always counts EOF as a line. So proper return is taken if the file does
 * not have a newline at the end.
 */
lng get_line_num(str filename)
{
	FILE *f;
	char c;
	long lines = 0;

	f = fopen(filename, "r");

	if(f == NULL)
		return 0;

	while((c = fgetc(f)) != EOF)
		if(c == '\n')
			lines++;

	fclose(f);

/* 	if(c != '\n') */
/* 		lines++; */

	return lines;
}

/*
 * returns number of file_paths in repo_path.
 *
 * stores each file_path in ret_file_paths.
 *
 * repo_path may be either a file that is containing file_paths in the repo one per line,
 * or a (recursive) directory containing the repository.
 *
 * TODO: if a directory path is given, traverse the directory recursively and collect all the files.
 */
lng get_file_paths(str repo_path, str** ret_file_paths)
{
	long num_file_paths = 0;
	struct stat s;
	str* file_paths = NULL;
	if( stat(repo_path,&s) == 0 )
	{
		if( s.st_mode & S_IFDIR )
		{
			/* it's a directory */
			/* traverse and collect all the files */
		}
		else if( s.st_mode & S_IFREG )
		{
			/* it's a file */
			/* each line is a file_path */

			FILE *file;
			num_file_paths = get_line_num(repo_path);
			printf("num_file_paths: %ld\n", num_file_paths);

			*ret_file_paths = file_paths = (str*) GDKmalloc(num_file_paths * sizeof(str));
			assert(file_paths != NULL);

			file = fopen (repo_path, "r");

			if ( file != NULL )
			{
				char line [255]; /* or other suitable maximum line size */
				long i = 0;
				while ( fgets ( line, sizeof(line), file ) != NULL ) /* read a line */
				{
					int len_line = strlen(line);
					/* 			if(len_line == 1) */
					/* 				continue; */
					if(line[len_line-1] == '\n')
						line[len_line-1] = '\0';
					file_paths[i] = GDKstrdup(line);
					i++;
				}
				fclose ( file );
			}
			else
			{
				perror ( repo_path ); /* why didn't the file open? */
			}
		}
		else
		{
			/* something else */
			return -1;
		}
	}
	else
	{
		/* error */
		return -1;
	}

	return num_file_paths;

}

/*
 * fills the temp_container structure with the "mseed" metadata tables' info.
 *
 * returns error or MAL_SUCCEED
 *
 * TODO: This function is now hardcoding every info. It can be made generic,
 * because required info is in sql_catalog.
 */
str mseed_create_temp_container(temp_container* ret_tc)
{
	/* cat: (metadata) catalog, fil: (metadata) files. */
	int num_tables = 2;
	int num_c_fil = 8;
	int num_c_cat = 7;
	int c, t;

	str sch_name = "mseed";

	str cn_fil[] = {"file_location", "dataquality", "network", "station", "location", "channel", "encoding", "byte_order"};
	str cn_cat[] = {"file_location", "seq_no", "record_length", "start_time", "frequency", "sample_count", "sample_type"};

	str cts_fil[] = {"string", "char", "string", "string", "string", "string", "tinyint", "boolean"};
	str cts_cat[] = {"string", "int", "int", "timestamp", "double", "bigint", "char"};

	sht ct_fil[] = {TYPE_str, TYPE_str, TYPE_str, TYPE_str, TYPE_str, TYPE_str, TYPE_bte, TYPE_bit};
	sht ct_cat[] = {TYPE_str, TYPE_int, TYPE_int, TYPE_timestamp, TYPE_dbl, TYPE_lng, TYPE_str};

	str tn[] = {"files", "catalog"};
	int num_c[] = {8, 7};

	bat *cb_fil = (bat*)GDKmalloc(num_c_fil*sizeof(bat));

	bat *cb_cat = (bat*)GDKmalloc(num_c_cat*sizeof(bat));

	temp_subcontainer *tscs = (temp_subcontainer*)GDKmalloc(num_tables*sizeof(temp_subcontainer));

	BAT *aBAT;

	assert(cb_fil!=NULL);
	assert(cb_cat!=NULL);
	assert(tscs!=NULL);

	/* cb_fil */
	for(c = 0; c < num_c_fil; c++)
	{
		aBAT = BATnew(TYPE_void, ct_fil[c], 0); /* create empty BAT for each column. */
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BATseqbase(aBAT, 0);
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BBPkeepref(cb_fil[c] = aBAT->batCacheid);
	}

	/* cb_cat */
	for(c = 0; c < num_c_cat; c++)
	{
		aBAT = BATnew(TYPE_void, ct_cat[c], 0); /* create empty BAT for each column. */
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BATseqbase(aBAT, 0);
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BBPkeepref(cb_cat[c] = aBAT->batCacheid);
	}

	(tscs+0)->column_bats = cb_fil;

	(tscs+1)->column_bats = cb_cat;

	(tscs+0)->column_names = (str*) GDKmalloc(num_c[0]*sizeof(str));
	(tscs+0)->column_types_strs = (str*) GDKmalloc(num_c[0]*sizeof(str));
	for(c = 0; c < num_c[0]; c++)
	{
		(tscs+0)->column_names[c] = GDKstrdup(cn_fil[c]);
		(tscs+0)->column_types_strs[c] = GDKstrdup(cts_fil[c]);
	}

	(tscs+1)->column_names = (str*) GDKmalloc(num_c[1]*sizeof(str));
	(tscs+1)->column_types_strs = (str*) GDKmalloc(num_c[1]*sizeof(str));
	for(c = 0; c < num_c[1]; c++)
	{
		(tscs+1)->column_names[c] = GDKstrdup(cn_cat[c]);
		(tscs+1)->column_types_strs[c] = GDKstrdup(cts_cat[c]);
	}

	ret_tc->schema_name = sch_name;
	ret_tc->tables_columns = tscs;

	ret_tc->table_names = (str*) GDKmalloc(num_tables*sizeof(str));
	ret_tc->num_columns = (int*) GDKmalloc(num_tables*sizeof(int));
	for(t = 0; t < num_tables; t++)
	{
		ret_tc->table_names[t] = GDKstrdup(tn[t]);
		ret_tc->num_columns[t] = num_c[t];
	}

	ret_tc->num_tables = num_tables;

	return MAL_SUCCEED;
}

/*
 * fills the temp_container structure with the "mseed" meta-data and data tables' info.
 *
 * returns error or MAL_SUCCEED
 *
 * TODO: This function is now hardcoding every info. It can be made generic,
 * because required info is in sql_catalog.
 */
str mseed_create_temp_container_with_data_tables(temp_container* ret_tc)
{
	/* cat: (metadata) catalog, fil: (metadata) files. */
	int num_tables = 3;
	int num_c_fil = 8;
	int num_c_cat = 7;
	int num_c_dat = 4;
	int c, t;

	str sch_name = "mseed";

	str cn_fil[] = {"file_location", "dataquality", "network", "station", "location", "channel", "encoding", "byte_order"};
	str cn_cat[] = {"file_location", "seq_no", "record_length", "start_time", "frequency", "sample_count", "sample_type"};
	str cn_dat[] = {"file_location", "seq_no", "sample_time", "sample_value"};

	str cts_fil[] = {"string", "char", "string", "string", "string", "string", "tinyint", "boolean"};
	str cts_cat[] = {"string", "int", "int", "timestamp", "double", "bigint", "char"};
	str cts_dat[] = {"string", "int", "timestamp", "int"};

	sht ct_fil[] = {TYPE_str, TYPE_str, TYPE_str, TYPE_str, TYPE_str, TYPE_str, TYPE_bte, TYPE_bit};
	sht ct_cat[] = {TYPE_str, TYPE_int, TYPE_int, TYPE_timestamp, TYPE_dbl, TYPE_lng, TYPE_str};
	sht ct_dat[] = {TYPE_str, TYPE_int, TYPE_timestamp, TYPE_int};

	str tn[] = {"files", "catalog", "data"};
	int num_c[] = {8, 7, 4};

	bat *cb_fil = (bat*)GDKmalloc(num_c_fil*sizeof(bat));
	bat *cb_cat = (bat*)GDKmalloc(num_c_cat*sizeof(bat));
	bat *cb_dat = (bat*)GDKmalloc(num_c_dat*sizeof(bat));

	temp_subcontainer *tscs = (temp_subcontainer*)GDKmalloc(num_tables*sizeof(temp_subcontainer));

	BAT *aBAT;

	assert(cb_fil!=NULL);
	assert(cb_cat!=NULL);
	assert(cb_dat!=NULL);
	assert(tscs!=NULL);

	/* cb_fil */
	for(c = 0; c < num_c_fil; c++)
	{
		aBAT = BATnew(TYPE_void, ct_fil[c], 0); /* create empty BAT for each column. */
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BATseqbase(aBAT, 0);
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BBPkeepref(cb_fil[c] = aBAT->batCacheid);
	}

	/* cb_cat */
	for(c = 0; c < num_c_cat; c++)
	{
		aBAT = BATnew(TYPE_void, ct_cat[c], 0); /* create empty BAT for each column. */
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BATseqbase(aBAT, 0);
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BBPkeepref(cb_cat[c] = aBAT->batCacheid);
	}

	/* cb_dat */
	for(c = 0; c < num_c_dat; c++)
	{
		aBAT = BATnew(TYPE_void, ct_dat[c], 0); /* create empty BAT for each column. */
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BATseqbase(aBAT, 0);
		if ( aBAT == NULL)
			throw(MAL,"mseed_create_temp_container",MAL_MALLOC_FAIL);
		BBPkeepref(cb_dat[c] = aBAT->batCacheid);
	}

	(tscs+0)->column_bats = cb_fil;
	(tscs+1)->column_bats = cb_cat;
	(tscs+2)->column_bats = cb_dat;

	(tscs+0)->column_names = (str*) GDKmalloc(num_c[0]*sizeof(str));
	(tscs+0)->column_types_strs = (str*) GDKmalloc(num_c[0]*sizeof(str));
	for(c = 0; c < num_c[0]; c++)
	{
		(tscs+0)->column_names[c] = GDKstrdup(cn_fil[c]);
		(tscs+0)->column_types_strs[c] = GDKstrdup(cts_fil[c]);
	}

	(tscs+1)->column_names = (str*) GDKmalloc(num_c[1]*sizeof(str));
	(tscs+1)->column_types_strs = (str*) GDKmalloc(num_c[1]*sizeof(str));
	for(c = 0; c < num_c[1]; c++)
	{
		(tscs+1)->column_names[c] = GDKstrdup(cn_cat[c]);
		(tscs+1)->column_types_strs[c] = GDKstrdup(cts_cat[c]);
	}

	(tscs+2)->column_names = (str*) GDKmalloc(num_c[2]*sizeof(str));
	(tscs+2)->column_types_strs = (str*) GDKmalloc(num_c[2]*sizeof(str));
	for(c = 0; c < num_c[2]; c++)
	{
		(tscs+2)->column_names[c] = GDKstrdup(cn_dat[c]);
		(tscs+2)->column_types_strs[c] = GDKstrdup(cts_dat[c]);
	}

	ret_tc->schema_name = sch_name;
	ret_tc->tables_columns = tscs;

	ret_tc->table_names = (str*) GDKmalloc(num_tables*sizeof(str));
	ret_tc->num_columns = (int*) GDKmalloc(num_tables*sizeof(int));
	for(t = 0; t < num_tables; t++)
	{
		ret_tc->table_names[t] = GDKstrdup(tn[t]);
		ret_tc->num_columns[t] = num_c[t];
	}

	ret_tc->num_tables = num_tables;

	return MAL_SUCCEED;
}

/*
 * concatenates num_words_to_concat strings that are in words_to_concat into
 * one string and stores it in ret_concatenated.
 *
 * returns the total_len of the resulting string without the null terminator.
 */
int concatenate_strs(str* words_to_concat, int num_words_to_concat, str* ret_concatenated)
{
	int w;
	int total_len = 1; /* null terminator */
	str tmp;

	for(w = 0; w < num_words_to_concat; w++)
		total_len += strlen(words_to_concat[w]);

	*ret_concatenated = tmp = (str)GDKmalloc(total_len*sizeof(char));

	for(w = 0; w < num_words_to_concat; w++)
	{
		tmp = stpcpy(tmp, words_to_concat[w]);
	}
	*tmp = '\0';

	return total_len-1; /* without null terminator; */
}

/*
 * forms and executes sql 'CREATE FUNCTION' queries according to the attributes of
 * the tables_to_be_filled which are in temp_container tc.
 *
 * returns error or MAL_SUCCEED.
 */
str prepare_insertion(Client cntxt, temp_container* tc)
{
/* form a sql query str like this: */
/* 	CREATE FUNCTION mseed_register_fil(ticket bigint, table_idx int) */
/* 	RETURNS table(file_location string, dataquality char, network string, station string, location string, channel string, encoding tinyint, byte_order boolean) external name registrar.register_table; */

	int t, c;
	str space = " ";
	str comma_space = ", ";

	for(t = 0; t < tc->num_tables; t++)
	{
		int concat_len, num_words_to_concat;
		str q, msg, concatenated=NULL;
		str* words_to_concat;

		if(tc->num_columns[t] <= 0)
			break; /* not a metadata table */

		num_words_to_concat = 4*(tc->num_columns[t]) - 1;
		words_to_concat = (str*)GDKmalloc(num_words_to_concat*sizeof(str));

		for(c = 0; c < tc->num_columns[t]; c++)
		{
			words_to_concat[4*c] = tc->tables_columns[t].column_names[c];
			words_to_concat[4*c+1] = space;
			words_to_concat[4*c+2] = tc->tables_columns[t].column_types_strs[c];
			if(c != tc->num_columns[t]-1)
				words_to_concat[4*c+3] = comma_space;
		}

		concat_len = concatenate_strs(words_to_concat, num_words_to_concat, &concatenated);
		if(concat_len < 1 || concatenated == NULL)
		{
			throw(MAL,"registrar.prepare_insertion",MAL_MALLOC_FAIL);
		}

		q = (str)GDKmalloc(512*sizeof(char));
		sprintf(q, "CREATE FUNCTION %s_%s_reg(ticket bigint, table_idx int) RETURNS table(%s) external name registrar.register_table;\n", tc->schema_name, tc->table_names[t], concatenated);

		if((msg =SQLstatementIntern(cntxt,&q,"registrar.create.function",TRUE,FALSE))!= MAL_SUCCEED)
		{/* create function query not succeeded, what to do */
			return msg;
		}

	}

	return MAL_SUCCEED;

}

/*
 * forms and executes sql 'INSERT INTO ... SELECT * FROM' queries for each of
 * the tables_to_be_filled which are in temp_container tc. Uses the new sql
 * functions created by a prepare_insertion call.
 *
 * returns error or MAL_SUCCEED.
 */
str insert_into_vault(Client cntxt, MalBlkPtr mb, temp_container* tc)
{
/* form a sql query str like this: */
/* INSERT INTO mseed.files SELECT * FROM mseed_files_reg(ticket, table_idx); */

	int t;
	long ticket = (long) tc;
	mvc *m = NULL;
	str msg;

	for(t = 0; t < tc->num_tables; t++)
	{
		str q = (str)GDKmalloc(512*sizeof(char));
		sprintf(q, "INSERT INTO %s.%s SELECT * FROM %s_%s_reg(%ld, %d);\n", tc->schema_name, tc->table_names[t], tc->schema_name, tc->table_names[t], ticket, t);

		if((msg =SQLstatementIntern(cntxt,&q,"registrar.insert",TRUE,FALSE))!= MAL_SUCCEED)
		{/* insert into query not succeeded, what to do */
			return msg;
		}

	}

	if((msg = getSQLContext(cntxt, mb, &m, NULL))!= MAL_SUCCEED)
	{/* getting mvc failed, what to do */
		return msg;
	}

	if(mvc_commit(m, 0, NULL) < 0)
	{/* committing failed */
		throw(MAL,"registrar.insert_into_vault", "committing failed\n");
	}

	return MAL_SUCCEED;
}


/*
 * frees the memory that tc occupies and releases the references to the BATs
 *
 * returns MAL_SUCCEED.
 */
str register_clean_up(temp_container* tc)
{
	int t, c;

	for(t = 0; t < tc->num_tables; t++)
	{
		for(c = 0; c < tc->num_columns[t]; c++)
		{
			BBPdecref(tc->tables_columns[t].column_bats[c], TRUE);
			GDKfree(tc->tables_columns[t].column_names[c]);
			GDKfree(tc->tables_columns[t].column_types_strs[c]);
		}

		GDKfree(tc->tables_columns[t].column_bats);
		GDKfree(tc->table_names[t]);

	}

	GDKfree(tc->tables_columns);
/* 	GDKfree(tc->schema_name); */
	GDKfree(tc->table_names);
	GDKfree(tc->num_columns);

	GDKfree(tc);

	return MAL_SUCCEED;
}


/*
 * appends the metadata of the input "mseed" file provided in the file_path,
 * to the end of BATs of temp_container ret_tc.
 *
 * returns error or MAL_SUCCEED.
 *
 * WARNING: this is the DEVELOPER-PROVIDED function.
 *
 * TODO: A better interface can be provided to submit values for the attributes
 * of tables_to_be_filled.
 */
str mseed_register(str file_path, temp_container* ret_tc)
{

	MSRecord *msr = NULL;
	int retcode;
	short int verbose = 1;
	BAT *aBAT = NULL;
	int files_done = FALSE;
	timestamp start_timestamp;
	int seq_no_fake = 1;
	lng st;
	str ch = (str) GDKmalloc(2*sizeof(char));
	ch[1] = '\0';

	while ((retcode = ms_readmsr (&msr, file_path, 0, NULL, NULL, 1, 0, verbose)) == MS_NOERROR)
	{
		if(!files_done)
		{
			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[0])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) file_path, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[0]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[1])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			ch[0] = msr->dataquality;
			BUNappend(aBAT, (ptr) ch, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[1]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[2])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->network, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[2]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[3])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->station, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[3]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[4])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->location, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[4]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[5])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->channel, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[5]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[6])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) &(msr->encoding), FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[6]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[7])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) &(msr->byteorder), FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[7]);

			files_done = TRUE;
		}

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[0])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) file_path, FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[0]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[1])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
/* 		BUNappend(aBAT, (ptr) &(msr->sequence_number), FALSE); */
		BUNappend(aBAT, (ptr) &(seq_no_fake), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[1]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[2])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) &(msr->reclen), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[2]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[3])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		st = (lng) msr->starttime / 1000;
		MTIMEtimestamp_lng(&start_timestamp, &st);
		BUNappend(aBAT, (ptr) &start_timestamp, FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[3]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[4])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) &(msr->samprate), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[4]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[5])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) &(msr->samplecnt), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[5]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[6])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		ch[0] = msr->sampletype;
		BUNappend(aBAT, (ptr) ch, FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[6]);

		seq_no_fake++;
	}

	/* Cleanup memory and close file */
	ms_readmsr (&msr, NULL, 0, NULL, NULL, 0, 0, 0);

	if ( retcode != MS_ENDOFFILE )
		throw(MAL, "mseed_register", "Cannot read %s: %s\n", file_path, ms_errorstr(retcode));

	return MAL_SUCCEED;
}

/*
 * appends the meta-data and actual data of the input "mseed" file provided in the file_path,
 * to the end of BATs of temp_container ret_tc.
 *
 * returns error or MAL_SUCCEED.
 *
 * WARNING: this may be an optional DEVELOPER-PROVIDED function.
 *
 * TODO: A better interface can be provided to submit values for the attributes
 * of tables_to_be_filled.
 */
str mseed_register_and_mount(str file_path, temp_container* ret_tc)
{

	MSRecord *msr = NULL;
	int retcode;
	short int verbose = 1;
	short int data_flag = 1;
	BAT *aBAT = NULL;
	BAT *btime = NULL, *bdata = NULL, *bfile = NULL, *bseqno = NULL;
	int files_done = FALSE;
	timestamp start_timestamp;
	int seq_no_fake = 1;
	lng st;
	long i;
	str ch = (str) GDKmalloc(2*sizeof(char));
	ch[1] = '\0';

	while ((retcode = ms_readmsr (&msr, file_path, 0, NULL, NULL, 1, data_flag, verbose)) == MS_NOERROR)
	{
		if(!files_done)
		{
			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[0])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) file_path, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[0]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[1])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			ch[0] = msr->dataquality;
			BUNappend(aBAT, (ptr) ch, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[1]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[2])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->network, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[2]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[3])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->station, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[3]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[4])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->location, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[4]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[5])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) msr->channel, FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[5]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[6])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) &(msr->encoding), FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[6]);

			if ((aBAT = BATdescriptor(ret_tc->tables_columns[0].column_bats[7])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			BUNappend(aBAT, (ptr) &(msr->byteorder), FALSE);
			BBPreleaseref(ret_tc->tables_columns[0].column_bats[7]);

			files_done = TRUE;
		}

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[0])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) file_path, FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[0]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[1])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
/* 		BUNappend(aBAT, (ptr) &(msr->sequence_number), FALSE); */
		BUNappend(aBAT, (ptr) &(seq_no_fake), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[1]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[2])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) &(msr->reclen), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[2]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[3])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		st = (lng) msr->starttime / 1000;
		MTIMEtimestamp_lng(&start_timestamp, &st);
		BUNappend(aBAT, (ptr) &start_timestamp, FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[3]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[4])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) &(msr->samprate), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[4]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[5])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		BUNappend(aBAT, (ptr) &(msr->samplecnt), FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[5]);

		if ((aBAT = BATdescriptor(ret_tc->tables_columns[1].column_bats[6])) == NULL)
			throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
		ch[0] = msr->sampletype;
		BUNappend(aBAT, (ptr) ch, FALSE);
		BBPreleaseref(ret_tc->tables_columns[1].column_bats[6]);

		/* mount */
		{
			int seq_no = seq_no_fake;
			double sample_interval = HPTMODULUS / msr->samprate; /* calculate sampling interval from frequency */
			long sampling_time = msr->starttime;

			long num_samples = msr->samplecnt;
			int *data_samples = msr->datasamples;

			if ((bfile = BATdescriptor(ret_tc->tables_columns[2].column_bats[0])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			if ((bseqno = BATdescriptor(ret_tc->tables_columns[2].column_bats[1])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			if ((btime = BATdescriptor(ret_tc->tables_columns[2].column_bats[2])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);
			if ((bdata = BATdescriptor(ret_tc->tables_columns[2].column_bats[3])) == NULL)
				throw(MAL, "mseed_register", RUNTIME_OBJECT_MISSING);

			for(i = 0; i<num_samples; i++)
			{

				timestamp sampling_timestamp;
				lng st = (lng) sampling_time / 1000;
				MTIMEtimestamp_lng(&sampling_timestamp, &st);

				/* For each sample add one row to the table */
				BUNappend(bfile, (ptr) file_path, FALSE);
				BUNappend(bseqno, (ptr) &seq_no, FALSE);
				BUNappend(btime, (ptr) &sampling_timestamp, FALSE);
				BUNappend(bdata, (ptr) (data_samples+i), FALSE);
				sampling_time += sample_interval;
			}

			BBPreleaseref(ret_tc->tables_columns[2].column_bats[0]);
			BBPreleaseref(ret_tc->tables_columns[2].column_bats[1]);
			BBPreleaseref(ret_tc->tables_columns[2].column_bats[2]);
			BBPreleaseref(ret_tc->tables_columns[2].column_bats[3]);
		}

		seq_no_fake++;
	}

	/* Cleanup memory and close file */
	ms_readmsr (&msr, NULL, 0, NULL, NULL, 0, 0, 0);

	if ( retcode != MS_ENDOFFILE )
		throw(MAL, "mseed_register", "Cannot read %s: %s\n", file_path, ms_errorstr(retcode));

	return MAL_SUCCEED;
}

/*
 * takes a repository path repo_path, finds out the files in it, creates a
 * temp_container of the metadata to be inserted, for each file calls the
 * developer-provided register function which fills in the temp_container,
 * then using prepare_insertion and insert_into_vault calls appends the
 * metadata to the tables_to_be_filled.
 *
 * returns error or MAL_SUCCEED.
 *
 * can be called from MAL or SQL levels.
 */
str register_repo(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str *repo_path = (str*) getArgReference(stk,pci,pci->retc); /* arg 1: repo_path */
	int mode = *(int*) getArgReference(stk,pci,pci->retc+1); /* arg 2: mode 0:register only, mode 1: register+mount */
	str *file_paths = NULL;
	long num_file_paths;
	temp_container *tc;
	long i;
	str err = NULL;
	long start, finish;

	/* fetch file_paths from repo_path */
	num_file_paths = get_file_paths(*repo_path, &file_paths);
	if(num_file_paths < 1 && file_paths == NULL)
	{/* problematic repo, what to do */
		throw(MAL,"registrar.register_repo", "Problematic repository: %s\n", err);
	}

	/* create temp_container */
	tc = (temp_container*)GDKmalloc(sizeof(temp_container));
	assert(tc != NULL);
	if(mode == 0)
		err = mseed_create_temp_container(tc); /* depending on design can get different argument(s) */
	else
		err = mseed_create_temp_container_with_data_tables(tc); /* depending on design can get different argument(s) */
	if(err != MAL_SUCCEED)
	{/* temp_container creation failed, what to do */
		throw(MAL,"registrar.register_repo", "temp_container creation failed: %s\n", err);
	}

	start = GDKms();
	/* loop through the file_paths in repo */
	if(mode == 0)
	{
		for(i = 0; i < num_file_paths; i++)
		{
			err = mseed_register(file_paths[i], tc);
			if(err != MAL_SUCCEED)
			{/* current file cannot be registered, what to do */
	/* 			throw(MAL,"registrar.register_repo", "Current file cannot be registered: %s\n", err); */
				printf("registrar.register_repo: current file cannot be registered: %s\n", err);
			}
		}
	}
	else
	{
		for(i = 0; i < num_file_paths; i++)
		{
			err = mseed_register_and_mount(file_paths[i], tc);
			if(err != MAL_SUCCEED)
			{/* current file cannot be registered, what to do */
			/* 			throw(MAL,"registrar.register_repo", "Current file cannot be registered: %s\n", err); */
			printf("registrar.register_repo: current file cannot be registered and/or mounted: %s\n", err);
			}
		}
	}
	finish = GDKms();
	printf("# Time for extraction and transformation of (meta-)data: %ld milliseconds\n", finish - start);

	start = GDKms();
	/* prepare sql functions for inserting temp_container into tables_to_be_filled */
	err = prepare_insertion(cntxt, tc);
	if(err != MAL_SUCCEED)
	{/* preparing the insertion failed, what to do */
		throw(MAL,"registrar.register_repo", "Insertion prepare failed: %s\n", err);
	}

	/* insert temp_container into tables_to_be_filled */
	err = insert_into_vault(cntxt, mb, tc);
	if(err != MAL_SUCCEED)
	{/* inserting the temp_container into one of the tables failed, what to do */
		throw(MAL,"registrar.register_repo", "Inserting the temp_container into one of the tables failed: %s\n", err);
	}
	finish = GDKms();
	printf("# Time for loading of (meta-)data: %ld milliseconds\n", finish - start);

	err = register_clean_up(tc);
	if(err != MAL_SUCCEED)
	{/* inserting the temp_container into one of the tables failed, what to do */
		throw(MAL,"registrar.register_repo", "Cleaning up the temp_container failed: %s\n", err);
	}

	return MAL_SUCCEED;
}

/*
 * maps the BATs of temp_container to the columns of one of the tables_to_be_filled,
 * because SQL does not allow functions to return arbitrary tables without predefining
 * them. prepare_insertion functions predefines them and this function does the mapping.
 *
 * takes temp_container pointer as a long int (ticket), since it is only called from
 * SQL level. table_idx specifies for which of the tables it is called for.
 *
 * returns error or MAL_SUCCEED.
 */
str register_table(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int c;
	lng ticket = *(lng*) getArgReference(stk,pci,pci->retc); /* arg 1: ticket to the temp_container */
	int table_idx = *(int*) getArgReference(stk,pci,pci->retc+1); /* arg 2: index of the table to be registered in the temp_container */

	temp_container *tc = (temp_container*) ticket; /* filled temp_container taken */

	cntxt = cntxt; /* to escape 'unused' parameter error. */
	mb = mb; /* to escape 'unused' parameter error. */

	if(pci->retc != tc->num_columns[table_idx])
	{/* inconsistency in the number of return BATs, what to do */
		throw(MAL,"registrar.register_table", OPERATION_FAILED);
	}

	for(c = 0; c < pci->retc; c++)
	{
		*(int*) getArgReference(stk,pci,c) = tc->tables_columns[table_idx].column_bats[c];
		BBPincref(tc->tables_columns[table_idx].column_bats[c], TRUE);
	}

	return MAL_SUCCEED;
}










