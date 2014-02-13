#include <pthread.h>
#include "monetdb_config.h"
#include "bam.h"
#include "kstring.h"
#include "bam_sql.h"
#include "bam_loader.h"


/* 
 * Masks that say which fields have to be loaded
 * TRUE (FALSE) at a certain point says that this field is loaded (not loaded)
 */
bit alignments_mask[11]; /* No field for virtual_offset, since we always want that */
bit alignments_extra_mask;

/*
* File format specific structures
*/
typedef struct {
    char tag[3];
    str value;
} _bam_header_option;

typedef struct {
    char header_tag[3];
    _bam_header_option *options;
    sht nr_options;
} _bam_header_line;


/* Storage for a BAM file */

typedef struct {    
    /* Header fields */
    sht file_id;
    str file_location;
    sht dbschema;
    str storage_mask;
    
    /* Counters for encountered data */
    unsigned int cnt_sq;
    unsigned int cnt_rg;
    unsigned int cnt_pg;
    lng cnt_alignments;
    lng cnt_alignments_extra;
    lng cnt_alignments_paired_primary;
    lng cnt_alignments_paired_secondary;
    
    /* File paths to binary files */
    char fp_files[7][128];
    char fp_sq[7][128];
    char fp_rg[13][128];
    char fp_pg[6][128];
    char fp_alignments[12][128]; /* Used for unpaired alignments in the case of schema 1 */
    char fp_alignments_extra[4][128];
    char fp_alignments_paired_primary[23][128]; /* Only used for schema 1 */
    char fp_alignments_paired_secondary[23][128]; /* Only used for schema 1 */
    
    /* FILE pointers to binary files */
    FILE *files[7];
    FILE *sq[7];
    FILE *rg[13];
    FILE *pg[6];
    FILE *alignments[12]; /* Used for unpaired alignments in the case of schema 1 */
    FILE *alignments_extra[4];
    FILE *alignments_paired_primary[23];
    FILE *alignments_paired_secondary[23];
} _bam_storage;

typedef struct {
    int max_nr_threads;
    bit *thread_available;
    pthread_mutex_t threads_data_lock;
} _threads_data;

typedef struct {
    _threads_data *d;
    bit concurrent_readers;
    pthread_t *threads; /* All readers should have a thread pool separate from the writers */
    int thread_id;
    sht dbschema;
    str storage_mask;
    Client cntxt;
    MalBlkPtr mb;
    str *file_paths;
    sht nr_file_paths;
    sht *current_file;
    sht start_file_id;
    pthread_mutex_t *current_file_lock;
    pthread_mutex_t *client_lock;
} _reader_thread_data;

typedef struct {
    lng  virtual_offset;
    char qname[MAX_QNAME_LENGTH];
    sht  flag;
    str  rname;
    int  pos;
    sht  mapq;
    char cigar[MAX_CIGAR_LENGTH];
    str  rnext;
    int  pnext;
    int  tlen;
    char seq[MAX_SEQ_LENGTH];
    char qual[MAX_SEQ_LENGTH];
    
    bit written;
} _alignment; /* Used for temporarily storing alignment data when loading dbschema 1 */

#define _kth_bit(nr, k) (((nr) & (1 << (k))) == (1 << (k)))

#define _mult_segm(a) (_kth_bit((a).flag, 0))
#define _prop_alig(a) (_kth_bit((a).flag, 1))
#define _segm_unma(a) (_kth_bit((a).flag, 2))
#define _next_unma(a) (_kth_bit((a).flag, 3))
#define _segm_reve(a) (_kth_bit((a).flag, 4))
#define _next_reve(a) (_kth_bit((a).flag, 5))
#define _firs_segm(a) (_kth_bit((a).flag, 6))
#define _last_segm(a) (_kth_bit((a).flag, 7))
#define _seco_alig(a) (_kth_bit((a).flag, 8))
#define _qual_cont(a) (_kth_bit((a).flag, 9))
#define _opti_dupl(a) (_kth_bit((a).flag, 10))

#define _append_alignment(a, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12) \
    _append_lng(f1, (a).virtual_offset); \
    if(f2 != NULL) _append_str(f2, (a).qname); \
    _append_sht(f3, (a).flag); \
    _append_str(f4, (a).rname); \
    _append_int(f5, (a).pos); \
    _append_sht(f6, (a).mapq); \
    _append_str(f7, (a).cigar); \
    _append_str(f8, (a).rnext); \
    _append_int(f9, (a).pnext); \
    _append_int(f10, (a).tlen); \
    _append_str(f11, (a).seq); \
    _append_str(f12, (a).qual); \

#define _append_alignment_unpaired(a, bs) \
    _append_alignment(a, bs->alignments[0], bs->alignments[1], bs->alignments[2], \
        bs->alignments[3], bs->alignments[4], bs->alignments[5], bs->alignments[6], bs->alignments[7], \
        bs->alignments[8], bs->alignments[9], bs->alignments[10], bs->alignments[11])
        
#define _append_alignment_prim_paired_l(a, bs) \
    _append_alignment(a, bs->alignments_paired_primary[0], bs->alignments_paired_primary[2], \
        bs->alignments_paired_primary[3], bs->alignments_paired_primary[4], bs->alignments_paired_primary[5], \
        bs->alignments_paired_primary[6], bs->alignments_paired_primary[7], bs->alignments_paired_primary[8], \
        bs->alignments_paired_primary[9], bs->alignments_paired_primary[10], bs->alignments_paired_primary[11], \
        bs->alignments_paired_primary[12])
        
#define _append_alignment_prim_paired_r(a, bs) \
    _append_alignment(a, bs->alignments_paired_primary[1], NULL, bs->alignments_paired_primary[13], \
        bs->alignments_paired_primary[14], bs->alignments_paired_primary[15], bs->alignments_paired_primary[16], \
        bs->alignments_paired_primary[17], bs->alignments_paired_primary[18], bs->alignments_paired_primary[19], \
        bs->alignments_paired_primary[20], bs->alignments_paired_primary[21], bs->alignments_paired_primary[22])
        
#define _append_alignment_seco_paired_l(a, bs) \
    _append_alignment(a, bs->alignments_paired_secondary[0], bs->alignments_paired_secondary[2], \
        bs->alignments_paired_secondary[3], bs->alignments_paired_secondary[4], bs->alignments_paired_secondary[5], \
        bs->alignments_paired_secondary[6], bs->alignments_paired_secondary[7], bs->alignments_paired_secondary[8], \
        bs->alignments_paired_secondary[9], bs->alignments_paired_secondary[10], bs->alignments_paired_secondary[11], \
        bs->alignments_paired_secondary[12])
        
#define _append_alignment_seco_paired_r(a, bs) \
    _append_alignment(a, bs->alignments_paired_secondary[1], NULL, bs->alignments_paired_secondary[13], \
        bs->alignments_paired_secondary[14], bs->alignments_paired_secondary[15], bs->alignments_paired_secondary[16], \
        bs->alignments_paired_secondary[17], bs->alignments_paired_secondary[18], bs->alignments_paired_secondary[19], \
        bs->alignments_paired_secondary[20], bs->alignments_paired_secondary[21], bs->alignments_paired_secondary[22])

FILE *_log;


static void
_set_masks(int dbschema, str storage_mask)
{
    int i;
    if(dbschema == 2)
    {
        for(i=0; i<9; ++i)
            alignments_mask[i] = TRUE;
        alignments_mask[9] = FALSE;
        alignments_mask[10] = FALSE;
        
        alignments_extra_mask = FALSE;
    }
    else if(strlen(storage_mask) == 12) /* Masks with length != 12 will just be ignored */
    {
        for(i=0; i<11; ++i)
            alignments_mask[i] = storage_mask[i] == '0' ? 0 : 1;
        alignments_extra_mask = storage_mask[11] != '0';
    }
    else
    {
        for(i=0; i<11; ++i)
            alignments_mask[i] = TRUE;
        alignments_extra_mask = TRUE;
    }
}

static str
_init_bam_storage(_bam_storage *bs, sht file_id, str file_location, sht dbschema, str storage_mask)
{
    str flush_dir_ptn = DIR_FLUSH"/%d";
    char flush_dir[64];
    unsigned int i;
    
    mkdir(DIR_FLUSH, 0777);
    
    bs->file_id = file_id;
    bs->file_location = file_location;
    bs->dbschema = dbschema;
    bs->storage_mask = storage_mask;
        
    bs->cnt_sq = 0;
    bs->cnt_rg = 0;
    bs->cnt_pg = 0;
    bs->cnt_alignments = 0;
    bs->cnt_alignments_extra = 0;
    bs->cnt_alignments_paired_primary = 0;
    bs->cnt_alignments_paired_secondary = 0;
    
    sprintf(flush_dir, flush_dir_ptn, file_id);
    mkdir(flush_dir, 0777);
    
    for(i=0; i<7; ++i)
    {
        sprintf(bs->fp_files[i], "%s/files_%d", flush_dir, i);
        if((bs->files[i] = fopen(bs->fp_files[i], "w")) == NULL)
            return "Error opening binary file for table 'files'";
    }
    for(i=0; i<7; ++i)
    {
        sprintf(bs->fp_sq[i], "%s/sq_%d", flush_dir, i);
        if((bs->sq[i] = fopen(bs->fp_sq[i], "w")) == NULL)
            return "Error opening binary file for table 'sq'";
    }
    for(i=0; i<13; ++i)
    {
        sprintf(bs->fp_rg[i], "%s/rg_%d", flush_dir, i);
        if((bs->rg[i] = fopen(bs->fp_rg[i], "w")) == NULL)
            return "Error opening binary file for table 'rg'";
    }
    for(i=0; i<6; ++i)
    {
        sprintf(bs->fp_pg[i], "%s/pg_%d", flush_dir, i);
        if((bs->pg[i] = fopen(bs->fp_pg[i], "w")) == NULL)
            return "Error opening binary file for table 'pg'";
    }
    for(i=0; i<12; ++i)
    {
        if(i > 0 && !alignments_mask[i-1]) continue;
        sprintf(bs->fp_alignments[i], "%s/alignments_%d", flush_dir, i);
        if((bs->alignments[i] = fopen(bs->fp_alignments[i], "w")) == NULL)
        {
            if(dbschema == 1) return "Error opening binary file for table 'unpaired_alignments'";
            else              return "Error opening binary file for table 'alignments'";
        }
    }
    if(alignments_extra_mask)
    {
        for(i=0; i<4; ++i)
        {
            sprintf(bs->fp_alignments_extra[i], "%s/alignments_extra_%d", flush_dir, i);
            if((bs->alignments_extra[i] = fopen(bs->fp_alignments_extra[i], "w")) == NULL)
                return "Error opening binary file for table 'alignments_extra'";
        }
    }
    if(dbschema == 1)
    {
        for(i=0; i<23; ++i)
        {
            sprintf(bs->fp_alignments_paired_primary[i], "%s/alignments_paired_primary_%d", flush_dir, i);
            if((bs->alignments_paired_primary[i] = fopen(bs->fp_alignments_paired_primary[i], "w")) == NULL)
                return "Error opening binary file for table 'paired_primary_alignments'";
            sprintf(bs->fp_alignments_paired_secondary[i], "%s/alignments_paired_secondary_%d", flush_dir, i);
            if((bs->alignments_paired_secondary[i] = fopen(bs->fp_alignments_paired_secondary[i], "w")) == NULL)
                return "Error opening binary file for table 'paired_secondary_alignments'";
        }
    }
    
    return MAL_SUCCEED;
}

static void
_free_bam_storage(_bam_storage *bs)
{
    unsigned int i;
    
    for(i=0; i<7; ++i)
        fclose(bs->files[i]);
    for(i=0; i<7; ++i)
        fclose(bs->sq[i]);
    for(i=0; i<13; ++i)
        fclose(bs->rg[i]);
    for(i=0; i<6; ++i)
        fclose(bs->pg[i]);
    for(i=0; i<12; ++i)
    {
        if(i > 0 && !alignments_mask[i-1]) continue;
        fclose(bs->alignments[i]);
    }
    if(alignments_extra_mask)
        for(i=0; i<4; ++i)
            fclose(bs->alignments_extra[i]);
    if(bs->dbschema == 1)
    {
        for(i=0; i<23; ++i)
        {
            fclose(bs->alignments_paired_primary[i]);
            fclose(bs->alignments_paired_secondary[i]);
        }
    }
}




/* Helper functions */

static void 
_append_to_log(str mssg)
{
    printf("bam_loader log: %s\n", mssg);
    if(_log != NULL)
    {
        fprintf(_log, "> %s\n\n", mssg);
        fflush(_log);
    }
}

static void
_open_log(void)
{
    _log = fopen(LOGPATH, "a");
    if(_log == NULL)
        printf("Error opening logfile '%s' for writing.\n", LOGPATH);
    else
        fprintf(_log, "=============================================================\n\n");
}

static void
_close_log(void)
{
    if(_log != NULL)
    {
        fprintf(_log, "\n=============================================================\n\n\n\n\n\n\n\n\n\n");
        fclose(_log);
    }
}


/* returns strlen(ret) or -1 when GDKmalloc on *ret fails
*  src will be advanced during the reading.
*/
static int 
_read_string_until_delim(str *src, str *ret, char *delims, sht nr_delims) 
{
    int i, size = 0;
    int end = FALSE;
    while(TRUE)
    {
        /* is the current character one of the delimiters? */
        for(i = 0; i < nr_delims; ++i)
        {
            if(*(*src+size) == delims[i])
            {
                /* end this loop, we have encountered a delimiter at *(src+size) */
                /* this means that the length of the string before this delimiter equals size */
                end = TRUE;
                break;
            }
        }
        if(end)
            break;
        ++size;
    }
    
    /* we now know the size, copy the right part of src to ret */
    *ret = GDKmalloc((size + 1) * sizeof(char));
    if(*ret == NULL)
        return -1;
        
    strncpy(*ret, *src, size);
    *(*ret+size) = '\0';
    
    /* and advance src */
    *src += size;
    
    /* done, return size of *ret */
    return size;
}

static str
_run_sql_from_file(Client cntxt, pthread_mutex_t *client_lock, str filename, 
    char search[][MAX_SQL_SEARCH_REPLACE_CHARS], char replace[][MAX_SQL_SEARCH_REPLACE_CHARS], int nr_replacement_strings, bit check_masks)
{
    char err[256];
    char filepath[512];
    str result = NULL;
    str file_contents;
    sprintf(filepath, DIR_SQL"/%s", filename);
    file_contents = get_sql_from_file(filepath, search, replace, nr_replacement_strings, err);
    if(file_contents == NULL)
        throw(MAL, "_run_sql_from_file", "%s", err);
        
    /* If we have to check the masks, build a new string in which the lines that start with 'if mask[i]' are removed if !mask[i] */
    if(check_masks)
    {
        str tmp = file_contents;
        str s = GDKmalloc(strlen(file_contents) * sizeof(char));
        int s_index = 0;
        char mask_index_str[3];
        
        if(s == NULL)
            throw(MAL, "_run_sql_from_file", MAL_MALLOC_FAIL);
        
        while(TRUE)
        {
            sht mask_index_sht = -1;
            if(tmp[0] == 'i' &&
               tmp[1] == 'f' &&
               tmp[2] == ' ' &&
               tmp[3] == 'm' &&
               tmp[4] == 'a' &&
               tmp[5] == 's' &&
               tmp[6] == 'k' &&
               tmp[7] == '[')
            {
                mask_index_str[0] = tmp[8];
                mask_index_str[1] = tmp[9] == ']' ? '\0' : tmp[9];
                mask_index_str[2] = '\0';
                mask_index_sht = strtol(mask_index_str, NULL, 10);
                tmp += tmp[9] == ']' ? 10 : 11;
            }
            
            /* Remove white space */
            while(*tmp == ' ')
                tmp++;
            
            if(mask_index_sht < 0 || (mask_index_sht == 11 && alignments_extra_mask) || alignments_mask[mask_index_sht])
            {
                /* Mask filter wasn't found or the appropriate mask bit is set: copy contents */
                while(*tmp != '\n' && *tmp != '\0')
                {
                    s[s_index++] = *tmp;
                    tmp++;
                }
                s[s_index++] = *tmp;
                if(*tmp == '\0') break;
                tmp++;
            }
            else
            {
                /* Skip current line */
                while(*tmp != '\n' && *tmp != '\0')
                    tmp++;
                if(*tmp == '\0') 
                {
                    s[s_index++] = '\0';
                    break;
                }
                tmp++;
            }
        }
        
        GDKfree(file_contents);
        file_contents = s; /* file_contents points to s from now on, so GDKfree(file_contents) further down the road will free s */
    }
        
    _append_to_log(file_contents);
    
    if(client_lock != NULL) pthread_mutex_lock(client_lock);
    result = SQLstatementIntern(cntxt, &file_contents, "bam.create_alignment_tables", TRUE, FALSE);
    if(client_lock != NULL) pthread_mutex_unlock(client_lock);
    
    GDKfree(file_contents);
    return result;
}


static int 
_get_file_paths(str filelist_path, str *ret_file_paths)
{
    int nr_file_paths = 0;
    size_t max_bam_fp_length = MAX_BAM_FILEPATH_LENGTH;
    str line = ret_file_paths[0];
    int line_size;
    FILE *f = fopen(filelist_path, "r");
    
    if(f == NULL)
        return -1;
        
    while((line_size = getline(&line, &max_bam_fp_length, f)) > 0)
    {
        if(line[0] == '\n') break;
        if(line[line_size-1] == '\n')
            line[line_size-1] = '\0'; /* Newline character is not part of the filepath */
        line = ret_file_paths[++nr_file_paths];
    }

    return nr_file_paths;
}


/* This method retrieves the number of records in the files table and returns this number + 1 as the new file_id
* It would be prettier to use a sequence for this purpose. However, this was more complicated to achieve, so therefore
* the method is implemented like this. Furthermore, after insertions, we would need to retrieve these newly inserted 
* auto_incr values, since we have to use them as foreign keys in all other tables. This makes it even more complicated.
* 
* Note: the client context doesn't need to be protected with a mutex, since right now _next_file_id is never called in
* a multithreaded piece of code
*/
static str
_next_file_id(Client cntxt, MalBlkPtr mb, sht *next_file_id)
{
    mvc *m = NULL;
    sql_schema *s;
    sql_table *t;
    sql_column *c;
    BAT *b = NULL;
    BATiter li;
    sht i;
    BUN p = 0, q = 0;
    sht max_file_id = 0;
    str msg;
    
    if((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
        throw(MAL, "_next_file_id", "Error retrieving SQLContext: %s\n", msg);
    if((s = mvc_bind_schema(m, "bam")) == NULL)
        throw(MAL, "_next_file_id", "Error binding schema 'bam'\n");
    if((t = mvc_bind_table(m, s, "files")) == NULL)
        throw(MAL, "_next_file_id", "Error binding table 'files'\n");
    if((c = mvc_bind_column(m, t, "file_id")) == NULL)
        throw(MAL, "_next_file_id", "Error binding first column of table 'files'\n");
        
    for(i=0; i<3; ++i)
    {
        b = store_funcs.bind_col(m->session->tr, c, i);
        if(b != NULL)
        {
            li = bat_iterator(b);
            BATloop(b, p, q) {
                sht t = *(sht *)BUNtail(li, p);
                max_file_id = MAX(max_file_id, t);
            }
            BBPreleaseref(b->batCacheid);
        }
    }
    *next_file_id = max_file_id+1;
    return MAL_SUCCEED;
}


/*
* Considers the first rule of the input (*header) as a header line and attempts to parse it into a _bam_header_line structure
* The *eof flag will be set to TRUE if the input doesn't contain a header line anymore
*/
static str 
_parse_bam_header_line(str *header, _bam_header_line *ret_hl, bit *eof)
{
    _bam_header_option *opt;
    sht nr_options = 0;
        
    /* start by stripping \n, \r, \t and spaces */
    while(**header == '\n' || **header == '\r' || **header == '\t' || **header == ' ')
    {
        (*header)++;
    }
    
    if(**header == '\0')
    {
        /* eof reached */
        *eof = TRUE;
        return MAL_SUCCEED;
    }
    
    if(**header != '@')
        /* first character on header line should really be @ */
        throw(MAL, "_parse_bam_header_line", "Header line that doesn't start with '@' detected\n");
    
    /* strip the @ */
    (*header)++;
    
    /* eof not reached, so a header tag should be present, store it as lng as no \0 is there */
    if(**header == '\0' || *(*header+1) == '\0')
        throw(MAL, "_parse_bam_header_line", "Unexpected end of header\n");
    ret_hl->header_tag[0] = **header;
    ret_hl->header_tag[1] = *(*header+1);
    ret_hl->header_tag[2] = '\0';
    *header += 2;
    
    /* if this is a comment, we only need to read the rest of the line */
    if(strcmp(ret_hl->header_tag, "CO") == 0)
    {
        /* initialize opt */
        opt = (_bam_header_option *)GDKmalloc(sizeof(_bam_header_option));
        opt->tag[0] = '\0'; /* indicate that no tag exists for this option */
        _read_string_until_delim(header, &opt->value, "\n\0", 2);
        ret_hl->options = opt; /* option only has to point to a single _bam_header_option in this case */
        ret_hl->nr_options = 1;
        return MAL_SUCCEED;
    }
    
    /* reserve enough space for the options (max 12 for RG) */
    ret_hl->options = GDKmalloc(12 * sizeof(_bam_header_option));
    
    /* now get all options */
    while(TRUE) /* iterate once in this loop for every option */
    {
        /* throw away tab(s)/space(s) */
        while(**header == '\t' || **header == ' ')
            (*header)++;
            
        /* if after this, a newline or \0 is presented, we are done */
        if(**header == '\n' || **header == '\0')
            break;
            
        /* a new option will be presented, see if we don't already have too many options */
        if(nr_options == 12)
            throw(MAL, "_parse_bam_header_line", "More than 12 options in header line\n");
        
        /* make opt point to the right _bam_header_option */
        opt = &ret_hl->options[nr_options++];
        
        /* read tag */
        if(**header == '\0' || *(*header+1) == '\0')
            throw(MAL, "_parse_bam_header_line", "Unexpected end of header line\n");
        opt->tag[0] = **header;
        opt->tag[1] = *(*header+1);
        opt->tag[2] = '\0';
        *header += 2;
        
        /* a colon should be presented at this point */
        if(**header != ':')
            throw(MAL, "_parse_bam_header_line", "Expected a colon (:) after option tag in header line\n");
        (*header)++;
        
        /* read value of this option */
        _read_string_until_delim(header, &opt->value, "\t\n\0", 3);
    }
    
    ret_hl->nr_options = nr_options;
    
    /* real number of options is now known, shrink the options array in the header line */
    ret_hl->options = GDKrealloc(ret_hl->options, nr_options * sizeof(_bam_header_option));
    
    return MAL_SUCCEED;
}

/*
* Free the memory occupied by a _bam_header_line structure
*/
static void 
_free_bam_header_line(_bam_header_line *hl)
{
    sht o;
    if(hl == NULL)
        return;
    
    for(o=0; o<hl->nr_options; ++o)
        GDKfree(hl->options[o].value);
    GDKfree(hl->options);
    GDKfree(hl);
}

#define _append_str(file, s) fprintf(file, "%s\n", s)
#define _append_sht(file, i) fwrite(&i, sizeof(sht), 1, file)
#define _append_int(file, i) fwrite(&i, sizeof(int), 1, file)
#define _append_lng(file, i) fwrite(&i, sizeof(lng), 1, file)

#define _append_option_cond_str(file, opt, cmp, flag) \
    if(strcmp((opt).tag, (cmp)) == 0) \
    { \
        _append_str(file, (opt).value); \
        (flag) = TRUE; \
        continue; \
    }

#define _append_option_cond_int(file, opt, cmp, flag, l, s) \
    if(strcmp((opt).tag, (cmp)) == 0) \
    { \
        (l) = strtol((opt).value, &s, 10); \
        if((s) == ((opt).value) || (l) == LONG_MIN || (l) == LONG_MAX) \
            l = -1; \
        _append_int(file, l); \
        (flag) = TRUE; \
        continue; \
    }
    
#define _append_option_cond_lng(file, opt, cmp, flag, l, s) \
    if(strcmp((opt).tag, (cmp)) == 0) \
    { \
        (l) = strtol((opt).value, &s, 10); \
        if((s) == ((opt).value) || (l) == LONG_MIN || (l) == LONG_MAX) \
            l = -1; \
        _append_lng(file, l); \
        (flag) = TRUE; \
        continue; \
    }


/*
* Process the ASCII BAM header provided in the header argument. Store the parsed contents of the header in the _temp_container structure
*/
static str 
_parse_bam_header(_bam_storage *bs, _threads_data *d, int thread_id, str header)
{
    _bam_header_line *hl;
    int i, o;
    str err = NULL;
    
    /* declare variables for checking mandatory fields */
    int nr_hd_lines = 0; /* used to restrict the total number of header lines to max 1 */
    char hd_comment[MAX_HEADER_COMMENT_LENGTH];
    bit comment_found = FALSE;
    bit hd_fields_found[2] = {FALSE, FALSE};
    bit sq_fields_found[6];
    bit rg_fields_found[12];
    bit pg_fields_found[5];
    bit eof = FALSE;
    str s;
    lng l;
    
    hd_comment[0] = '\0';
    (void)d;
    (void)thread_id;
    _append_sht(bs->files[0], bs->file_id);
    _append_str(bs->files[1], bs->file_location);
    _append_sht(bs->files[2], bs->dbschema);
    _append_str(bs->files[3], bs->storage_mask);
        
    /* loop will run until no more header lines are found */
    while(TRUE) 
    {
        /* reserve new memory for this header line */
        hl = (_bam_header_line *)GDKmalloc(sizeof(_bam_header_line));
        
        if(hl == NULL)
            throw(MAL, "_parse_bam_header", MAL_MALLOC_FAIL);
        
        hl->nr_options = 0;
        
        /* try to read the next header line */
        if((err = _parse_bam_header_line(&header, hl, &eof)) != MAL_SUCCEED)
        {
            _free_bam_header_line(hl);
            throw(MAL, "_parse_bam_header", "Error while reading header line: %s\n", err);
        }
        
        /* if eof is set to TRUE by _parse_bam_header_line, this indicates that we reached the end of the header */
        if(eof)
        {
            GDKfree(hl);
            break;
        }
            
        /* read and interpret the header tag */
        if(strcmp(hl->header_tag, "HD") == 0)
        {
            ++nr_hd_lines;
            if(nr_hd_lines > 1)
            {
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "More than one HD line found in header\n");
            }
            
            for(o = 0; o < hl->nr_options; ++o)
            {
                _append_option_cond_str(bs->files[4], hl->options[o], "VN", hd_fields_found[0]);
                _append_option_cond_str(bs->files[5], hl->options[o], "SO", hd_fields_found[1]);

                /* if this point is reached, option wasn't recognized */
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "Unknown option found in header tag HD");
            }
            if(!hd_fields_found[0])
            {
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "VN tag not found in HD header line\n");
            }
            /* We don't have to populate the sorting_order with str_nil if it wasn't found, since this is done at the end of this function anyway */
        }
        else if(strcmp(hl->header_tag, "SQ") == 0)
        {
            ++bs->cnt_sq;
            _append_sht(bs->sq[1], bs->file_id);
            
            for(i=0; i<6; ++i)
                sq_fields_found[i] = FALSE;
            
            for(o = 0; o < hl->nr_options; ++o)
            {
                _append_option_cond_str(bs->sq[0], hl->options[o], "SN", sq_fields_found[0]);
                _append_option_cond_int(bs->sq[2], hl->options[o], "LN", sq_fields_found[1], l, s);
                _append_option_cond_int(bs->sq[3], hl->options[o], "AS", sq_fields_found[2], l, s);
                _append_option_cond_str(bs->sq[4], hl->options[o], "M5", sq_fields_found[3]);
                _append_option_cond_str(bs->sq[5], hl->options[o], "SP", sq_fields_found[4]);
                _append_option_cond_str(bs->sq[6], hl->options[o], "UR", sq_fields_found[5]);
                
                /* if this point is reached, option wasn't recognized */
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "Unknown option found in header tag SQ");
            }
            if(!sq_fields_found[0])
                err = "SN tag not found in SQ header line\n";
            if(!sq_fields_found[1])   
                err = "LN tag not found in SQ header line\n";
                
            if(err != NULL)
            {
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "%s", err);
            }

            if(!sq_fields_found[2]) _append_int(bs->sq[3], int_nil);
            if(!sq_fields_found[3]) _append_str(bs->sq[4], str_nil);
            if(!sq_fields_found[4]) _append_str(bs->sq[5], str_nil);
            if(!sq_fields_found[5]) _append_str(bs->sq[6], str_nil);
        }
        else if(strcmp(hl->header_tag, "RG") == 0)
        {
            ++bs->cnt_rg;
            _append_sht(bs->rg[1], bs->file_id);
                
            for(i=0; i<12; ++i)
                rg_fields_found[i] = FALSE;
                
            for(o = 0; o < hl->nr_options; ++o)
            {
                _append_option_cond_str(bs->rg[0], hl->options[o], "ID", rg_fields_found[0]);
                _append_option_cond_str(bs->rg[2], hl->options[o], "CN", rg_fields_found[1]);
                _append_option_cond_str(bs->rg[3], hl->options[o], "DS", rg_fields_found[2]);
                _append_option_cond_lng(bs->rg[4], hl->options[o], "DT", rg_fields_found[3], l, s);
                _append_option_cond_str(bs->rg[5], hl->options[o], "FO", rg_fields_found[4]);
                _append_option_cond_str(bs->rg[6], hl->options[o], "KS", rg_fields_found[5]);
                _append_option_cond_str(bs->rg[7], hl->options[o], "LB", rg_fields_found[6]);
                _append_option_cond_str(bs->rg[8], hl->options[o], "PG", rg_fields_found[7]);
                _append_option_cond_int(bs->rg[9], hl->options[o], "PI", rg_fields_found[8], l, s);
                _append_option_cond_str(bs->rg[10], hl->options[o], "PL", rg_fields_found[9]);
                _append_option_cond_str(bs->rg[11], hl->options[o], "PU", rg_fields_found[10]);
                _append_option_cond_str(bs->rg[12], hl->options[o], "SM", rg_fields_found[11]);
                
                /* if this point is reached, option wasn't recognized */
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "Unknown option found in header tag RG");
            }
            if(!rg_fields_found[0])
            {
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "ID tag not found in RG header line\n");
            }

            if(!rg_fields_found[1]) _append_str(bs->rg[2], str_nil);
            if(!rg_fields_found[2]) _append_str(bs->rg[3], str_nil);
            if(!rg_fields_found[3]) _append_lng(bs->rg[4], lng_nil);
            if(!rg_fields_found[4]) _append_str(bs->rg[5], str_nil);
            if(!rg_fields_found[5]) _append_str(bs->rg[6], str_nil);
            if(!rg_fields_found[6]) _append_str(bs->rg[7], str_nil);
            if(!rg_fields_found[7]) _append_str(bs->rg[8], str_nil);
            if(!rg_fields_found[8]) _append_int(bs->rg[9], int_nil);
            if(!rg_fields_found[9]) _append_str(bs->rg[10], str_nil);
            if(!rg_fields_found[10]) _append_str(bs->rg[11], str_nil);
            if(!rg_fields_found[11]) _append_str(bs->rg[12], str_nil);
        }
        else if(strcmp(hl->header_tag, "PG") == 0)
        {
            ++bs->cnt_pg;
            _append_sht(bs->pg[1], bs->file_id);
                
            for(i=0; i<5; ++i)
                pg_fields_found[i] = FALSE;
                
            for(o = 0; o < hl->nr_options; ++o)
            {
                _append_option_cond_str(bs->pg[0], hl->options[o], "ID", pg_fields_found[0]);
                _append_option_cond_str(bs->pg[2], hl->options[o], "PN", pg_fields_found[1]);
                _append_option_cond_str(bs->pg[3], hl->options[o], "CL", pg_fields_found[2]);
                _append_option_cond_str(bs->pg[4], hl->options[o], "PP", pg_fields_found[3]);
                _append_option_cond_str(bs->pg[5], hl->options[o], "VN", pg_fields_found[4]);
                
                /* if this point is reached, option wasn't recognized */
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "Unknown option found in header tag PG");
            }
            if(!pg_fields_found[0])
            {
                _free_bam_header_line(hl);
                throw(MAL, "_parse_bam_header", "ID tag not found in PG header line\n");
            }
            if(!pg_fields_found[1]) _append_str(bs->pg[2], str_nil);
            if(!pg_fields_found[2]) _append_str(bs->pg[3], str_nil);
            if(!pg_fields_found[3]) _append_str(bs->pg[4], str_nil);
            if(!pg_fields_found[4]) _append_str(bs->pg[5], str_nil);
        }
        else if(strcmp(hl->header_tag, "CO") == 0)
        {
            /* a comment hl only has a single option, of which the tag = NULL and the value contains the actual comment */
            /* use newCommentLength and commentRealloc to add the comment from this header line to the comments already */
            /* stored in the comment variable. Seperate different comments with a newline */
            if(comment_found)
                strcat(hd_comment, "\n");
            strcat(hd_comment, hl->options[0].value);
            comment_found = TRUE;
        }
        else
        {
            _free_bam_header_line(hl);
            throw(MAL, "_parse_bam_header", "Incorrect header tag found in BAM file\n");
        }
        
        /* everything went ok, free the header line and move on to the next header line */
        _free_bam_header_line(hl);
    }
    
    if(!hd_fields_found[0])
        _append_str(bs->files[4], str_nil);
    if(!hd_fields_found[1])
        _append_str(bs->files[5], str_nil);
    _append_str(bs->files[6], (comment_found ? hd_comment : str_nil));
    
    return MAL_SUCCEED;
}

/*
* Given a Samtools native structure bam1_t, retrieve all information from it and append it to the _temp_container.
* Retrieval code is inspired by the code found in bam.c::bam_format1_core
*
* Old implementation actually called bam.c::bam_format1_core and then parsed the resulting SAM-string. This new and less lame
* implementation has been compared with the old implementation on the file toy.bam and resulted in the exact same database contents.
*
* The cigarbuf, seqbuf and qualbuf provide storage for these three fields, so mallocs aren't needed every time this function is called
*/
static str 
_process_alignment(_bam_storage *bs, lng virtual_offset, bam_header_t *header, bam1_t *alignment,
    str cigarbuf, str seqbuf, str qualbuf, _alignment *a)
{
    uint8_t *s;
    int i;
    
    if(bs->dbschema != 1)
    {
        ++bs->cnt_alignments;
            
        /* In this case, write all data to the files immediately */
        _append_lng(bs->alignments[0], virtual_offset);
        
        //qname
        if(alignments_mask[0])
            _append_str(bs->alignments[1], bam1_qname(alignment));
        
        //flag
        if(alignments_mask[1])
        {
            sht flag = alignment->core.flag;
            _append_sht(bs->alignments[2], flag);
        }
        
        //rname
        if(alignments_mask[2])
        {
            if(alignment->core.tid < 0)
                _append_str(bs->alignments[3], "*");
            else
                _append_str(bs->alignments[3], header->target_name[alignment->core.tid]);
        }
        
        //pos
        if(alignments_mask[3])
        {
            int pos = alignment->core.pos + 1;
            _append_int(bs->alignments[4], pos);
        }
        
        //mapq
        if(alignments_mask[4])
        {
            sht mapq = alignment->core.qual;
            _append_sht(bs->alignments[5], mapq);
        }
        
        //cigar
        if(alignments_mask[5])
        {
            if(alignment->core.n_cigar == 0)
                _append_str(bs->alignments[6], "*");
            else
            {
                uint32_t *cigar_bin = bam1_cigar(alignment);
                int index = 0;
                
                for (i=0; i<alignment->core.n_cigar; ++i) 
                {
                    sprintf(&cigarbuf[index], "%d%c", cigar_bin[i]>>BAM_CIGAR_SHIFT, bam_cigar_opchr(cigar_bin[i]));
                    index += strlen(&cigarbuf[index]);
                }
                _append_str(bs->alignments[6], cigarbuf);
            }
        }
        
        //rnext
        if(alignments_mask[6])
        {
            if(alignment->core.mtid < 0)
                _append_str(bs->alignments[7], "*");
            else if(alignment->core.mtid == alignment->core.tid)
                _append_str(bs->alignments[7], "=");
            else
                _append_str(bs->alignments[7], header->target_name[alignment->core.mtid]);
        }
            
        //pnext
        if(alignments_mask[7])
        {
            int pnext = alignment->core.mpos + 1;
            _append_int(bs->alignments[8], pnext);
        }
        
        //tlen
        if(alignments_mask[8])
            _append_int(bs->alignments[9], alignment->core.isize);
        
        //seq and qual
        if(alignment->core.l_qseq)
        {
            if(alignments_mask[9])
            {
                s = bam1_seq(alignment);
                for (i=0; i<alignment->core.l_qseq; ++i) 
                    seqbuf[i] = bam_nt16_rev_table[bam1_seqi(s, i)];
                seqbuf[alignment->core.l_qseq] = '\0';
                _append_str(bs->alignments[10], seqbuf);
            }
            if(alignments_mask[10])
            {
                s = bam1_qual(alignment);
                if (s[0] == 0xff)
                    _append_str(bs->alignments[11], "*");
                else
                {
                    for (i=0; i<alignment->core.l_qseq; ++i) 
                        qualbuf[i] = s[i] + 33;
                    qualbuf[alignment->core.l_qseq] = '\0';
                    _append_str(bs->alignments[11], qualbuf);
                }
            }
        }
        else
        {
            if(alignments_mask[9])  _append_str(bs->alignments[10], "*");
            if(alignments_mask[10]) _append_str(bs->alignments[11], "*");
        }
    }
    else
    {
        /* We are loading for dbschema = 1, meaning that the data should be written to the alignment structure
         * the alignment mask isn't used here, since we do not offer a dvf implementation for the paired scheme */
        a->virtual_offset = virtual_offset;
        
        //qname
        sprintf(a->qname, "%s", bam1_qname(alignment));
        
        //flag
        a->flag = alignment->core.flag;
        
        //rname
        if(alignment->core.tid < 0)
            a->rname = "*";
        else
            a->rname = header->target_name[alignment->core.tid];
        
        //pos
        a->pos = alignment->core.pos + 1;
        
        //mapq
        a->mapq = alignment->core.qual;
        
        //cigar
        if(alignment->core.n_cigar == 0)
        {
            a->cigar[0] = '*';
            a->cigar[1] = '\0';
        }
        else
        {
            uint32_t *cigar_bin = bam1_cigar(alignment);
            int index = 0;
            
            for (i=0; i<alignment->core.n_cigar; ++i) 
            {
                sprintf(&a->cigar[index], "%d%c", cigar_bin[i]>>BAM_CIGAR_SHIFT, bam_cigar_opchr(cigar_bin[i]));
                index += strlen(&a->cigar[index]);
            }
        }
        
        //rnext
        if(alignment->core.mtid < 0)
            a->rnext = "*";
        else if(alignment->core.mtid == alignment->core.tid)
            a->rnext = "=";
        else
            a->rnext = header->target_name[alignment->core.mtid];
            
        //pnext
        a->pnext = alignment->core.mpos + 1;
        
        //tlen
        a->tlen = alignment->core.isize;
        
        //seq and qual
        if(alignment->core.l_qseq)
        {
            s = bam1_seq(alignment);
            for (i=0; i<alignment->core.l_qseq; ++i) 
                a->seq[i] = bam_nt16_rev_table[bam1_seqi(s, i)];
            a->seq[alignment->core.l_qseq] = '\0';
            s = bam1_qual(alignment);
            if (s[0] == 0xff)
            {
                a->qual[0] = '*';
                a->qual[1] = '\0';
            }
            else
            {
                for (i=0; i<alignment->core.l_qseq; ++i) 
                    a->qual[i] = s[i] + 33;
                a->qual[alignment->core.l_qseq] = '\0';
            }
        }
        else
        {
            a->seq[0] = '*';
            a->seq[1] = '\0';
            a->qual[0] = '*';
            a->qual[1] = '\0';
        }
        a->written = FALSE;
    }
    
    if(alignments_extra_mask)
    {
        /* parse auxiliary data */
        s = bam1_aux(alignment);
        while (s < alignment->data + alignment->data_len) {
            char tag_str[3] = {(char)s[0], (char)s[1], '\0'};
            char type_str[2] = {(char)s[2], '\0'};
            char type = (char)s[2];
            kstring_t aux_value_stream;
            aux_value_stream.l = aux_value_stream.m = 0; aux_value_stream.s = 0;
            s += 3; 
            
            if(type == 'C' || type == 'c' || type == 'S' || type == 's' || type == 'I')
                type_str[0] = 'i';
            else if(type != 'A' && type != 'i' && type != 'f' && type != 'd' && type != 'Z' && type != 'H' && type != 'B')
                throw(MAL, "_process_alignment", "Unknown type found in optional tag: %c\n", type);
                
            if      (type == 'A')                {                    kputc(*s            , &aux_value_stream);         ++s;    }
            else if (type == 'C')                { type_str[0] = 'i'; kputw(*s            , &aux_value_stream);         ++s;    }
            else if (type == 'c')                { type_str[0] = 'i'; kputw(*(int8_t*)s   , &aux_value_stream);         ++s;    }
            else if (type == 'S')                { type_str[0] = 'i'; kputw(*(uint16_t*)s , &aux_value_stream);         s += 2; }
            else if (type == 's')                { type_str[0] = 'i'; kputw(*(int16_t*)s  , &aux_value_stream);         s += 2; }
            else if (type == 'I')                { type_str[0] = 'i'; kputuw(*(uint32_t*)s, &aux_value_stream);         s += 4; }
            else if (type == 'i')                {                    kputw(*(int32_t*)s  , &aux_value_stream);         s += 4; }
            else if (type == 'f')                {                    ksprintf(&aux_value_stream, "%g", *(float*)s);    s += 4; }
            else if (type == 'd')                {                    ksprintf(&aux_value_stream, "%lg", *(double*)s);  s += 8; }
            else if (type == 'Z' || type == 'H') {                    while (*s) { kputc(*s, &aux_value_stream); ++s; } ++s;    }
            else if (type == 'B') 
            {
                uint8_t sub_type = *(s++);
                int32_t n;
                memcpy(&n, s, 4);
                s += 4; // no point to the start of the array
                kputc(sub_type, &aux_value_stream); // write the typing
                for (i = 0; i < n; ++i) 
                {
                    kputc(',', &aux_value_stream);
                    if      ('c' == sub_type) { kputw(*(int8_t*)s, &aux_value_stream)        ; ++s; }
                    else if ('C' == sub_type) { kputw(*(uint8_t*)s, &aux_value_stream)       ; ++s;    }
                    else if ('s' == sub_type) { kputw(*(int16_t*)s, &aux_value_stream)       ; s += 2; }
                    else if ('S' == sub_type) { kputw(*(uint16_t*)s, &aux_value_stream)      ; s += 2; }
                    else if ('i' == sub_type) { kputw(*(int32_t*)s, &aux_value_stream)       ; s += 4; }
                    else if ('I' == sub_type) { kputuw(*(uint32_t*)s, &aux_value_stream)     ; s += 4; }
                    else if ('f' == sub_type) { ksprintf(&aux_value_stream, "%g", *(float*)s); s += 4; }
                }
            }                
            
            ++bs->cnt_alignments_extra;
            _append_str(bs->alignments_extra[0], tag_str);
            _append_lng(bs->alignments_extra[1], virtual_offset);
            _append_str(bs->alignments_extra[2], type_str);
            _append_str(bs->alignments_extra[3], (aux_value_stream.s == NULL ? str_nil : aux_value_stream.s));
            
            if(aux_value_stream.s != NULL) 
                free(aux_value_stream.s); /* Can't use GDKfree here, since the kstring file doesn't use the GDK versions... */
        }
    }
    return MAL_SUCCEED;
}

/*
 * Function is called when a collection of alignments is collected that have the same qname; using this 
 * group primary and secondary pairs are attempted to form; the remainder is considered unpaired.
 * Note that due to the filtering that is done on any alignment, there should not exist alignments in the array
 * with firs_segm == last_segm. Furthermore, secondary alignments should all have rname <> '*', pos > 0,
 * rnext <> '*' and pnext > 0
 */
     
static void
_complete_qname_group(_alignment alignments[], int nr_alignments, _bam_storage *bs)
{
    int i, j, nr_primary = 0;
    _alignment *a, *a2, *prim_firs_segm = NULL, *prim_last_segm = NULL;
    
    
    /* Start with handling the primary alignments */
    for(i=0; i<nr_alignments; ++i)
    {
        a = &alignments[i];
        if(!_seco_alig(*a))
        {
            /* a points to a primary alignment */
            ++nr_primary;
            if(_firs_segm(*a)) prim_firs_segm = a;
            else               prim_last_segm = a;
        }
    }
    if(nr_primary == 2 && prim_firs_segm != NULL && prim_last_segm != NULL)
    {
        _append_alignment_prim_paired_l(*prim_firs_segm, bs);
        _append_alignment_prim_paired_r(*prim_last_segm, bs);
        prim_firs_segm->written = TRUE;
        prim_last_segm->written = TRUE;
        ++bs->cnt_alignments_paired_primary;
    }
    
    /* Now handle the secondary alignments */
    for(i=0; i<nr_alignments; ++i)
    {
        a = &alignments[i];
        if(a->written || !_seco_alig(*a))
            continue;
            
        for(j=i+1; j<nr_alignments; ++j)
        {
            /* Loop starts from j=i+1 since we have symmetry; if a and b are found to be a pair, b and a 
             * will also be a pair */
            a2 = &alignments[j];
            if(a2->written || !_seco_alig(*a2))
                continue;
                
            if(_firs_segm(*a) == _last_segm(*a2) &&
               ((strcmp(a->rnext, "=") == 0 && strcmp(a->rname, a2->rname) == 0) || strcmp(a->rnext, a2->rname) == 0) &&
               a->pnext == a2->pos &&
               ((strcmp(a2->rnext, "=") == 0 && strcmp(a2->rname, a->rname) == 0) || strcmp(a2->rnext, a->rname) == 0) &&
               a2->pnext == a->pos)
            {
                /* a and a2 form a secondary pair, write them */
                if(_firs_segm(*a))
                {
                    _append_alignment_seco_paired_l(*a, bs);
                    _append_alignment_seco_paired_r(*a2, bs);
                }
                else
                {
                    _append_alignment_seco_paired_l(*a2, bs);
                    _append_alignment_seco_paired_r(*a, bs);
                }
                a->written = TRUE;
                a2->written = TRUE;
                ++bs->cnt_alignments_paired_secondary;
            }
        }
    }
    
    /* Now write all alignments that have not been written yet */
    for(i=0; i<nr_alignments; ++i)
    {
        if(!alignments[i].written)
        {
            _append_alignment_unpaired(alignments[i], bs);
            alignments[i].written = TRUE;
            ++bs->cnt_alignments;
        }
    }
}


/*
* Given a filepath, the BAM file at this location will be tried to read. If this succeeds, all data contained
* in it will be stored in the _temp_container structure.
* file_id will be used both as a primary key in the files table for this file and as a foreign key in all other
* tables that hold foreign key relations to the files table. 
*/
static str 
_load_file(_bam_storage *bs, _threads_data *d, int thread_id)
{
    bamFile input;
    bam_header_t *header;
    str headertext_consumable; /* this will point to header->text and will be consumed during the parsing process */
    
    char cigarbuf[MAX_CIGAR_LENGTH];
    char seqbuf[MAX_SEQ_LENGTH];
    char qualbuf[MAX_SEQ_LENGTH];
    
    _alignment alignments[MAX_ALIGNMENTS_PER_QNAME];
    int alig_index = 0;
    
    bam1_t *alignment;
    int alignment_bytes_read;
    str err;
    int bufsize = 255;
    str buf = GDKmalloc(bufsize*sizeof(char));
    lng voffset;
    int start;
    float duration_sec;
    
    _alignment *a;
    
    if(buf == NULL)
        throw(MAL, "_loadfile", MAL_MALLOC_FAIL);
        
    snprintf(buf, bufsize, "Opening file %s...", bs->file_location);
    _append_to_log(buf);
    
    /* first try to open file */
    input = bam_open(bs->file_location, "r");
    if(input == NULL)
        throw(MAL, "loadfile", "Cannot read %s \n", bs->file_location);
    
    _append_to_log("Processing BAM file header...");
    
    /* now retrieve header */
    start = GDKms();
    header = bam_header_read(input);
    if(header == NULL)
        throw(MAL, "loadfile", "Error reading header of %s \n", bs->file_location);
    headertext_consumable = header->text;
    if((err = _parse_bam_header(bs, d, thread_id, headertext_consumable)) != MAL_SUCCEED)
    {
        bam_header_destroy(header);
        throw(MAL, "loadfile", "Error processing bam header: %s\n", err);
    }
    duration_sec = (GDKms() - start) / 1000.0f;
    snprintf(buf, bufsize, "Processed bam header in %f seconds = %f minutes = %f hours", duration_sec, duration_sec/60, duration_sec/3600);
    _append_to_log(buf);
    
    /* now retrieve and process all alignments */
    _append_to_log("Processing alignments...");
    start = GDKms();
    voffset = bam_tell(input);
    alignment = bam_init1();
    
    while((alignment_bytes_read = bam_read1(input, alignment)) >= 0)
    {
        if(bs->dbschema == 1 && alig_index > 0 && strcmp(bam1_qname(alignment), alignments[alig_index-1].qname) != 0)
        {
            /* Qnames do not match, so the previous alignments can be considered complete. Use this knowledge
             * to write the alignments for that qname to suitable files.
             */
            _complete_qname_group(alignments, alig_index, bs);
             
            /* All alignments for the previous qname are written to files, we can now start overwriting them */
            alig_index = 0;
        }
        if((err = _process_alignment(bs, voffset, header, alignment, cigarbuf, seqbuf, qualbuf, &alignments[alig_index])) != MAL_SUCCEED)
            throw(MAL, "loadfile", "Error processing bam alignment: %s\n", err);
        
        /*voffset can be updated for the next iteration at this point already */
        voffset = bam_tell(input);
        
        if(bs->dbschema == 1)
        {
            /* We are building the paired schema. Therefore, alignments[alig_index] is now filled with alignment data 
               In some cases, we can dump it in unpaired storage immediately */
            a = &alignments[alig_index];
            if(
                _firs_segm(*a) == _last_segm(*a) ||
                (
                    _seco_alig(*a) && 
                    (   
                        strcmp(a->rname, "*") == 0 || a->pos <= 0 ||
                        strcmp(a->rnext, "*") == 0 || a->pnext <= 0
                    )
                )
            )
            {
                _append_alignment_unpaired(*a, bs);
                a->written = TRUE;
                ++bs->cnt_alignments;
            }
            else
            {
                /* The alignment can not be written yet, so store it by increasing the index of the next alignment */
                ++alig_index;
            }
        }
    }
    /* alignments will still contain at least one alignment, so empty it */
    _complete_qname_group(alignments, alig_index, bs);
    
    bam_destroy1(alignment);
    bam_header_destroy(header);
    bam_close(input);
    
    duration_sec = (GDKms() - start) / 1000.0f;
    snprintf(buf, bufsize, "Processed "LLFMT" alignments and "LLFMT" alignment_extras in %f seconds = %f minutes = %f hours", 
        bs->cnt_alignments, bs->cnt_alignments_extra, duration_sec, duration_sec/60, duration_sec/3600);
    _append_to_log(buf);
    
    GDKfree(buf);
    return MAL_SUCCEED;
}

/**
 * Copies the binary files, of which the paths are stored in _bam_storage, into the database using the Client context.
 * The copy into statement from MonetDB will move the non-string files to the dbfarm. The string files are to be removed
 * manually. This is also done in this function. So after calling _copy_into_db, the whole flush directory of this _bam_storage
 * should be empty.
 * Note: At the time this function receives the _bam_storage, all malloced fields in _bam_storage have already been freed.
 *       It should thus only be used to retrieve the stack fields of the struct.
 */
static str
_copy_into_db(Client cntxt, pthread_mutex_t *client_lock, _bam_storage *bs, sht dbschema)
{
    str query = GDKmalloc(2048 * sizeof(char));
    str err;
    int i;
    
    /*
    char search[2][MAX_SQL_SEARCH_REPLACE_CHARS] = {"alignments_i"};
    char replace[2][MAX_SQL_SEARCH_REPLACE_CHARS]; */
    
    /* Everything in one query didn't work; foreign key constraints of alignments_extra got violated for whatever reason. */
    /*sprintf(query, 
        "COPY BINARY INTO bam.files FROM ('%s', '%s', '%s', '%s', '%s');\n"
        "COPY BINARY INTO bam.sq FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"
        "COPY BINARY INTO bam.rg FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"
        "COPY BINARY INTO bam.pg FROM ('%s', '%s', '%s', '%s', '%s', '%s');\n"
        "COPY BINARY INTO bam.%salignments_%d FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n"
        "COPY BINARY INTO bam.alignments_extra_%d FROM ('%s', '%s', '%s', '%s');"
        , 
        bs->fp_files[0], bs->fp_files[1], bs->fp_files[2], bs->fp_files[3], bs->fp_files[4],
        bs->fp_sq[0], bs->fp_sq[1], bs->fp_sq[2], bs->fp_sq[3], bs->fp_sq[4], bs->fp_sq[5], bs->fp_sq[6],
        bs->fp_rg[0], bs->fp_rg[1], bs->fp_rg[2], bs->fp_rg[3], bs->fp_rg[4], bs->fp_rg[5], bs->fp_rg[6], bs->fp_rg[7], 
            bs->fp_rg[8], bs->fp_rg[9], bs->fp_rg[10], bs->fp_rg[11], bs->fp_rg[12], 
        bs->fp_pg[0], bs->fp_pg[1], bs->fp_pg[2], bs->fp_pg[3], bs->fp_pg[4], bs->fp_pg[5], 
        (dbschema == 0 ? "" : "unpaired_"), bs->file_id, bs->fp_alignments[0], bs->fp_alignments[1], bs->fp_alignments[2], 
            bs->fp_alignments[3], bs->fp_alignments[4], bs->fp_alignments[5], bs->fp_alignments[6], bs->fp_alignments[7], 
            bs->fp_alignments[8], bs->fp_alignments[9], bs->fp_alignments[10], bs->fp_alignments[11],
        bs->file_id, bs->fp_alignments_extra[0], bs->fp_alignments_extra[1], bs->fp_alignments_extra[2], bs->fp_alignments_extra[3]
        );*/
        
    /* So now, copy into queries are executed one by one (but we execute all queries as one atomic thing using the client lock) */
    if(client_lock != NULL) pthread_mutex_lock(client_lock);
    
    sprintf(query, "COPY BINARY INTO bam.files FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s');\n",
        bs->fp_files[0], bs->fp_files[1], bs->fp_files[2], bs->fp_files[3], bs->fp_files[4], bs->fp_files[5], bs->fp_files[6]);
    _append_to_log(query);
    if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_files", TRUE, FALSE)) != MAL_SUCCEED)
    {
        if(client_lock != NULL) pthread_mutex_unlock(client_lock);
        GDKfree(query);
        return err;
    }
    
    if(bs->cnt_sq > 0)
    {
        sprintf(query, "COPY BINARY INTO bam.sq FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s');\n",
            bs->fp_sq[0], bs->fp_sq[1], bs->fp_sq[2], bs->fp_sq[3], bs->fp_sq[4], bs->fp_sq[5], bs->fp_sq[6]);
        _append_to_log(query);
        if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_sq", TRUE, FALSE)) != MAL_SUCCEED)
        {
            if(client_lock != NULL) pthread_mutex_unlock(client_lock);
            GDKfree(query);
            return err;
        }
    }
    
    if(bs->cnt_rg > 0)
    {
        sprintf(query, "COPY BINARY INTO bam.rg FROM ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');\n",
            bs->fp_rg[0], bs->fp_rg[1], bs->fp_rg[2], bs->fp_rg[3], bs->fp_rg[4], bs->fp_rg[5], bs->fp_rg[6], bs->fp_rg[7], 
            bs->fp_rg[8], bs->fp_rg[9], bs->fp_rg[10], bs->fp_rg[11], bs->fp_rg[12]);
        _append_to_log(query);
        if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_rg", TRUE, FALSE)) != MAL_SUCCEED)
        {
            if(client_lock != NULL) pthread_mutex_unlock(client_lock);
            GDKfree(query);
            return err;
        }
    }
    
    if(bs->cnt_pg > 0)
    {
        sprintf(query, "COPY BINARY INTO bam.pg FROM ('%s', '%s', '%s', '%s', '%s', '%s');\n",
            bs->fp_pg[0], bs->fp_pg[1], bs->fp_pg[2], bs->fp_pg[3], bs->fp_pg[4], bs->fp_pg[5]);
        _append_to_log(query);
        if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_pg", TRUE, FALSE)) != MAL_SUCCEED)
        {
            if(client_lock != NULL) pthread_mutex_unlock(client_lock);
            GDKfree(query);
            return err;
        }
    }
    
    if(bs->cnt_alignments > 0)
    {
        char buf[128];
        sprintf(query, "COPY BINARY INTO bam.%salignments_%d FROM ('%s'",
            (dbschema == 0 ? "" : (dbschema == 1 ? "unpaired_" : "intern_")), bs->file_id, bs->fp_alignments[0]);
        
        for(i=0; i<11; ++i)
        {
            if(alignments_mask[i])
            {
                sprintf(buf, ", '%s'", bs->fp_alignments[i+1]);
                strcat(query, buf);
            }
        }
        strcat(query, ");\n");
        _append_to_log(query);
        if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_alignments", TRUE, FALSE)) != MAL_SUCCEED)
        {
            if(client_lock != NULL) pthread_mutex_unlock(client_lock);
            GDKfree(query);
            return err;
        }
    }
    
    if(bs->cnt_alignments_extra > 0)
    {
        sprintf(query, "COPY BINARY INTO bam.%salignments_extra_%d FROM ('%s', '%s', '%s', '%s');\n",
            (dbschema == 2 ? "intern_" : ""), bs->file_id, bs->fp_alignments_extra[0], bs->fp_alignments_extra[1],
            bs->fp_alignments_extra[2], bs->fp_alignments_extra[3]);
        _append_to_log(query);
        if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_alignments_extra", TRUE, FALSE)) != MAL_SUCCEED)
        {
            if(client_lock != NULL) pthread_mutex_unlock(client_lock);
            GDKfree(query);
            return err;
        }
    }
    
    if(bs->cnt_alignments_paired_primary > 0)
    {
        char buf[128];
        sprintf(query, "COPY BINARY INTO bam.paired_primary_alignments_%d FROM (", bs->file_id);
        for(i=0; i<23; ++i)
        {
            sprintf(buf, "%s'%s'", i > 0 ? ", " : "", bs->fp_alignments_paired_primary[i]);
            strcat(query, buf);
        }
        strcat(query, ");\n");
        _append_to_log(query);
        if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_alignments_paired_primary", TRUE, FALSE)) != MAL_SUCCEED)
        {
            if(client_lock != NULL) pthread_mutex_unlock(client_lock);
            GDKfree(query);
            return err;
        }
    }
    
    if(bs->cnt_alignments_paired_secondary > 0)
    {
        char buf[128];
        sprintf(query, "COPY BINARY INTO bam.paired_secondary_alignments_%d FROM (", bs->file_id);
        for(i=0; i<23; ++i)
        {
            sprintf(buf, "%s'%s'", i > 0 ? ", " : "", bs->fp_alignments_paired_secondary[i]);
            strcat(query, buf);
        }
        strcat(query, ");\n");
        _append_to_log(query);
        if((err = SQLstatementIntern(cntxt, &query, "bam.copy_binary_into_alignments_paired_secondary", TRUE, FALSE)) != MAL_SUCCEED)
        {
            if(client_lock != NULL) pthread_mutex_unlock(client_lock);
            GDKfree(query);
            return err;
        }
    }
    
    if(client_lock != NULL) pthread_mutex_unlock(client_lock);
    
    GDKfree(query);
    
    /* Manually remove files that are not removed by copy into statements. For a given table, this are all string files for that table or,
     * in case the binary files contain no records whatsoever, this are all binary files for that table */
    for(i=1; i<7; ++i)
        unlink(bs->fp_files[i]);
        
    if(bs->cnt_sq > 0)
    {
        unlink(bs->fp_sq[0]);
        for(i=4; i<7; ++i)
            unlink(bs->fp_sq[i]);
    }
    else
        for(i=0; i<7; ++i)
                unlink(bs->fp_sq[i]);
    
    if(bs->cnt_rg > 0)
    {
        unlink(bs->fp_rg[0]);
        unlink(bs->fp_rg[2]);
        unlink(bs->fp_rg[3]);
        for(i=5; i<9; ++i)
            unlink(bs->fp_rg[i]);
        for(i=10; i<13; ++i)
            unlink(bs->fp_rg[i]);
    }
    else
        for(i=0; i<13; ++i)
            unlink(bs->fp_rg[i]);
        
    if(bs->cnt_pg > 0)
    {
        unlink(bs->fp_pg[0]);
        for(i=2; i<6; ++i)
            unlink(bs->fp_pg[i]);
    }
    else
        for(i=0; i<6; ++i)
            unlink(bs->fp_pg[i]);
    
    if(bs->cnt_alignments > 0)
    {
        if(alignments_mask[0]) unlink(bs->fp_alignments[1]);
        if(alignments_mask[2]) unlink(bs->fp_alignments[3]);
        if(alignments_mask[5]) unlink(bs->fp_alignments[6]);
        if(alignments_mask[6]) unlink(bs->fp_alignments[7]);
        if(alignments_mask[9]) unlink(bs->fp_alignments[10]);
        if(alignments_mask[10]) unlink(bs->fp_alignments[11]);
    }
    else
        for(i=0; i<12; ++i)
            if(i == 0 || alignments_mask[i-1])
                unlink(bs->fp_alignments[i]);
    
    if(alignments_extra_mask)
    {
        if(bs->cnt_alignments_extra > 0)
        {
            unlink(bs->fp_alignments_extra[0]);
            unlink(bs->fp_alignments_extra[2]);
            unlink(bs->fp_alignments_extra[3]);
        }
        else
            for(i=0; i<4; ++i)
                unlink(bs->fp_alignments_extra[i]);
    }
    
    if(dbschema == 1)
    {
        if(bs->cnt_alignments_paired_primary > 0)
        {
            unlink(bs->fp_alignments_paired_primary[2]);
            unlink(bs->fp_alignments_paired_primary[4]);
            unlink(bs->fp_alignments_paired_primary[7]);
            unlink(bs->fp_alignments_paired_primary[8]);
            unlink(bs->fp_alignments_paired_primary[11]);
            unlink(bs->fp_alignments_paired_primary[12]);
            unlink(bs->fp_alignments_paired_primary[14]);
            unlink(bs->fp_alignments_paired_primary[17]);
            unlink(bs->fp_alignments_paired_primary[18]);
            unlink(bs->fp_alignments_paired_primary[21]);
            unlink(bs->fp_alignments_paired_primary[22]);
        }
        else
            for(i=0; i<23; ++i)
                unlink(bs->fp_alignments_paired_primary[i]);
                
        if(bs->cnt_alignments_paired_secondary > 0)
        {
            unlink(bs->fp_alignments_paired_secondary[2]);
            unlink(bs->fp_alignments_paired_secondary[4]);
            unlink(bs->fp_alignments_paired_secondary[7]);
            unlink(bs->fp_alignments_paired_secondary[8]);
            unlink(bs->fp_alignments_paired_secondary[11]);
            unlink(bs->fp_alignments_paired_secondary[12]);
            unlink(bs->fp_alignments_paired_secondary[14]);
            unlink(bs->fp_alignments_paired_secondary[17]);
            unlink(bs->fp_alignments_paired_secondary[18]);
            unlink(bs->fp_alignments_paired_secondary[21]);
            unlink(bs->fp_alignments_paired_secondary[22]);
        }
        else
            for(i=0; i<23; ++i)
                unlink(bs->fp_alignments_paired_secondary[i]);
    }
            
    /* All data is stored into the database, if we have a paired schema, we will do the pairing right now */
    /* REPLACED WITH INTERNAL SPLITTING OF DATA
    if(dbschema == 1)
    {
        sprintf(replace[0], "alignments_%d", bs->file_id);
        if((err = _run_sql_from_file(cntxt, client_lock, SQL_FILE_DIVIDE_DATA, search, replace, 1)) != MAL_SUCCEED)
            return err;
    }*/
    
    return MAL_SUCCEED;
}

    
static void *
_load_next_file(void *d)
{
    _reader_thread_data *data = (_reader_thread_data *)d;
    int bufsize = 255;
    str buf;
    
    int start = GDKms();
    float duration_sec;
    
    sht next_file_id = 0;
    str err;
    
    _bam_storage bs;
    
    str file_path;
    
    char search[2][MAX_SQL_SEARCH_REPLACE_CHARS] = {"alignments_i", "alignments_extra_i"};
    char replace[2][MAX_SQL_SEARCH_REPLACE_CHARS];
    
    /* Figure out the filepath/file_id on which we should currently be working */
    
    if(data->d != NULL) pthread_mutex_lock(data->current_file_lock);
    if(*(data->current_file) == data->nr_file_paths - 1)
    {
        /* Some thread is already working on the last file.. we can safely return */
        if(data->d != NULL)
        {
            pthread_mutex_unlock(data->current_file_lock);
            
            /* Don't forget to set the availability of this thread to TRUE */
            data->d->thread_available[data->thread_id] = TRUE;
        }
        
        /* And return */
        return (void *)MAL_SUCCEED;
    }
    ++(*data->current_file);
    file_path = data->file_paths[*data->current_file];
    next_file_id = data->start_file_id + *data->current_file;
    if(data->d != NULL) pthread_mutex_unlock(data->current_file_lock);
    
    /* Create the actual tables that will store the alignment information for this file */
    sprintf(replace[0], "alignments_%d", next_file_id);
    sprintf(replace[1], "alignments_extra_%d", next_file_id);
    
    if((err = _run_sql_from_file(data->cntxt, data->client_lock, 
        (data->dbschema == 0 ? SQL_FILE_CREATE_ALIGNMENTS_STORAGE_0 : 
            (data->dbschema == 1 ? SQL_FILE_CREATE_ALIGNMENTS_STORAGE_1 : SQL_FILE_CREATE_ALIGNMENTS_STORAGE_2)), 
        search, replace, 2, data->dbschema == 0)) != MAL_SUCCEED)
        return err;
    
    if((err = _init_bam_storage(&bs, next_file_id, file_path, data->dbschema, data->storage_mask)) != MAL_SUCCEED)
        throw(MAL, "_load_next_file", "Error initializing _bam_storage structure: %s\n", err);
    if((err = _load_file(&bs, data->d, data->thread_id)) != MAL_SUCCEED)
    {
        _free_bam_storage(&bs);
        return (void *)err;
    }
    
    _free_bam_storage(&bs); /* bam_storage can be freed now. The opened binary files will then be closed, such that the copy into
                             * statement can do its work. Furthermore, filepaths will remain present in bs, since these are not malloced */
    
    if((err = _copy_into_db(data->cntxt, data->client_lock, &bs, data->dbschema)) != MAL_SUCCEED)
        return (void *)err;
    
    buf = GDKmalloc(bufsize * sizeof(char));
    if(buf != NULL)
    {
        duration_sec = (GDKms() - start) / 1000.0f;
        snprintf(buf, bufsize, "Thread %d: Processed file '%s' in %f seconds = %f minutes = %f hours", data->thread_id, file_path, duration_sec, duration_sec/60, duration_sec/3600);
        _append_to_log(buf);
        GDKfree(buf);
    }
    
    return _load_next_file(d);
}


str 
bam_loader_repos(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{        
    str *filelist_path = (str*) getArgReference(stk,pci,pci->retc); /* arg 1: path to file in which list of bam files is stored */
    sht dbschema = *(sht*) getArgReference(stk,pci,pci->retc+1); /* arg 2: dbschema to use */
    str *storage_mask = (str *) getArgReference(stk,pci,pci->retc+2); /* arg 3: storage mask to use, used if non null to init alignments_mask */
    int nr_threads = *(sht*) getArgReference(stk,pci,pci->retc+3); /* arg 4: max number of threads that will be used by bam_loader */
    
    int start = GDKms();
    float duration_sec;
    int bufsize = 255;
    str buf;
        
    str file_paths[MAX_NR_BAM_FILES];
    sht nr_file_paths;
    sht current_file = -1;
    sht next_file_id;
    
    int i;
    str err;
    
    
    _set_masks(dbschema, *storage_mask);
    
    for(i=0; i<MAX_NR_BAM_FILES; ++i)
    {
        file_paths[i] = (str)GDKmalloc(MAX_BAM_FILEPATH_LENGTH * sizeof(char));
        if(file_paths[i] == NULL)
            throw(MAL, "bam_loader_repos", MAL_MALLOC_FAIL);
    }
    
    
    if(dbschema < 0 || dbschema > 2)
        throw(MAL, "bam_loader_repos", "Invalid argument received for dbschema: %d\n", dbschema);
        
    if((err = _next_file_id(cntxt, mb, &next_file_id)) != MAL_SUCCEED)
        throw(MAL, "bam_loader_repos", "%s", err);
        
    /* Get the file list */
    if((nr_file_paths = _get_file_paths(*filelist_path, file_paths)) < 0)
        throw(MAL, "bam_loader_repos", "Couldn't retrieve file list from file '%s'\n", *filelist_path);
        
    _open_log();
    if(nr_threads < 2)
    {   
        _reader_thread_data data = {
            .d = NULL,
            .thread_id = 0,
            .dbschema = dbschema,
            .storage_mask = *storage_mask,
            .cntxt = cntxt,
            .mb = mb,
            .file_paths = file_paths,
            .nr_file_paths = nr_file_paths, 
            .current_file = &current_file, 
            .start_file_id = next_file_id,
            .current_file_lock = NULL,
            .client_lock = NULL
        };
        if((err = (str)_load_next_file((void *)&data)) != MAL_SUCCEED)
            throw(MAL, "bam_loader_repos", "Error on loading file: %s\n", err);
    }
    else
    {
        _threads_data d = {
            .max_nr_threads = nr_threads,
            .thread_available = (bit *)GDKmalloc(nr_threads * sizeof(bit))
        };
        pthread_t *threads = (pthread_t *)GDKzalloc(nr_threads * sizeof(pthread_t));
        _reader_thread_data *data = (_reader_thread_data *)GDKmalloc(nr_threads * sizeof(_reader_thread_data));
        
        pthread_mutex_t current_file_lock;
        pthread_mutex_t client_lock;
        
        void **ret = GDKmalloc(MIN(nr_threads, nr_file_paths) * sizeof(str));
        
        int i;
        
        if(threads == NULL || d.thread_available == NULL || data == NULL || ret == NULL)
            throw(MAL, "bam_loader_repos", MAL_MALLOC_FAIL);
            
        if(pthread_mutex_init(&current_file_lock, NULL) != 0 || pthread_mutex_init(&client_lock, NULL) != 0 || pthread_mutex_init(&d.threads_data_lock, NULL) != 0)
            throw(MAL, "bam_loader_repos", "Error on initializing mutex\n");
            
        for(i=0; i<nr_threads; ++i)
            d.thread_available[i] = TRUE;
            
        /* Initialize thread data */
        for(i=0; i<nr_threads; ++i)
        {   
            data[i].d = &d;
            data[i].thread_id = i;
            data[i].dbschema = dbschema;
            data[i].cntxt = cntxt;
            data[i].mb = mb;
            data[i].file_paths = file_paths;
            data[i].nr_file_paths = nr_file_paths;
            data[i].current_file = &current_file;
            data[i].start_file_id = next_file_id;
            data[i].current_file_lock = &current_file_lock;
            data[i].client_lock = &client_lock;
        }
        
        /* Create threads */
        for(i=0; i<MIN(nr_threads, nr_file_paths); ++i)
        {
            if(pthread_create(&threads[i], NULL, _load_next_file, (void *)&data[i]) != 0)
                throw(MAL, "bam_loader_repos", "Error on creating thread\n");
            d.thread_available[i] = FALSE;
        }
        
        /* And wait for them to finish */
        for(i=0; i<MIN(nr_threads, nr_file_paths); ++i)
            pthread_join(threads[i], &ret[i]);
        
        /* Check return values */
        for(i=0; i<MIN(nr_threads, nr_file_paths); ++i)
        {
            str ret_i = (str)ret[i];
            if(ret_i != MAL_SUCCEED)
                throw(MAL, "bam_loader_repos", "%s", ret_i);
        }
        
        pthread_mutex_destroy(&current_file_lock);
        pthread_mutex_destroy(&client_lock);
        pthread_mutex_destroy(&d.threads_data_lock);
        
        GDKfree(threads);
        GDKfree(d.thread_available);
        GDKfree(data);
        GDKfree(ret);
    }

    buf = GDKmalloc(bufsize * sizeof(char));
    if(buf != NULL)
    {
        duration_sec = (GDKms() - start) / 1000.0f;
        snprintf(buf, bufsize, "bam_loader_repos finished in %f seconds = %f minutes = %f hours", duration_sec, duration_sec/60, duration_sec/3600);
        _append_to_log(buf);
        GDKfree(buf);
    }
    
    _close_log();

    for(i=0; i<MAX_NR_BAM_FILES; ++i)
        GDKfree(file_paths[i]);
    
    return MAL_SUCCEED;
}

str 
bam_loader_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    str *file_path = (str*) getArgReference(stk,pci,pci->retc); /* arg 1: repo_path */
    sht dbschema = *(sht*) getArgReference(stk,pci,pci->retc+1); /* arg 2: schema to use, see bam_loader.h for possible options */
    str *storage_mask = (str *) getArgReference(stk,pci,pci->retc+2); /* arg 3: storage mask to use, used if non null to init alignments_mask */
    
    int start = GDKms();
    float duration_sec;
    int bufsize = 255;
    str buf;
    
    str file_paths[1];
    sht nr_file_paths = 1;
    sht current_file = -1;
    sht next_file_id;
    
    str err;
    
    _set_masks(dbschema, *storage_mask);
    
    if((err = _next_file_id(cntxt, mb, &next_file_id)) != MAL_SUCCEED)
        throw(MAL, "bam_loader_file", "%s", err);
    
    file_paths[0] = *file_path;
    
    _open_log();
    
    {
        _reader_thread_data data = {
            .d = NULL,
            .thread_id = 0,
            .dbschema = dbschema,
            .storage_mask = *storage_mask,
            .cntxt = cntxt,
            .mb = mb,
            .file_paths = file_paths,
            .nr_file_paths = nr_file_paths, 
            .current_file = &current_file, 
            .start_file_id = next_file_id,
            .current_file_lock = NULL,
            .client_lock = NULL
        };
        if((err = (str)_load_next_file((void *)&data)) != MAL_SUCCEED)
        {
            _close_log();
            throw(MAL, "bam_loader_file", "Error on loading file: %s\n", err);
        }
    }
    
    buf = GDKmalloc(bufsize * sizeof(char));
    if(buf != NULL)
    {
        duration_sec = (GDKms() - start) / 1000.0f;
        snprintf(buf, bufsize, "bam_loader_file finished in %f seconds = %f minutes = %f hours", duration_sec, duration_sec/60, duration_sec/3600);
        _append_to_log(buf);
        GDKfree(buf);
    }
    
    _close_log();
    
    return MAL_SUCCEED;
}

str
bam_drop_file(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    sht file_id = *(sht*) getArgReference(stk,pci,pci->retc);
    sht dbschema = *(sht*) getArgReference(stk,pci,pci->retc + 1);
    str sql_filename;
    mvc *m;
    sql_schema *s;
    char alignments_extra_table[64];
    str msg;
    str err;
    char search[2][MAX_SQL_SEARCH_REPLACE_CHARS] = {"alignments_i", "alignments_extra_i"};
    char replace[2][MAX_SQL_SEARCH_REPLACE_CHARS];
    char q[256];
    str query = q;
    
    switch(dbschema)
    {
        case 0: sql_filename = SQL_FILE_DROP_ALIGNMENTS_STORAGE_0; break;
        case 1: sql_filename = SQL_FILE_DROP_ALIGNMENTS_STORAGE_1; break;
        case 2: sql_filename = SQL_FILE_DROP_ALIGNMENTS_STORAGE_2; break;
        default: throw(MAL, "bam_drop_file", "Invalid dbschema given: %d", dbschema);
    }
    
    /* Find out whether or not an alignments_extra_{dbschema} table exists */
    if((msg = getSQLContext(cntxt, mb, &m, NULL)) != MAL_SUCCEED)
        throw(MAL, "bam_drop_file", "Error retrieving SQLContext: %s\n", msg);
    if((s = mvc_bind_schema(m, "bam")) == NULL)
        throw(MAL, "bam_drop_file", "Error binding schema 'bam'\n");
    sprintf(alignments_extra_table, "alignments_extra_%d", file_id);
    alignments_extra_mask = (mvc_bind_table(m, s, alignments_extra_table) != NULL); /* By setting the mask, _run_sql_from_file can use it */
    
    _open_log();

    sprintf(replace[0], "alignments_%d", file_id);
    sprintf(replace[1], "alignments_extra_%d", file_id);
    if((err = _run_sql_from_file(cntxt, NULL, sql_filename, search, replace, 2, dbschema == 0)) != MAL_SUCCEED)
    {
        _close_log();
        throw(MAL, "bam_drop_file", "Error when dropping alignment tables: %s\n", err);
    }
    
    /* all alignment data for file {file_id} are removed; now remove all header data for this file */
    sprintf(q, "DELETE FROM bam.pg WHERE file_id = %d;", file_id);
    if((err = SQLstatementIntern(cntxt, &query, "bam.drop_from_pg", TRUE, FALSE)) != MAL_SUCCEED)
    {
        _close_log();
        throw(MAL, "bam_drop_file", "Error when dropping records from bam.pg: %s\n", err);
    }
    sprintf(q, "DELETE FROM bam.rg WHERE file_id = %d;", file_id);
    if((err = SQLstatementIntern(cntxt, &query, "bam.drop_from_rg", TRUE, FALSE)) != MAL_SUCCEED)
    {
        _close_log();
        throw(MAL, "bam_drop_file", "Error when dropping records from bam.rg: %s\n", err);
    }
    sprintf(q, "DELETE FROM bam.sq WHERE file_id = %d;", file_id);
    if((err = SQLstatementIntern(cntxt, &query, "bam.drop_from_sq", TRUE, FALSE)) != MAL_SUCCEED)
    {
        _close_log();
        throw(MAL, "bam_drop_file", "Error when dropping records from bam.sq: %s\n", err);
    }
    sprintf(q, "DELETE FROM bam.files WHERE file_id = %d;", file_id);
    if((err = SQLstatementIntern(cntxt, &query, "bam.drop_from_files", TRUE, FALSE)) != MAL_SUCCEED)
    {
        _close_log();
        throw(MAL, "bam_drop_file", "Error when dropping records from bam.files: %s\n", err);
    }
    
    _close_log();
    (void)stk;
    (void)pci;
    (void)mb;
    
    return MAL_SUCCEED;
}
