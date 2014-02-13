#include <string.h>
#include <stdio.h>
#include "bam_sql.h"

static int
_replace_in_sql_line(str src, str search, str replace)
{
    str src_cpy = src;
    char buffer[MAX_SQL_LINE_LENGTH];
    int buffer_index = 0;
    str res;
    int search_len = strlen(search);
    int replace_len = strlen(replace);
    int i;
    while((res = strstr(src_cpy, search)) != NULL)
    {
        /* res is a pointer into *src; copy everything until this pointer to the result buffer */
        while(src_cpy != res)
        {
            buffer[buffer_index++] = *src_cpy;
            ++src_cpy;
        }
        
        /* first part copied; throw away search string and insert replace string */
        src_cpy += search_len;
        for(i=0; i<replace_len; ++i)
        {
            buffer[buffer_index++] = replace[i];
        }
    }
    
    /* And copy remaining part of src */
    while(*src_cpy != '\n' && *src_cpy != '\0')
    {
        buffer[buffer_index++] = *src_cpy;
        ++src_cpy;
    }
    if(*src_cpy == '\n')
        buffer[buffer_index++] = '\n';
    buffer[buffer_index] = '\0';
    
    strcpy(src, buffer);
    return buffer_index;
}

/* Loads text from file 'filename', replaces every occurrence of search[i] with replace[i] and tries to execute the result as SQL. */
str
get_sql_from_file(str filepath, char search[][MAX_SQL_SEARCH_REPLACE_CHARS], char replace[][MAX_SQL_SEARCH_REPLACE_CHARS], int nr_replacement_strings, str out_err)
{
    FILE *f;
    str file_contents = GDKmalloc(MAX_SQL_FILE_LENGTH * sizeof(char));
    str file_contents_pointer;
    int i;
    unsigned int line_len;

    file_contents_pointer = file_contents;
    
    f = fopen(filepath, "r");
    if(f == NULL)
    {
        sprintf(out_err, "Unable to open SQL file '%s' for reading.\n", filepath);
        GDKfree(file_contents);
        return NULL;
    }
        
    while(fgets(file_contents_pointer, MAX_SQL_LINE_LENGTH, f) != NULL)
    {
        for(i=0; i<nr_replacement_strings; ++i)
        {
            line_len = _replace_in_sql_line(file_contents_pointer, search[i], replace[i]);
        }
        file_contents_pointer += (nr_replacement_strings == 0 ? strlen(file_contents_pointer) : line_len);
    }
    
    fclose(f);
    return file_contents;
}
