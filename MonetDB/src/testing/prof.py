#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

# This program makes an output file prof_(original file name) which can be
# used to monitor the system performances like page faults, time taken
# etc..for all/any of the SQL commands.
# To get the system performances of all SQl commands, type at prompt
# python prof.py input_file_name.mil
# Then compile the output file as follows
# Mserver --dbname=gold prof_input_file_name.mil
# To get the system performances of selected SQL commands, make a file
# with any name (your_file) which has SQl commands in seperate lines.
# python prof.py input_file_name.mil your_file
# Then compile the output file as follows
# Mserver --dbname=gold prof_input_file_name.mil
# You can now observe the results....

import string
import re
import sys
import os

# some helper variables
_dquoted_string = r'"([^"\\]|\\.)*"' # double-quoted string
_squoted_string = r"'([^'\\]|\\.)*'" # single-quoted string
_quoted_string = '(%s|%s)' % (_dquoted_string, _squoted_string) # quoted string
_mquoted_string = '[^\'"]*(%s[^\'"]*)*' % _quoted_string # any number of quoted strings

re_clbracket = re.compile('^'+_mquoted_string+r'\}.', re.MULTILINE) # line with unquoted closing bracket
re_clbracket_end = re.compile('^'+_mquoted_string+r'\}'+'[\n\t ]*$', re.MULTILINE) # line ending with unquoted closing barcket
re_hash = re.compile('^'+_mquoted_string+'#.', re.MULTILINE)
re_mquoted_string_only = re.compile('^'+_mquoted_string + '$', re.MULTILINE)

# Opens the input file
def prof(input_fil_name, commands_fil_name = None):
    input_fil = open(input_fil_name, 'r')

    # The final file which has the tags of mprof which are used in measuring system performances

    fin_fil_name = 'prof_' + input_fil_name

    fin_fil = open(fin_fil_name, 'w')
    fin_fil.close()
    fin_fil = open(fin_fil_name, 'r+')

    if commands_fil_name is None:
        commands_fil_name = 'commands.txt'
    commands_fil = open(commands_fil_name, 'r+')

    command_no=0
    command_name = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    for line2 in commands_fil.readlines() :
        command_no=command_no+1
        command_name[command_no]=0

    counter = 1
    bool = 0
    count = 0
    quit = 0
    concatenate = 0
    temp_sentence = ''
    beeninhash = 0
    close_brac = 0
    prev_with_semicolon = 0

    fin_fil.write('module(mprof);\n')

    global_tag_name = 'global_' + input_fil_name
    g_bpatt = 'pmB("' + global_tag_name + '");'
    g_epatt = 'pmE("' + global_tag_name + '");'
    fin_fil.write(g_bpatt+'\n')

    for line in input_fil.readlines() :
        if string.find(line, '#') == -1 :
            if string.find(line, 'quit;') != -1 :
                quit = 1
                fin_fil.write(g_epatt+'\n')
                fin_fil.write('printf("#~BeginProfilingOutput~#\\n");\n')
                fin_fil.write('pmSummary();\n')
                fin_fil.write('printf("#~EndProfilingOutput~#\\n");\n')
        semicolon = 0
        if string.find(line,';') != -1 :
            with_semicolon = 1
        else :
            with_semicolon = 0
        first_time_in_loop = 1
        for splitted_sentence in string.split(line,";") :
            if re_clbracket_end.search(splitted_sentence) is not None:
                close_brac = 1
            if concatenate == 1 :
                if semicolon == 0 :
                    splitted_sentence = temp_sentence + splitted_sentence
                else :
                    splitted_sentence = temp_sentence + ';' + splitted_sentence
                concatenate = 0
                if (beeninhash == 1 and string.find(splitted_sentence, '#') != -1) :
                    if semicolon == 1 :
                        with_semicolon = 0
                if (string.find(splitted_sentence, '#') != -1 and string.find(splitted_sentence, '\n') == -1) :
                    concatenate = 1
                    temp_sentence = splitted_sentence
                else :
                    temp_sentence = ''

            if string.find(splitted_sentence, '#') != -1 and \
               re.search('.*#[^;]*.[^\n\t ]',line) is not None and \
               re_hash.search(splitted_sentence) is not None:
                if beeninhash == 0 :
                    concatenate = 1
                    temp_sentence = splitted_sentence
                    semicolon = 1
                beeninhash = 1

            if string.find(splitted_sentence, '"') != -1 and \
               re_mquoted_string_only.search(splitted_sentence) is None:
                temp_sentence = splitted_sentence
                concatenate = 1
                semicolon = 1

            if string.find(splitted_sentence, '#') == -1 :
##                if (with_semicolon == 0 and
                if string.find(splitted_sentence, '\n') != -1 and splitted_sentence!='\n':
                    if string.find(splitted_sentence,'{') == -1 and string.find(splitted_sentence,'}') == -1:
                        temp_sentence = splitted_sentence
                        if string.find(temp_sentence,'\n') != -1 :
                            t_sentence = string.split(splitted_sentence,"\n")
                            temp_sentence = t_sentence[0]
                        concatenate = 1

            if close_brac == 1 :
                with_semicolon = 0
                prev_with_semicolon = with_semicolon

            flag = 0
            if (splitted_sentence!='\n' and concatenate == 0) :
                beeninhash = 0
                concatenate = 0
                if string.find(temp_sentence,'#') == -1 :
                    flag = 1
                    cnt = 0
                    for j in splitted_sentence :
                        cnt = cnt +1
                        if (j != ' ' and j!='\n') :
                            if first_time_in_loop == 0 :
                                splitted_sentence=splitted_sentence[cnt-1:]
                            flag = 0
                            break
                if flag == 0 :
                    if with_semicolon == 1 :
                        if first_time_in_loop == 1 or string.find(splitted_sentence,'\n') == -1 or string.find(splitted_sentence,'#') != -1:
                            line1 = splitted_sentence + ';\n'
                    else :
                        line1 = splitted_sentence
                    if close_brac == 1 :
                        with_semicolon = prev_with_semicolon
                        close_brac = 0
                    commands_fil.seek(0)
                    command_no = 0
                    prev_line2 = ''
                    multiple = 0
                    found = 0
                    for line2 in commands_fil.readlines() :
                        command_no=command_no + 1
                        count = count + 1
                        line2 = line2[0:10]
                        for temp in range(10):
                            if line2[temp] == ' ' or line2[temp] == '\n' :
                                line2=line2[0:temp]
                                break
                        if re.search(r'\b'+line2+r'\b',line1) is not None:
                            if string.find(line1,'#') == -1 :
                                found = 1
##                                if (line2 !='join' or ( line2 == 'join' and re.search(r'\b' + 'semijoin' + r'\b',splitted_sentence) == -1)) :
                                if prev_line2!='' :
                                    multiple = 1
                                if multiple == 0 :
                                    command_name[command_no]=command_name[command_no] + 1
                                bpatt = 'pmB("' + line2 + "%d" %command_name[command_no] + '");'
                                epatt = 'pmE("' + line2 + "%d" %command_name[command_no] + '");'
                                if multiple == 1 :
                                    line2 = line2+'.'+prev_line2

                                    bpatt = 'pmB("' + line2 + '");'
                                    epatt = 'pmE("' + line2 + '");'
                                prev_line2 = line2

                    if found == 1 :
                        if re_clbracket.search(line1) is None:
                            bra_place = string.find(line1,'{')
                            if bra_place != -1 :
                                temp_line1 = line1[0:bra_place+1]
                                line1 = line1[bra_place+1:]
                                fin_fil.write(temp_line1)

                        fin_fil.write('\n' + bpatt + '\n')
                        fin_fil.write(line1)
                        fin_fil.write(epatt + '\n\n')
                        found = 0
                        counter = counter + 1
                        bool = 1
                    if (bool == 0) :
                        fin_fil.write(line1)
                    bool = 0
            first_time_in_loop = 0
    if quit == 0 :
        fin_fil.write(g_epatt+'\n')
        fin_fil.write('printf("#~BeginProfilingOutput~#\\n");\n')
        fin_fil.write('pmSummary();\n')
        fin_fil.write('printf("#~EndProfilingOutput~#\\n");\n')

    print 'done.'
    commands_fil.close()
    # The output file prof_... has the original file with the mprof tags at
    # the appropriate places. These mprof tags help us in measuring the system
    # performances like page faults, time taken etc for the SQL commands.
    fin_fil.close()

if __name__ == '__main__' or sys.argv[0] == __name__:
    if not (2 <= len(sys.argv) <= 3):
        print 'Usage: %s input_file [command_file]' % sys.argv[0]
        sys.exit(1)
    input_fil_name = sys.argv[1]
    commands_fil_name = None
    if len(sys.argv) > 2:
        commands_fil_name = sys.argv[2]

    prof(input_fil_name, commands_fil_name)
