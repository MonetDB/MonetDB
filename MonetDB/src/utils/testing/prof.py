#!/usr/local/bin/python

# This program makes an output file prof_(original file name) which can be
# used to monitor the system performances like page faults, time taken
# etc..for all/any of the SQL commands.
# To get the system performances of all SQl commands, type at prompt
# python prof.py input_file_name.mil
# Then compile the output file as follows
# Mserver -db gold prof_input_file_name.mil
# To get the system performances of selected SQL commands, make a file
# with any name (your_file) which has SQl commands in seperate lines.
# python prof.py input_file_name.mil your_file
# Then compile the output file as follows
# Mserver -db gold prof_input_file_name.mil
# You can now observe the results....

import regsub,regex,string,os
from sys import argv
import os

dn = os.path.dirname(argv[0])

try:
    srcdir = sys.argv[1]
except:
    srcdir = "."

# Opens the input file
countr=0
for check_fil_name in argv[1:] :
    countr = countr +1
    if countr == 1 :
        input_fil_name = argv[1]
    if countr == 2 :
        commands_fil_name = argv[2]

input_fil = open(input_fil_name, 'r')

# The final file which has the tags of mprof which are used in measuring system performances

fin_fil_name = 'prof_' + input_fil_name

fin_fil = open(fin_fil_name, 'w')
fin_fil.close()
fin_fil = open(fin_fil_name, 'r+')

if countr == 1 :
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
    if regex.search('#',line) == -1 :
        if regex.search('quit;',line) != -1 :
            quit = 1
            fin_fil.write(g_epatt+'\n')
            fin_fil.write('printf("#~BeginProfilingOutput~#\\n");\n')
            fin_fil.write('pmSummary();\n')
            fin_fil.write('printf("#~EndProfilingOutput~#\\n");\n')
    semicolon = 0
    if regex.search(';',line) != -1 :
        with_semicolon = 1
    else :
        with_semicolon = 0
    first_time_in_loop = 1
    for splitted_sentence in regsub.split(line,";") :
        if regex.search('^[^"\']*\(\("\([^"\]\|[\]"\|[\][^"]\)*"\|\'\([^\']\|[\].\)\'\)[^"\']*\)*}\([\n\t ]*\)$',splitted_sentence) != -1 :
            close_brac = 1
        if concatenate == 1 :
            if semicolon == 0 :
                splitted_sentence = temp_sentence + splitted_sentence
            else :
                splitted_sentence = temp_sentence + ';' + splitted_sentence
            concatenate = 0
            if (beeninhash == 1 and regex.search('#',splitted_sentence) != -1) :
                if semicolon == 1 :
                    with_semicolon = 0
            if (regex.search('#',splitted_sentence) != -1 and regex.search('\n',splitted_sentence) == -1) :
                concatenate = 1
                temp_sentence = splitted_sentence
            else :
                temp_sentence = ''

        if regex.search('#',splitted_sentence) != -1 :
            if regex.search('\(.\)*\(#\)[^;]*\(.\)\([^\n\t ]\)',line) != -1 :
                if regex.search('^[^"\']*\(\("\([^"\]\|[\]"\|[\][^"]\)*"\|\'\([^\']\|[\].\)\'\)[^"\']*\)*#.',splitted_sentence) != -1 :
                    if beeninhash == 0 :
                        concatenate = 1
                        temp_sentence = splitted_sentence
                        semicolon = 1
                    beeninhash = 1

        if regex.search('"',splitted_sentence) != -1 :
            if regex.search('^[^"\']*\(\("\([^"\]\|[\]"\|[\][^"]\)*"\|\'\([^\']\|[\].\)\'\)[^"\']*\)*$',splitted_sentence) == -1 :
                temp_sentence = splitted_sentence
                concatenate = 1
                semicolon = 1

        if regex.search('#',splitted_sentence) == -1 :
#                       if (with_semicolon == 0 and
            if (regex.search('\n',splitted_sentence) != -1 and splitted_sentence!='\n') :
                if (regex.search('{',splitted_sentence) == -1 and regex.search('}',splitted_sentence) == -1) :
                    temp_sentence = splitted_sentence
                    if regex.search('\n',temp_sentence) != -1 :
                        t_sentence = regsub.split(splitted_sentence,"\n")
                        temp_sentence = t_sentence[0]
                    concatenate = 1

        if close_brac == 1 :
            with_semicolon = 0
            prev_with_semicolon = with_semicolon

        flag = 0
        if (splitted_sentence!='\n' and concatenate == 0) :
            beeninhash = 0
            concatenate = 0
            if regex.search('#',temp_sentence) == -1 :
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
                    if (first_time_in_loop == 1 or regex.search('\n',splitted_sentence) == -1 or regex.search('#',splitted_sentence) != -1) :
                        line1 = splitted_sentence + ';\n'
                else :
                    line1 = splitted_sentence
                if close_brac == 1 :
                    with_semicolon = prev_with_semicolon
                    close_brac = 0
                commands_fil.seek(0);
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
                    if regex.search('\<'+line2+'\>',line1) != -1 :
                        if regex.search('#',line1) == -1 :
                            found = 1
#                                                       if (line2 !='join' or ( line2 == 'join' and regex.search('\<' + 'semijoin' + '\>',splitted_sentence) == -1)) :
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
                    if regex.search('^[^"\']*\(\("\([^"\]\|[\]"\|[\][^"]\)*"\|\'\([^\']\|[\].\)\'\)[^"\']*\)*}.',line1) == -1 :
                        bra_place = regex.search('{',line1)
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
fin_fil.close();
