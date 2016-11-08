# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

'''
The output of the Stethoscope can be saved in JSON format using the -j flag.
This program converts such a Stethoscope output file into a .dot file
Finalize the dot picture using:
dot  <basename.dot> -Tpdf -o <basename.pdf>
'''
import os
import argparse
import json

parser = argparse.ArgumentParser(description="Generate a .dot file from a Stethoscope MAL trace")
parser.add_argument('--statement', action='store_true', default= False, help="show the complete MAL instruction")
parser.add_argument('--usec', action='store_true', default= False, help="show the execution time in microseconds")
parser.add_argument('--time', action='store_true', default= False, help="show the execution start time")
parser.add_argument('--pc', action='store_true', default= True, help="show the program counter")
parser.add_argument('inputfiles', type = str, nargs= '*')

args = parser.parse_args()

def showFlowNode(event):
    dotfile.write('n'+ str(event['pc']) )
    dotfile.write('[fontsize=8,')
    if args.statement :
        stmt = event['short'].replace('\\\\','').replace('\\"','"').replace('\"','"').replace('"','\\"')
        shape ='shape=box,'
        lab =stmt
    if args.pc:
        shape = 'shape=circle,'
        lab= str(event['pc'])
    if args.usec:
        shape = 'shape=circle,'
        lab= str(event['usec'])
    if args.time:
        shape = 'shape=box,'
        lab= str(event['ctime'])
    dotfile.write(shape + 'label="'+ lab + '"]\n');

def showFlowInput(event):
    for pc in event['prereq']:
        dotfile.write('n' + str(pc) +' -> n'+ str(event['pc']) + '\n')

# Get the input file, which should be a JSON array object
for name in args.inputfiles:
    try:
        print('Process file:'+name)
        f = open(name,'r')
    except IOerror as e:
        print('Can not access input file')
        exit

    (basename,ext) = os.path.splitext(name)
    print(basename,ext)
    try:
        dotfile = open(basename +'.dot','w')
    except IOerror as e:
        print('Can not create .dot file')
        exit

    src = f.read()
    events= json.loads(src)

    # initialize the dot file
    dotfile.write('digraph '+ basename + '{\n')
    for e in events:
        if e['state'] == 'done':
            showFlowNode(e)
    for e in events:
        if e['state'] == 'done':
            showFlowInput(e)
    dotfile.write('}\n')

    print("Finalize the dot picture using:\ndot " + basename + ".dot -Tpdf -o "+basename+ ".pdf")

