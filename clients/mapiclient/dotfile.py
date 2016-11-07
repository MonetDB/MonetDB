# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

'''
The output of the Stethoscope can be save in JSON format using the -j flag.
This program converts such a Stethoscope output file into a .dot file
Finalize the dot picture using:
dot  <basename.dot> -Tpdf -o <basename.pdf>
'''
import os
import argparse
import json

parser = argparse.ArgumentParser(description="Generate a .dot file from a Stethoscope MAL trace")
parser.add_argument('inputfiles', type = str, nargs= '*')

args = parser.parse_args()
print(args)

def showFlowNode(event):
    dotfile.write('n'+ str(event['pc']) )
    stmt = event['short'].replace('\\\\','').replace('\\"','"').replace('\"','"').replace('"','\\"')
    dotfile.write('[fontsize=8, shape=box, label="' + stmt + '"]\n')

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

    print("Finalize the dot picture: dot" + basename + ".dot -Tpdf -o "+basename+ ".pdf"

