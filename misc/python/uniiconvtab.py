#!/usr/bin/python3

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

import subprocess

def mktab(input):
    output = []
    PL1 = 0
    O = 0
    valdict = {}
    values = [None]
    for line in input:
        cp, name, val = line.rstrip('\n').split(',', 2)
        codepoint = int(cp)
        if val == '?':
            # ? is default replacement
            continue
        if 0x0370 <= codepoint <= 0x1CFF or \
           0x1F00 <= codepoint <= 0x1FFF:
            # skip lots of non-Latin based scripts
            continue
        if not val in valdict:
            valdict[val] = len(values)
            values.append(val)
        c = valdict[val]
        if codepoint <= 0x7F:
            output.append(f'[0x{codepoint:02X}] = {c},'
                          f'\t/* U+{codepoint:04X}: {name} */')
        elif codepoint <= 0x7FF:
            L1 = (codepoint >> 6) | 0xC0
            L2 = (codepoint & 0x3F) # | 0x80
            if L1 != PL1:
                PL1 = L1
                if O == 0:
                    O += 256
                else:
                    O += 64
                output.append(f'[0x{L1:02X}] = {O} - 0x80,'
                              f'\t/* {L1:o} ... */')
            output.append(f'[{O}+0x{L2:02X}] = {c},'
                          f'\t/* U+{codepoint:04X}: {name} */')
        elif codepoint <= 0xFFFF:
            L1 = (codepoint >> 12) | 0xE0
            L2 = ((codepoint >> 6) & 0x3F) # | 0x80
            L3 = (codepoint & 0x3F) # | 0x80
            if L1 != PL1:
                PL1 = L1
                PL2 = L2
                O += 64
                O1 = O
                output.append(f'[0x{L1:02X}] = {O1} - 0x80,'
                              f'\t/* {L1:o} ... */')
                O += 64
                output.append(f'[{O1}+0x{L2:02X}] = {O} - 0x80,'
                              f'\t/* {L1:o} {L2|0x80:o} ... */')
            elif L2 != PL2:
                PL2 = L2
                O += 64
                output.append(f'[{O1}+0x{L2:02X}] = {O} - 0x80,'
                              f'\t/* {L1:o} {L2|0x80:o} ... */')
            output.append(f'[{O}+0x{L3:02X}] = {c},'
                          f'\t/* U+{codepoint:04X}: {name} */')
        else:
            L1 = (codepoint >> 18) | 0xF0
            L2 = ((codepoint >> 12) & 0x3F) # | 0x80
            L3 = ((codepoint >> 6) & 0x3F) # | 0x80
            L4 = (codepoint & 0x3F) # | 0x80
            if L1 != PL1:
                PL1 = L1
                PL2 = L2
                PL3 = L3
                O += 64
                O1 = O
                output.append(f'[0x{L1:02X}] = {O1} - 0x80,'
                              f'\t/* {L1:o} ... */')
                O += 64
                O2 = O
                output.append(f'[{O1}+0x{L2:02X}] = {O2} - 0x80,'
                              f'\t/* {L1:o} {L2|0x80:o} ... */')
                O += 64
                output.append(f'[{O2}+0x{L3:02X}] = {O} - 0x80,'
                              f'\t/* {L1:o} {L2|0x80:o} {L3|0x80:o} ... */')
            elif L2 != PL2:
                PL2 = L2
                PL3 = L3
                O += 64
                O2 = O
                output.append(f'[{O1}+0x{L2:02X}] = {O2} - 0x80,'
                              f'\t/* {L1:o} {L2|0x80:o} ... */')
                O += 64
                output.append(f'[{O2}+0x{L3:02X}] = {O} - 0x80,'
                              f'\t/* {L1:o} {L2|0x80:o} {L3|0x80:o} ... */')
            elif L3 != PL3:
                PL3 = L3
                O += 64
                output.append(f'[{O2}+0x{L3:02X}] = {O} - 0x80,'
                              f'\t/* {L1:o} {L2|0x80:o} {L3|0x80:o} ... */')
            output.append(f'[{O}+0x{L4:02X}] = {c},'
                          f'\t/* U+{codepoint:04X}: {name} */')
    O += 64
    return O, output, values

def main():
    p1 = subprocess.Popen(['bash', '-c', r'while read line; do cp=${line%%\;*}; line=${line#*\;}; name=${line%%\;*}; if ((0x$cp < 160 && 0x$cp <= 0xFFFF)) || [[ $name == *,* ]]; then continue; fi; echo -e "$((0x$cp)),$name,\\u$cp"; done'], stdin=open('/usr/share/unicode/ucd/UnicodeData.txt'), stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['iconv', '-futf-8', '-tascii//translit'], stdin=p1.stdout, stdout=subprocess.PIPE, text=True)
    p1.stdout.close()

    cnt, lines, values = mktab(p2.stdout)
    print('static const char *const valtab[] = {')
    i = 0
    for val in values:
        if val is None:
            print('\tNULL,')
        else:
            print(f'\t[{i}] = "', val.replace('\\', r'\\').replace('"', r'\"'), '",', sep='')
        i += 1
    print('};')
    print(f'static const int16_t asciify[{cnt}] = {{')
    for line in lines:
        print('\t', line, sep='')
    print('};')

if __name__ == '__main__':
    main()
