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

def init():
    strs = [None]
    strdict = {}
    folder = {}
    for line in open('/usr/share/unicode/ucd/CaseFolding.txt'):
        if line.startswith('#'):
            continue
        fields = [x.strip() for x in line.split(';')]
        if not fields[0]:
            continue            # empty line
        if fields[1] in ('S', 'T'):
            continue
        folded = tuple([int(x, 16) for x in fields[2].split()])
        if len(folded) > 1:
            if folded not in strdict:
                strdict[folded] = len(strs)
                strs.append(folded)
        folder[int(fields[0], 16)] = strdict.get(folded, folded)
    casing = {}
    for line in open('/usr/share/unicode/ucd/SpecialCasing.txt'):
        if line.startswith('#'):
            continue
        line = line.rstrip('\n')
        if not line:
            continue
        fields = line.split(';')
        if len(fields) > 5:
            # conditional casing
            continue
        codepoint = int(fields[0], 16)
        lower = tuple([int(x, 16) for x in fields[1].split()])
        # title = tuple([int(x, 16) for x in fields[2].split()])
        upper = tuple([int(x, 16) for x in fields[3].split()])
        if len(lower) > 1 and lower not in strdict:
            strdict[lower] = len(strs)
            strs.append(lower)
        if len(upper) > 1 and upper not in strdict:
            strdict[upper] = len(strs)
            strs.append(upper)
        casing[codepoint] = (strdict.get(lower), strdict.get(upper))
    return casing, folder, strs

def mktab(column, casing, folder):
    output = []
    PL1 = 0
    O = 0
    for line in open('/usr/share/unicode/ucd/UnicodeData.txt'):
        fields = line.rstrip('\n').split(';')
        codepoint = int(fields[0], 16)
        if column == 0:         # case fold
            fold = folder.get(codepoint)
            if fold is None:
                if codepoint <= 0x7F:
                    conversion = codepoint
                else:
                    continue
            elif type(fold) == type(0):
                conversion = -fold
            else:
                conversion = fold[0]
        elif not fields[column]:
            if codepoint in casing:
                conversion = casing[codepoint][13 - column]
                if conversion is None:
                    continue
                conversion = -conversion
            elif codepoint >= 128:
                continue
            else:
                conversion = codepoint
        else:
            conversion = int(fields[column], 16)
        if conversion < 0:
            convstr = f'{conversion}'
        else:
            convstr = f'0x{conversion:04X}'
        name = fields[1]
        if name.startswith('<') and codepoint >= 128:
            continue
        if codepoint <= 0x7F:
            output.append(f'[0x{codepoint:02X}] = {convstr},'
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
            output.append(f'[{O}+0x{L2:02X}] = {convstr},'
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
            output.append(f'[{O}+0x{L3:02X}] = {convstr},'
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
            output.append(f'[{O}+0x{L4:02X}] = {convstr},'
                          f'\t/* U+{codepoint:04X}: {name} */')
    O += 64
    return O, output

def main():
    casing, folder, strs = init()

    print('static const char *const specialcase[] = {')
    lineno = 0
    for line in strs:
        if line is None:
            print('\tNULL,')
        else:
            l = ''
            for cp in line:
                if cp < 0x80:
                    c = chr(cp)
                    if c in ('"', '\\'):
                        l += '\\'
                    l += c
                elif cp < 0x800:
                    l += f'\\x{0xC0 | (cp >> 6):02X}' \
                        f'\\x{0x80 | (cp & 0x3F):02X}'
                elif cp < 0x10000:
                    l += f'\\x{0xE0 | (cp >> 12):02X}' \
                        f'\\x{0x80 | ((cp >> 6) & 0x3F):02X}' \
                        f'\\x{0x80 | (cp & 0x3F):02X}'
                else:
                    l += f'\\x{0xF0 | (cp >> 18):02X}' \
                        f'\\x{0x80 | ((cp >> 12) & 0x3F):02X}' \
                        f'\\x{0x80 | ((cp >> 6) & 0x3F):02X}' \
                        f'\\x{0x80 | (cp & 0x3F):02X}'
            print(f'\t[{lineno}] = "{l}",')
        lineno += 1
    print('};')

    cnt, lines = mktab(13, casing, folder)
    print(f'static const int lowercase[{cnt}] = {{')
    for line in lines:
        print('\t', line, sep='')
    print('};')

    cnt, lines = mktab(12, casing, folder)
    print(f'static const int uppercase[{cnt}] = {{')
    for line in lines:
        print('\t', line, sep='')
    print('};')

    cnt, lines = mktab(0, casing, folder)
    print(f'static const int casefold[{cnt}] = {{')
    for line in lines:
        print('\t', line, sep='')
    print('};')

if __name__ == '__main__':
    main()
