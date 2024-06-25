#!/bin/bash

# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

{
    sed -n -e '/# Mn/d' \
	-e 's/^\([0-9A-F][0-9A-F]*\)\.\.\([0-9A-F][0-9A-F]*\) *; *[FW].*/0x\1 0x\2/p' \
	-e 's/^\([0-9A-F][0-9A-F]*\) *; [FW].*/0x\1 0x\1/p' \
	< /usr/share/unicode/ucd/EastAsianWidth.txt | {
	while read line; do
	    line=($line)
	    f=$((${line[0]%}))
	    l=$((${line[1]%}))
	    if [[ -n $prevl ]]; then
		if (($prevl+1 == $f)); then
		    prevl=$l
		else
		    printf '\t{ 0x%05X, 0x%05X, 2 },\n' $prevf $prevl
		    prevf=$f
		    prevl=$l
		fi
	    else
		prevf=$f
		prevl=$l
	    fi
	done
	printf '\t{ 0x%05X, 0x%05X, 2 },\n' $prevf $prevl
    }

    sed -n '/^00AD/d;s/^\([0-9A-F][0-9A-F]*\);[^;]*;\(Me\|Mn\|Cf\);.*/0x\1/p' \
	< /usr/share/unicode/ucd/UnicodeData.txt | {
	while read line; do
	    u=$(($line))
	    if [[ -n $prevf ]]; then
		if (($prevl+1 == $u)); then
		    prevl=$u
		else
		    printf '\t{ 0x%05X, 0x%05X, 0 },\n' $prevf $prevl
		    prevf=$u
		    prevl=$u
		fi
	    else
		prevf=$u
		prevl=$u
	    fi
	done
	printf '\t{ 0x%05X, 0x%05X, 0 },\n' $prevf $prevl
    }
} | sort | sed 's/0x0\([0-9A-F][0-9A-F][0-9A-F][0-9A-F]\)/0x\1/g'
