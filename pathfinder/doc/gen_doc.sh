#!/bin/sh

# The contents of this file are subject to the Pathfinder Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
# the License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the Pathfinder system.
#
# The Original Code has initially been developed by the Database &
# Information Systems Group at the University of Konstanz, Germany and
# is now maintained by the Database Systems Group at the Technische
# Universitaet Muenchen, Germany.  Portions created by the University of
# Konstanz and the Technische Universitaet Muenchen are Copyright (C)
# 2000-2005 University of Konstanz and (C) 2005-2008 Technische
# Universitaet Muenchen, respectively.  All Rights Reserved.

#
# Generate Pathfinder source code documentation. Run with
# the Pathfinder root directory (where, e.g., you find the
# ./configure script) as the current working directory.


# See if we have doxygen available
echo -n 'Checking for doxygen... '
if type doxygen > /dev/null 2> /dev/null
then
        echo 'yes'
else
        echo 'no'
        echo 'Unable to find doxygen on your machine. Exiting.'
        exit 1
fi

# Check for AT&T dot
echo -n 'Checking for AT&T dot... '
if type dot > /dev/null 2> /dev/null
then
        HAVE_DOT=YES
        echo 'yes'
else
        HAVE_DOT=NO
        echo 'no'
fi

# Check for (pdf)latex
echo -n 'Checking for latex... '
if type pdflatex > /dev/null 2> /dev/null
then
        HAVE_PDFLATEX=YES
        HAVE_LATEX=YES
        echo 'pdflatex'
else
        HAVE_PDFLATEX=NO
        if type latex > /dev/null 2> /dev/null
        then
                HAVE_LATEX=YES
                echo 'latex'

                HAVE_DVIPS=NO
                HAVE_PS2PDF=NO

                echo -n 'Checking for dvips... '
                if type dvips > /dev/null 2> /dev/null
                then
                        echo 'yes'
                        HAVE_DVIPS=YES
                        echo -n 'Checking for ps2pdf... '
                        if type ps2pdf > /dev/null 2> /dev/null
                        then
                                echo 'yes'
                                HAVE_PS2PDF=YES
                        fi
                fi
        else
                HAVE_LATEX=NO
                echo 'no'
        fi
fi

# Now create doxygen.cf
echo -n 'Creating doxygen configuration file... '
cat doc/doxygen.cf.in \
        | sed -e "s/@@HAVE_LATEX@@/${HAVE_LATEX}/g" \
        | sed -e "s/@@HAVE_PDFLATEX@@/${HAVE_PDFLATEX}/g" \
        | sed -e "s/@@HAVE_DOT@@/${HAVE_DOT}/g" \
        > doc/doxygen.cf
if test $? -ne 0
then
        echo 'failed.'
        echo 'Unable to create doxygen configuration file.'
        echo 'Giving up. Sorry.'
        exit 1
fi
echo 'done.'

# Invoke doxygen
echo 'Invoking doxygen...'
doxygen doc/doxygen.cf
if test $? -ne 0
then
        echo 'Error running doxygen. Giving up. Sorry.'
        exit 1
fi

# Now generate the PDF documentation
if test ${HAVE_LATEX} = 'YES' -o ${HAVE_LATEX} = 'yes'
then
        echo 'Doxygen has finished successfully.'
        echo 'Now generating documentation with latex...'
        if test -f doc/latex/Makefile
        then
                (cd doc/latex ; make)
        fi

        if test ${HAVE_PDFLATEX} = 'NO' -o ${HAVE_PDFLATEX} = 'no'
        then
                # Process DVI file with dvips if available
                if test -f doc/latex/refman.dvi \
                        -a \( ${HAVE_DVIPS} = 'YES' -o ${HAVE_DVIPS} = 'yes' \)
                then
                        dvips doc/latex/refman.dvi -o doc/latex/refman.ps
                fi

                # Process PS file with ps2pdf if available
                if test -f doc/latex/refman.ps \
                        -a \( ${HAVE_PS2PDF} = 'YES' \
                              -o ${HAVE_PS2PDF} = 'yes' \)
                then
                        ps2pdf doc/latex/refman.ps doc/latex/refman.pdf
                fi
        fi
fi

# Report success to user
echo
echo 'Generation of Pathfinder compiler documentation finished.'
if test -f doc/html/index.html
then
        echo 'Point your browser to'
        echo "    file:///`pwd`/doc/html/index.html"
        echo 'to view the HTML documentation.'
fi
if test -f doc/latex/refman.pdf -o -f doc/latex/refman.dvi
then
        echo 'A printable documentation can be found at'

        for format in pdf ps dvi
        do
                if test -f doc/latex/refman.${format}
                then
                        echo "    `pwd`/doc/latex/refman.${format}"
                fi
        done
fi
if test -d doc/man/man3
then
        echo 'You may also find the man pages useful.'
        echo "Just add `pwd`/doc/man to your MANPATH"
        echo 'environment variable.'
        echo 'There is one man page for each Pathfinder source file.'
fi
