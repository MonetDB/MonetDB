{--

    XQuery user-defined functions.

    Copyright Notice:
    -----------------

     The contents of this file are subject to the MonetDB Public
     License Version 1.0 (the "License"); you may not use this file
     except in compliance with the License. You may obtain a copy of
     the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html

     Software distributed under the License is distributed on an "AS
     IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
     implied. See the License for the specific language governing
     rights and limitations under the License.

     The Original Code is the ``Pathfinder'' system. The Initial
     Developer of the Original Code is the Database & Information
     Systems Group at the University of Konstanz, Germany. Portions
     created by U Konstanz are Copyright (C) 2000-2004 University
     of Konstanz. All Rights Reserved.

     Contributors:
             Torsten Grust <torsten.grust@uni-konstanz.de>

    $Id$

--}

module UDF (udfs) where

import Core
import DM (QName)

--         name    args    body
udfs :: [(QName, ([QName], Core))]
udfs = [-- user in XMark Q17
	("convert", (["v"],
		     XTIMES (XDBL 2.20731) (XVAR "v")))
       ,
	-- poor man's positional access: e[n]
        -- define function nth ($e, $n) {
        --   for $i at $p in $e
        --   return if $p = $n 
        --          then $i
        --          else ()
        -- }
        ("nth", (["e","n"],
                XFORAT "i" "p" (XVAR "e")
                               (XIF (XEQ (XVAR "p") (XVAR "n"))
                                    (XVAR "i")
                                    XEMPTY
                               )
                )
        )
       ]