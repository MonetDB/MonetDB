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

module XMark where

import Core
import DM

-- xmark_Q1:
-- for $b in fn:doc ("auction.xml")/site/people/person
-- return if fn:data ($b/id/text()) = "person0"
--        then $b/name/text()
--        else ()
xmark_Q1 = XFOR "b" (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                         (Child, XMLElem, ["site"]))
                           (Child, XMLElem, ["people"]))
                    (Child, XMLElem, ["person"]))
                    (XIF (XEQ (XFNDATA (XPATH (XPATH (XVAR "b") 
                                                     (Child, XMLElem, ["id"]))
                                              (Child, XMLText, [])))
                              (XSTR "person0")
                         )
                         (XPATH (XPATH (XVAR "b") 
                                        (Child, XMLElem, ["name"]))
                                (Child, XMLText, []))
                         XEMPTY
                    )

-- xmark_Q2:
-- for $b in fn:doc("auction.xml")/site/open_auctions/open_auction
-- return element {"increase"} 
--                { for $x at $p in $b/bidder
--                  return if $p = 1 then $x/increase/text()
--                         else ()
--                }
xmark_Q2 = XFOR "b" (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                         (Child, XMLElem, ["site"]))
                                  (Child, XMLElem, ["open_auctions"]))
                           (Child, XMLElem, ["open_auction"]))
                    (XELEM (XSTR "increase") 
                         (XFORAT "x" "p"
                                 (XPATH (XVAR "b") 
                                        (Child, XMLElem, ["bidder"]))
                                 (XIF (XEQ (XVAR "p") (XINT 1))
                                      (XPATH (XPATH (XVAR "x") 
                                                (Child, XMLElem, ["increase"]))
                                             (Child, XMLText, []))
                                      XEMPTY
                                 )
                         )
                    )


-- xmark_Q5:
-- fn:count(for $i in fn:doc("auction.xml")/site/closed_auctions/closed_auction
--          return if xs:int(fn:data($i/price/text())) > 40
--                 then $i/price
--                 else ())
xmark_Q5 = XFNCOUNT 
             (XFOR "i" (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                            (Child, XMLElem, ["site"]))
                                     (Child, XMLElem, ["closed_auctions"]))
                              (Child, XMLElem, ["closed_auction"]))
                    (XIF (XGT (XCASTINT (XFNDATA (XPATH (XPATH (XVAR "i")
                                                (Child, XMLElem, ["price"]))
                                                    (Child, XMLText, [])))
                              )
                              (XINT 40)
                         )
                         (XPATH (XVAR "i") 
                                (Child, XMLElem, ["price"]))
                         XEMPTY
                    )
             )

-- xmark_Q6:
-- for $b in fn:doc("auction.xml")/site/regions
-- return fn:count($b//item)
xmark_Q6 = XFOR "b" (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                  (Child, XMLElem, ["site"]))
                           (Child, XMLElem, ["regions"]))
                    (XFNCOUNT (XPATH (XPATH (XVAR "b")
                                            (Descendant_or_self, XMLNode, []))
                                     (Child, XMLElem, ["item"]))
                    )

-- xmark_Q7:
-- for $p in fn:doc("auction.xml")/site
-- return fn:count($p//description) +
--        fn:count($p//annotation)  +
--        fn:count($p//email)
xmark_Q7 = XFOR "p" (XPATH (XFNDOC (XSTR "auction.xml") )
                           (Child, XMLElem, ["site"]))
                 (XPLUS (XFNCOUNT (XPATH (XPATH (XVAR "p")
                                            (Descendant_or_self, XMLNode, []))
                                         (Child, XMLElem, ["description"])))
                  (XPLUS (XFNCOUNT (XPATH (XPATH (XVAR "p")
                                             (Descendant_or_self, XMLNode, []))
                                          (Child, XMLElem, ["annotation"])))
                         (XFNCOUNT (XPATH (XPATH (XVAR "p")
                                             (Descendant_or_self, XMLNode, []))
                                          (Child, XMLElem, ["email"])))
                  )
                 )


-- xmark_Q8:
-- for $p in fn:doc("auction.xml")/site/people/person
-- return let $a := for $t in fn:doc("auction.xml")/site/closed_auctions/
--                                                              closed_auction
--                  return if fn:data($t/buyer/person/text()) = 
--                            fn:data($p/id/text())
--                         then $t
--                         else ()
--        return element {"item"} { element {"person"} { $p/name/text() },
--                                 text { xs:string(fn:count($a)) }
--                                }
xmark_Q8' =
 XFOR "p" (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                               (Child, XMLElem, ["site"]))
                        (Child, XMLElem, ["people"]))
                 (Child, XMLElem, ["person"]))
          (XLET "a" (XFOR "t" (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                                   (Child, XMLElem, ["site"]))
                                            (Child, XMLElem, ["closed_auctions"]))
                                     (Child, XMLElem, ["closed_auction"]))
                              (XIF (XEQ (XFNDATA (XPATH (XPATH (XPATH (XVAR "t")
                                                      (Child, XMLElem, ["buyer"]))
                                                     (Child, XMLElem, ["person"]))
                                                    (Child, XMLText, []))
                                        )
                                        (XFNDATA (XPATH (XPATH (XVAR "p")
                                                        (Child, XMLElem, ["id"]))
                                                       (Child, XMLText, []))
                                        )
                                   )
                                   (XVAR "t")
                                   XEMPTY
                              )
                    )
                    (XELEM (XSTR "item")
                           (XSEQ (XELEM (XSTR "person")
                                        (XPATH (XPATH (XVAR "p")
                                                      (Child, XMLElem, ["name"]))
                                               (Child, XMLText, []))
                                 )
                                 (XTEXT (XCASTSTR (XFNCOUNT (XVAR "a")))
                                 )
                           )
                    )
          )

xmark_Q8 =
 XLET "d" (XFNDOC (XSTR "auction.xml"))
 (XFOR "p" (XPATH (XPATH (XPATH (XVAR "d")
                               (Child, XMLElem, ["site"]))
                        (Child, XMLElem, ["people"]))
                 (Child, XMLElem, ["person"]))
          (XLET "a" (XFOR "t" (XPATH (XPATH (XPATH (XVAR "d")
                                                   (Child, XMLElem, ["site"]))
                                            (Child, XMLElem, ["closed_auctions"]))
                                     (Child, XMLElem, ["closed_auction"]))
                              (XIF (XEQ (XFNDATA (XPATH (XPATH (XPATH (XVAR "t")
                                                      (Child, XMLElem, ["buyer"]))
                                                     (Child, XMLElem, ["person"]))
                                                    (Child, XMLText, []))
                                        )
                                        (XFNDATA (XPATH (XPATH (XVAR "p")
                                                        (Child, XMLElem, ["id"]))
                                                       (Child, XMLText, []))
                                        )
                                   )
                                   (XVAR "t")
                                   XEMPTY
                              )
                    )
                    (XELEM (XSTR "item")
                           (XSEQ (XELEM (XSTR "person")
                                        (XPATH (XPATH (XVAR "p")
                                                      (Child, XMLElem, ["name"]))
                                               (Child, XMLText, []))
                                 )
                                 (XTEXT (XCASTSTR (XFNCOUNT (XVAR "a")))
                                 )
                           )
                    )
          )
 )


-- xmark_Q13:
-- for $i in fn:doc("auction.xml")/site/regions/australia/item
-- return element {"item"} { element {"name"} { $i/name/text() },
--                           $i/description
-- 
--                         }
xmark_Q13 = XFOR "i" (XPATH (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                                 (Child, XMLElem, ["site"]))
                                          (Child, XMLElem, ["regions"]))
                                   (Child, XMLElem, ["australia"]))
                            (Child, XMLElem, ["item"]))
                  (XELEM (XSTR "item")
                         (XSEQ (XELEM (XSTR "name")
                                      (XPATH (XPATH (XVAR "i")
                                                    (Child, XMLElem, ["name"]))
                                             (Child, XMLText, []))
                               )
                               (XPATH (XVAR "i")
                                      (Child, XMLElem, ["description"]))
                         )
                  )

-- xmark_Q17:
-- for $p in fn:doc("auction.xml")/site/people/person
-- return if fn:empty ($p/homepage/text())
--        then element {"person"} { $p/name/text() }
--        else ()
xmark_Q17 = XFOR "p" (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                          (Child, XMLElem, ["site"]))
                                   (Child, XMLElem, ["people"]))
                            (Child, XMLElem, ["person"]))
                  (XIF (XFNEMPTY (XPATH (XPATH (XVAR "p")
                                               (Child, XMLElem, ["homepage"]))
                                        (Child, XMLText, []))
                       )
                       (XELEM (XSTR "person") 
                              (XPATH (XPATH (XVAR "p")
                                            (Child, XMLElem, ["name"]))
                                     (Child, XMLText, []))
                       )
                       XEMPTY
                  )

-- xmark_Q18:
-- define function convert ($v) { 2.20731 * $v }      (: see UDF.hs :)
-- 
-- for $i in fn:doc("auction.xml")/site/open_auctions/open_auction
-- return convert(xs:double(fn:data($i/reserve/text())))
xmark_Q18 = XFOR "i" (XPATH (XPATH (XPATH (XFNDOC (XSTR "auction.xml"))
                                          (Child, XMLElem, ["site"]))
                                   (Child, XMLElem, ["open_auctions"]))
                            (Child, XMLElem, ["open_auction"]))
                     (XFUN "convert" 
                           [XCASTDBL (XFNDATA (XPATH (XPATH (XVAR "i")
                                                 (Child, XMLElem, ["reserve"]))
                                               (Child, XMLText, [])))
                           ]
                     )



