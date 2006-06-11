(: Semantics of `as xs:decimal':

    - Check if bound expression is subtype of xs:decimal during
      static typing.
    - Set $a's *static* type to xs:decimal.

   The *dynamic* type of $a, however, is *not* affected by the
   `as xs:decimal'.  $a (and thus the result as well) will have
   dynamic type xs:integer :)
let $a as xs:decimal := 42 return $a
