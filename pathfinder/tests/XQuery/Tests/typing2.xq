(: Function foo() accepts everything that is a subtype of
   xs:decimal.  During execution, however, arguments will
   keep their *dynamic* type (``function conversion'').
   The result is treated equally (i.e., though the result
   has static type xs:decimal and is thus guaranteed to
   be a subtype thereof, values will keep their *dynamic*
   type).  The overall result is thus 42 (as an xs:integer).

   This query will not be handled correctly by the milprint_summer
   back-end.  The test, however, is a reminder to do it better in
   the algebra branch.
:)

declare function foo ($x as xs:decimal) as xs:decimal
{
  $x
};

foo (42)
