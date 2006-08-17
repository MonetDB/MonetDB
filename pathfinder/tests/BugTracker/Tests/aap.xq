module namespace aap = "http://test.cwi.nl/aap";

declare function aap:increment($val as xs:integer) as xs:integer
{
  $val + 1
};

declare function aap:decrement($val as xs:integer) as xs:integer
{
  $val - 1
};


