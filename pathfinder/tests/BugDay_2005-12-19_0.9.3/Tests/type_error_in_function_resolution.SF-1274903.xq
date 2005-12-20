declare namespace local = "urn:local";
declare variable $db := doc("big.xml")/database;

declare function local:relations($ignore)
{
  for $r in $db/relation
  return $r
};

<html>{
  local:relations($db)
}</html>
