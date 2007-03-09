module namespace cq = "utwente";

declare updating function cq:PUT($uri as xs:string, $doc as element())
{
  if (substring($uri, 1,4) = 'tmp/')
  then put($doc, $uri)
  else error('PUT: only relative URIs starting with tmp/ allowed')
};

declare function cq:GET($uri as xs:string)
{ doc($uri) };
