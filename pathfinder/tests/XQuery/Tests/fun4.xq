declare function faculty_wrapper ($x as xs:integer) as xs:integer
{
    faculty ($x)
};

declare function faculty ($x as xs:integer) as xs:integer
{
    if ($x eq 1) then 1 else $x * faculty ($x -1)
};

faculty_wrapper (6)
