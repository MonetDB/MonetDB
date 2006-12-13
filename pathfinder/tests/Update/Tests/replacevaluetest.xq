let $d := doc("replacevaluetest.xml")
return (do replace value of exactly-one($d//child) with "replacement",
        do insert <stuff/> as first into exactly-one($d//content))
