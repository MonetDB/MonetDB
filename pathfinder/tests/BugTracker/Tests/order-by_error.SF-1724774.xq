let $data := <aap><b/><c/></aap>
for $i in $data/*
order by "" descending
return $i
