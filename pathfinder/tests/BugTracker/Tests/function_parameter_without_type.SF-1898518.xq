declare function getData1($d) { $d//data[@num="1"] };
let $d := <b><c><data num="1">211098</data></c></b>
return <result>{getData1($d)}</result>
<>
declare function getData2($d as element()) { $d//data[@num="2"] };
let $d := <b><c><data num="2">211098</data></c></b>
return <result>{getData2($d)}</result>
