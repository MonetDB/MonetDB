(
 let $x := <x id="0"><z id="0">zero</z></x>
 return $x/z[@id = 0]
,
 let $x := <x id="0"><z id="0">zero</z></x>
 return $x/*[@id = 0]
,
 let $x := <x id="0"><z id="0">zero</z></x>
 return $x[@id = 0]
)
