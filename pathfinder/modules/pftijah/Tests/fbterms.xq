let $d := pf:collection("thesis.xml")
let $opt1 := <TijahOptions ft-index="porter" returnNumber="4"/>
let $opt2 := <TijahOptions ft-index="snowball" returnNumber="4"/>
let $opt3 := <TijahOptions ft-index="nostem" returnNumber="4"/>
let $opts := ($opt1, $opt2, $opt3)
return <res>
{ 
   for $opt in $opts
   for $e in $d//section
   return <bubble stemming="{$opt/@ft-index}"> { tijah:fb-terms($e, $opt) }
   </bubble>
}
</res>
