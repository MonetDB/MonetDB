let $d := pf:collection("thesis.xml")
let $opt1 := <TijahOptions ft-index="porter"/>
let $opt2 := <TijahOptions ft-index="snowball"/>
let $opt3 := <TijahOptions ft-index="nostem"/>
let $opts := ($opt1, $opt2, $opt3)
return <res>
{ 
   for $opt in $opts
   for $e in $d//section
   return <section stemming="{$opt/@ft-index}"> { tijah:terms($e, $opt) }
   </section>
}
</res>
