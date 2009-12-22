let $d := pf:collection("thesis.xml")
let $opt := <TijahOptions ft-index="snowball"/>
return <res>
{ 
   for $e in $d//section
   return <section> { tijah:terms($e, $opt) }
   </section>
}
</res>
