let $d := pf:collection("thesis.xml")
let $opt := <TijahOptions stemmedterms="true" ft-index="snowball"/>
return <terms>
{
   for $e at $r in $d//section
   return <section num="{$r}"> 
   {
      for $t in tijah:terms($e, $opt)
      return <term name="{$t}"> {tijah:tf($e, $t, $opt)} </term>
   }
   </section>
}
</terms>
