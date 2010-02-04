let $opt := <TijahOptions ft-index="snowball"/>
let $queries := ("//section[about(., retrieval)]", "//para[about(., retrieval)]", "//section[about(., pathfinder)]")

return <res>
{
   for $q at $qn in $queries
   let $qid := tijah:queryall-id($q,$opt)
   return <results query-number="{$qn}" result-number="{tijah:resultsize($qid)}">
   {
      for $n at $r in tijah:nodes($qid)
      return <node rank="{$r}">{$n}</node>
   }
   </results>
}
</res>
