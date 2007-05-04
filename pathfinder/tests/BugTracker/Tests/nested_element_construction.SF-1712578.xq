let $tags := ("a","b","c","d")
return
<result>{
   (<tags>{$tags}</tags>
,
   for $i in $tags
   return
      element {$i} {
         for $j in $tags
         return element {$j} {}
      }
   )
}</result>
