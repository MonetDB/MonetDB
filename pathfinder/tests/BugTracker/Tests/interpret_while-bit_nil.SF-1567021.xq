declare function func()
{
   let $nonmultiple := if (<a/>/name() eq "") then 1 else ()
      ,$xlist := <cand a="1"/>
      ,$cnt := if ($xlist/@a = $nonmultiple) then 1 else 0
   return
      <a><nonmultiple>{$nonmultiple}</nonmultiple>
         <count>{$cnt}</count>
      </a>
};

(: TEST :)

<res>{ func() }</res>
