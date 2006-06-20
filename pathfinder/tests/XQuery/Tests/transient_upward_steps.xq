let $foo := <a foo="bar"><b>textnode</b><d><e/><f/></d><c foo2="bar"/></a>//e return
let $bar := <a foo="bar"><b>textnode</b><d><e/><f/></d><c foo2="bar"/></a>//(b|c|e) return
(: textnodes introduced for formatting :)
<res>{ text {"
  "},
  (: check if parent and ancestor can cope with transient nodes :)
  <res1>{ ($foo, $bar)/parent::* }</res1>, text {"
  "},
  <res2>{ ($foo, $bar)/ancestor::* }</res2>, text {"
  "},
  <res2>{ ($foo, $bar)/ancestor-or-self::* }</res2>, text {"
  "},
  (: check if parent and ancestor can cope with multiple iterations :)
  <res3>{ for $a in ($foo, $bar) return $a/parent::* }</res3>, text {"
  "},
  <res4>{ for $a in ($foo, $bar) return $a/ancestor::* }</res4>, text {"
  "},
  (: check if parent and ancestor can cope with empty input relations :)
  <res5>{ for $a in ($foo, $bar) return $a/blub/parent::* }</res5>, text {"
  "},
  <res6>{ for $a in ($foo, $bar) return $a/blub/ancestor::* }</res6>, text {"
  "}
}</res>
