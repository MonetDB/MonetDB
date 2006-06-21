let $d := doc("video.xml")
for $b in distinct-values($d//song/@artist)
return ( 
  let $s := $d//song[@artist=$b]
  return element { "artist" } {
    attribute { "name" } { $b },
    element { "songs" } { $d//song[@artist=$b]/select-narrow::song },
    element { "scenes" } { $d//song[@artist=$b]/select-wide::scene }
  }
)

