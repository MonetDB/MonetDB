for $t in ( <doc>
              <greet kind="informal">Hi </greet>
              <greet kind="casual">Hello </greet>
              <location kind="global">World</location>
              <location kind="local">Amsterdam</location>
            </doc> )
let $x := $t/greet[@kind="casual"]/text()
let $y := $t/location[@kind="global"]/text()
return <example> { $x, $y } </example>
