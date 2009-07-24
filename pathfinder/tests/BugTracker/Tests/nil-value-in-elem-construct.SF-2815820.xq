let $out := (
for $a in (<a x="5"/>)[@x=("5","20")]
return <b>{ $a/xxx }</b>
)
return <c>{ $out/@*, $out }</c>
