for $i in doc("lltest.xml")//b
return element { "test" } {
  $i/select-narrow::c
}
