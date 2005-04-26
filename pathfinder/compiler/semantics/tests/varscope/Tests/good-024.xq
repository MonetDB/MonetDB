typeswitch ("foo")
  case $a as xs:integer return $a + 1
  default return 42
