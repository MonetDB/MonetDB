typeswitch ("foo")
  case $a as xs:integer return (42, $c)
  default $c return 42
