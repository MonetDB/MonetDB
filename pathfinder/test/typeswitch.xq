typeswitch (/foo)
  case element                            return "element"
  case element of type foo                return "elem of type foo"
  case element foo                        return "elem foo"
  case element foo of type bar            return "elem foo of type bar"
  case element foo context schema-context return "elem foo in context"
  default                                 return "default"
