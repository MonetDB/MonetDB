for $a in (1, "foo", 2.3, 3E1, <a/>, attribute b {"b"}, text {"text"})
return typeswitch ($a)
        case xs:integer return "integer"
        case xs:double return "double"
        case xs:decimal return "decimal"
        case xs:string return "string"
        case node() return "node()"
        case attribute() return "attribute()"
        case element() return "element()"
        case text() return "text()"
        default return "null"

