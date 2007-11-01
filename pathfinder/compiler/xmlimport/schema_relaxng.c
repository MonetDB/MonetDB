/****************************************************************************
*****************************************************************************
**  A U T O M A T I C A L L Y   G E N E R A T E D   C O D E
**  do not edit here!
*****************************************************************************
*****************************************************************************/


/* 
  =============================================================================
  =============================================================================
  This is the original schema in "RELAX NG Compact Syntax"
  (but libxml2 supports only the "RELAX NG XML Syntax")
  =============================================================================
  =============================================================================
*/

/*

grammar {

#*******************************************************************************
#*******************************************************************************
# This is how an xml-serialized logical algebra plan looks in general,
# the operator-specific content of the node-element is defined beneath
#*******************************************************************************
#*******************************************************************************

    start =
        element logical_query_plan {
            element node {
                attribute id {xsd:integer},
#                attribute id {xsd:ID},
                PFLA_OPERATORSPECIFIC_CONTENT
            }*
        }

#*******************************************************************************
#*******************************************************************************
# Definiton of the edge-element
#*******************************************************************************
#*******************************************************************************

    EDGE =
        element edge {
            attribute to {xsd:integer}
#            attribute to {xsd:IDREF}
        }

#*******************************************************************************
#*******************************************************************************
# Some value constraints
#*******************************************************************************
#*******************************************************************************

    PFLA_DATATYPE_ATOMIC =
        "nat" | "int" | "str" | "dec" | "dbl" | "bool" | "uA" | "qname" |
        "node" | "attr" | "attrID" | "afrag" | "pnode" | "pre" | "pfrag"

    PFLA_ATTRIBUTE_NAME =
        "(NULL)" | "iter" | "item" | "pos" | "iter1" | "item1" | "pos1" |
        "inner" | "outer" | "sort" | "sort1" | "sort2" | "sort3" | "sort4" |
        "sort5" | "sort6" | "sort7" | "ord" | "iter2" | "iter3" | "iter4" |
        "iter5" | "iter6" | "res" | "res1" | "cast" | "item2" | "item2" |
        "item3" | "item4" | "item5" | "item6" | "item7" |
        xsd:string {pattern = 'item[1-9]{1}[0-9]*'}


#*******************************************************************************
#*******************************************************************************
# Definitions of the Operator-specific content of the node-element
#*******************************************************************************
#*******************************************************************************

    PFLA_OPERATORSPECIFIC_CONTENT =
            PFLA_SERIALIZE |
            PFLA_ATACH


#*******************************************************************************
#*******************************************************************************

#<content>
#   <column name="COLNAME" new="false" function="pos"/>
#   <column name="COLNAME" new="false" function="item"/>
# </content>

    PFLA_SERIALIZE =
        attribute kind {"serialize"},
        element content {
            element column {
                attribute name {PFLA_ATTRIBUTE_NAME},
                attribute new {"false"},
                attribute function {"pos"}

            },
            element column {
                attribute name {PFLA_ATTRIBUTE_NAME},
                attribute new {"false"},
                attribute function {"item"}
            }
        },
        EDGE,
        EDGE

#*******************************************************************************
#*******************************************************************************

#<content>
#   <column name="COLNAME" new="true">
#     <value type="DATATYPE">VALUE</value>
#   </column>
# </content>

    PFLA_ATACH =
        attribute kind {"attach"},
        element content {
            element column {
                attribute name {PFLA_ATTRIBUTE_NAME},
                attribute new {"true"},
                element value {
                    attribute type {PFLA_DATATYPE_ATOMIC},
                    text
                }
            }
        },
        EDGE

} # grammar


*/


/* 
  =============================================================================
  =============================================================================
  This is the schema in "RELAX NG XML Syntax"
  (automatically generated from the original schema)
  =============================================================================
  =============================================================================
*/

char* schema_relaxNG = 

"<?xml version=\"1.0\" ?>"
"<grammar xmlns=\"http://relaxng.org/ns/structure/1.0\" datatypeLibrary=\"http://www.w3.org/2001/XMLSchema-datatypes\">"
"  <!--"
"    *******************************************************************************"
"    *******************************************************************************"
"    This is how an xml-serialized logical algebra plan looks in general,"
"    the operator-specific content of the node-element is defined beneath"
"    *******************************************************************************"
"    *******************************************************************************"
"  -->"
"  <start>"
"    <element name=\"logical_query_plan\">"
"      <zeroOrMore>"
"        <element name=\"node\">"
"          <attribute name=\"id\">"
"            <data type=\"integer\"/>"
"          </attribute>"
"          <!--                attribute id {xsd:ID}, -->"
"          <ref name=\"PFLA_OPERATORSPECIFIC_CONTENT\"/>"
"        </element>"
"      </zeroOrMore>"
"    </element>"
"  </start>"
"  <!--"
"    *******************************************************************************"
"    *******************************************************************************"
"    Definiton of the edge-element"
"    *******************************************************************************"
"    *******************************************************************************"
"  -->"
"  <define name=\"EDGE\">"
"    <element name=\"edge\">"
"      <attribute name=\"to\">"
"        <data type=\"integer\"/>"
"      </attribute>"
"      <!--            attribute to {xsd:IDREF} -->"
"    </element>"
"  </define>"
"  <!--"
"    *******************************************************************************"
"    *******************************************************************************"
"    Some value constraints"
"    *******************************************************************************"
"    *******************************************************************************"
"  -->"
"  <define name=\"PFLA_DATATYPE_ATOMIC\">"
"    <choice>"
"      <value>nat</value>"
"      <value>int</value>"
"      <value>str</value>"
"      <value>dec</value>"
"      <value>dbl</value>"
"      <value>bool</value>"
"      <value>uA</value>"
"      <value>qname</value>"
"      <value>node</value>"
"      <value>attr</value>"
"      <value>attrID</value>"
"      <value>afrag</value>"
"      <value>pnode</value>"
"      <value>pre</value>"
"      <value>pfrag</value>"
"    </choice>"
"  </define>"
"  <define name=\"PFLA_ATTRIBUTE_NAME\">"
"    <choice>"
"      <value>(NULL)</value>"
"      <value>iter</value>"
"      <value>item</value>"
"      <value>pos</value>"
"      <value>iter1</value>"
"      <value>item1</value>"
"      <value>pos1</value>"
"      <value>inner</value>"
"      <value>outer</value>"
"      <value>sort</value>"
"      <value>sort1</value>"
"      <value>sort2</value>"
"      <value>sort3</value>"
"      <value>sort4</value>"
"      <value>sort5</value>"
"      <value>sort6</value>"
"      <value>sort7</value>"
"      <value>ord</value>"
"      <value>iter2</value>"
"      <value>iter3</value>"
"      <value>iter4</value>"
"      <value>iter5</value>"
"      <value>iter6</value>"
"      <value>res</value>"
"      <value>res1</value>"
"      <value>cast</value>"
"      <value>item2</value>"
"      <value>item2</value>"
"      <value>item3</value>"
"      <value>item4</value>"
"      <value>item5</value>"
"      <value>item6</value>"
"      <value>item7</value>"
"      <data type=\"string\">"
"        <param name=\"pattern\">item[1-9]{1}[0-9]*</param>"
"      </data>"
"    </choice>"
"  </define>"
"  <!--"
"    *******************************************************************************"
"    *******************************************************************************"
"    Definitions of the Operator-specific content of the node-element"
"    *******************************************************************************"
"    *******************************************************************************"
"  -->"
"  <define name=\"PFLA_OPERATORSPECIFIC_CONTENT\">"
"    <choice>"
"      <ref name=\"PFLA_SERIALIZE\"/>"
"      <ref name=\"PFLA_ATACH\"/>"
"    </choice>"
"  </define>"
"  <!--"
"    *******************************************************************************"
"    *******************************************************************************"
"  -->"
"  <!--"
"    <content>"
"      <column name=\"COLNAME\" new=\"false\" function=\"pos\"/>"
"      <column name=\"COLNAME\" new=\"false\" function=\"item\"/>"
"    </content>"
"  -->"
"  <define name=\"PFLA_SERIALIZE\">"
"    <attribute name=\"kind\">"
"      <value>serialize</value>"
"    </attribute>"
"    <element name=\"content\">"
"      <element name=\"column\">"
"        <attribute name=\"name\">"
"          <ref name=\"PFLA_ATTRIBUTE_NAME\"/>"
"        </attribute>"
"        <attribute name=\"new\">"
"          <value>false</value>"
"        </attribute>"
"        <attribute name=\"function\">"
"          <value>pos</value>"
"        </attribute>"
"      </element>"
"      <element name=\"column\">"
"        <attribute name=\"name\">"
"          <ref name=\"PFLA_ATTRIBUTE_NAME\"/>"
"        </attribute>"
"        <attribute name=\"new\">"
"          <value>false</value>"
"        </attribute>"
"        <attribute name=\"function\">"
"          <value>item</value>"
"        </attribute>"
"      </element>"
"    </element>"
"    <ref name=\"EDGE\"/>"
"    <ref name=\"EDGE\"/>"
"  </define>"
"  <!--"
"    *******************************************************************************"
"    *******************************************************************************"
"  -->"
"  <!--"
"    <content>"
"      <column name=\"COLNAME\" new=\"true\">"
"        <value type=\"DATATYPE\">VALUE</value>"
"      </column>"
"    </content>"
"  -->"
"  <define name=\"PFLA_ATACH\">"
"    <attribute name=\"kind\">"
"      <value>attach</value>"
"    </attribute>"
"    <element name=\"content\">"
"      <element name=\"column\">"
"        <attribute name=\"name\">"
"          <ref name=\"PFLA_ATTRIBUTE_NAME\"/>"
"        </attribute>"
"        <attribute name=\"new\">"
"          <value>true</value>"
"        </attribute>"
"        <element name=\"value\">"
"          <attribute name=\"type\">"
"            <ref name=\"PFLA_DATATYPE_ATOMIC\"/>"
"          </attribute>"
"          <text/>"
"        </element>"
"      </element>"
"    </element>"
"    <ref name=\"EDGE\"/>"
"  </define>"
"</grammar>"
"<!-- grammar -->"
;
