
i=0

f = open("large_text_node.xml","w")

f.write( "<aap>\n" )
while i < 6000000:
        f.write( '&quot;\n' )
        i+=1;
f.write( "</aap>\n" )

f.close()
