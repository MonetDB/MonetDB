select xmlserialize( document, 
		xmlroot( 
			xmlforest(
				xmlelement(name="id", V.name) as string,
				xmlcomment( "a real victim")
			),
			version "versioninfo",
			standalone no value
		) )
from victim V;
