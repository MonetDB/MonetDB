<?php
# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at 
# http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2002 CWI.  
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
?>

<?php
require("Mapi.php");

$monet = new Mapi;
$monet->init( $monet->hostname(), $monet->portnr(), "niels" );
	echo $monet->cmd($HTTP_POST_VARS["cmd"]);
$monet->disconnect();
?>

<FORM action="MapiClient.php" method="post">
<input type=text name=cmd>
</FORM>
