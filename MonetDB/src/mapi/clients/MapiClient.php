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
