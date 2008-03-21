$XQUERYCLIENT -s "pf:add-doc(\"$TSTSRCDIR/update-stress.SF-1896755.xml\",\"update-stress.SF-1896755.xml\",10)"
$XQUERYCLIENT $TSTSRCDIR/update-stress.SF-1896755-xq &
$XQUERYCLIENT $TSTSRCDIR/update-stress.SF-1896755-xq &
$XQUERYCLIENT $TSTSRCDIR/update-stress.SF-1896755-xq &
wait
