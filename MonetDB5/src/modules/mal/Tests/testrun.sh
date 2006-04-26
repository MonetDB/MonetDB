mserver5 --set monet_prompt='' <fastcrack_select_void_low_hgh.mal |tail -1 
mserver5 --set monet_prompt='' <fastcrack_select_void_mid.mal |tail -1 
mserver5 --set monet_prompt='' <fastcrack_select_low_hgh.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_select_mid.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_scan.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_copy.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_EQ_mid.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_LE_mid.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_low_hgh.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_ALL.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sort.mal |tail -1

echo 'tail<=MID , tail>MID'
mserver5 --set monet_prompt='' <fastcrack_ZeroOrdered.mal |egrep -v '^[# ]|, true\]$|^\[ "" \]$'
echo 'tail<MID , tail==MID , tail>MID'
mserver5 --set monet_prompt='' <fastcrack_OneOrdered.mal |egrep -v '^[# ]|, true\]$|^\[ "" \]$'
echo 'tail<=LOW , LOW<tail<=HGH , tail>HGH'
mserver5 --set monet_prompt='' <fastcrack_ThreeOrdered.mal |egrep -v '^[# ]|, true\]$|^\[ "" \]$'
#echo 'tail<LOW , tail==LOW , LOW<tail<HGH , tail==HGH , tail>HGH'
#mserver5 --set monet_prompt='' <fastcrack_TwoOrdered.mal |egrep -v '^[# ]|, true\]$|^\[ "" \]$'
