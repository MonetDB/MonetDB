mserver5 --set monet_prompt='' <fastcrack_select_void_low_hgh.mal |tail -1 
mserver5 --set monet_prompt='' <fastcrack_select_void_mid.mal   |tail -1 
mserver5 --set monet_prompt='' <fastcrack_select_low_hgh.mal   |tail -1
mserver5 --set monet_prompt='' <fastcrack_select_mid.mal   |tail -1
mserver5 --set monet_prompt='' <fastcrack_scan.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_copy.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_EQ_mid.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_LE_mid.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_low_hgh.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sel_ALL.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_sort.mal |tail -1

echo 'tail<=MID , tail>MID'
mserver5 --set monet_prompt='' <fastcrack_Zero_MK.mal |tail -1
#mserver5 --set monet_prompt='' <fastcrack_Zero_SI.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_Zero_sm.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_Zero_SM.mal |tail -1
echo 'tail<MID , tail==MID , tail>MID'
mserver5 --set monet_prompt='' <fastcrack_One_MK.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_One_SI.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_One_sm.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_One_SM.mal |tail -1
echo 'tail<=LOW , LOW<tail<=HGH , tail>HGH'
mserver5 --set monet_prompt='' <fastcrack_Three_MK.mal |tail -1
#mserver5 --set monet_prompt='' <fastcrack_Three_SI.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_Three_sm.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_Three_SM.mal |tail -1
echo 'tail<LOW , tail==LOW , LOW<tail<HGH , tail==HGH , tail>HGH'
mserver5 --set monet_prompt='' <fastcrack_Two_MK.mal |tail -1
#mserver5 --set monet_prompt='' <fastcrack_Two_SI.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_Two_sm.mal |tail -1
mserver5 --set monet_prompt='' <fastcrack_Two_SM.mal |tail -1
