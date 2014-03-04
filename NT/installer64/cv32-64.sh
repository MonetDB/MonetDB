# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

# script to convert a *.vdproj for 32 bits to one for 64 bits.
# this script is mostly to document what I did, not for regular use.

cp ../installer32/*.{vdproj,sln} .
sed -i -e '/TargetPlatform/s/3:0/3:1/' \
       -e 's/win32/win64/' \
       -e 's/\[ProgramFilesFolder\]/[ProgramFiles64Folder]/' \
       -e 's/C:\\\\Program Files.*\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x86.msm/C:\\\\Program Files\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x86_x64.msm/' \
       -e 's/"ProductCode" = "8:{.*/"ProductCode" = "8:{ACC32EDD-13CE-4079-A6E7-D9DD94DA42EE}"/' \
       -e 's/"PackageCode" = "8:{.*/"PackageCode" = "8:{332EB6D8-73DD-48CA-83E7-BB1922FFE3BD}"/' \
       -e 's/"UpgradeCode" = "8:{C1F69378-3F5C-4120-8224-32F07D3458F3}"/"UpgradeCode" = "8:{95ACBC8C-BC4B-4901-AF70-48B54A5C20F7}"/' \
       -e 's/"UpgradeCode" = "8:{92C89C36-0E86-45E1-B3D8-0D6C91108F30}"/"UpgradeCode" = "8:{8E6CDFDE-39B9-43D9-97B3-2440C012845C}"/' \
       -e 's/"UpgradeCode" = "8:{730C595B-DBA6-48D7-94B8-A98780AC92B6}"/"UpgradeCode" = "8:{839D3C90-B578-41E2-A004-431440F9E899}"/' \
    *.vdproj
