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

# script to convert a *.vdproj for 64 bits to one for 32 bits.
# this script is mostly to document what I did, not for regular use.

cp ../installer64/*.{vdproj,sln} .
sed -i -e '/TargetPlatform/s/3:1/3:0/' \
       -e 's/win64/win32/' \
       -e 's/\[ProgramFiles64Folder\]/[ProgramFilesFolder]/' \
       -e 's/C:\\\\Program Files (x86)\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x64.msm/C:\\\\Program Files\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x86.msm/' \
       -e 's/"ProductCode" = "8:{.*/"ProductCode" = "8:{66BABD32-D69D-4A89-A7F3-2655D4CD0641}"/' \
       -e 's/"PackageCode" = "8:{.*/"PackageCode" = "8:{D9B2D386-1461-43BC-9A63-93F1BA0D7921}"/' \
       -e 's/"UpgradeCode" = "8:{.*/"UpgradeCode" = "8:{730C595B-DBA6-48D7-94B8-A98780AC92B6}"/' \
    *.vdproj
