# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

# script to convert a *.vdproj for 64 bits to one for 32 bits.
# this script is mostly to document what I did, not for regular use.

cp ../installer64/*.{vdproj,sln} .
sed -i~ -e '/TargetPlatform/s/3:1/3:0/' \
	-e 's/win64/win32/' \
	-e 's/\[ProgramFiles64Folder\]/[ProgramFilesFolder]/' \
	-e 's/C:\\\\Program Files.*\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x64.msm/C:\\\\Program Files\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x86.msm/' \
	-e 's/"ProductCode" = "8:{.*/"ProductCode" = "8:{66BABD32-D69D-4A89-A7F3-2655D4CD0641}"/' \
	-e 's/"PackageCode" = "8:{.*/"PackageCode" = "8:{D9B2D386-1461-43BC-9A63-93F1BA0D7921}"/' \
	-e 's/"UpgradeCode" = "8:{95ACBC8C-BC4B-4901-AF70-48B54A5C20F7}"/"UpgradeCode" = "8:{C1F69378-3F5C-4120-8224-32F07D3458F3}"/' \
	-e 's/"UpgradeCode" = "8:{8E6CDFDE-39B9-43D9-97B3-2440C012845C}"/"UpgradeCode" = "8:{92C89C36-0E86-45E1-B3D8-0D6C91108F30}"/' \
	-e 's/"UpgradeCode" = "8:{839D3C90-B578-41E2-A004-431440F9E899}"/"UpgradeCode" = "8:{730C595B-DBA6-48D7-94B8-A98780AC92B6}"/' \
    *.vdproj
