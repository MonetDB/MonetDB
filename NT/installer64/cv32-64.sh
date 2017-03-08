# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

# script to convert a *.vdproj for 32 bits to one for 64 bits.
# this script is mostly to document what I did, not for regular use.

cp ../installer32/*.{vdproj,sln} .
sed -i~ -e '/TargetPlatform/s/3:0/3:1/' \
	-e 's/win32/win64/' \
	-e 's/\[ProgramFilesFolder\]/[ProgramFiles64Folder]/' \
	-e 's/C:\\\\Program Files.*\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x86.msm/C:\\\\Program Files\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC100_CRT_x86_x64.msm/' \
	-e 's/"ProductCode" = "8:{.*/"ProductCode" = "8:{ACC32EDD-13CE-4079-A6E7-D9DD94DA42EE}"/' \
	-e 's/"PackageCode" = "8:{.*/"PackageCode" = "8:{332EB6D8-73DD-48CA-83E7-BB1922FFE3BD}"/' \
	-e 's/"UpgradeCode" = "8:{C1F69378-3F5C-4120-8224-32F07D3458F3}"/"UpgradeCode" = "8:{95ACBC8C-BC4B-4901-AF70-48B54A5C20F7}"/' \
	-e 's/"UpgradeCode" = "8:{92C89C36-0E86-45E1-B3D8-0D6C91108F30}"/"UpgradeCode" = "8:{8E6CDFDE-39B9-43D9-97B3-2440C012845C}"/' \
	-e 's/"UpgradeCode" = "8:{730C595B-DBA6-48D7-94B8-A98780AC92B6}"/"UpgradeCode" = "8:{839D3C90-B578-41E2-A004-431440F9E899}"/' \
    *.vdproj
