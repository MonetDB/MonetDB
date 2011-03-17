# script to convert a *.vdproj for 32 bits to one for 64 bits.
# this script is mostly to document what I did, not for regular use.

cp ../installer32/*.{vdproj,sln} .
sed -i -e '/TargetPlatform/s/3:0/3:1/' \
       -e 's/win32/win64/' \
       -e 's/\[ProgramFilesFolder\]/[ProgramFiles64Folder]/' \
       -e 's/C:\\\\Program Files\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC90_CRT_x86.msm/C:\\\\Program Files (x86)\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC90_CRT_x86_x64.msm/' \
       -e 's/C:\\\\Program Files\\\\Intel\\\\Compiler\\\\11.1\\\\046\\\\bin\\\\ia32\\\\libmmd.dll/C:\\\\Program Files (x86)\\\\Intel\\\\Compiler\\\\11.1\\\\046\\\\bin\\\\intel64\\\\libmmd.dll/' \
       -e 's/{C1F69378-3F5C-4120-8224-32F07D3458F3}/{95ACBC8C-BC4B-4901-AF70-48B54A5C20F7}/' \
       -e 's/{4F980AC7-863E-4C5E-A3BF-138F9DEDDB8A}/{AE963DBC-EE5D-4FCB-AAFD-37FED2302241}/' \
       -e 's/{92C89C36-0E86-45E1-B3D8-0D6C91108F30}/{8E6CDFDE-39B9-43D9-97B3-2440C012845C}/' \
       -e 's/{730C595B-DBA6-48D7-94B8-A98780AC92B6}/{839D3C90-B578-41E2-A004-431440F9E899}/' \
    *.vdproj


