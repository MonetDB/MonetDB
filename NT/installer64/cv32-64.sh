# script to convert a *.vdproj for 32 bits to one for 64 bits.
# this script is mostly to document what I did, not for regular use.

cp ../installer32/*.{vdproj,sln} .
sed -i -e '/TargetPlatform/s/3:0/3:1/' \
       -e 's/win32/win64/' \
       -e 's/\[ProgramFilesFolder\]/[ProgramFiles64Folder]/' \
       -e 's/C:\\\\Program Files\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC90_CRT_x86.msm/C:\\\\Program Files (x86)\\\\Common Files\\\\Merge Modules\\\\Microsoft_VC90_CRT_x86_x64.msm/' \
       -e 's/C:\\\\Program Files\\\\Intel\\\\Compiler\\\\11.1\\\\046\\\\bin\\\\ia32\\\\libmmd.dll/C:\\\\Program Files (x86)\\\\Intel\\\\Compiler\\\\11.1\\\\046\\\\bin\\\\intel64\\\\libmmd.dll/' \
    *.vdproj
