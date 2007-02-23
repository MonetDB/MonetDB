To create an installer for 64 bit Windows from the checked in 32-bit
installer do the following:

Load the solution file and convert it to VS2005.

Select MonetDB4-Installer and change the following properties:
- InstallAllUsers -> True
- TargetPlatform -> x64

From the file list, remove VC_User_CRT71_RTL_X86_---.msm
Add merge module Microsoft_VC80_CRT_x86_x64.msm

Remove PHP files.
