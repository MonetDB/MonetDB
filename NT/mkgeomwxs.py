# python mkgeomwxs.py VERSION makedefs.txt PREFIX > PREFIX/MonetDB5-Geom-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\candle.exe" -nologo -arch x64/x86 PREFIX/MonetDB5-Geom-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\light.exe" -nologo -sice:ICE03 -sice:ICE60 -sice:ICE82 -ext WixUIExtension PREFIX/MonetDB5-Geom-Installer.wixobj

import sys, os

# doesn't change
upgradecode = {'x64': '{8E6CDFDE-39B9-43D9-97B3-2440C012845C}',
               'x86': '{92C89C36-0E86-45E1-B3D8-0D6C91108F30}'}

def comp(features, id, depth, files):
    indent = ' ' * depth
    for f in files:
        print('%s<Component Id="_%d" Guid="*">' % (indent, id))
        print('%s  <File DiskId="1" KeyPath="yes" Name="%s" Source="%s"/>' % (indent, f.split('\\')[-1], f))
        print('%s</Component>' % indent)
        features.append('_%d' % id)
        id += 1
    return id

def main():
    if len(sys.argv) != 4:
        print(r'Usage: mkgeomwxs.py version makedefs.txt installdir')
        return 1
    makedefs = {}
    for line in open(sys.argv[2]):
        key, val = line.strip().split('=', 1)
        makedefs[key] = val
    if makedefs['bits'] == '64':
        folder = r'ProgramFiles64Folder'
        arch = 'x64'
    else:
        folder = r'ProgramFilesFolder'
        arch = 'x86'
    vs = os.getenv('vs')        # inherited from TestTools\common.bat
    features = []
    print(r'<?xml version="1.0"?>')
    print(r'<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">')
    print(r'  <Product Id="*" Language="1033" Manufacturer="MonetDB" Name="MonetDB5 SQL GIS Module" UpgradeCode="%s" Version="%s">' % (upgradecode[arch], sys.argv[1]))
    print(r'    <Package Id="*" Comments="MonetDB5/SQL/GIS Module" Compressed="yes" InstallerVersion="301" Keywords="MonetDB5 MonetDB SQL GIS Database" Languages="1033" Manufacturer="MonetDB BV" Platform="%s"/>' % arch)
    print(r'    <MajorUpgrade AllowDowngrades="no" DowngradeErrorMessage="A later version of [ProductName] is already installed." AllowSameVersionUpgrades="no"/>')
    print(r'    <WixVariable Id="WixUILicenseRtf" Value="license.rtf"/>')
    print(r'    <WixVariable Id="WixUIBannerBmp" Value="banner.bmp"/>')
    # print(r'    <WixVariable Id="WixUIDialogBmp" Value="backgroundRipple.bmp"/>')
    print(r'    <Property Id="INSTALLDIR">')
    print(r'      <RegistrySearch Id="MonetDBRegistry" Key="Software\[Manufacturer]\MonetDB5" Name="InstallPath" Root="HKLM" Type="raw"/>')
    print(r'    </Property>')
    print(r'    <Property Id="WIXUI_INSTALLDIR" Value="INSTALLDIR"/>')
    print(r'    <Property Id="ARPPRODUCTICON" Value="monetdb.ico"/>')
    print(r'    <Media Id="1" Cabinet="monetdb.cab" EmbedCab="yes"/>')
    print(r'    <Condition Message="MonetDB5-SQL needs to be installed first.">')
    print(r'      INSTALLDIR')
    print(r'    </Condition>')
    print(r'    <Directory Id="TARGETDIR" Name="SourceDir">')
    print(r'      <Merge Id="VCRedist" DiskId="1" Language="0" SourceFile="C:\Program Files (x86)\Common Files\Merge Modules\Microsoft_VC%s0_CRT_%s.msm"/>' % (vs, arch))
    print(r'      <Directory Id="%s">' % folder)
    print(r'        <Directory Id="ProgramFilesMonetDB" Name="MonetDB">')
    print(r'          <Directory Id="INSTALLDIR" Name="MonetDB5">')
    id = 1
    print(r'            <Directory Id="bin" Name="bin">')
    id = comp(features, id, 14,
              [r'%s\bin\geos_c.dll' % makedefs['LIBGEOS']])
    print(r'            </Directory>')
    print(r'            <Directory Id="lib" Name="lib">')
    print(r'              <Directory Id="monetdb5" Name="monetdb5">')
    print(r'                <Directory Id="autoload" Name="autoload">')
    id = comp(features, id, 18,
              [r'lib\monetdb5\autoload\%s' % x for x in sorted(filter(lambda x: x.endswith('.mal') and ('geom' in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5', 'autoload'))))])
    print(r'                </Directory>')
    print(r'                <Directory Id="createdb" Name="createdb">')
    id = comp(features, id, 18,
              [r'lib\monetdb5\createdb\%s' % x for x in sorted(filter(lambda x: x.endswith('.sql') and ('geom' in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5', 'createdb'))))])
    print(r'                </Directory>')
    id = comp(features, id, 16,
              [r'lib\monetdb5\%s' % x for x in sorted(filter(lambda x: x.endswith('.mal') and ('geom' in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5'))))])
    id = comp(features, id, 16,
              [r'lib\monetdb5\%s' % x for x in sorted(filter(lambda x: x.startswith('lib_') and x.endswith('.dll') and ('geom' in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5'))))])
    print(r'              </Directory>')
    print(r'            </Directory>')
    print(r'          </Directory>')
    print(r'        </Directory>')
    print(r'      </Directory>')
    print(r'    </Directory>')
    print(r'    <Feature Id="Complete" ConfigurableDirectory="INSTALLDIR" Title="MonetDB/SQL">')
    for f in features:
        print(r'      <ComponentRef Id="%s"/>' % f)
    print(r'      <MergeRef Id="VCRedist"/>')
    print(r'    </Feature>')
    # the <UI> section was lifted from
    # ...\SDK\wixui\WixUI_InstallDir.wxs and modified to remove the
    # InstallDirDlg subsection
    # see http://wixtoolset.org/documentation/manual/v3/wixui/wixui_customizations.html
    print(r'    <UI Id="MyWixUI_InstallDir">')
    print(r'      <TextStyle Id="WixUI_Font_Normal" FaceName="Tahoma" Size="8"/>')
    print(r'      <TextStyle Id="WixUI_Font_Bigger" FaceName="Tahoma" Size="12"/>')
    print(r'      <TextStyle Id="WixUI_Font_Title" FaceName="Tahoma" Size="9" Bold="yes"/>')
    print(r'      <Property Id="DefaultUIFont" Value="WixUI_Font_Normal"/>')
    print(r'      <Property Id="WixUI_Mode" Value="InstallDir"/>')
    print(r'      <DialogRef Id="BrowseDlg"/>')
    print(r'      <DialogRef Id="DiskCostDlg"/>')
    print(r'      <DialogRef Id="ErrorDlg"/>')
    print(r'      <DialogRef Id="FatalError"/>')
    print(r'      <DialogRef Id="FilesInUse"/>')
    print(r'      <DialogRef Id="MsiRMFilesInUse"/>')
    print(r'      <DialogRef Id="PrepareDlg"/>')
    print(r'      <DialogRef Id="ProgressDlg"/>')
    print(r'      <DialogRef Id="ResumeDlg"/>')
    print(r'      <DialogRef Id="UserExit"/>')
    print(r'      <Publish Dialog="BrowseDlg" Control="OK" Event="DoAction" Value="WixUIValidatePath" Order="3">1</Publish>')
    print(r'      <Publish Dialog="BrowseDlg" Control="OK" Event="SpawnDialog" Value="InvalidDirDlg" Order="4"><![CDATA[NOT WIXUI_DONTVALIDATEPATH AND WIXUI_INSTALLDIR_VALID<>"1"]]></Publish>')
    print(r'      <Publish Dialog="ExitDialog" Control="Finish" Event="EndDialog" Value="Return" Order="999">1</Publish>')
    print(r'      <Publish Dialog="WelcomeDlg" Control="Next" Event="NewDialog" Value="LicenseAgreementDlg">NOT Installed</Publish>')
    print(r'      <Publish Dialog="WelcomeDlg" Control="Next" Event="NewDialog" Value="VerifyReadyDlg">Installed AND PATCH</Publish>')
    print(r'      <Publish Dialog="LicenseAgreementDlg" Control="Back" Event="NewDialog" Value="WelcomeDlg">1</Publish>')
    print(r'      <Publish Dialog="LicenseAgreementDlg" Control="Next" Event="NewDialog" Value="VerifyReadyDlg">LicenseAccepted = "1"</Publish>')
    print(r'      <Publish Dialog="VerifyReadyDlg" Control="Back" Event="NewDialog" Value="LicenseAgreementDlg" Order="1">NOT Installed</Publish>')
    print(r'      <Publish Dialog="VerifyReadyDlg" Control="Back" Event="NewDialog" Value="MaintenanceTypeDlg" Order="2">Installed AND NOT PATCH</Publish>')
    print(r'      <Publish Dialog="VerifyReadyDlg" Control="Back" Event="NewDialog" Value="WelcomeDlg" Order="2">Installed AND PATCH</Publish>')
    print(r'      <Publish Dialog="MaintenanceWelcomeDlg" Control="Next" Event="NewDialog" Value="MaintenanceTypeDlg">1</Publish>')
    print(r'      <Publish Dialog="MaintenanceTypeDlg" Control="RepairButton" Event="NewDialog" Value="VerifyReadyDlg">1</Publish>')
    print(r'      <Publish Dialog="MaintenanceTypeDlg" Control="RemoveButton" Event="NewDialog" Value="VerifyReadyDlg">1</Publish>')
    print(r'      <Publish Dialog="MaintenanceTypeDlg" Control="Back" Event="NewDialog" Value="MaintenanceWelcomeDlg">1</Publish>')
    print(r'      <Property Id="ARPNOMODIFY" Value="1"/>')
    print(r'    </UI>')
    print(r'    <UIRef Id="WixUI_Common"/>')
    print(r'    <UIRef Id="MyWixUI_InstallDir"/>')
    print(r'    <UIRef Id="WixUI_ErrorProgressText"/>')
    print(r'    <Icon Id="monetdb.ico" SourceFile="monetdb.ico"/>')
    print(r'  </Product>')
    print(r'</Wix>')

main()
