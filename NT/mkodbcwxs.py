# python mkodbcwxs.py VERSION makedefs.txt PREFIX > PREFIX/MonetDB-ODBC-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\candle.exe" -nologo -arch x64/x86 PREFIX/MonetDB-ODBC-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\light.exe" -nologo -sice:ICE03 -sice:ICE60 -sice:ICE82 -ext WixUIExtension PREFIX/MonetDB-ODBC-Installer.wixobj

import sys, os

# doesn't change
upgradecode = {'x64': '{95ACBC8C-BC4B-4901-AF70-48B54A5C20F7}',
               'x86': '{C1F69378-3F5C-4120-8224-32F07D3458F3}'}

def comp(features, id, depth, files, fid=None, name=None, args=None, sid=None, vital=None):
    indent = ' ' * depth
    for f in files:
        print('%s<Component Id="_%d" Guid="*">' % (indent, id))
        print('%s  <File DiskId="1"%s KeyPath="yes" Name="%s" Source="%s"%s%s' % (indent, fid and (' Id="%s"' % fid) or '', f.split('\\')[-1], f, vital and (' Vital="%s"' % vital) or '', name and '>' or '/>'))
        if name:
            print('%s    <Shortcut Id="%s" Advertise="yes"%s Directory="ProgramMenuDir" Icon="monetdb.ico" IconIndex="0" Name="%s" WorkingDirectory="INSTALLDIR"/>' % (indent, sid, args and (' Arguments="%s"' % args) or '', name))
            print('%s  </File>' % indent)
        print('%s</Component>' % indent)
        features.append('_%d' % id)
        id += 1
    return id

def main():
    if len(sys.argv) != 4:
        print(r'Usage: mkodbcwxs.py version makedefs.txt installdir')
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
    features = []
    print(r'<?xml version="1.0"?>')
    print(r'<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">')
    print(r'  <Product Id="*" Language="1033" Manufacturer="MonetDB" Name="MonetDB ODBC Driver" UpgradeCode="%s" Version="%s">' % (upgradecode[arch], sys.argv[1]))
    print(r'    <Package Id="*" Comments="MonetDB ODBC Driver" Compressed="yes" InstallerVersion="301" Keywords="MonetDB SQL ODBC" Languages="1033" Manufacturer="MonetDB BV" Platform="%s"/>' % arch)
    print(r'    <MajorUpgrade AllowDowngrades="no" DowngradeErrorMessage="A later version of [ProductName] is already installed." AllowSameVersionUpgrades="no"/>')
    print(r'    <WixVariable Id="WixUILicenseRtf" Value="license.rtf"/>')
    print(r'    <WixVariable Id="WixUIBannerBmp" Value="banner.bmp"/>')
    # print(r'    <WixVariable Id="WixUIDialogBmp" Value="backgroundRipple.bmp"/>')
    print(r'    <Property Id="WIXUI_INSTALLDIR" Value="INSTALLDIR"/>')
    print(r'    <Media Id="1" Cabinet="monetdb.cab" EmbedCab="yes"/>')
    print(r'    <CustomAction Id="driverinstall" FileKey="odbcinstall" ExeCommand="/Install" Execute="deferred" Impersonate="no"/>')
    print(r'    <CustomAction Id="driveruninstall" FileKey="odbcinstall" ExeCommand="/Uninstall" Execute="deferred" Impersonate="no"/>')
    print(r'    <Directory Id="TARGETDIR" Name="SourceDir">')
    print(r'      <Merge Id="VCRedist" DiskId="1" Language="0" SourceFile="C:\Program Files (x86)\Common Files\Merge Modules\Microsoft_VC140_CRT_%s.msm"/>' % arch)
    print(r'      <Directory Id="%s">' % folder)
    print(r'        <Directory Id="ProgramFilesMonetDB" Name="MonetDB">')
    print(r'          <Directory Id="INSTALLDIR" Name="MonetDB ODBC Driver">')
    id = 1
    print(r'            <Directory Id="lib" Name="lib">')
    id = comp(features, id, 14,
              [r'lib\libmapi.dll',
               r'lib\libMonetODBC.dll',
               r'lib\libMonetODBCs.dll',
               r'lib\libstream.dll',
               r'%s\bin\iconv.dll' % makedefs['LIBICONV'],
               r'%s\bin\libbz2.dll' % makedefs['LIBBZIP2'],
               r'%s\bin\libeay32.dll' % makedefs['LIBOPENSSL'],
               r'%s\bin\ssleay32.dll' % makedefs['LIBOPENSSL'],
               r'%s\bin\zlib1.dll' % makedefs['LIBZLIB']])
    print(r'            </Directory>')
    id = comp(features, id, 12,
              [r'license.rtf'])
    id = comp(features, id, 12,
              [r'bin\odbcinstall.exe'],
              fid = 'odbcinstall')
    id = comp(features, id, 12,
              [r'website.html'],
              name = 'MonetDB Web Site',
              sid = 'website_html',
              vital = 'no')
    print(r'          </Directory>')
    print(r'        </Directory>')
    print(r'      </Directory>')
    print(r'      <Directory Id="ProgramMenuFolder" Name="Programs">')
    print(r'        <Directory Id="ProgramMenuDir" Name="MonetDB">')
    print(r'          <Component Id="ProgramMenuDir" Guid="*">')
    features.append('ProgramMenuDir')
    print(r'            <RemoveFolder Id="ProgramMenuDir" On="uninstall"/>')
    print(r'            <RegistryValue Key="Software\[Manufacturer]\[ProductName]" KeyPath="yes" Root="HKCU" Type="string" Value=""/>')
    print(r'          </Component>')
    print(r'        </Directory>')
    print(r'      </Directory>')
    print(r'    </Directory>')
    print(r'    <Feature Id="Complete" ConfigurableDirectory="INSTALLDIR" Title="MonetDB ODBC Driver">')
    for f in features:
        print(r'      <ComponentRef Id="%s"/>' % f)
    print(r'      <MergeRef Id="VCRedist"/>')
    print(r'    </Feature>')
    print(r'    <UIRef Id="WixUI_InstallDir"/>')
    print(r'    <UIRef Id="WixUI_ErrorProgressText"/>')
    print(r'    <Icon Id="monetdb.ico" SourceFile="monetdb.ico"/>')
    print(r'    <InstallExecuteSequence>')
    print(r'      <Custom Action="driverinstall" Before="RegisterUser">')
    print(r'        NOT Installed OR REINSTALL')
    print(r'      </Custom>')
    print(r'      <Custom Action="driveruninstall" Before="UnpublishComponents">')
    print(r'        Installed AND (REINSTALL OR (REMOVE AND NOT UPGRADINGPRODUCTCODE))')
    print(r'      </Custom>')
    print(r'    </InstallExecuteSequence>')
    print(r'  </Product>')
    print(r'</Wix>')

main()
