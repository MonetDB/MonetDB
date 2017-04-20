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
    print(r'    <Media Id="1" Cabinet="monetdb.cab" EmbedCab="yes"/>')
    print(r'    <Directory Id="TARGETDIR" Name="SourceDir">')
    print(r'      <Merge Id="VCRedist" DiskId="1" Language="0" SourceFile="C:\Program Files (x86)\Common Files\Merge Modules\Microsoft_VC140_CRT_%s.msm"/>' % arch)
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
    print(r'    <UIRef Id="WixUI_InstallDir"/>')
    print(r'    <UIRef Id="WixUI_ErrorProgressText"/>')
    print(r'  </Product>')
    print(r'</Wix>')

main()
