# python mksqlwxs.py VERSION makedefs.txt PREFIX > PREFIX/MonetDB5-SQL-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\candle.exe" -nologo -arch x64/x86 PREFIX/MonetDB5-SQL-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\light.exe" -nologo -sice:ICE03 -sice:ICE60 -sice:ICE82 -ext WixUIExtension PREFIX/MonetDB5-SQL-Installer.wixobj

import sys, os

# doesn't change
upgradecode = {'x64': '{839D3C90-B578-41E2-A004-431440F9E899}',
               'x86': '{730C595B-DBA6-48D7-94B8-A98780AC92B6}'}

def comp(features, id, depth, files, name=None, args=None, sid=None, vital=None):
    indent = ' ' * depth
    for f in files:
        print('%s<Component Id="_%d" Guid="*">' % (indent, id))
        print('%s  <File DiskId="1" KeyPath="yes" Name="%s" Source="%s"%s%s' % (indent, f.split('\\')[-1], f, vital and (' Vital="%s"' % vital) or '', name and '>' or '/>'))
        if name:
            print('%s    <Shortcut Id="%s" Advertise="yes"%s Directory="ProgramMenuDir" Icon="monetdb.ico" IconIndex="0" Name="%s" WorkingDirectory="INSTALLDIR"/>' % (indent, sid, args and (' Arguments="%s"' % args) or '', name))
            print('%s  </File>' % indent)
        print('%s</Component>' % indent)
        features.append('_%d' % id)
        id += 1
    return id

def main():
    if len(sys.argv) != 4:
        print(r'Usage: mksqlwxs.py version makedefs.txt installdir')
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
    print(r'  <Product Id="*" Language="1033" Manufacturer="MonetDB" Name="MonetDB5" UpgradeCode="%s" Version="%s">' % (upgradecode[arch], sys.argv[1]))
    print(r'    <Package Id="*" Comments="MonetDB5/SQL Server and Client" Compressed="yes" InstallerVersion="301" Keywords="MonetDB5 MonetDB SQL Database" Languages="1033" Manufacturer="MonetDB BV" Platform="%s"/>' % arch)
    print(r'    <MajorUpgrade AllowDowngrades="no" DowngradeErrorMessage="A later version of [ProductName] is already installed." AllowSameVersionUpgrades="no"/>')
    print(r'    <WixVariable Id="WixUILicenseRtf" Value="license.rtf"/>')
    print(r'    <WixVariable Id="WixUIBannerBmp" Value="banner.bmp"/>')
    # print(r'    <WixVariable Id="WixUIDialogBmp" Value="backgroundRipple.bmp"/>')
    print(r'    <Property Id="INSTALLDIR">')
    print(r'      <RegistrySearch Id="MonetDBRegistry" Key="Software\[Manufacturer]\[ProductName]" Name="InstallPath" Root="HKLM" Type="raw"/>')
    print(r'    </Property>')
    print(r'    <Property Id="WIXUI_INSTALLDIR" Value="INSTALLDIR"/>')
    print(r'    <Media Id="1" Cabinet="monetdb.cab" EmbedCab="yes"/>')
    print(r'    <Directory Id="TARGETDIR" Name="SourceDir">')
    print(r'      <Merge Id="VCRedist" DiskId="1" Language="0" SourceFile="C:\Program Files (x86)\Common Files\Merge Modules\Microsoft_VC%s0_CRT_%s.msm"/>' % (vs, arch))
    print(r'      <Directory Id="%s">' % folder)
    print(r'        <Directory Id="ProgramFilesMonetDB" Name="MonetDB">')
    print(r'          <Directory Id="INSTALLDIR" Name="MonetDB5">')
    print(r'            <Component Id="registry">')
    print(r'              <RegistryKey Key="Software\[Manufacturer]\[ProductName]" Root="HKLM">')
    print(r'                <RegistryValue Name="InstallPath" Type="string" Value="[INSTALLDIR]"/>')
    print(r'              </RegistryKey>')
    print(r'            </Component>')
    features.append('registry')
    id = 1
    print(r'            <Directory Id="bin" Name="bin">')
    id = comp(features, id, 14,
              [r'bin\mclient.exe',
               r'bin\mserver5.exe',
               r'bin\msqldump.exe',
               r'bin\stethoscope.exe',
               r'lib\libbat.dll',
               r'lib\libmapi.dll',
               r'lib\libmonetdb5.dll',
               r'lib\libstream.dll',
               r'%s\bin\iconv.dll' % makedefs['LIBICONV'],
               r'%s\bin\libbz2.dll' % makedefs['LIBBZIP2'],
               r'%s\bin\libeay32.dll' % makedefs['LIBOPENSSL'],
               r'%s\bin\libxml2.dll' % makedefs['LIBXML2'],
               r'%s\bin\pcre.dll' % makedefs['LIBPCRE'],
               r'%s\bin\ssleay32.dll' % makedefs['LIBOPENSSL'],
               r'%s\bin\zlib1.dll' % makedefs['LIBZLIB']])
    print(r'            </Directory>')
    print(r'            <Directory Id="etc" Name="etc">')
    id = comp(features, id, 14, [r'etc\.monetdb'])
    print(r'            </Directory>')
    print(r'            <Directory Id="include" Name="include">')
    print(r'              <Directory Id="monetdb" Name="monetdb">')
    id = comp(features, id, 16,
              [r'include\monetdb\mapi.h',
               r'include\monetdb\stream.h',
               r'include\monetdb\stream_socket.h'],
              vital = 'no')
    print(r'              </Directory>')
    print(r'            </Directory>')
    print(r'            <Directory Id="lib" Name="lib">')
    print(r'              <Directory Id="monetdb5" Name="monetdb5">')
    print(r'                <Directory Id="autoload" Name="autoload">')
    id = comp(features, id, 18,
              [r'lib\monetdb5\autoload\%s' % x for x in sorted(filter(lambda x: x.endswith('.mal') and ('geom' not in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5', 'autoload'))))])
    print(r'                </Directory>')
    print(r'                <Directory Id="createdb" Name="createdb">')
    id = comp(features, id, 18,
              [r'lib\monetdb5\createdb\%s' % x for x in sorted(filter(lambda x: x.endswith('.sql') and ('geom' not in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5', 'createdb'))))])
    print(r'                </Directory>')
    id = comp(features, id, 16,
              [r'lib\monetdb5\%s' % x for x in sorted(filter(lambda x: x.endswith('.mal') and ('geom' not in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5'))))])
    id = comp(features, id, 16,
              [r'lib\monetdb5\%s' % x for x in sorted(filter(lambda x: x.startswith('lib_') and x.endswith('.dll') and ('geom' not in x), os.listdir(os.path.join(sys.argv[3], 'lib', 'monetdb5'))))])
    print(r'              </Directory>')
    print(r'            </Directory>')
    print(r'            <Directory Id="share" Name="share">')
    print(r'              <Directory Id="doc" Name="doc">')
    print(r'                <Directory Id="MonetDB_SQL" Name="MonetDB-SQL">')
    id = comp(features, id, 18, [r'share\doc\MonetDB-SQL\dump-restore.html',
                                 r'share\doc\MonetDB-SQL\dump-restore.txt'],
              vital = 'no')
    id = comp(features, id, 18,
              [r'website.html'],
              name = 'MonetDB Web Site',
              sid = 'website_html',
              vital = 'no')
    print(r'                </Directory>')
    print(r'              </Directory>')
    print(r'            </Directory>')
    id = comp(features, id, 12,
              [r'license.rtf',
               r'M5server.bat',
               r'msqldump.bat',
               r'pyapi_locatepython.bat',
               r'stethoscope.bat'])
    id = comp(features, id, 12,
              [r'mclient.bat'],
              name = 'MonetDB SQL Client',
              args = '/STARTED-FROM-MENU -lsql -Ecp437',
              sid = 'mclient_bat')
    id = comp(features, id, 12,
              [r'MSQLServer.bat'],
              name = 'MonetDB SQL Server',
              sid = 'msqlserver_bat')
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
    print(r'    <Feature Id="Complete" ConfigurableDirectory="INSTALLDIR" Title="MonetDB/SQL">')
    for f in features:
        print(r'      <ComponentRef Id="%s"/>' % f)
    print(r'      <MergeRef Id="VCRedist"/>')
    print(r'    </Feature>')
    print(r'    <UIRef Id="WixUI_InstallDir"/>')
    print(r'    <UIRef Id="WixUI_ErrorProgressText"/>')
    print(r'    <Icon Id="monetdb.ico" SourceFile="monetdb.ico"/>')
    print(r'  </Product>')
    print(r'</Wix>')

main()
