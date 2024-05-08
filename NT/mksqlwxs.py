# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2024 MonetDB Foundation;
# Copyright August 2008 - 2023 MonetDB B.V.;
# Copyright 1997 - July 2008 CWI.

# python mksqlwxs.py VERSION BITS PREFIX > PREFIX/MonetDB5-SQL-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\candle.exe" -nologo -arch x64/x86 PREFIX/MonetDB5-SQL-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\light.exe" -nologo -sice:ICE03 -sice:ICE60 -sice:ICE82 -ext WixUIExtension PREFIX/MonetDB5-SQL-Installer.wixobj

import sys, os

# doesn't change
upgradecode = {
    'x64': '{839D3C90-B578-41E2-A004-431440F9E899}',
    'x86': '{730C595B-DBA6-48D7-94B8-A98780AC92B6}'
}
# the Geom upgrade codes that we are replacing
geomupgradecode = {
    'x64': '{8E6CDFDE-39B9-43D9-97B3-2440C012845C}',
    'x86': '{92C89C36-0E86-45E1-B3D8-0D6C91108F30}'
}

def comp(features, id, depth, files, name=None, args=None, sid=None, vital=None):
    indent = ' ' * depth
    for f in files:
        print('{}<Component Id="_{}" Guid="*">'.format(indent, id))
        print('{}  <File DiskId="1" KeyPath="yes" Name="{}" Source="{}"{}{}'.format(indent, f.split('\\')[-1], f, vital and (' Vital="{}"'.format(vital)) or '', name and '>' or '/>'))
        if name:
            print('{}    <Shortcut Id="{}" Advertise="yes"{} Directory="ProgramMenuDir" Icon="monetdb.ico" IconIndex="0" Name="{}" WorkingDirectory="INSTALLDIR"/>'.format(indent, sid, args and (' Arguments="{}"'.format(args)) or '', name))
            print('{}  </File>'.format(indent))
        print('{}</Component>'.format(indent))
        features.append('_{}'.format(id))
        id += 1
    return id

def main():
    if len(sys.argv) != 4:
        print(r'Usage: mksqlwxs.py version bits installdir')
        return 1
    version = sys.argv[1]
    if sys.argv[2] == '64':
        folder = r'ProgramFiles64Folder'
        arch = 'x64'
        libcrypto = '-x64'
        vcpkg = r'C:\vcpkg\installed\x64-windows\{}'
    else:
        folder = r'ProgramFilesFolder'
        arch = 'x86'
        libcrypto = ''
        vcpkg = r'C:\vcpkg\installed\x86-windows\{}'
    vcdir = os.getenv('VCINSTALLDIR')
    if vcdir is None:
        vsdir = os.getenv('VSINSTALLDIR')
        if vsdir is not None:
            vcdir = os.path.join(vsdir, 'VC')
    if vcdir is None:
        if os.path.exists(r'C:\Program Files\Microsoft Visual Studio\2022\Community\VC'):
            vcdir = r'C:\Program Files\Microsoft Visual Studio\2022\Community\VC'
        elif os.path.exists(r'C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC'):
            vcdir = r'C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC'
        elif os.path.exists(r'C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC'):
            vcdir = r'C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC'
        else:
            print(r"Don't know which visual studio directory to use")
            return 1
    msvc = os.path.join(vcdir, r'Redist\MSVC')
    features = []
    extend = []
    debug = []
    geom = []
    pyapi3 = []
    print(r'<?xml version="1.0"?>')
    print(r'<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">')
    print(r'  <Product Id="*" Language="1033" Manufacturer="MonetDB" Name="MonetDB5" UpgradeCode="{}" Version="{}">'.format(upgradecode[arch], version))
    print(r'    <Package Id="*" Comments="MonetDB5/SQL Server and Client" Compressed="yes" InstallerVersion="301" Keywords="MonetDB5 MonetDB SQL Database" Languages="1033" Manufacturer="MonetDB Foundation" Platform="{}"/>'.format(arch))
    print(r'    <Upgrade Id="{}">'.format(geomupgradecode[arch]))
    # up to and including 11.29.3, the geom module can not be
    # uninstalled if MonetDB/SQL is not installed; this somehow also
    # precludes the upgrade to this version
    print(r'      <UpgradeVersion OnlyDetect="no" Minimum="11.29.3" IncludeMinimum="no" Maximum="{}" Property="GEOMINSTALLED"/>'.format(version))
    print(r'    </Upgrade>')
    print(r'    <MajorUpgrade AllowDowngrades="no" DowngradeErrorMessage="A later version of [ProductName] is already installed." AllowSameVersionUpgrades="no"/>')
    print(r'    <WixVariable Id="WixUILicenseRtf" Value="share\license.rtf"/>')
    print(r'    <WixVariable Id="WixUIBannerBmp" Value="share\banner.bmp"/>')
    # print(r'    <WixVariable Id="WixUIDialogBmp" Value="backgroundRipple.bmp"/>')
    print(r'    <Property Id="INSTALLDIR">')
    print(r'      <RegistrySearch Id="MonetDBRegistry" Key="Software\[Manufacturer]\[ProductName]" Name="InstallPath" Root="HKLM" Type="raw"/>')
    print(r'    </Property>')
    print(r'    <Property Id="DEBUGEXISTS">')
    print(r'      <DirectorySearch Id="CheckFileDir1" Path="[INSTALLDIR]\bin" Depth="0">')
    print(r'        <FileSearch Id="CheckFile1" Name="mserver5.pdb"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    print(r'    <Property Id="INCLUDEEXISTS">')
    print(r'      <DirectorySearch Id="CheckFileDir2" Path="[INSTALLDIR]\include\monetdb" Depth="0">')
    print(r'        <FileSearch Id="CheckFile2" Name="gdk.h"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    print(r'    <Property Id="GEOMMALEXISTS">')
    print(rf'      <DirectorySearch Id="CheckFileDir3" Path="[INSTALLDIR]\lib\monetdb5{version}" Depth="0">')
    print(r'        <FileSearch Id="CheckFile3" Name="geom.mal"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    print(r'    <Property Id="GEOMLIBEXISTS">')
    print(rf'      <DirectorySearch Id="CheckFileDir4" Path="[INSTALLDIR]\lib\monetdb5{version}" Depth="0">')
    print(r'        <FileSearch Id="CheckFile4" Name="_geom.dll"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    print(r'    <Property Id="PYAPI3EXISTS">')
    print(r'      <DirectorySearch Id="CheckFileDir5" Path="[INSTALLDIR]" Depth="0">')
    print(r'        <FileSearch Id="CheckFile5" Name="pyapi_locatepython3.bat"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    # up to and including 11.29.3, the geom module can not be
    # uninstalled if MonetDB/SQL is not installed; this somehow also
    # precludes the upgrade to this version, therefore we disallow
    # running the current installer
    print(r'    <Property Id="OLDGEOMINSTALLED">')
    print(r'      <ProductSearch UpgradeCode="{}" Minimum="11.1.1" Maximum="11.29.3" IncludeMinimum="yes" IncludeMaximum="yes"/>'.format(geomupgradecode[arch]))
    print(r'    </Property>')
    print(r'    <Condition Message="Please uninstall MonetDB5 SQL GIS Module first, then rerun and select to install Complete package.">')
    print(r'      NOT OLDGEOMINSTALLED')
    print(r'    </Condition>')
    print(r'    <Property Id="ApplicationFolderName" Value="MonetDB"/>')
    print(r'    <Property Id="WixAppFolder" Value="WixPerMachineFolder"/>')
    print(r'    <Property Id="WIXUI_INSTALLDIR" Value="INSTALLDIR"/>')
    print(r'    <Property Id="ARPPRODUCTICON" Value="share\monetdb.ico"/>')
    print(r'    <Media Id="1" Cabinet="monetdb.cab" EmbedCab="yes"/>')
    print(r'    <Directory Id="TARGETDIR" Name="SourceDir">')
    d = sorted(os.listdir(msvc))[-1]
    msm = '_CRT_{}.msm'.format(arch)
    for f in sorted(os.listdir(os.path.join(msvc, d, 'MergeModules'))):
        if msm in f:
            fn = f
    print(r'      <Merge Id="VCRedist" DiskId="1" Language="0" SourceFile="{}\{}\MergeModules\{}"/>'.format(msvc, d, fn))
    print(r'      <Directory Id="{}">'.format(folder))
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
               rf'bin\bat{version}.dll',
               rf'bin\mapi{version}.dll',
               rf'bin\monetdb5{version}.dll',
               r'bin\monetdbe.dll',
               rf'bin\monetdbsql{version}.dll',
               rf'bin\stream{version}.dll',
               vcpkg.format(r'bin\iconv-2.dll'),
               vcpkg.format(r'bin\bz2.dll'),
               vcpkg.format(r'bin\charset-1.dll'), # for iconv-2.dll
               vcpkg.format(r'bin\getopt.dll'),
               vcpkg.format(r'bin\libcrypto-3{}.dll'.format(libcrypto)),
               vcpkg.format(r'bin\libssl-3{}.dll'.format(libcrypto)),
               vcpkg.format(r'bin\libxml2.dll'),
               vcpkg.format(r'bin\lz4.dll'),
               vcpkg.format(r'bin\liblzma.dll'),
               vcpkg.format(r'bin\pcre.dll'),
               vcpkg.format(r'bin\zlib1.dll')])
    id = comp(debug, id, 14,
              [r'bin\mclient.pdb',
               r'bin\mserver5.pdb',
               r'bin\msqldump.pdb',
               rf'lib\bat{version}.pdb',
               rf'lib\mapi{version}.pdb',
               rf'lib\monetdb5{version}.pdb',
               rf'lib\monetdbsql{version}.pdb',
               rf'lib\stream{version}.pdb'])
    id = comp(geom, id, 14,
              [vcpkg.format(r'bin\geos_c.dll'),
               vcpkg.format(r'bin\geos.dll')])
    print(r'            </Directory>')
    print(r'            <Directory Id="etc" Name="etc">')
    id = comp(features, id, 14, [r'etc\.monetdb'])
    print(r'            </Directory>')
    print(r'            <Directory Id="include" Name="include">')
    print(r'              <Directory Id="monetdb" Name="monetdb">')
    id = comp(extend, id, 16,
              sorted([r'include\monetdb\{}'.format(x) for x in filter(lambda x: (x.startswith('gdk') or x.startswith('monet') or x.startswith('mal') or x.startswith('sql') or x.startswith('rel') or x.startswith('store') or x.startswith('opt_backend')) and x.endswith('.h'), os.listdir(os.path.join(sys.argv[3], 'include', 'monetdb')))] +
                     [r'include\monetdb\copybinary.h',
                      r'include\monetdb\mapi.h',
                      r'include\monetdb\mapi_querytype.h',
                      r'include\monetdb\msettings.h',
                      r'include\monetdb\matomic.h',
                      r'include\monetdb\mel.h',
                      r'include\monetdb\mstring.h',
                      r'include\monetdb\stream.h',
                      r'include\monetdb\stream_socket.h']),
              vital = 'no')
    print(r'              </Directory>')
    print(r'            </Directory>')
    print(r'            <Directory Id="lib" Name="lib">')
    print(r'              <Directory Id="monetdb5" Name="monetdb5">')
    id = comp(features, id, 16,
              [rf'lib\monetdb5{version}\{x}' for x in sorted(filter(lambda x: x.startswith('_') and x.endswith('.dll') and ('geom' not in x) and ('pyapi' not in x) and ('opt_sql_append' not in x) and ('microbenchmark' not in x) and ('udf' not in x), os.listdir(os.path.join(sys.argv[3], 'lib', f'monetdb5{version}'))))])
    id = comp(debug, id, 16,
              [rf'lib\monetdb5{version}\{x}' for x in sorted(filter(lambda x: x.startswith('_') and x.endswith('.pdb') and ('geom' not in x) and ('opt_sql_append' not in x) and ('microbenchmark' not in x) and ('udf' not in x), os.listdir(os.path.join(sys.argv[3], 'lib', f'monetdb5{version}'))))])
    id = comp(geom, id, 16,
              [rf'lib\monetdb5{version}\{x}' for x in sorted(filter(lambda x: x.startswith('_') and (x.endswith('.dll') or x.endswith('.pdb')) and ('geom' in x), os.listdir(os.path.join(sys.argv[3], 'lib', f'monetdb5{version}'))))])
    id = comp(pyapi3, id, 16,
              [rf'lib\monetdb5{version}\_pyapi3.dll'])
    print(r'              </Directory>')
    id = comp(extend, id, 14,
              [rf'lib\bat{version}.lib',
               rf'lib\mapi{version}.lib',
               rf'lib\monetdb5{version}.lib',
               r'lib\monetdbe.lib',
               rf'lib\monetdbsql{version}.lib',
               rf'lib\stream{version}.lib',
               vcpkg.format(r'lib\iconv.lib'),
               vcpkg.format(r'lib\bz2.lib'),
               vcpkg.format(r'lib\charset.lib'),
               vcpkg.format(r'lib\getopt.lib'),
               vcpkg.format(r'lib\libxml2.lib'),
               vcpkg.format(r'lib\lz4.lib'),
               vcpkg.format(r'lib\lzma.lib'),
               vcpkg.format(r'lib\pcre.lib'),
               vcpkg.format(r'lib\zlib.lib')])
    print(r'            </Directory>')
    print(r'            <Directory Id="share" Name="share">')
    print(r'              <Directory Id="doc" Name="doc">')
    print(r'                <Directory Id="MonetDB_SQL" Name="MonetDB-SQL">')
    id = comp(features, id, 18, [r'share\doc\MonetDB-SQL\dump-restore.html',
                                 r'share\doc\MonetDB-SQL\dump-restore.txt'],
              vital = 'no')
    id = comp(features, id, 18,
              [r'share\website.html'],
              name = 'MonetDB Web Site',
              sid = 'website_html',
              vital = 'no')
    print(r'                </Directory>')
    print(r'              </Directory>')
    print(r'            </Directory>')
    id = comp(features, id, 12,
              [r'share\license.rtf',
               r'M5server.bat',
               r'msqldump.bat'])
    id = comp(pyapi3, id, 12,
              [r'pyapi_locatepython3.bat'])
    id = comp(features, id, 12,
              [r'mclient.bat'],
              name = 'MonetDB SQL Client',
              args = '/STARTED-FROM-MENU -lsql -Ecp437',
              sid = 'mclient_bat')
    id = comp(features, id, 12,
              [r'MSQLserver.bat'],
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
    print(r'    <Feature Id="Complete" ConfigurableDirectory="INSTALLDIR" Display="expand" InstallDefault="local" Title="MonetDB/SQL" Description="The complete package.">')
    print(r'      <Feature Id="MainServer" AllowAdvertise="no" Absent="disallow" Title="MonetDB/SQL" Description="The MonetDB/SQL server.">')
    for f in features:
        print(r'        <ComponentRef Id="{}"/>'.format(f))
    print(r'        <MergeRef Id="VCRedist"/>')
    print(r'      </Feature>')
    print(r'      <Feature Id="PyAPI3" Level="1000" AllowAdvertise="no" Absent="allow" Title="Include embedded Python 3" Description="Files required for using embedded Python 3.">')
    for f in pyapi3:
        print(r'        <ComponentRef Id="{}"/>'.format(f))
    print(r'        <Condition Level="1">PYAPI3EXISTS</Condition>')
    print(r'      </Feature>')
    print(r'      <Feature Id="Extend" Level="1000" AllowAdvertise="no" Absent="allow" Title="Extend MonetDB/SQL" Description="Files required for extending MonetDB (include files and .lib files).">')
    for f in extend:
        print(r'        <ComponentRef Id="{}"/>'.format(f))
    print(r'        <Condition Level="1">INCLUDEEXISTS</Condition>')
    print(r'      </Feature>')
    print(r'      <Feature Id="Debug" Level="1000" AllowAdvertise="no" Absent="allow" Title="MonetDB/SQL Debug Files" Description="Files useful for debugging purposes (.pdb files).">')
    for f in debug:
        print(r'        <ComponentRef Id="{}"/>'.format(f))
    print(r'        <Condition Level="1">DEBUGEXISTS</Condition>')
    print(r'      </Feature>')
    print(r'      <Feature Id="GeomModule" Level="1000" AllowAdvertise="no" Absent="allow" Title="Geom Module" Description="The GIS (Geographic Information System) extension for MonetDB/SQL.">')
    for f in geom:
        print(r'        <ComponentRef Id="{}"/>'.format(f))
    print(r'        <Condition Level="1">GEOMMALEXISTS OR GEOMLIBEXISTS</Condition>')
    print(r'      </Feature>')
    print(r'    </Feature>')
    print(r'    <UIRef Id="WixUI_Mondo"/>')
    print(r'    <UIRef Id="WixUI_ErrorProgressText"/>')
    print(r'    <Icon Id="monetdb.ico" SourceFile="share\monetdb.ico"/>')
    print(r'  </Product>')
    print(r'</Wix>')

main()
