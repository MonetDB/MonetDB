# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.

# python mksqlwxs.py VERSION BITS PREFIX > PREFIX/MonetDB-SQL-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\candle.exe" -nologo \
#     -arch x64/x86 PREFIX/MonetDB-SQL-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\light.exe" -nologo \
#     -sice:ICE03 -sice:ICE60 -sice:ICE82 -ext WixUIExtension \
#     PREFIX/MonetDB-SQL-Installer.wixobj

import sys
import os

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


def comp(features, id, depth, files,
         name=None, args=None, sid=None, vital=None):
    indent = ' ' * depth
    for f in files:
        print(f'{indent}<Component Id="_{id}" Guid="*">')
        print('{}  <File DiskId="1" KeyPath="yes" Name="{}" Source="{}"{}{}'
              .format(indent, f.split('\\')[-1], f,
                      f' Vital="{vital}"' if vital else '',
                      '>' if name else '/>'))
        if name:
            print(('{}    <Shortcut Id="{}" Advertise="yes"{}'
                   ' Directory="ProgramMenuDir" Icon="monetdb.ico"'
                   ' IconIndex="0" Name="{}" WorkingDirectory="INSTALLDIR"/>')
                  .format(indent, sid,
                          f' Arguments="{args}"' if args else '',
                          name))
            print(f'{indent}  </File>')
        print(f'{indent}</Component>')
        features.append(f'_{id}')
        id += 1
    return id


vcsearch = (
    r'C:\Program Files\Microsoft Visual Studio\2022\Community\VC',
    r'C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC',
    r'C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC',
    )


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
        for vcdir in vcsearch:
            if os.path.exists(vcdir):
                break
        else:
            print(r"Don't know which visual studio directory to use")
            return 1
    msvc = os.path.join(vcdir, r'Redist\MSVC')
    features = []
    extend = []
    debug = []
    geom = []
    print(r'<?xml version="1.0"?>')
    print(r'<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">')
    print(r'  <Product Id="*" Language="1033" Manufacturer="MonetDB"'
          fr' Name="MonetDB" UpgradeCode="{upgradecode[arch]}"'
          fr' Version="{version}">')
    print(r'    <Package Id="*" Comments="MonetDB/SQL Server and Client"'
          r' Compressed="yes" InstallerVersion="301"'
          r' Keywords="MonetDB SQL Database" Languages="1033"'
          fr' Manufacturer="MonetDB Foundation" Platform="{arch}"/>')
    print(fr'    <Upgrade Id="{geomupgradecode[arch]}">')
    # up to and including 11.29.3, the geom module can not be
    # uninstalled if MonetDB/SQL is not installed; this somehow also
    # precludes the upgrade to this version
    print(r'      <UpgradeVersion OnlyDetect="no" Minimum="11.29.3"'
          fr' IncludeMinimum="no" Maximum="{version}"'
          r' Property="GEOMINSTALLED"/>')
    print(r'    </Upgrade>')
    print(r'    <MajorUpgrade AllowDowngrades="no"'
          r' DowngradeErrorMessage="A later version of [ProductName]'
          r' is already installed." AllowSameVersionUpgrades="no"/>')
    print(r'    <WixVariable Id="WixUILicenseRtf" Value="share\license.rtf"/>')
    print(r'    <WixVariable Id="WixUIBannerBmp" Value="share\banner.bmp"/>')
    # print(r'    <WixVariable Id="WixUIDialogBmp"'
    #       r' Value="backgroundRipple.bmp"/>')
    print(r'    <Property Id="INSTALLDIR">')
    print(r'      <RegistrySearch Id="MonetDBRegistry"'
          r' Key="Software\[Manufacturer]\[ProductName]" Name="InstallPath"'
          r' Root="HKLM" Type="raw"/>')
    print(r'    </Property>')
    print(r'    <Property Id="DEBUGEXISTS">')
    print(r'      <DirectorySearch Id="CheckFileDir1" Path="[INSTALLDIR]\bin"'
          r' Depth="0">')
    print(r'        <FileSearch Id="CheckFile1" Name="mserver5.pdb"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    print(r'    <Property Id="INCLUDEEXISTS">')
    print(r'      <DirectorySearch Id="CheckFileDir2"'
          r' Path="[INSTALLDIR]\include\monetdb" Depth="0">')
    print(r'        <FileSearch Id="CheckFile2" Name="gdk.h"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    print(r'    <Property Id="GEOMMALEXISTS">')
    print(r'      <DirectorySearch Id="CheckFileDir3"'
          r' Path="[INSTALLDIR]\lib\monetdb5" Depth="0">')
    print(r'        <FileSearch Id="CheckFile3" Name="geom.mal"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    print(r'    <Property Id="GEOMLIBEXISTS">')
    print(r'      <DirectorySearch Id="CheckFileDir4"'
          r' Path="[INSTALLDIR]\lib\monetdb5" Depth="0">')
    print(r'        <FileSearch Id="CheckFile4" Name="_geom.dll"/>')
    print(r'      </DirectorySearch>')
    print(r'    </Property>')
    # up to and including 11.29.3, the geom module can not be
    # uninstalled if MonetDB/SQL is not installed; this somehow also
    # precludes the upgrade to this version, therefore we disallow
    # running the current installer
    print(r'    <Property Id="OLDGEOMINSTALLED">')
    print(fr'      <ProductSearch UpgradeCode="{geomupgradecode[arch]}"'
          r' Minimum="11.1.1" Maximum="11.29.3" IncludeMinimum="yes"'
          r' IncludeMaximum="yes"/>')
    print(r'    </Property>')
    print(r'    <Condition Message="Please uninstall MonetDB SQL GIS Module'
          r' first, then rerun and select to install Complete package.">')
    print(r'      NOT OLDGEOMINSTALLED')
    print(r'    </Condition>')
    print(r'    <Property Id="ApplicationFolderName" Value="MonetDB"/>')
    print(r'    <Property Id="WixAppFolder" Value="WixPerMachineFolder"/>')
    print(r'    <Property Id="WIXUI_INSTALLDIR" Value="INSTALLDIR"/>')
    print(r'    <Property Id="ARPPRODUCTICON" Value="share\monetdb.ico"/>')
    print(r'    <Media Id="1" Cabinet="monetdb.cab" EmbedCab="yes"/>')
    print(r'    <Directory Id="TARGETDIR" Name="SourceDir">')
    d = sorted(os.listdir(msvc))[-1]
    msm = f'_CRT_{arch}.msm'
    for f in sorted(os.listdir(os.path.join(msvc, d, 'MergeModules'))):
        if msm in f:
            fn = f
    print(r'      <Merge Id="VCRedist" DiskId="1" Language="0"'
          fr' SourceFile="{msvc}\{d}\MergeModules\{fn}"/>')
    print(r'      <Directory Id="{}">'.format(folder))
    print(r'        <Directory Id="ProgramFilesMonetDB" Name="MonetDB">')
    print(r'          <Directory Id="INSTALLDIR" Name="MonetDB">')
    print(r'            <Component Id="registry">')
    print(r'              <RegistryKey'
          r' Key="Software\[Manufacturer]\[ProductName]" Root="HKLM">')
    print(r'                <RegistryValue Name="InstallPath" Type="string"'
          r' Value="[INSTALLDIR]"/>')
    print(r'              </RegistryKey>')
    print(r'            </Component>')
    features.append('registry')
    id = 1
    print(r'            <Directory Id="bin" Name="bin">')
    id = comp(features, id, 14,
              [r'bin\mclient.exe',
               r'bin\mserver5.exe',
               r'bin\msqldump.exe',
               fr'bin\bat-{version}.dll',
               fr'bin\mapi-{version}.dll',
               fr'bin\monetdb5-{version}.dll',
               r'bin\monetdbe.dll',
               fr'bin\monetdbsql-{version}.dll',
               fr'bin\stream-{version}.dll',
               fr'bin\mutils-{version}.dll',
               vcpkg.format(r'bin\bz2.dll'),
               vcpkg.format(r'bin\charset-1.dll'),  # for iconv-2.dll
               vcpkg.format(r'bin\getopt.dll'),
               vcpkg.format(r'bin\iconv-2.dll'),
               vcpkg.format(fr'bin\libcrypto-3{libcrypto}.dll'),
               vcpkg.format(r'bin\liblzma.dll'),
               vcpkg.format(fr'bin\libssl-3{libcrypto}.dll'),
               vcpkg.format(r'bin\libxml2.dll'),
               vcpkg.format(r'bin\lz4.dll'),
               vcpkg.format(r'bin\pcre2-8.dll'),
               vcpkg.format(r'bin\z.dll')])
    id = comp(debug, id, 14,
              [r'bin\mclient.pdb',
               r'bin\mserver5.pdb',
               r'bin\msqldump.pdb',
               fr'lib\bat-{version}.pdb',
               fr'lib\mapi-{version}.pdb',
               fr'lib\monetdb5-{version}.pdb',
               fr'lib\monetdbsql-{version}.pdb',
               fr'lib\stream-{version}.pdb',
               fr'lib\mutils-{version}.pdb'])
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
              sorted([fr'include\monetdb\{x}'
                      for x in filter(
                              lambda x: ((x.startswith('gdk')
                                          or x.startswith('monet')
                                          or x.startswith('mal')
                                          or x.startswith('sql')
                                          or x.startswith('rel')
                                          or x.startswith('store')
                                          or x.startswith('opt_backend'))
                                         and x.endswith('.h')),
                              os.listdir(os.path.join(sys.argv[3],
                                                      'include',
                                                      'monetdb')))]
                     + [r'include\monetdb\copybinary.h',
                        r'include\monetdb\mapi.h',
                        r'include\monetdb\mapi_querytype.h',
                        r'include\monetdb\msettings.h',
                        r'include\monetdb\matomic.h',
                        r'include\monetdb\mel.h',
                        r'include\monetdb\mstring.h',
                        r'include\monetdb\stream.h',
                        r'include\monetdb\stream_socket.h']),
              vital='no')
    print(r'              </Directory>')
    print(r'            </Directory>')
    print(r'            <Directory Id="lib" Name="lib">')
    print(r'              <Directory Id="monetdb5" Name="monetdb5">')
    id = comp(features, id, 16,
              [fr'lib\monetdb5-{version}\{x}'
               for x in sorted(
                       filter(
                           lambda x: (x.startswith('_')
                                      and x.endswith('.dll')
                                      and ('geom' not in x)
                                      and ('microbenchmark' not in x)),
                           os.listdir(os.path.join(sys.argv[3],
                                                   'lib',
                                                   f'monetdb5-{version}'))))])
    id = comp(debug, id, 16,
              [fr'lib\monetdb5-{version}\{x}'
               for x in sorted(
                       filter(lambda x: (x.startswith('_')
                                         and x.endswith('.pdb')
                                         and ('geom' not in x)
                                         and ('microbenchmark' not in x)),
                              os.listdir(
                                  os.path.join(sys.argv[3],
                                               'lib',
                                               f'monetdb5-{version}'))))])
    id = comp(geom, id, 16,
              [fr'lib\monetdb5-{version}\{x}'
               for x in sorted(
                       filter(lambda x: (x.startswith('_')
                                         and (x.endswith('.dll')
                                              or x.endswith('.pdb'))
                                         and ('geom' in x)),
                              os.listdir(
                                  os.path.join(sys.argv[3],
                                               'lib',
                                               f'monetdb5-{version}'))))])
    print(r'              </Directory>')
    id = comp(extend, id, 14,
              [fr'lib\bat-{version}.lib',
               fr'lib\mapi-{version}.lib',
               fr'lib\monetdb5-{version}.lib',
               r'lib\monetdbe.lib',
               fr'lib\monetdbsql-{version}.lib',
               fr'lib\stream-{version}.lib',
               fr'lib\mutils-{version}.lib',
               vcpkg.format(r'lib\bz2.lib'),
               vcpkg.format(r'lib\charset.lib'),
               vcpkg.format(r'lib\getopt.lib'),
               vcpkg.format(r'lib\iconv.lib'),
               vcpkg.format(r'lib\libxml2.lib'),
               vcpkg.format(r'lib\lz4.lib'),
               vcpkg.format(r'lib\lzma.lib'),
               vcpkg.format(r'lib\pcre2-8.lib'),
               vcpkg.format(r'lib\z.lib')])
    print(r'            </Directory>')
    print(r'            <Directory Id="share" Name="share">')
    print(r'              <Directory Id="doc" Name="doc">')
    print(r'                <Directory Id="MonetDB_SQL" Name="MonetDB-SQL">')
    id = comp(features, id, 18, [r'share\doc\MonetDB-SQL\dump-restore.html',
                                 r'share\doc\MonetDB-SQL\dump-restore.txt'],
              vital='no')
    id = comp(features, id, 18,
              [r'share\website.html'],
              name='MonetDB Web Site',
              sid='website_html',
              vital='no')
    print(r'                </Directory>')
    print(r'              </Directory>')
    print(r'            </Directory>')
    id = comp(features, id, 12,
              [r'share\license.rtf',
               r'M5server.bat',
               r'msqldump.bat'])
    id = comp(features, id, 12,
              [r'mclient.bat'],
              name='MonetDB SQL Client',
              args='/STARTED-FROM-MENU -lsql -Ecp437',
              sid='mclient_bat')
    id = comp(features, id, 12,
              [r'MSQLserver.bat'],
              name='MonetDB SQL Server',
              sid='msqlserver_bat')
    print(r'          </Directory>')
    print(r'        </Directory>')
    print(r'      </Directory>')
    print(r'      <Directory Id="ProgramMenuFolder" Name="Programs">')
    print(r'        <Directory Id="ProgramMenuDir" Name="MonetDB">')
    print(r'          <Component Id="ProgramMenuDir" Guid="*">')
    features.append('ProgramMenuDir')
    print(r'            <RemoveFolder Id="ProgramMenuDir" On="uninstall"/>')
    print(r'            <RegistryValue'
          r' Key="Software\[Manufacturer]\[ProductName]" KeyPath="yes"'
          r' Root="HKCU" Type="string" Value=""/>')
    print(r'          </Component>')
    print(r'        </Directory>')
    print(r'      </Directory>')
    print(r'    </Directory>')
    print(r'    <Feature Id="Complete" ConfigurableDirectory="INSTALLDIR"'
          r' Display="expand" InstallDefault="local" Title="MonetDB/SQL"'
          r' Description="The complete package.">')
    print(r'      <Feature Id="MainServer" AllowAdvertise="no"'
          r' Absent="disallow" Title="MonetDB/SQL"'
          r' Description="The MonetDB/SQL server.">')
    for f in features:
        print(fr'        <ComponentRef Id="{f}"/>')
    print(r'        <MergeRef Id="VCRedist"/>')
    print(r'      </Feature>')
    print(r'      <Feature Id="Extend" Level="1000" AllowAdvertise="no"'
          r' Absent="allow" Title="Extend MonetDB/SQL"'
          r' Description="Files required for extending MonetDB (include files'
          r' and .lib files).">')
    for f in extend:
        print(fr'        <ComponentRef Id="{f}"/>')
    print(r'        <Condition Level="1">INCLUDEEXISTS</Condition>')
    print(r'      </Feature>')
    print(r'      <Feature Id="Debug" Level="1000" AllowAdvertise="no"'
          r' Absent="allow" Title="MonetDB/SQL Debug Files"'
          r' Description="Files useful for debugging purposes (.pdb files).">')
    for f in debug:
        print(fr'        <ComponentRef Id="{f}"/>')
    print(r'        <Condition Level="1">DEBUGEXISTS</Condition>')
    print(r'      </Feature>')
    print(r'      <Feature Id="GeomModule" Level="1000" AllowAdvertise="no"'
          r' Absent="allow" Title="Geom Module" Description="The GIS'
          r' (Geographic Information System) extension for MonetDB/SQL.">')
    for f in geom:
        print(fr'        <ComponentRef Id="{f}"/>')
    print(r'        <Condition Level="1">GEOMMALEXISTS OR GEOMLIBEXISTS'
          r'</Condition>')
    print(r'      </Feature>')
    print(r'    </Feature>')
    print(r'    <UIRef Id="WixUI_Mondo"/>')
    print(r'    <UIRef Id="WixUI_ErrorProgressText"/>')
    print(r'    <Icon Id="monetdb.ico" SourceFile="share\monetdb.ico"/>')
    print(r'  </Product>')
    print(r'</Wix>')


main()
