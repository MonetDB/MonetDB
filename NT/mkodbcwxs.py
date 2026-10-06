# SPDX-License-Identifier: MPL-2.0
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# For copyright information, see the file debian/copyright.

# python mkodbcwxs.py VERSION BITS PREFIX > PREFIX/MonetDB-ODBC-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\candle.exe" -nologo \
#     -arch x64/x86 PREFIX/MonetDB-ODBC-Installer.wxs
# "c:\Program Files (x86)\WiX Toolset v3.10\bin\light.exe" -nologo \
#     -sice:ICE03 -sice:ICE60 -sice:ICE82 -ext WixUIExtension \
#     PREFIX/MonetDB-ODBC-Installer.wixobj

import sys
import os

# doesn't change
upgradecode = {'x64': '{95ACBC8C-BC4B-4901-AF70-48B54A5C20F7}',
               'x86': '{C1F69378-3F5C-4120-8224-32F07D3458F3}'}


def comp(features, id, depth, files,
         fid=None, name=None, args=None, sid=None, vital=None):
    indent = ' ' * depth
    for f in files:
        print(f'{indent}<Component Id="_{id}" Guid="*">')
        print('{}  <File DiskId="1"{} KeyPath="yes" Name="{}" Source="{}"{}{}'
              .format(indent, f' Id="{fid}"' if fid else '',
                      f.split('\\')[-1], f,
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
        print(r'Usage: mkodbcwxs.py version bits installdir')
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
    print(r'<?xml version="1.0"?>')
    print(r'<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">')
    print(r'  <Product Id="*" Language="1033" Manufacturer="MonetDB"'
          fr' Name="MonetDB ODBC Driver" UpgradeCode="{upgradecode[arch]}"'
          fr' Version="{version}">')
    print(r'    <Package Id="*" Comments="MonetDB ODBC Driver"'
          r' Compressed="yes" InstallerVersion="301"'
          r' Keywords="MonetDB SQL ODBC" Languages="1033"'
          fr' Manufacturer="MonetDB Foundation" Platform="{arch}"/>')
    print(r'    <MajorUpgrade AllowDowngrades="no"'
          r' DowngradeErrorMessage="A later version of [ProductName]'
          r' is already installed." AllowSameVersionUpgrades="no"/>')
    print(r'    <WixVariable Id="WixUILicenseRtf" Value="share\license.rtf"/>')
    print(r'    <WixVariable Id="WixUIBannerBmp" Value="share\banner.bmp"/>')
    # print(r'    <WixVariable Id="WixUIDialogBmp"'
    #       r' Value="backgroundRipple.bmp"/>')
    print(r'    <Property Id="WIXUI_INSTALLDIR" Value="INSTALLDIR"/>')
    print(r'    <Property Id="ARPPRODUCTICON" Value="share\monetdb.ico"/>')
    print(r'    <Media Id="1" Cabinet="monetdb.cab" EmbedCab="yes"/>')
    print(r'    <CustomAction Id="driverinstall" FileKey="odbcinstall"'
          r' ExeCommand="/Install" Execute="deferred" Impersonate="no"/>')
    print(r'    <CustomAction Id="driveruninstall" FileKey="odbcinstall"'
          r' ExeCommand="/Uninstall" Execute="deferred" Impersonate="no"/>')
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
    print(r'          <Directory Id="INSTALLDIR" Name="MonetDB ODBC Driver">')
    id = 1
    print(r'            <Directory Id="lib" Name="lib">')
    id = comp(features, id, 14,
              [rf'bin\mapi-{version}.dll',
               rf'lib\mapi-{version}.pdb',
               r'lib\MonetODBC.dll',
               r'lib\MonetODBC.pdb',
               r'lib\MonetODBCs.dll',
               r'lib\MonetODBCs.pdb',
               rf'bin\stream-{version}.dll',
               rf'lib\stream-{version}.pdb',
               rf'bin\mutils-{version}.dll',
               rf'lib\mutils-{version}.pdb',
               vcpkg.format(r'bin\iconv-2.dll'),
               vcpkg.format(r'bin\bz2.dll'),
               vcpkg.format(r'bin\charset-1.dll'),  # for iconv-2.dll
               vcpkg.format(fr'bin\libcrypto-3{libcrypto}.dll'),
               vcpkg.format(fr'bin\libssl-3{libcrypto}.dll'),
               vcpkg.format(r'bin\lz4.dll'),
               vcpkg.format(r'bin\liblzma.dll'),
               vcpkg.format(r'bin\z.dll')])
    print(r'            </Directory>')
    id = comp(features, id, 12,
              [r'share\license.rtf'])
    id = comp(features, id, 12,
              [r'bin\odbcinstall.exe'],
              fid='odbcinstall')
    id = comp(features, id, 12,
              [r'share\website.html'],
              name='MonetDB Web Site',
              sid='website_html',
              vital = 'no')
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
          r' Title="MonetDB ODBC Driver">')
    for f in features:
        print(fr'      <ComponentRef Id="{f}"/>')
    print(r'      <MergeRef Id="VCRedist"/>')
    print(r'    </Feature>')
    print(r'    <UIRef Id="WixUI_InstallDir"/>')
    print(r'    <UIRef Id="WixUI_ErrorProgressText"/>')
    print(r'    <Icon Id="monetdb.ico" SourceFile="share\monetdb.ico"/>')
    print(r'    <InstallExecuteSequence>')
    print(r'      <Custom Action="driverinstall" Before="RegisterUser">')
    print(r'        NOT Installed OR REINSTALL')
    print(r'      </Custom>')
    print(r'      <Custom Action="driveruninstall"'
          r' Before="UnpublishComponents">')
    print(r'        Installed AND (REINSTALL OR (REMOVE'
          r' AND NOT UPGRADINGPRODUCTCODE))')
    print(r'      </Custom>')
    print(r'    </InstallExecuteSequence>')
    print(r'  </Product>')
    print(r'</Wix>')

main()
