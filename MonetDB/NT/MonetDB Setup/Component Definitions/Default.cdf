[Components]
component0=SQL Test Files
component1=Test Files
component2=Program Files
component3=SQL Development Files
component4=Development Files
component5=SQL Program Files

[TopComponents]
component0=SQL Test Files
component1=Test Files
component2=Program Files
component3=SQL Development Files
component4=Development Files
component5=SQL Program Files

[SQL Test Files]
required0=Test Files
SELECTED=Yes
FILENEED=STANDARD
required1=SQL Program Files
HTTPLOCATION=
STATUS=
UNINSTALLABLE=Yes
TARGET=<TARGETDIR>
FTPLOCATION=
VISIBLE=Yes
DESCRIPTION=This component contains the MonetDB SQL test programs.  The actual tests are not included, so this component is of limited use.
DISPLAYTEXT=
IMAGE=
DEFSELECTION=Yes
filegroup0=SQL Test Files
COMMENT=
INCLUDEINBUILD=Yes
INSTALLATION=ALWAYSOVERWRITE
COMPRESSIFSEPARATE=No
MISC=
ENCRYPT=No
DISK=ANYDISK
TARGETDIRCDROM=
PASSWORD=
TARGETHIDDEN=General Application Destination

[SetupType]
setuptype0=Compact
setuptype1=Typical
setuptype2=Custom

[Test Files]
required0=Program Files
SELECTED=Yes
FILENEED=STANDARD
HTTPLOCATION=
STATUS=
UNINSTALLABLE=Yes
TARGET=<TARGETDIR>
FTPLOCATION=
VISIBLE=Yes
DESCRIPTION=This component contains the MonetDB test programs.  The actual tests are not included, so this component is of limited use.
DISPLAYTEXT=
IMAGE=
DEFSELECTION=Yes
filegroup0=Test Files
requiredby0=SQL Test Files
COMMENT=
INCLUDEINBUILD=Yes
INSTALLATION=ALWAYSOVERWRITE
COMPRESSIFSEPARATE=No
MISC=
ENCRYPT=No
DISK=ANYDISK
TARGETDIRCDROM=
PASSWORD=
TARGETHIDDEN=General Application Destination

[SetupTypeItem-Compact]
Comment=
item0=Program Files
item1=SQL Program Files
Descrip=Minimum set of files required to run the MonetDB and MonetDB SQL servers.
DisplayText=

[Program Files]
SELECTED=Yes
FILENEED=STANDARD
HTTPLOCATION=
STATUS=
UNINSTALLABLE=Yes
TARGET=<TARGETDIR>
FTPLOCATION=
VISIBLE=Yes
DESCRIPTION=This component includes all files needed to run the MonetDB server.  This component is required.
DISPLAYTEXT=
IMAGE=
DEFSELECTION=Yes
filegroup0=Library Files
requiredby0=Test Files
COMMENT=
INCLUDEINBUILD=Yes
filegroup1=Program Files
requiredby1=Development Files
INSTALLATION=ALWAYSOVERWRITE
requiredby2=SQL Program Files
COMPRESSIFSEPARATE=No
MISC=
ENCRYPT=No
DISK=ANYDISK
TARGETDIRCDROM=
PASSWORD=
TARGETHIDDEN=General Application Destination

[SetupTypeItem-Custom]
Comment=
item0=Program Files
item1=SQL Program Files
Descrip=Pick and choose components to install.
DisplayText=

[SQL Development Files]
required0=Development Files
SELECTED=Yes
FILENEED=STANDARD
required1=SQL Program Files
HTTPLOCATION=
STATUS=
UNINSTALLABLE=Yes
TARGET=<TARGETDIR>
FTPLOCATION=
VISIBLE=Yes
DESCRIPTION=This component contains the files needed to extend the MonetDB SQL server.
DISPLAYTEXT=
IMAGE=
DEFSELECTION=Yes
filegroup0=SQL Development Files
COMMENT=
INCLUDEINBUILD=Yes
INSTALLATION=ALWAYSOVERWRITE
COMPRESSIFSEPARATE=No
MISC=
ENCRYPT=No
DISK=ANYDISK
TARGETDIRCDROM=
PASSWORD=
TARGETHIDDEN=General Application Destination

[Info]
Type=CompDef
Version=1.00.000
Name=

[Development Files]
required0=Program Files
SELECTED=Yes
FILENEED=STANDARD
HTTPLOCATION=
STATUS=
UNINSTALLABLE=Yes
TARGET=<TARGETDIR>
FTPLOCATION=
VISIBLE=Yes
DESCRIPTION=This component contains the files needed to extend MonetDB.
DISPLAYTEXT=
IMAGE=
DEFSELECTION=Yes
filegroup0=Development Files
requiredby0=SQL Development Files
COMMENT=
INCLUDEINBUILD=Yes
INSTALLATION=ALWAYSOVERWRITE
COMPRESSIFSEPARATE=No
MISC=
ENCRYPT=No
DISK=ANYDISK
TARGETDIRCDROM=
PASSWORD=
TARGETHIDDEN=General Application Destination

[SetupTypeItem-Typical]
Comment=
item0=Program Files
item1=SQL Program Files
Descrip=All files required to run the MonetDB and MonetDB SQL servers.
DisplayText=

[SQL Program Files]
required0=Program Files
SELECTED=Yes
FILENEED=STANDARD
HTTPLOCATION=
STATUS=
UNINSTALLABLE=Yes
TARGET=<TARGETDIR>
FTPLOCATION=
VISIBLE=Yes
DESCRIPTION=This component contains the MonetDB SQL server.
DISPLAYTEXT=
IMAGE=
DEFSELECTION=Yes
filegroup0=SQL Program Files
requiredby0=SQL Test Files
COMMENT=
INCLUDEINBUILD=Yes
requiredby1=SQL Development Files
INSTALLATION=ALWAYSOVERWRITE
COMPRESSIFSEPARATE=No
MISC=
ENCRYPT=No
DISK=ANYDISK
TARGETDIRCDROM=
PASSWORD=
TARGETHIDDEN=General Application Destination

