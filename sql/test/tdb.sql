START TRANSACTION;

DECLARE SQLS_MAX_DIRNAME_LENGTH 	smallint;
DECLARE SQLS_MAX_FILENAME_LENGTH 	smallint;
DECLARE SQLS_MAX_ARTISTNAME_LENGTH 	smallint;
DECLARE SQLS_MAX_SONGTITLE_LENGTH 	smallint;
DECLARE SQLS_MAX_ALBUMTITLE_LENGTH 	smallint;
DECLARE SQLS_MAX_GENRENAME_LENGTH 	smallint;
DECLARE SQLS_MAX_PLAYLISTNAME_LENGTH 	smallint;

SET SQLS_MAX_DIRNAME_LENGTH =		260;
SET SQLS_MAX_FILENAME_LENGTH =		260;
SET SQLS_MAX_ARTISTNAME_LENGTH =	100;
SET SQLS_MAX_SONGTITLE_LENGTH =		100;
SET SQLS_MAX_ALBUMTITLE_LENGTH =	100;
SET SQLS_MAX_GENRENAME_LENGTH =		50;
SET SQLS_MAX_PLAYLISTNAME_LENGTH =	100;

create table songTable (
iSongId integer,
UNIQUE (iSongId),
cSongTitle varchar(SQLS_MAX_SONGTITLE_LENGTH),
iArtistId integer,
iAlbumId integer,
iTrackNr integer,
iTrackLength integer,
iNrPlayed integer,
cFileName varchar(SQLS_MAX_FILENAME_LENGTH),
iDirId integer,
iYear integer,
iGenreId integer,
iBitRate integer,
iSampleRate integer,
iFileSize integer,
iMediaType integer,
PRIMARY KEY (cSongTitle, iArtistId, iAlbumId, cFileName, iDirId, iGenreId)
);

create table artistTable (
iArtistId integer,
UNIQUE (iArtistId),
cArtistName varchar(SQLS_MAX_ARTISTNAME_LENGTH),
PRIMARY KEY (cArtistName)
);

create table albumTable (
iAlbumId integer,
UNIQUE (iAlbumId),
cAlbumTitle varchar(SQLS_MAX_ALBUMTITLE_LENGTH),
PRIMARY KEY (cAlbumTitle)
);

create table genreTable (
iGenreId integer,
UNIQUE (iGenreId),
cGenreName varchar(SQLS_MAX_GENRENAME_LENGTH),
PRIMARY KEY (cGenreName)
);

create table dirTable (
iDirId integer,
cDirName varchar(SQLS_MAX_DIRNAME_LENGTH),
iParentDirId integer,
PRIMARY KEY (cDirName, iDirId)
);

create table playlistTable (
iPlaylistId integer,
UNIQUE (iPlaylistId),
cPlaylistName varchar(SQLS_MAX_PLAYLISTNAME_LENGTH),
cFileName varchar(SQLS_MAX_FILENAME_LENGTH),
iDirId integer,
PRIMARY KEY (cPlaylistName)
);

create table playsongTable (
iPlaysongId integer,
UNIQUE (iPlaysongId),
iPlaylistId integer,
iSongId integer,
iOrderNr integer,
PRIMARY KEY (iPlaylistId, iSongId)
);

COMMIT;
