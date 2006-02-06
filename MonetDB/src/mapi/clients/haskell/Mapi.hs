-- The contents of this file are subject to the MonetDB Public License
-- Version 1.1 (the "License"); you may not use this file except in
-- compliance with the License. You may obtain a copy of the License at
-- http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
--
-- Software distributed under the License is distributed on an "AS IS"
-- basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
-- License for the specific language governing rights and limitations
-- under the License.
--
-- The Original Code is the MonetDB Database System.
--
-- The Initial Developer of the Original Code is CWI.
-- Portions created by CWI are Copyright (C) 1997-2006 CWI.
-- All Rights Reserved.

-- ----------------------------------------------------------------------------
-- 
-- Haskell-Mapi Version 0.1b
--
-- Compiles with GHC version 5.0.2 
--
-- ----------------------------------------------------------------------------
-- Example Usage:
--
-- import Mapi
-- 
-- user     = "username"
-- mserver  = "medusa.cwi.nl"
-- mapiport = 1234
-- 
-- main :: IO ()
-- main = do
--     m <- mapiConnect user mserver mapiport
--     result <- mapiQuery m "ls;"
--     putStrLn result
--     mapiDisconnect m
--
-- Compile with :
--   ghc -package net -c Mapi.hs       -- To compile the module
--   ghc -package net test.hs Mapi.hs  -- To compile the binary 
--                                        (module must be compiled already !)
-- 
-- ----------------------------------------------------------------------------
-- Implementation :

module Mapi (Mapi, mapiConnect, mapiQuery, mapiDisconnect) where

import Socket
import IO

-- ----------------------------------------------------------------------------

type Mapi = Handle

-- ----------------------------------------------------------------------------

mapiConnect :: String -> String -> Int -> IO Mapi
mapiConnect user mserver mapiport = do
    sock <- connectTo mserver (PortNumber (fromIntegral mapiport))
    hSetBuffering sock NoBuffering
    hPutStrLn sock (user++":passwd:mil:line")
    mapiResult sock
    return sock
	
mapiSend :: Mapi -> String -> IO ()
mapiSend sock mil = do
    hPutStrLn sock mil

mapiAnswer :: Mapi -> IO String 
mapiAnswer sock = do
    c <- hGetChar sock
    if c == '\001'
        then return ""
        else do l <- mapiAnswer sock
                return (c:l)

mapiPrompt :: Mapi -> IO String 
mapiPrompt sock = mapiAnswer sock

mapiResult :: Mapi -> IO String 
mapiResult sock = do
    result <- mapiAnswer sock
    mapiPrompt sock
    return result

mapiQuery :: Mapi -> String -> IO String 
mapiQuery sock mil = do
    mapiSend sock mil
    result <- mapiResult sock
    return result

mapiDisconnect :: Mapi -> IO ()
mapiDisconnect sock = do
    mapiSend sock "quit;"
    hClose sock

-- ----------------------------------------------------------------------------

