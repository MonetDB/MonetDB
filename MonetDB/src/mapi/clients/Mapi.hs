
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
    hPutStrLn sock user
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

