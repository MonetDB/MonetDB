/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file daemon.h
 * Pathfinder daemon and TCP/IP support.
 *
 * $Id$
 */

#ifndef DAEMON_H
#define DAEMON_H

#include <sys/stat.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>

#include "pathfinder.h"

PFrc_t PFdaemonize (int port);

#endif

/* vim:set shiftwidth=4 expandtab: */
