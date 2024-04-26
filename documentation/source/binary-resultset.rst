.. SPDX-License-Identifier: MPL-2.0
..
.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 2024 MonetDB Foundation;
.. Copyright August 2008 - 2023 MonetDB B.V.;
.. Copyright 1997 - July 2008 CWI.

************************
Binary Result set format
************************

Note: this explanation will eventually be folded into a more comprehensive
description of the MAPI protocol on the MonetDB website.
In the mean time, it lives in this directory.


Overview
========

When MonetDB executes a query it immediately sends the first `N` rows of the
result set to the client, where `N` is determined by the ``reply_size`` setting.
If the client needs more it can use the ``Xexport <startrow> <count>`` command
to request more rows.

Recently we have added the ``Xexportbin <startrow> <count>`` command which
behaves the same but uses a binary format that may be more efficient to parse.

The server advertises its support for ``Xexportbin`` in the eighth field of its
connect challenge. For example,

    bL1sNfkaa:mserver:9:RIPEMD160,SHA512,SHA384,SHA256,SHA224,SHA1,COMPRESSION_LZ4:LIT:SHA512:sql=6:**BINARY=1**:

Currently it sends ``BINARY=1``. In the future it may send a higher number if
variants are added.


Binary result set layout
========================

In response to ``Xexportbin <startrow> <count>`` the server returns a big blob
of bytes delimited by a flush. In other words, the end is marked by the final
8KiB MAPI block having its end-flag enabled, just like a regular response. (We
can make this explanation more clear when the text is embedded in an overall
description of the MAPI protocol.)

To interpret the bytes, first look at the first character. If it starts with
an exclamation mark, the rest of the message is an error message.
Otherwise, look at the final 8 bytes. These form a 64 bit
server-endian integer. In the current version of the protocol the number will
always be either positive or negative and never zero.

If the number is negative, an error has occurred, and the negated number is the
byte offset of the error message, counting from the start of the response. The
end of the error message is marked by a nul byte. Note: the error message starts
with an exclamation mark, just as in the textual protocol.

If the number is positive, it is the byte offset of the table of contents of
the response.  This is a sequence of 16-byte entries, one for each column
of the result set.  Each entry consists of the starting offset and the length
in bytes of the data for that column, again expressed as 64 bits server-endian
integers.

The byte layout of each individual column is identical to what would have been
produced by ``COPY select_query INTO BINARY 'file1', 'file2', ...``.

