/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"

#include "stream.h"
#include "mapi.h"
#include "mapi_intern.h"

#include <wincrypt.h>

MapiMsg
croak_win32(Mapi mid, const char *action, DWORD error)
{
	return mapi_printError(mid, action, MERROR, "System Error #%d", error);
}

static MapiMsg
process_sysstore_item(Mapi mid, X509_STORE *x509_store, int nr, const CERT_CONTEXT *item)
{
	DWORD typ = item->dwCertEncodingType;
	const unsigned char *data = item->pbCertEncoded;
	size_t size = item->cbCertEncoded;

	bool is_x509 = (typ & X509_ASN_ENCODING);
	bool is_pkcs7 = (typ & PKCS_7_ASN_ENCODING);

	// mapi_log_data(mid, "CERT", (char*)data, size);

	if (!is_x509)
		return mapi_printError(mid, __func__, MERROR, "sys store certificate #%d must be in X509 format", nr);

	X509 *x509 = d2i_X509(NULL, &data, (long)size);
	if (!x509)
		return croak_openssl(mid, __func__, "sys store certificate #%d: d2i_X509");

	MapiMsg msg;
	if (1 == X509_STORE_add_cert(x509_store, x509)) {
		msg = MOK;
	} else {
		msg = mapi_printError(mid, __func__, MERROR, "sys store certificate #%d: X509_STORE_add_cert");
	}
	X509_free(x509);

	return msg;
}

MapiMsg
add_system_certificates(Mapi mid, SSL_CTX *ctx)
{
	MapiMsg msg;
	X509_STORE *x509_store = SSL_CTX_get_cert_store(ctx);
	HCERTSTORE sysstore = NULL;
	const CERT_CONTEXT *item = NULL;

	mapi_log_record(mid, "CONN", "Enumerating system certificates");

	sysstore = CertOpenSystemStoreW(0, L"ROOT");
	if (!sysstore)
		return croak_win32(mid, __func__, GetLastError());

	int count = 0;
	while (1) {
		item = CertEnumCertificatesInStore(sysstore, item);
		if (item == NULL)
			break;
		msg = process_sysstore_item(mid, x509_store, ++count, item);
		if (msg != MOK) {
			CertFreeCertificateContext(item);
			CertCloseStore(sysstore, 0);
			return msg;
		}
	}

	// We get here if CertEnumCertificatesInStore returned NULL.
	DWORD error = GetLastError();
	assert(item == NULL);
	CertCloseStore(sysstore, 0);
	switch (error) {
		case 0:
		case CRYPT_E_NOT_FOUND:
		case ERROR_NO_MORE_FILES:
			// Normal exit codes according to the documentation at
			// https://learn.microsoft.com/en-us/windows/win32/api/wincrypt/nf-wincrypt-certenumcertificatesinstore
			mapi_log_record(mid, "CONN", "Found %d certificates", count);
			return MOK;
		default:
			// Anything else is problematic
			return croak_win32(mid, __func__, error);
	}
}


