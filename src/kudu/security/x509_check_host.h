/*
 * Copyright 1999-2016 The OpenSSL Project Authors. All Rights Reserved.
 *
 * Licensed under the OpenSSL license (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 */

// The following is ported from the OpenSSL-1.1.0b library.

#ifndef X509_CHECK_HOST_H
#define X509_CHECK_HOST_H

#include <stdlib.h>
// IWYU pragma: no_include <openssl/x509.h>
// IWYU pragma: no_include "openssl/x509.h"

typedef struct x509_st X509;

/* Flags for X509_check_* functions */

/*
 * Always check subject name for host match even if subject alt names present
 */
# define X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT    0x1
/* Disable wildcard matching for dnsName fields and common name. */
# define X509_CHECK_FLAG_NO_WILDCARDS    0x2
/* Wildcards must not match a partial label. */
# define X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS 0x4
/* Allow (non-partial) wildcards to match multiple labels. */
# define X509_CHECK_FLAG_MULTI_LABEL_WILDCARDS 0x8
/* Constraint verifier subdomain patterns to match a single labels. */
# define X509_CHECK_FLAG_SINGLE_LABEL_SUBDOMAINS 0x10
/* Never check the subject CN */
# define X509_CHECK_FLAG_NEVER_CHECK_SUBJECT    0x20
/*
 * Match reference identifiers starting with "." to any sub-domain.
 * This is a non-public flag, turned on implicitly when the subject
 * reference identity is a DNS name.
 */
# define _X509_CHECK_FLAG_DOT_SUBDOMAINS 0x8000

// Checks if the certificate Subject Alternative Name (SAN) or Subject CommonName (CN)
// matches the specified host name, which must be encoded in the preferred name syntax
// described in section 3.5 of RFC 1034.
int X509_check_host(X509 *x, const char *chk, size_t chklen,
                    unsigned int flags, char **peername);

#endif // X509_CHECK_HOST_H
