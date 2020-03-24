/*
 * Copyright 1999-2016 The OpenSSL Project Authors. All Rights Reserved.
 *
 * Licensed under the OpenSSL license (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 */

// The following is ported from the OpenSSL-1.1.0b library. The implementations
// of the functions are for the most part the same except where mentioned in special
// comments. Explicit casts were also added to bypass compilation errors.

#include "kudu/security/x509_check_host.h"

#include <openssl/asn1.h>
#include <openssl/crypto.h>
#include <openssl/obj_mac.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <strings.h>

#include <cstring>

// Ported from include/openssl/crypto.h from OpenSSL-1.1.0b
// Modifed to use __FILE__ and __LINE__ instead of OPENSSL_FILE and OPENSSL_LINE.
# define OPENSSL_strndup(str, n) \
        CRYPTO_strndup(str, n, __FILE__, __LINE__)

// Ported from crypto/o_str.c from OpenSSL-1.1.0b.
// Modified to use strnlen() instead of OPENSSL_strnlen()
char *CRYPTO_strndup(const char *str, size_t s, const char* file, int line)
{
    size_t maxlen;
    char *ret;

    if (str == NULL)
        return NULL;

    maxlen = strnlen(str, s);

    ret = (char*)CRYPTO_malloc(maxlen + 1, file, line);
    if (ret) {
        memcpy(ret, str, maxlen);
        ret[maxlen] = '\0';
    }
    return ret;
}

// The remaining code is ported form crypto/x509v3/v3_utl.c

typedef int (*equal_fn) (const unsigned char *pattern, size_t pattern_len,
                         const unsigned char *subject, size_t subject_len,
                         unsigned int flags);

/* Skip pattern prefix to match "wildcard" subject */
static void skip_prefix(const unsigned char **p, size_t *plen,
                        size_t subject_len,
                        unsigned int flags)
{
    const unsigned char *pattern = *p;
    size_t pattern_len = *plen;

    /*
     * If subject starts with a leading '.' followed by more octets, and
     * pattern is longer, compare just an equal-length suffix with the
     * full subject (starting at the '.'), provided the prefix contains
     * no NULs.
     */
    if ((flags & _X509_CHECK_FLAG_DOT_SUBDOMAINS) == 0)
        return;

    while (pattern_len > subject_len && *pattern) {
        if ((flags & X509_CHECK_FLAG_SINGLE_LABEL_SUBDOMAINS) &&
            *pattern == '.')
            break;
        ++pattern;
        --pattern_len;
    }

    /* Skip if entire prefix acceptable */
    if (pattern_len == subject_len) {
        *p = pattern;
        *plen = pattern_len;
    }
}

/* Compare while ASCII ignoring case. */
static int equal_nocase(const unsigned char *pattern, size_t pattern_len,
                        const unsigned char *subject, size_t subject_len,
                        unsigned int flags)
{
    skip_prefix(&pattern, &pattern_len, subject_len, flags);
    if (pattern_len != subject_len)
        return 0;
    while (pattern_len) {
        unsigned char l = *pattern;
        unsigned char r = *subject;
        /* The pattern must not contain NUL characters. */
        if (l == 0)
            return 0;
        if (l != r) {
            if ('A' <= l && l <= 'Z')
                l = (l - 'A') + 'a';
            if ('A' <= r && r <= 'Z')
                r = (r - 'A') + 'a';
            if (l != r)
                return 0;
        }
        ++pattern;
        ++subject;
        --pattern_len;
    }
    return 1;
}

/* Compare using memcmp. */
static int equal_case(const unsigned char *pattern, size_t pattern_len,
                      const unsigned char *subject, size_t subject_len,
                      unsigned int flags)
{
    skip_prefix(&pattern, &pattern_len, subject_len, flags);
    if (pattern_len != subject_len)
        return 0;
    return !memcmp(pattern, subject, pattern_len);
}

/*
 * RFC 5280, section 7.5, requires that only the domain is compared in a
 * case-insensitive manner.
 */
static int equal_email(const unsigned char *a, size_t a_len,
                       const unsigned char *b, size_t b_len,
                       unsigned int unused_flags)
{
    size_t i = a_len;
    if (a_len != b_len)
        return 0;
    /*
     * We search backwards for the '@' character, so that we do not have to
     * deal with quoted local-parts.  The domain part is compared in a
     * case-insensitive manner.
     */
    while (i > 0) {
        --i;
        if (a[i] == '@' || b[i] == '@') {
            if (!equal_nocase(a + i, a_len - i, b + i, a_len - i, 0))
                return 0;
            break;
        }
    }
    if (i == 0)
        i = a_len;
    return equal_case(a, i, b, i, 0);
}

/*
 * Compare the prefix and suffix with the subject, and check that the
 * characters in-between are valid.
 */
static int wildcard_match(const unsigned char *prefix, size_t prefix_len,
                          const unsigned char *suffix, size_t suffix_len,
                          const unsigned char *subject, size_t subject_len,
                          unsigned int flags)
{
    const unsigned char *wildcard_start;
    const unsigned char *wildcard_end;
    const unsigned char *p;
    int allow_multi = 0;
    int allow_idna = 0;

    if (subject_len < prefix_len + suffix_len)
        return 0;
    if (!equal_nocase(prefix, prefix_len, subject, prefix_len, flags))
        return 0;
    wildcard_start = subject + prefix_len;
    wildcard_end = subject + (subject_len - suffix_len);
    if (!equal_nocase(wildcard_end, suffix_len, suffix, suffix_len, flags))
        return 0;
    /*
     * If the wildcard makes up the entire first label, it must match at
     * least one character.
     */
    if (prefix_len == 0 && *suffix == '.') {
        if (wildcard_start == wildcard_end)
            return 0;
        allow_idna = 1;
        if (flags & X509_CHECK_FLAG_MULTI_LABEL_WILDCARDS)
            allow_multi = 1;
    }
    /* IDNA labels cannot match partial wildcards */
    if (!allow_idna &&
        subject_len >= 4 && strncasecmp((char *)subject, "xn--", 4) == 0)
        return 0;
    /* The wildcard may match a literal '*' */
    if (wildcard_end == wildcard_start + 1 && *wildcard_start == '*')
        return 1;
    /*
     * Check that the part matched by the wildcard contains only
     * permitted characters and only matches a single label unless
     * allow_multi is set.
     */
    for (p = wildcard_start; p != wildcard_end; ++p)
        if (!(('0' <= *p && *p <= '9') ||
              ('A' <= *p && *p <= 'Z') ||
              ('a' <= *p && *p <= 'z') ||
              *p == '-' || (allow_multi && *p == '.')))
            return 0;
    return 1;
}

#define LABEL_START     (1 << 0)
#define LABEL_END       (1 << 1)
#define LABEL_HYPHEN    (1 << 2)
#define LABEL_IDNA      (1 << 3)

static const unsigned char *valid_star(const unsigned char *p, size_t len,
                                       unsigned int flags)
{
    const unsigned char *star = 0;
    size_t i;
    int state = LABEL_START;
    int dots = 0;
    for (i = 0; i < len; ++i) {
        /*
         * Locate first and only legal wildcard, either at the start
         * or end of a non-IDNA first and not final label.
         */
        if (p[i] == '*') {
            int atstart = (state & LABEL_START);
            int atend = (i == len - 1 || p[i + 1] == '.');
            /*-
             * At most one wildcard per pattern.
             * No wildcards in IDNA labels.
             * No wildcards after the first label.
             */
            if (star != NULL || (state & LABEL_IDNA) != 0 || dots)
                return NULL;
            /* Only full-label '*.example.com' wildcards? */
            if ((flags & X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS)
                && (!atstart || !atend))
                return NULL;
            /* No 'foo*bar' wildcards */
            if (!atstart && !atend)
                return NULL;
            star = &p[i];
            state &= ~LABEL_START;
        } else if (('a' <= p[i] && p[i] <= 'z')
                   || ('A' <= p[i] && p[i] <= 'Z')
                   || ('0' <= p[i] && p[i] <= '9')) {
            if ((state & LABEL_START) != 0
                && len - i >= 4 && strncasecmp((char *)&p[i], "xn--", 4) == 0)
                state |= LABEL_IDNA;
            state &= ~(LABEL_HYPHEN | LABEL_START);
        } else if (p[i] == '.') {
            if ((state & (LABEL_HYPHEN | LABEL_START)) != 0)
                return NULL;
            state = LABEL_START;
            ++dots;
        } else if (p[i] == '-') {
            /* no domain/subdomain starts with '-' */
            if ((state & LABEL_START) != 0)
                return NULL;
            state |= LABEL_HYPHEN;
        } else
            return NULL;
    }

    /*
     * The final label must not end in a hyphen or ".", and
     * there must be at least two dots after the star.
     */
    if ((state & (LABEL_START | LABEL_HYPHEN)) != 0 || dots < 2)
        return NULL;
    return star;
}

/* Compare using wildcards. */
static int equal_wildcard(const unsigned char *pattern, size_t pattern_len,
                          const unsigned char *subject, size_t subject_len,
                          unsigned int flags)
{
    const unsigned char *star = NULL;

    /*
     * Subject names starting with '.' can only match a wildcard pattern
     * via a subject sub-domain pattern suffix match.
     */
    if (!(subject_len > 1 && subject[0] == '.'))
        star = valid_star(pattern, pattern_len, flags);
    if (star == NULL)
        return equal_nocase(pattern, pattern_len,
                            subject, subject_len, flags);
    return wildcard_match(pattern, star - pattern,
                          star + 1, (pattern + pattern_len) - star - 1,
                          subject, subject_len, flags);
}

/*
 * Compare an ASN1_STRING to a supplied string. If they match return 1. If
 * cmp_type > 0 only compare if string matches the type, otherwise convert it
 * to UTF8.
 */

static int do_check_string(const ASN1_STRING *a, int cmp_type, equal_fn equal,
                           unsigned int flags, const char *b, size_t blen,
                           char **peername)
{
    int rv = 0;

    if (!a->data || !a->length)
        return 0;
    if (cmp_type > 0) {
        if (cmp_type != a->type)
            return 0;
        if (cmp_type == V_ASN1_IA5STRING)
            rv = equal(a->data, a->length, (unsigned char *)b, blen, flags);
        else if (a->length == (int)blen && !memcmp(a->data, b, blen))
            rv = 1;
        if (rv > 0 && peername)
            *peername = OPENSSL_strndup((char *)a->data, a->length);
    } else {
        int astrlen;
        unsigned char *astr;
        astrlen = ASN1_STRING_to_UTF8(&astr, (ASN1_STRING*)a);
        if (astrlen < 0) {
            /*
             * -1 could be an internal malloc failure or a decoding error from
             * malformed input; we can't distinguish.
             */
            return -1;
        }
        rv = equal(astr, astrlen, (unsigned char *)b, blen, flags);
        if (rv > 0 && peername)
            *peername = OPENSSL_strndup((char *)astr, astrlen);
            //*peername = strndup((char *)astr, astrlen);
        OPENSSL_free(astr);
    }
    return rv;
}

static int do_x509_check(X509 *x, const char *chk, size_t chklen,
                         unsigned int flags, int check_type, char **peername)
{
    GENERAL_NAMES *gens = NULL;
    X509_NAME *name = NULL;
    int i;
    int cnid = NID_undef;
    int alt_type;
    int san_present = 0;
    int rv = 0;
    equal_fn equal;

    /* See below, this flag is internal-only */
    flags &= ~_X509_CHECK_FLAG_DOT_SUBDOMAINS;
    if (check_type == GEN_EMAIL) {
        cnid = NID_pkcs9_emailAddress;
        alt_type = V_ASN1_IA5STRING;
        equal = equal_email;
    } else if (check_type == GEN_DNS) {
        cnid = NID_commonName;
        /* Implicit client-side DNS sub-domain pattern */
        if (chklen > 1 && chk[0] == '.')
            flags |= _X509_CHECK_FLAG_DOT_SUBDOMAINS;
        alt_type = V_ASN1_IA5STRING;
        if (flags & X509_CHECK_FLAG_NO_WILDCARDS)
            equal = equal_nocase;
        else
            equal = equal_wildcard;
    } else {
        alt_type = V_ASN1_OCTET_STRING;
        equal = equal_case;
    }

    if (chklen == 0)
        chklen = strlen(chk);

    gens = (GENERAL_NAMES*)X509_get_ext_d2i(x, NID_subject_alt_name, NULL, NULL);
    if (gens) {
        for (i = 0; i < sk_GENERAL_NAME_num(gens); i++) {
            GENERAL_NAME *gen;
            ASN1_STRING *cstr;
            gen = sk_GENERAL_NAME_value(gens, i);
            if (gen->type != check_type)
                continue;
            san_present = 1;
            if (check_type == GEN_EMAIL)
                cstr = gen->d.rfc822Name;
            else if (check_type == GEN_DNS)
                cstr = gen->d.dNSName;
            else
                cstr = gen->d.iPAddress;
            /* Positive on success, negative on error! */
            if ((rv = do_check_string(cstr, alt_type, equal, flags,
                                      chk, chklen, peername)) != 0)
                break;
        }
        GENERAL_NAMES_free(gens);
        if (rv != 0)
            return rv;
        if (san_present && !(flags & X509_CHECK_FLAG_ALWAYS_CHECK_SUBJECT))
            return 0;
    }

    /* We're done if CN-ID is not pertinent */
    if (cnid == NID_undef || (flags & X509_CHECK_FLAG_NEVER_CHECK_SUBJECT))
        return 0;

    i = -1;
    name = X509_get_subject_name(x);
    while ((i = X509_NAME_get_index_by_NID(name, cnid, i)) >= 0) {
        const X509_NAME_ENTRY *ne = X509_NAME_get_entry(name, i);
        const ASN1_STRING *str = X509_NAME_ENTRY_get_data((X509_NAME_ENTRY*)ne);

        /* Positive on success, negative on error! */
        if ((rv = do_check_string(str, -1, equal, flags,
                                  chk, chklen, peername)) != 0)
            return rv;
    }
    return 0;
}

int X509_check_host(X509 *x, const char *chk, size_t chklen,
                    unsigned int flags, char **peername)
{
    if (chk == NULL)
        return -2;
    /*
     * Embedded NULs are disallowed, except as the last character of a
     * string of length 2 or more (tolerate caller including terminating
     * NUL in string length).
     */
    if (chklen == 0)
        chklen = strlen(chk);
    else if (memchr(chk, '\0', chklen > 1 ? chklen - 1 : chklen))
        return -2;
    if (chklen > 1 && chk[chklen - 1] == '\0')
        --chklen;
    return do_x509_check(x, chk, chklen, flags, GEN_DNS, peername);
}
