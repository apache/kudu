// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/security/test/test_certs.h"

#include <string>

#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

using std::string;

namespace kudu {
namespace security {

//
// The easiest way to create RSA private key and CA self-signed certificate pair
// is using the couple of commands below:
//
//   openssl genrsa -out ca.pkey.pem 2048
//   openssl req -new -x509 -batch -days 3650 -key ca.pkey.pem -out ca.cert.pem
//
// NOTE:
//   The latter command uses configuration properties from default configuration
//   file of the OpenSSL library.  Also, it runs in batch mode due to the
//   '-batch' flag. To specify custom certificate subject properties, omit
//   the '-batch' flag and run the command in interactive mode. If more
//   customization is needed, see the other methods below.
//
////////////////////////////////////////////////////////////////////////////
//
// The other way to create RSA private key and CA self-signed certificate pair
// is using OpenSSL's CA.sh script in $OPENSSL_SRC_ROOT/apps:
//
//   cp $OPENSSL_SRC_ROOT/CA.sh .
//   chmod +x CA.sh
//   ./CA.sh -newca
//
// Find the newly generated files at the following locations:
//   * demoCA/cacert.pem:         self-signed CA certificate
//   * demoCA/private/cakey.pem:  encrypted CA private key
//
// To decrypt the generated private key, run the following command and provide
// the pass phrase (assuming that was an RSA key):
//
//   openssl rsa -in ./demoCA/private/cakey.pem
//
////////////////////////////////////////////////////////////////////////////
//
// Besides, the following sequence of commands can used to create
// a private key and CA certficate with custom properties.
//
//  * Create a separate directory, e.g.:
//
//      mkdir /tmp/cert && cd /tmp/cert
//
//  * Create custom my.cnf configuration file for the OpenSSL library, copying
//    the default one and modifying the result, if necessary.
//
//      cp $OPENSSL_CFG_ROOT/etc/openssl.cnf my.cnf
//      vim my.cnf
//
//  * Create the CA directory structure which matches the directory structure
//    of the 'default_ca' section from the configuration file, e.g.:
//
//      mkdir -p demoCA/certs demoCA/crl demoCA/newcerts demoCA/private
//      touch demoCA/index.txt
//
//  * Create private key and certificate signing request (CSR):
//
//      openssl req -new -keyout ca.pkey.pem -out ca.req.pem \
//        -subj "/C=US/ST=CA/O=MyCompany/CN=MyName/emailAddress=my@email.com" \
//        -passin pass:mega_pass -passout pass:mega_pass -batch
//
//  * Create a self-signed certificate using the newly generated CSR as input:
//
//      openssl ca -config my.cnf -create_serial -days 3650 \
//        -keyfile ca.pkey.pem -selfsign -extensions v3_ca \
//        -outdir ./ -out ca.cert.pem -passin pass:mega_pass -batch \
//        -infiles ca.req.pem
//
// The encryped private key is in ca.pkey.pem, the certificate is in
// ca.cert.pem.  To decrypt the generated private key, execute the following
// (assuming that was an RSA key):
//
//   openssl rsa -passin pass:mega_pass -in ./ca.pkey.pem
//
const char kCaCert[] = R"***(
-----BEGIN CERTIFICATE-----
MIIDizCCAnOgAwIBAgIJAIsQXjBhvdPoMA0GCSqGSIb3DQEBCwUAMFwxCzAJBgNV
BAYTAlVTMQswCQYDVQQIDAJDQTESMBAGA1UECgwJTXlDb21wYW55MQ8wDQYDVQQD
DAZNeU5hbWUxGzAZBgkqhkiG9w0BCQEWDG15QGVtYWlsLmNvbTAeFw0xNjEwMjUw
NjAxNThaFw0yNjEwMjMwNjAxNThaMFwxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJD
QTESMBAGA1UECgwJTXlDb21wYW55MQ8wDQYDVQQDDAZNeU5hbWUxGzAZBgkqhkiG
9w0BCQEWDG15QGVtYWlsLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAKexXVOe0SfNexxl1nqMBRy8MCYWTl1kbRt5VQ698aXYcPNBC7gnEBW+8Yaa
2f3Hl1Ye51zUGnOl4FU6HFDiIq59/lKCNG2X3amlYjzkImXn4M56r+5rEWs+HoHW
kuqmMaxnrJatM86Of0K3j5QrOUft/qT5R6vSPnFH/pz+6ccBkAGV0UFVdshYSGkx
KziVTdJ2Ri8oZgyeuReGxLkXOqKHzcOUFinvQ8fe8yaQr1kRAaPRo1eFqORXAMAU
4KyvfiVjZMEGj0p47IekJHVPVVMopEmMMjhzRfbrxrKrMcIG6e4acF1KAd4wGI9A
pCR3e1vcfbghDO7GhTMswLCnMYUCAwEAAaNQME4wHQYDVR0OBBYEFDc1+ybIwvG2
IvEuAusZ9GGMlga/MB8GA1UdIwQYMBaAFDc1+ybIwvG2IvEuAusZ9GGMlga/MAwG
A1UdEwQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAJT9fL/vtayfAHpdzFvdWBe+
R6y5HsVQQTBNF9x1eM6M0vGAlsXGgip3+RH7OMwurxNetL2mc03+PECas5LxB5Pr
u1+kwtmv5YyfQzou0VwztjcbK2OEpWKj16XX6NO403iKoRF4fLn0DjZQcB0oXw4s
vBxhNfz+SAsjsAMNgLHHXonJfg7wcdmNSp2N3TslGL/DH0bXMhsKx2CuMA3rd9WZ
mJjItRIk8qNjazlmG0KYxQclP3lGagIMHxU6tY+iBXs1JR1/AUnPl/GaPeayCJSR
3PB7R+MMrI0hfWFWkBt0D+UAKVa9to/N06wp4JqxEgOooU08PguXLIVDlW0xBcw=
-----END CERTIFICATE-----
)***";


// See the comment for kCaCert_
const char kCaPrivateKey[] = R"***(
-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAp7FdU57RJ817HGXWeowFHLwwJhZOXWRtG3lVDr3xpdhw80EL
uCcQFb7xhprZ/ceXVh7nXNQac6XgVTocUOIirn3+UoI0bZfdqaViPOQiZefgznqv
7msRaz4egdaS6qYxrGeslq0zzo5/QrePlCs5R+3+pPlHq9I+cUf+nP7pxwGQAZXR
QVV2yFhIaTErOJVN0nZGLyhmDJ65F4bEuRc6oofNw5QWKe9Dx97zJpCvWREBo9Gj
V4Wo5FcAwBTgrK9+JWNkwQaPSnjsh6QkdU9VUyikSYwyOHNF9uvGsqsxwgbp7hpw
XUoB3jAYj0CkJHd7W9x9uCEM7saFMyzAsKcxhQIDAQABAoIBABuZQ0TZ5I5qcRKR
aCUvGkBKcJo0HZ2dQ5+77lXIyRaEcsJ2OBmMxEbv8Aw5PBtaV/vihi1u8xOJf0xH
jhV5wj95mPu3Vi2bSu36vBpNaaPf783Lv1y73lgKFzdDO1bHF3HKdksuIlKifStb
zpOSMZE3CCvaowMSTRiTwsHP6mXIBdQ/TwAZHqGVTWDVGxc8JvoJ/3GjSgUIPKzy
I2aS/5DQ+zmLktuP61GFMJg9tCSrwZPDi/XAatpoAOC9eA7AqF/l1TiaXsQN95mr
mz2DkCoWRzAuDbya2Sh6nTJvpOMPAeXJ/MMZh9TWswJc4OAO2kZZsFfd0H6M1TKy
1eAYKVkCgYEA1JhkKQ2h4cVzqQ9A5+4C0q5+j/RFDUOVnNlIjQiM73RchNu713mK
zzhsom9S/6ZU8OH3TxzD54i2hHtX+QIJqVG0412QgAqAqnAKXGGkkAXiXGfGZhEW
UB3OuTMbhfVqrkpj0wAPiEJAAuek7zES2B+gURUC24aAfOWU8xMkSjMCgYEAye4U
e0NQ4HhhWRgWbgFYeAzsC/ezvlx30JjXiLPCNXGoLLJUCMjqWCPGYUvDonIJbxbj
+MYFkvYSDFGwTobKsB7FyT8DxPNus40zOh47y8QUK7jTL4nAmnBa3W9Oj00ceKpo
wKe/adc2xPrS7mnVpz3ZkJ4I9z/MbEinyV5UTWcCgYAy8gXmlJ67dM6/r6kVK0M/
65Lmulml0RFUUfmB2o+zfkYBjIqaG0U5XUMjNdxE6T4nr27NZY5IuMlMPCabxHI+
Qhc/+Rb8qAenUEwbUUbXQKG7FR9FLEkVj98PIIEy+9nBxI/ha31NYNroF0y+CRuD
8ShA5fEWXEgEJhwol+i1YwKBgEnGeiUuyvW4BZkPe+JlC3WRAwy8SydZkUzdCqIf
Su1LwS3TWXB8N2JMb8ZMcAWBtICp1FCnyJGQ5bcqgUevZ45BL/H+29mxNtjS1cx+
D0q7MMNom3/azEugkRImAIXKnoRXfj4lC4IX5yLAoSAJ+s1Hg52an5v16zIEuYiQ
tiwxAoGAOP8/yjMzit1hzk27k9IfQSLD+1SqKCsRdGbAIhFRFlz4RUQOly1dEX8M
qVmStlQ7N5gQWJSyDTe6rTe8pG9r030kNDJ+etr2KWpATGNaVWSmLWSYBXrPtejK
gmbcYCewtt7dFP9tvx6k7aUQ6CKzg0GxaIHQecNzjxYrw8sb4Js=
-----END RSA PRIVATE KEY-----
)***";

// Corresponding public key for the kCaPrivateKey
const char kCaPublicKey[] = R"***(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp7FdU57RJ817HGXWeowF
HLwwJhZOXWRtG3lVDr3xpdhw80ELuCcQFb7xhprZ/ceXVh7nXNQac6XgVTocUOIi
rn3+UoI0bZfdqaViPOQiZefgznqv7msRaz4egdaS6qYxrGeslq0zzo5/QrePlCs5
R+3+pPlHq9I+cUf+nP7pxwGQAZXRQVV2yFhIaTErOJVN0nZGLyhmDJ65F4bEuRc6
oofNw5QWKe9Dx97zJpCvWREBo9GjV4Wo5FcAwBTgrK9+JWNkwQaPSnjsh6QkdU9V
UyikSYwyOHNF9uvGsqsxwgbp7hpwXUoB3jAYj0CkJHd7W9x9uCEM7saFMyzAsKcx
hQIDAQAB
-----END PUBLIC KEY-----
)***";

// See the comment for kCaCert_
// (but use '-1' as number of days for the certificate expiration).
const char kCaExpiredCert[] = R"***(
-----BEGIN CERTIFICATE-----
MIIDjTCCAnWgAwIBAgIJALNJes+nGWH9MA0GCSqGSIb3DQEBCwUAMF0xCzAJBgNV
BAYTAlVTMQswCQYDVQQIDAJDQTETMBEGA1UECgwKRXhwQ29tcGFueTEQMA4GA1UE
AwwHRXhwTmFtZTEaMBgGCSqGSIb3DQEJARYLZXhwQGV4cC5jb20wHhcNMTYxMDI1
MTkzOTM4WhcNMTYxMDI0MTkzOTM4WjBdMQswCQYDVQQGEwJVUzELMAkGA1UECAwC
Q0ExEzARBgNVBAoMCkV4cENvbXBhbnkxEDAOBgNVBAMMB0V4cE5hbWUxGjAYBgkq
hkiG9w0BCQEWC2V4cEBleHAuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAzqPj5nRm57mr9YtZDvHREuVFHTwPcKzDeff9fnrKKwOJPSF0Bou/BjS1
S7yQYAtmT/EMi7qxEWjgrR1qW+muR8QN+zAwNdkdLrFK3SJigQ4a/OeSH86aHXUD
ekV8mgBgzP90osbHf7AiqrGzkYWq+ApTO/IgnXgaWbbdt5znGTW5lKQ4O2CYhpcM
MC1sBBjW7Qqx+Gi8iXub0zlJ2mVI8o+zb9qvSDb8fa0JYxasRDn/nB0wKZC3f/Gf
Rs+lJZUTEy5+eMhVdj1RjVBE+mgW7L27On24ViPU7B3DjM0SYnD6ZOUWMH0mtwO8
W3OoK8MJhPvFP7Lr5QfSjiBH+ryLOwIDAQABo1AwTjAdBgNVHQ4EFgQUsp8OZLl1
2Z/2aXBQRH0Z+nWxqXcwHwYDVR0jBBgwFoAUsp8OZLl12Z/2aXBQRH0Z+nWxqXcw
DAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEArWvFi13iqmvnY0xkgt3R
acurTvWcQzcUOgVPF8u1atj9d+0zrMk7Don1KQp6uYLdeNLL8NbL4oLxtidW/Yap
ZEbHVDQTeZAsT7Hr+0uD3vMUndsjG7C85tOhZMiGukFPhuaHE5KmQEy6nUCaJiAv
opZlNj1mEOGyshSXHsBATl9o33WLTLfPqrO3/12jExApHiADcON4RsPUV6M6k5A2
/KghYEPYAuFfXTsqj+W7HRL1UuiHJxW96ySQqYzQ86aRN2ZZlTdbDnIU5Jrb6YJB
hUALcxIUhtodui61zsJFIkVauxTxk7jNCwRvj4I1dSSFWA63t9eh7sKilLRCayNl
yQ==
-----END CERTIFICATE-----
)***";

// See the comment for kCaExpiredCert_
const char kCaExpiredPrivateKey[] = R"***(
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAzqPj5nRm57mr9YtZDvHREuVFHTwPcKzDeff9fnrKKwOJPSF0
Bou/BjS1S7yQYAtmT/EMi7qxEWjgrR1qW+muR8QN+zAwNdkdLrFK3SJigQ4a/OeS
H86aHXUDekV8mgBgzP90osbHf7AiqrGzkYWq+ApTO/IgnXgaWbbdt5znGTW5lKQ4
O2CYhpcMMC1sBBjW7Qqx+Gi8iXub0zlJ2mVI8o+zb9qvSDb8fa0JYxasRDn/nB0w
KZC3f/GfRs+lJZUTEy5+eMhVdj1RjVBE+mgW7L27On24ViPU7B3DjM0SYnD6ZOUW
MH0mtwO8W3OoK8MJhPvFP7Lr5QfSjiBH+ryLOwIDAQABAoIBABszgcWNXxpz24oI
HOIVvPLi0VVG2bV4WIcOuQTUPxaocYFljPNro+q6N39PxCWQephdX8xo9/QVvTWs
oJqWyUVTLo/5SO9dtDS4S+WOKC9a3vyZsyeSt8DW7W1EBmHzWMrDeeQPjKVnVzjn
CX9HfDkIiupiNh7kd3uF0evgsJ8lsZ65HtBq9MWu+mIR1H0EpRLxywdoRJLJ+JdW
g1fLFRuhnWo0GcEyBK45kLCoVJsRbCkFGf6uPDOOC0g5mIyxGclWeF6ps1OFnFyu
FWsYeMLSt5tYZfB0/QR46X9HQOhfLunjA04VBkScSRjlohGO4d20ZW7HlPY20CbR
1PHhEvkCgYEA98FYoovNezx8OgkcAtNOOTK7GpUaUfh3Xl5yPGgCqxoG8G+BTmKF
MGlIf6URKQA0BUtNdjIvfIcaIctj56qFwjHL6CbzR5MkXUZLlyl0XzYFXm/lavr4
Z5DHWdFo+GyFaiXIiVof93jAnOFgjSxdhHaEhQqj7pmaBoHVZqtwHFcCgYEA1YRH
xTzcWErp06KJTt+/P4YtWRh9GDBhhlO3oaGOANkEab8cGjRO9LJP24wyo7exXqGb
UjtEifEHtzhj6a/UwSAMsFcNhlQRvy525HD1gJmQ2m4wZ3GxztK4IZ4rVDjsB5/D
SMMBsDfs1r1iRwdSMHAOhrVH2l/DMFQLnx1x+b0CgYEAlQm6SA3RjlDUahUQxKJY
bBAYfeUz8BuHsz0dezkWYddGVVy+bGjXtkefVSn3KLL2mDi0YGXQKxkanzm636G0
1R0fjIfh0Syys2mWD1jgqGXW1Ph7Cd/vjl2Jjn5qpwahOzl/aSDOGhCJzdXGPyZx
Gz4wedfsxZuhDEkOFrUKvAECgYEAxHYYy8V6Qct8Z30wtmBuSvcdFtPPlsg9lCnH
13MdhG4q/1oXc40Z8VF45VyU48uL6rTsg7eBEyOyo8XBOS7Opnzk8ATJrwX/5lfM
kdnWK2QhwrqM00HsB5AgWN5+o9pUY5d/Sp4UGZ77z4MmwJBd8a/Jze1Tlf1zTi6n
GtsvGkkCgYAfILUAPf+ujgB9zdsJa+4l9XCEq0j39/Usfj0VrInNAk7RN8W0qNw7
ZLs3Qt2fgPO0CeMeVUVKcvdjlXq3EbrWKrsJLxy3Gb8ruBjIlJqncJn6mKslXS+l
H/sbP2R+P6RvQceLEEtk6ZZLiuScVmLtVOpUoUZb3Rx6a7GKbec7oQ==
-----END RSA PRIVATE KEY-----
)***";

// Corresponding public part of the kCaExpiredPrivateKey
const char kCaExpiredPublicKey[] = R"***(
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzqPj5nRm57mr9YtZDvHR
EuVFHTwPcKzDeff9fnrKKwOJPSF0Bou/BjS1S7yQYAtmT/EMi7qxEWjgrR1qW+mu
R8QN+zAwNdkdLrFK3SJigQ4a/OeSH86aHXUDekV8mgBgzP90osbHf7AiqrGzkYWq
+ApTO/IgnXgaWbbdt5znGTW5lKQ4O2CYhpcMMC1sBBjW7Qqx+Gi8iXub0zlJ2mVI
8o+zb9qvSDb8fa0JYxasRDn/nB0wKZC3f/GfRs+lJZUTEy5+eMhVdj1RjVBE+mgW
7L27On24ViPU7B3DjM0SYnD6ZOUWMH0mtwO8W3OoK8MJhPvFP7Lr5QfSjiBH+ryL
OwIDAQAB
-----END PUBLIC KEY-----
)***";

const char kCertDnsHostnamesInSan[] = R"***(
-----BEGIN CERTIFICATE-----
MIIEPzCCAyegAwIBAgIJAJoczuNKGspGMA0GCSqGSIb3DQEBCwUAMFwxCzAJBgNV
BAYTAlVTMQswCQYDVQQIDAJDQTESMBAGA1UECgwJTXlDb21wYW55MQ8wDQYDVQQD
DAZNeU5hbWUxGzAZBgkqhkiG9w0BCQEWDG15QGVtYWlsLmNvbTAeFw0xNzA0Mjgx
OTUwNTVaFw0yNzA0MjYxOTUwNTVaMAAwXDANBgkqhkiG9w0BAQEFAANLADBIAkEA
rpJhLdS/Euf2cu0hPXkvkocLO0XbNtFwXNjkOOjuJZd65FHqLb6TmmxxDpL7fB94
Mq1fD20fqdAgSVzljOyvuwIDAQABo4ICJjCCAiIwDgYDVR0PAQH/BAQDAgWgMCAG
A1UdJQEB/wQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjAMBgNVHRMBAf8EAjAAMIIB
3gYDVR0RBIIB1TCCAdGCDG1lZ2EuZ2lnYS5pb4ILZm9vLmJhci5jb22CggGydG9v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vLmxvb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29v
b29vb29vb29vb29vb29vb29vb29vb29vb29vb29vb29vbmcuaG9zdG5hbWUuaW8w
DQYJKoZIhvcNAQELBQADggEBAIKVVABj3nTyqDEnDKDfvS6QNEQSa1mw7WdFTpzH
6cbMfiixVyyLqfAV4NZ+PnIa4mpsWP5LrsrWFVK/HtiTX7Y8oW0qdA04WtYd9VUT
BgWKHyLygbA+PSZ6GdXFjZd8wDthK0qlT2MfZJUwD36eYnBxuonU8a4lmxaUG2zC
L8FplhNJUEt6XfJ0zZGx1VHe12LLjgMz3ShDAmD9DlHHFjJ1aQ/17OGmmjWmbWnm
an4ys5seqeHuK2WzP3NAx7LOwe/R1kHpEAX/Al6xyLIY3h7BBzurpgfrO6hTTECF
561gUMp+cAvogw074thF5j4b+uEK5Bl8nzN2h8BwwxwGzUo=
-----END CERTIFICATE-----
)***";

//
// The reference signatures were obtained by using the following sequence:
//  0. The reference private key was saved into /tmp/ca.pkey.pem file.
//  1. Put the input data into /tmp/in.txt file.
//  2. To sign the input data, run
//    openssl dgst -sign /tmp/ca.pkey.pem -sha512 -out /tmp/out /tmp/in.txt
//  3. To capture the signature in text format, run
//    base64 -b 60 /tmp/out
//
const char kDataTiny[] = "Tiny";
const char kSignatureTinySHA512[] =
    "omtvSpfj9tKo0RdI4zJwasWSQnXl++aKVjhH19ABJCd0haKT8RXNuhnxcbZU"
    "Y1ILE5F9YjVj+tN/7ah5WQZR5qlJ6GMFfCFBhOzvi/vf5PSbUrFfwFvFD6sq"
    "Bu0PWdwKM3t8/YFE2HcZWSzGCcasKlG/aw2eQCN3Kdv8QVMlC28CFA/EqQBt"
    "8Sfye1DLba33SzDpJqR2DduTFrEW2UffumpYIbkEcMwUSBFzfdp5hgWPowFb"
    "LrnKvyWKpEPMFGQmf5siyXSkbBIfL774tynhWN/lAUWykwXSUfGgi2G0NQvj"
    "xmuHhbxWpbW/31uMGssw92OfVQ/+aQ4pNmY9GbibcA==";

const char kDataShort[] = "ShortRefInputData";
const char kSignatureShortSHA512[] =
    "BHaDipr8ibn40BMD6+DlatKsjbmsGZsJIDlheppBjqv66eBDLKOVjpmpMLl9"
    "9lXCGUlVS+cNcVP4RPDzXNoXkpzUOJD3UQSnxCAm6tV1eGjD3SHi3fk6PCNc"
    "MhM/+09fA0WHdIdZm93cpHt6c9MFzB/dUjHJByhQ7Csmz2zdITyMIl3/D+bi"
    "ocW0aIibk0wNGn/FmXfgFDP+3pBS2bpS0AdFnckX8AqXHFMJnvqKYODpYCW8"
    "NWFSD1TgZOumu/gzxm+HySPezQ2j9tdR6nb9swfShvN+o0oBVGq5vgtgZMTM"
    "7Ws+BrasLfvQFkvtGMWB9VeH/rDlGOym8RwUrCIJJQ==";

const char kDataLong[] =
R"***(Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
)***";
const char kSignatureLongSHA512[] =
    "kc62qPHApVFbueR1xSCQJR5NomqDRzVA+4Xi9egVyfkKgpVhDAYGxbMl8OTY/YCb"
    "eQuwY+B7RGxF9sj3gvsq/dvrbIjLT3QDhs0bv+lXTtBQ5r9zrals3de0tEFrPoLr"
    "CkKPhVZaG+zwmUVltfsdlsqvepy6rNW7BocehvgpPTbzxgsZg4nUANsjSy8HBoDb"
    "xWyfbkMgBY4aWIH1g+wksq1DHzdTNdZCYstupRwVw/ESC+zrFQiZPFeRE/wCSeG/"
    "bd0L8TcotQHJchZ8THW0rEbuCg79I7Crd1KQYljBpOOhMYZEDEdM9L19JlaMlw+Z"
    "leyLfL8Bw3wCg9cMfNmQfQ==";


Status CreateTestSSLCerts(const string& dir,
                          string* cert_file,
                          string* key_file,
                          string* key_password) {
  const char* kCert = R"(
-----BEGIN CERTIFICATE-----
MIIDXTCCAkWgAwIBAgIJAOOmFHYkBz4rMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTYxMTAyMjI0OTQ5WhcNMTcwMjEwMjI0OTQ5WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29tZS1TdGF0ZTEhMB8GA1UECgwYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAppo9GwiDisQVYAF9NXl8ykqo0MIi5rfNwiE9kUWbZ2ejzxs+1Cf7WCn4
mzbkJx5ZscRjhnNb6dJxtZJeid/qgiNVBcNzh35H8J+ao0tEbHjCs7rKOX0etsFU
p4GQwYkdfpvVBsU8ciXvkxhvt1XjSU3/YJJRAvCyGVxUQlKiVKGCD4OnFNBwMdNw
7qI8ryiRv++7I9udfSuM713yMeBtkkV7hWUfxrTgQOLsV/CS+TsSoOJ7JJqHozeZ
+VYom85UqSfpIFJVzM6S7BTb6SX/vwYIoS70gubT3HbHgDRcMvpCye1npHL9fL7B
87XZn7wnnUem0eeCqWyUjJ82Uj9mQQIDAQABo1AwTjAdBgNVHQ4EFgQUOY7rpWGo
ZMrmyRZ9RohPWVwyPBowHwYDVR0jBBgwFoAUOY7rpWGoZMrmyRZ9RohPWVwyPBow
DAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEATKh3io8ruqbhmopY3xQW
A2pEhs4ZSu3H+AfULMruVsXKEZjWp27nTsFaxLZYUlzeZr0EcWwZ79qkcA8Dyj+m
VHhrCAPpcjsDACh1ZdUQAgASkVS4VQvkukct3DFa3y0lz5VwQIxjoQR5y6dCvxxX
T9NpRo/Z7pd4MRhEbz3NT6PScQ9f2MTrR0NOikLdB98JlpKQbEKxzbMhWDw4J3mr
mK6zdemjdCcRDsBVPswKnyAjkibXaZkpNRzjvDNAgO88MKlArCYoyRZqIfkcSXAw
wTdGQ+5GQLsY9zS49Rrhk9R7eOmDhaHybdRBDqW1JiCSmzURZAxlnrjox4GmC3JJ
aA==
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFDjBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQIA3+5nR+Jr18CAggA
MBQGCCqGSIb3DQMHBAhCwHFGbZEcBgSCBMjT9vVbrYpd1reGwfLhk8703ihlspZi
cm4Z2MM+lkJs0Pi8O4n+zNSTfgrEr2XGlKIktBWEBxbhYrdYy1bYm4Fu5Z055JFo
89L/9zT1Zm/agk3CUW+ljirYZF60t/dDWmzLwt9f4dp8m4etL/UwvMJ1NglyxMkj
c03+aWWh4wHLRGkGDJFsKEQY87LL+nAOZS7P1qY38HnzQTOgLyNpntXX3SryzvQ6
CpNIyQFhVGXfGn6DzGJB76heyLpCyAvIiP87vBS3zbnSqDM6v6PTW3SMo8R42RfL
d0CVmO8Z8NQeX29EkMHSRu7gCwXu1pf40QIog2vJZ7dmUgsU9GbBSg8l3nVWS6sm
AICNwPvHXRMMGX0wBJyK2ihuC7rVd5aZLgmu1sjlLYaB9KkoETcFFT8KbFpnd6aR
1whXQ4rPm1WyGtqbVGkZthisvGUeeGnbv2HXUVthSGleD+hQuwFXa8wE/8+Ruq9X
rNv/WMrf2NLIm+wbr19JzVSvLh+j7mIBMZmIwGQvBPo3/Tuq2zeyZdfSOroFcanq
Lyoc6yF5rAkU5BLVe36e48MarWICWDCxiz1n6tWdCpcXWfBvlWIkkjP1/rhqnOW3
DKNjTyGJhaVYydkseoGrrpj4gkyyWtpgw+8c7jdtk+7cmIxpXu3UU6fh+Yt3vEhF
VqHvCd+YuUgpJ+TW574xiau4xbyib5Qv6JAR1Qp3MtzZ1IngyCU4QqqgxBGMBVqc
2LI7Romw7icfdzJxMeMp9WXh8D0Bxx5kjDcO0eUnkpVkFyozXZkoLCnoJ8u3yJo6
yV4RQ4mOAWj7uZYg9KEUywNCHuIVPKG0CEfQkxKiPw4uvmdimKZ6Ij7aqrrc68Vi
oZnNHJEfJhnG78MKHgxXHNrMLFXBgPpLoBQxUBVhI2nq5D2l7gL5bKc9JZNg+mRJ
CouijXBHS1nZ/7GwVjLvNIGKWEsuiz1P0SYki1S02/3bBF9ySdeNGl1XTNqK4Xqy
arK4agJc89wg3N6SOIA+q8kA4LScafMtCkVDChw0CcLUrQERpH1tv1cqCt5zXF9n
5AnnWmEM2knlkHxXzg7k/1YXUz4JMmAhS4gVHuNU1uZR152lD+kgSy2K4sCyCfx2
iWFpDGj556AUxDRGrqKU7OLC/64AuNz5IJs8doDa29cGGKFw3/foRoOaya84ISGW
GTl2qDOHZrJbgR6BUpSh2E2mVyO3GwIBst6yIb5VaTpNuIwS6fhjC4fQZEV6hHcx
qNvHxTTvz6eag4TeUPR48h/kGsI44DB0r4I79WbTwg5dvdlYbchPIwAs888bxpd6
7ZxSg7EwuyHqJEL0FkWcDgw89+vLDETQiTwfscDxwm893gTymj5JPSDz35kudPlI
rsNfABLeXSg8Z8/7LsPP6Q48c1jisLVPPndV80cS791dvyXRxZWvX2z5UFuTDy3K
PV3L60mdejXudzFPfvovhgJDIWsKMmlxYplRWvG3WUXTck1Kb7KEcZmuo4nJMOID
6caoDNa5L9p5XH54sBCB7uqTNdqijaqq9iBFx/MqL3LHt6/wVF5J9g6PmuDxuYDX
tBKU0ns67U6wUxvLGBX/7RnWUibc5JwVGPBGw1E5u6MKWxW9Q6Dk1WakAtsqtDkR
WEc=
-----END ENCRYPTED PRIVATE KEY-----
)";
  const char* kKeyPassword = "test";

  *cert_file = JoinPathSegments(dir, "test.cert");
  *key_file = JoinPathSegments(dir, "test.key");
  *key_password = kKeyPassword;

  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kCert, *cert_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kKey, *key_file));
  return Status::OK();
}

} // namespace security
} // namespace kudu
