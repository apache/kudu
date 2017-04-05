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

Status CreateTestSSLCertWithPlainKey(const string& dir,
                                     string* cert_file,
                                     string* key_file) {
  const char* kCert = R"(
-----BEGIN CERTIFICATE-----
MIIEejCCA2KgAwIBAgIJAKMdvDR5PL82MA0GCSqGSIb3DQEBBQUAMIGEMQswCQYD
VQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZyYW5j
aXNjbzERMA8GA1UEChMIQ2xvdWRlcmExEjAQBgNVBAMTCWxvY2FsaG9zdDEhMB8G
CSqGSIb3DQEJARYSaGVucnlAY2xvdWRlcmEuY29tMB4XDTEzMDkyMjAwMjUxOFoX
DTQxMDIwNzAwMjUxOFowgYQxCzAJBgNVBAYTAlVTMRMwEQYDVQQIEwpDYWxpZm9y
bmlhMRYwFAYDVQQHEw1TYW4gRnJhbmNpc2NvMREwDwYDVQQKEwhDbG91ZGVyYTES
MBAGA1UEAxMJbG9jYWxob3N0MSEwHwYJKoZIhvcNAQkBFhJoZW5yeUBjbG91ZGVy
YS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCoUj3pMQ2ELkrz
zq+koixljVFBAEEqwUWSjA+GEKwfFb/UPRjeO/wrKndp2r83jc6KRt66rvAIl8cr
b54yTOsJ/ZcARrjTwG3IG8Tely/54ZQyH0ImdJyEbCSoI04zX3ovjlppz3g5xanj
WmpAh6pzPgBOTfisCLMPD70xQ8F//QWZdNatoly54STkTWoJv/Oll/UpXcBY8JOR
+ytX82eGgG4F8YoQqmbjrrN5JAmqLRiBAkr3WUena6ekqJBalJRzex/Wh8a9XEV7
9HFVVngBhezsOJgf81hzBzzhULKfxuXl8uaUj3Z9cZg39CDsyz+ULYbsPm8VoMUI
VCf7MUVTAgMBAAGjgewwgekwHQYDVR0OBBYEFK94kea7jIKQawAIb+0DqsA1rf6n
MIG5BgNVHSMEgbEwga6AFK94kea7jIKQawAIb+0DqsA1rf6noYGKpIGHMIGEMQsw
CQYDVQQGEwJVUzETMBEGA1UECBMKQ2FsaWZvcm5pYTEWMBQGA1UEBxMNU2FuIEZy
YW5jaXNjbzERMA8GA1UEChMIQ2xvdWRlcmExEjAQBgNVBAMTCWxvY2FsaG9zdDEh
MB8GCSqGSIb3DQEJARYSaGVucnlAY2xvdWRlcmEuY29tggkAox28NHk8vzYwDAYD
VR0TBAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAQEAEtkPPncCnN2IFVJvz04K+VsX
b6w3qwPynQKc67+++JkNb3TYKrh/0UVM1NrEOu3TGplqOrKgAlITuaWNqNOSBu1R
WJtrz85YkonED5awjjuALVEY82+c7pOXkuv5G5421RINfRn2hNzgw8VFb5CEvxHH
jER80Vx6UGKr/S649qTQ8AzVzTwWS86VsGI2azAD7D67G/IDGf+0P7FsXonKY+vl
vKzkfaO1+qEOLtDHV9mwlsxl3Re/MNym4ExWHi9txynCNiRZHqWoZUS+KyYqIR2q
seCrQwgi1Fer9Ekd5XNjWjigC3VC3SjMqWaxeKbZ2/AuABJMz5xSiRkgwphXEQ==
-----END CERTIFICATE-----
  )";
  const char* kKey = R"(
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAqFI96TENhC5K886vpKIsZY1RQQBBKsFFkowPhhCsHxW/1D0Y
3jv8Kyp3adq/N43Oikbeuq7wCJfHK2+eMkzrCf2XAEa408BtyBvE3pcv+eGUMh9C
JnSchGwkqCNOM196L45aac94OcWp41pqQIeqcz4ATk34rAizDw+9MUPBf/0FmXTW
raJcueEk5E1qCb/zpZf1KV3AWPCTkfsrV/NnhoBuBfGKEKpm466zeSQJqi0YgQJK
91lHp2unpKiQWpSUc3sf1ofGvVxFe/RxVVZ4AYXs7DiYH/NYcwc84VCyn8bl5fLm
lI92fXGYN/Qg7Ms/lC2G7D5vFaDFCFQn+zFFUwIDAQABAoIBABNTpiIxbLDhs998
uvQ3XsumR08kXVcwa/GgvWOSZIEJOUaAYWubDaBTNvTjlhMl6DI+YvKihZMnAkp9
fXefF1nFUWJJvI0ryi8w6RD54RtbCG4c4raRqysVU7wumZsSenAdc0o09UQE6zXc
uth/+1VSKCzVjRkLwquXDg0rD3vHfQHWQvQlzwUh3OACA3LfLezVqzrEB02YVRxm
xwg5veeMg6Aod8vsvsajry9eE0hKeFGonRANerL9lwZxzD2ZjU2fSEJYY3xxKVgi
o+QVTKaAt9pivOs10YVZDcIDH0xmDpxAkaLb5ZAbnjwhf7WGYgEm8VruAHkJxyXX
yPf3rpkCgYEA0dp/Xv5KBIkD6JJao8hnhtP5x9U7o/pTzRxaO3WUflvTI6DtC0nk
cTOwFVs4WljT76T937q2x4stuX0woHzvIaZ6wwZ2vv81ikDY6pE8eLWsH/xFAmkx
HBfkSijFgJV6EpTqUnFD7QKU89tzWrh/kxaMO1WgFaBhxPPs3K1LDTUCgYEAzVW5
3yjfVHNgjWTeAbnbToGvUihOidvIvS5cVo5q0Dhfabz0tiXFxAoQUGErUGPC8Nu2
n/HxTI3b0PbCCwjBsDNqX2kzxTSe5aTGIrBUWbped1bxme8jggXuWYbg8vvLpsYf
wAJPxnGIxW/v/aftHUhbTIuVfZX2+UnilrwiwWcCgYEAg8paz4eXaH277KVtMwq6
qZwac/tgNz0Qv/GcYVcYaLq2QNvhwoMnakhxvxfIrkS25PuTTJxwCaVIlAMhNMkB
TPrGghBfJtgUAb1z/Ow1NAG0FWpS1I7HfsMqZcBxOK2nOmA3QItNg11pujQJn+Ha
jL9OVj0SCkLs48nk6ToTtjkCgYEAh8YCtNwq6IWuN3CWGCAUMpIwIqxCWof48Zch
OZ7MZEiSVrG6QmMxpRJefTfzUyHUOj2eQZ7SxqMa0c8IuhEdOeyVjudaczD7TLAq
z68252oDovfbo8Tr/sL7OzmjryfuHqXtQqKEq5xRKvR8hYavlGhO7otx2uv5thcz
/CYE+UsCgYAsgewfzbcBqJwsAUs98DK99EU8VqKuaYlU5wNvAVb27O6sVeysSokM
G1TGIXJPphA3dSAe4Pf/j4ff/eGaS20FAFhs4BPpw0fAeCHpmD0BjIba0lxBS/gY
dc+JVPKL8Fe4a8fmsI6ndcZQ9qpOdZM5WOD0ldKRc+SsrYKkTmOOJQ==
-----END RSA PRIVATE KEY-----
  )";

  *cert_file = JoinPathSegments(dir, "test.cert");
  *key_file = JoinPathSegments(dir, "test.key");

  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kCert, *cert_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kKey, *key_file));
  return Status::OK();
}

Status CreateTestSSLCertWithEncryptedKey(const string& dir,
                                         string* cert_file,
                                         string* key_file,
                                         string* key_password) {
  const char* kCert = R"(
-----BEGIN CERTIFICATE-----
MIIFuTCCA6GgAwIBAgIJAMboiIQH/LDlMA0GCSqGSIb3DQEBCwUAMHMxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQxDzANBgNVBAMMBk15TmFtZTEbMBkGCSqGSIb3DQEJARYM
bXlAZW1haWwuY29tMB4XDTE3MDQxNDE3MzEzOVoXDTI3MDQxMjE3MzEzOVowczEL
MAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVy
bmV0IFdpZGdpdHMgUHR5IEx0ZDEPMA0GA1UEAwwGTXlOYW1lMRswGQYJKoZIhvcN
AQkBFgxteUBlbWFpbC5jb20wggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoIC
AQC+Av808cmgkgtLpH+ERAaJLgpSQ+l2UfUHTB4XeFXcWcxlsRyXqqNTh5NwkjMI
c6Ei8p12PBol9I2j//l9sCmLXWDXq2EFFkZ+tcszPjQiTmqzPeruAnamYzuQFioZ
mnNbPypD9qdk/IWY4XWMWOL/qIhnkNQvswSCqu7JA37xaiOqdLBYt/nSN9h5cOwi
iHQODY15OmrgAB4JO9brHdBp/fzoN3DkHpQ0V5rlEZ+5Ud9qDs3UEQMgo+ZV8wYL
KVb9/sUyWu+i1NJIAIhNt5oC8AXJJt+C5Bqme3+7mkWnnBo9DwsvnqDOjOY6AvpO
NHDeRgEBBelj8rGOGQAFgfTlv+w3kDas25oxmoeVXSPF94eU75bu/EE6GGNpe1EB
ZtfGUSfRLZwBMAeTZ7f1b9xgNygNpBGmwt9bg+qOZ6PYWkGIrP5+Umhjyss26j5r
PzJSummB93+69QoESLnF68WcFrR7fxN+oVra63kic/wvC3RH+P3lAIaYw9dKGtec
D3/F2xBp9+Q3nMJQ5MGDdv4wbWQ9lA63uwcWSGIP3R3KKrs4ULtvHIVQz3tgKbwu
Lw5LM7x3KnV1iMwfJC09I+lv8MxJBS7cxGU7UEyIasIirsZblPTBshjoKPax2RWR
I/VI9HwdA4cCk+zbvklK3hHgemCagVLIGwT5+tU7tY4UOQIDAQABo1AwTjAdBgNV
HQ4EFgQU+4nVu5ZsCzs3wxbyS8LNmkL849AwHwYDVR0jBBgwFoAU+4nVu5ZsCzs3
wxbyS8LNmkL849AwDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAgEAWHLA
OCjmeqf6AWpmoOOx3fUaGzaR0SG1Cn85nDMypdTrZtNUn9mkDDDXH40xP31vUyjx
i2Z68dvHsJCdqYL1KAwvvWIEt8xzIWI9ALCANl7JD1L224lUdXI/SgOVaba5bxf4
nVhqeGH5G7WB6g4ZoOkiICluCnjc5FWaZlENJ4oQLLS9mQE7sREouCy+lDnlUW0x
Mf1A5hiERTmvuy26b/lkjXfdGw0z/lNXNAS2k59cKHZ11FqzSLwzK2betmpJYzcq
N4kPMbfUDrN5x/pjAl//GATQgCiXCUjwGvKnhhXedLjnLUC7bxrAoDwKj986iKnO
v9wzukBC6t/Ao1COodDISzTLORTMIWLOjyg9bPVKSjdFxmKhpCUQQ3Jt6k9JOZtR
hvKVmDZBCB10eCJALsHlDWAy0DgjRrD1dnnXrOIUgq6ZLqtzAKGkQF5Y5sYEXyTm
fCFgiXHtU2haGzp5x+i/vz/E6bBsxJhUVzaWlP149WhQs4RO1YL3Iqsdcy0AcMA9
FUNW6C+37fVk6w1OJGcI4uTfgMpSJL7iTCSzuspR4lEPUHLIvB3kZKyjr7/eAiMg
NU9t8oyYtGfrXWXHEZ+d8vK7KCnvMZ2ezNtMC88tC8NtnJ8yBPxBLS7k1f/IrYVL
OHUKIgiZvAfTg3GSj/iiNespDd665okkzRb0QNQ=
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN ENCRYPTED PRIVATE KEY-----
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
TOQYXv+dMtOkYg==
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
