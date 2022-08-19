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
//   openssl genpkey -out ca.pkey.pem -algorithm RSA -pkeyopt rsa_keygen_bits:2048
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
// To decrypt and convert the generated private key to PKCS#8 format, run the
// following command and provide the pass phrase (assuming that was an RSA key):
//
//   openssl pkcs8 -in ./demoCA/private/cakey.pem -topk8
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
// ca.cert.pem.  To decrypt the generated private key and convert it to PKCS#8
// format, execute the following (assuming that was an RSA key):
//
//   openssl pkcs8 -passin pass:mega_pass -in ./ca.pkey.pem -topk8
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
-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCnsV1TntEnzXsc
ZdZ6jAUcvDAmFk5dZG0beVUOvfGl2HDzQQu4JxAVvvGGmtn9x5dWHudc1BpzpeBV
OhxQ4iKuff5SgjRtl92ppWI85CJl5+DOeq/uaxFrPh6B1pLqpjGsZ6yWrTPOjn9C
t4+UKzlH7f6k+Uer0j5xR/6c/unHAZABldFBVXbIWEhpMSs4lU3SdkYvKGYMnrkX
hsS5Fzqih83DlBYp70PH3vMmkK9ZEQGj0aNXhajkVwDAFOCsr34lY2TBBo9KeOyH
pCR1T1VTKKRJjDI4c0X268ayqzHCBunuGnBdSgHeMBiPQKQkd3tb3H24IQzuxoUz
LMCwpzGFAgMBAAECggEAG5lDRNnkjmpxEpFoJS8aQEpwmjQdnZ1Dn7vuVcjJFoRy
wnY4GYzERu/wDDk8G1pX++KGLW7zE4l/TEeOFXnCP3mY+7dWLZtK7fq8Gk1po9/v
zcu/XLveWAoXN0M7VscXccp2Sy4iUqJ9K1vOk5IxkTcIK9qjAxJNGJPCwc/qZcgF
1D9PABkeoZVNYNUbFzwm+gn/caNKBQg8rPIjZpL/kND7OYuS24/rUYUwmD20JKvB
k8OL9cBq2mgA4L14DsCoX+XVOJpexA33maubPYOQKhZHMC4NvJrZKHqdMm+k4w8B
5cn8wxmH1NazAlzg4A7aRlmwV93QfozVMrLV4BgpWQKBgQDUmGQpDaHhxXOpD0Dn
7gLSrn6P9EUNQ5Wc2UiNCIzvdFyE27vXeYrPOGyib1L/plTw4fdPHMPniLaEe1f5
AgmpUbTjXZCACoCqcApcYaSQBeJcZ8ZmERZQHc65MxuF9WquSmPTAA+IQkAC56Tv
MRLYH6BRFQLbhoB85ZTzEyRKMwKBgQDJ7hR7Q1DgeGFZGBZuAVh4DOwL97O+XHfQ
mNeIs8I1cagsslQIyOpYI8ZhS8OicglvFuP4xgWS9hIMUbBOhsqwHsXJPwPE826z
jTM6HjvLxBQruNMvicCacFrdb06PTRx4qmjAp79p1zbE+tLuadWnPdmQngj3P8xs
SKfJXlRNZwKBgDLyBeaUnrt0zr+vqRUrQz/rkua6WaXREVRR+YHaj7N+RgGMipob
RTldQyM13ETpPievbs1ljki4yUw8JpvEcj5CFz/5FvyoB6dQTBtRRtdAobsVH0Us
SRWP3w8ggTL72cHEj+FrfU1g2ugXTL4JG4PxKEDl8RZcSAQmHCiX6LVjAoGAScZ6
JS7K9bgFmQ974mULdZEDDLxLJ1mRTN0Koh9K7UvBLdNZcHw3YkxvxkxwBYG0gKnU
UKfIkZDltyqBR69njkEv8f7b2bE22NLVzH4PSrsww2ibf9rMS6CREiYAhcqehFd+
PiULghfnIsChIAn6zUeDnZqfm/XrMgS5iJC2LDECgYA4/z/KMzOK3WHOTbuT0h9B
IsP7VKooKxF0ZsAiEVEWXPhFRA6XLV0RfwypWZK2VDs3mBBYlLINN7qtN7ykb2vT
fSQ0Mn562vYpakBMY1pVZKYtZJgFes+16MqCZtxgJ7C23t0U/22/HqTtpRDoIrOD
QbFogdB5w3OPFivDyxvgmw==
-----END PRIVATE KEY-----
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
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDOo+PmdGbnuav1
i1kO8dES5UUdPA9wrMN59/1+esorA4k9IXQGi78GNLVLvJBgC2ZP8QyLurERaOCt
HWpb6a5HxA37MDA12R0usUrdImKBDhr855IfzpoddQN6RXyaAGDM/3Sixsd/sCKq
sbORhar4ClM78iCdeBpZtt23nOcZNbmUpDg7YJiGlwwwLWwEGNbtCrH4aLyJe5vT
OUnaZUjyj7Nv2q9INvx9rQljFqxEOf+cHTApkLd/8Z9Gz6UllRMTLn54yFV2PVGN
UET6aBbsvbs6fbhWI9TsHcOMzRJicPpk5RYwfSa3A7xbc6grwwmE+8U/suvlB9KO
IEf6vIs7AgMBAAECggEAGzOBxY1fGnPbiggc4hW88uLRVUbZtXhYhw65BNQ/Fqhx
gWWM82uj6ro3f0/EJZB6mF1fzGj39BW9NaygmpbJRVMuj/lI7120NLhL5Y4oL1re
/JmzJ5K3wNbtbUQGYfNYysN55A+MpWdXOOcJf0d8OQiK6mI2HuR3e4XR6+CwnyWx
nrke0Gr0xa76YhHUfQSlEvHLB2hEksn4l1aDV8sVG6GdajQZwTIErjmQsKhUmxFs
KQUZ/q48M44LSDmYjLEZyVZ4XqmzU4WcXK4Vaxh4wtK3m1hl8HT9BHjpf0dA6F8u
6eMDThUGRJxJGOWiEY7h3bRlbseU9jbQJtHU8eES+QKBgQD3wViii817PHw6CRwC
0045MrsalRpR+HdeXnI8aAKrGgbwb4FOYoUwaUh/pREpADQFS012Mi98hxohy2Pn
qoXCMcvoJvNHkyRdRkuXKXRfNgVeb+Vq+vhnkMdZ0Wj4bIVqJciJWh/3eMCc4WCN
LF2EdoSFCqPumZoGgdVmq3AcVwKBgQDVhEfFPNxYSunToolO378/hi1ZGH0YMGGG
U7ehoY4A2QRpvxwaNE70sk/bjDKjt7FeoZtSO0SJ8Qe3OGPpr9TBIAywVw2GVBG/
LnbkcPWAmZDabjBncbHO0rghnitUOOwHn8NIwwGwN+zWvWJHB1IwcA6GtUfaX8Mw
VAufHXH5vQKBgQCVCbpIDdGOUNRqFRDEolhsEBh95TPwG4ezPR17ORZh10ZVXL5s
aNe2R59VKfcosvaYOLRgZdArGRqfObrfobTVHR+Mh+HRLLKzaZYPWOCoZdbU+HsJ
3++OXYmOfmqnBqE7OX9pIM4aEInN1cY/JnEbPjB51+zFm6EMSQ4WtQq8AQKBgQDE
dhjLxXpBy3xnfTC2YG5K9x0W08+WyD2UKcfXcx2Ebir/WhdzjRnxUXjlXJTjy4vq
tOyDt4ETI7KjxcE5Ls6mfOTwBMmvBf/mV8yR2dYrZCHCuozTQewHkCBY3n6j2lRj
l39KnhQZnvvPgybAkF3xr8nN7VOV/XNOLqca2y8aSQKBgB8gtQA9/66OAH3N2wlr
7iX1cISrSPf39Sx+PRWsic0CTtE3xbSo3DtkuzdC3Z+A87QJ4x5VRUpy92OVercR
utYquwkvHLcZvyu4GMiUmqdwmfqYqyVdL6Uf+xs/ZH4/pG9Bx4sQS2TplkuK5JxW
Yu1U6lShRlvdHHprsYpt5zuh
-----END PRIVATE KEY-----
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
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCoUj3pMQ2ELkrz
zq+koixljVFBAEEqwUWSjA+GEKwfFb/UPRjeO/wrKndp2r83jc6KRt66rvAIl8cr
b54yTOsJ/ZcARrjTwG3IG8Tely/54ZQyH0ImdJyEbCSoI04zX3ovjlppz3g5xanj
WmpAh6pzPgBOTfisCLMPD70xQ8F//QWZdNatoly54STkTWoJv/Oll/UpXcBY8JOR
+ytX82eGgG4F8YoQqmbjrrN5JAmqLRiBAkr3WUena6ekqJBalJRzex/Wh8a9XEV7
9HFVVngBhezsOJgf81hzBzzhULKfxuXl8uaUj3Z9cZg39CDsyz+ULYbsPm8VoMUI
VCf7MUVTAgMBAAECggEAE1OmIjFssOGz33y69Ddey6ZHTyRdVzBr8aC9Y5JkgQk5
RoBha5sNoFM29OOWEyXoMj5i8qKFkycCSn19d58XWcVRYkm8jSvKLzDpEPnhG1sI
bhzitpGrKxVTvC6ZmxJ6cB1zSjT1RATrNdy62H/7VVIoLNWNGQvCq5cODSsPe8d9
AdZC9CXPBSHc4AIDct8t7NWrOsQHTZhVHGbHCDm954yDoCh3y+y+xqOvL14TSEp4
UaidEA16sv2XBnHMPZmNTZ9IQlhjfHEpWCKj5BVMpoC32mK86zXRhVkNwgMfTGYO
nECRotvlkBuePCF/tYZiASbxWu4AeQnHJdfI9/eumQKBgQDR2n9e/koEiQPoklqj
yGeG0/nH1Tuj+lPNHFo7dZR+W9MjoO0LSeRxM7AVWzhaWNPvpP3furbHiy25fTCg
fO8hpnrDBna+/zWKQNjqkTx4tawf/EUCaTEcF+RKKMWAlXoSlOpScUPtApTz23Na
uH+TFow7VaAVoGHE8+zcrUsNNQKBgQDNVbnfKN9Uc2CNZN4BudtOga9SKE6J28i9
LlxWjmrQOF9pvPS2JcXEChBQYStQY8Lw27af8fFMjdvQ9sILCMGwM2pfaTPFNJ7l
pMYisFRZul53VvGZ7yOCBe5ZhuDy+8umxh/AAk/GcYjFb+/9p+0dSFtMi5V9lfb5
SeKWvCLBZwKBgQCDylrPh5dofbvspW0zCrqpnBpz+2A3PRC/8ZxhVxhourZA2+HC
gydqSHG/F8iuRLbk+5NMnHAJpUiUAyE0yQFM+saCEF8m2BQBvXP87DU0AbQValLU
jsd+wyplwHE4rac6YDdAi02DXWm6NAmf4dqMv05WPRIKQuzjyeTpOhO2OQKBgQCH
xgK03Croha43cJYYIBQykjAirEJah/jxlyE5nsxkSJJWsbpCYzGlEl59N/NTIdQ6
PZ5BntLGoxrRzwi6ER057JWO51pzMPtMsCrPrzbnagOi99ujxOv+wvs7OaOvJ+4e
pe1CooSrnFEq9HyFhq+UaE7ui3Ha6/m2FzP8JgT5SwKBgCyB7B/NtwGonCwBSz3w
Mr30RTxWoq5piVTnA28BVvbs7qxV7KxKiQwbVMYhck+mEDd1IB7g9/+Ph9/94ZpL
bQUAWGzgE+nDR8B4IemYPQGMhtrSXEFL+Bh1z4lU8ovwV7hrx+awjqd1xlD2qk51
kzlY4PSV0pFz5KytgqROY44l
-----END PRIVATE KEY-----
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
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
PS4lBkgDhK2s1TPdYg==
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIJljBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQIQfgyMe//utcCAggA
MBQGCCqGSIb3DQMHBAiO+0dos6mhJQSCCVDmzHR2xvhXpuw/CxE8Qs49VndyqC3p
U5jIukFD79/Cyt6I7uH0TOqj2e+0ucVbYuMrx29xaD6WOef+SXV+Q5WMDdQ/5rYW
lY5Mgl8QxRYnIppWLk1Hn48289wzqkhhBSspRjWfQfDlSP8c1+FPzNJ8l0bk2N8l
erfkScbEAtI54e31nBjJb3Z7YEWbttPLD4FraX2bteA2F5Vgn5m5LoEmhmS8KO55
HIMO6HQVLpwpEX0k5oxi96+Sqh26ZkO72qBDhQ6hvhHSRfxXl73+oURyFcDZeRfF
CNAZHVBPSN7I76R+vK6HvkIS9nLIm3YQ5DGck9XzjdIaY49srBLT3ZTY4ObPtuay
MKtnfrfLSGD9VBuGcpJx0e5WqJgb1eVKcfRNRC+mp6vyJMqTVmoC+cjRIDg7As8D
+Mze0V3jYQfZtv6Cl6TZvCWmhtiVqdVnvz37NpExFe5PCqij+tDvAoUHLPltzkgw
+ivXbb08G+VjYVwGTWjvbMhnJ19LvOiKnfrE4MLmjiDvJToBD4uBN1NLjB2wE2Nb
J33m8OTTJyGPSbXBsb3L53nDhs1rB8y3YXIFU5e1W0IbnX3jU1Z9Bb+/YniGBcyQ
H1MX4Bz7mZBsU7wMT7z5g5qQfMxG2saRqcNseKOqVA2e9T9hUGmaac0//JHvrWTO
fJyiAL4HOa24uP6cmht03Ui+tBiEu6hAOlLrDsaXcdEZ4zv7pf784+XotY+J4S9E
emJLQnhxBFqwIy+6DSvKBhnVohCz5VCC/9ssRmpAhiCObFbUU8Np5nINOp9uzr66
n2QVEH6eTGccXDmx1K5z/+HlImVcKcpBJvYKdTpW0VxlDxjOp6DwxCiY1uvWytX4
GQ6qxtnCsA6K+j7UcgAHHOwN/ltkVUMOlFkwebu/AT337jR0tGXOxQLU3GT/nNLq
i+2L7I8yUpxVLxYshDhBFd4gKiaU5Uy9ADBbv8qAVOhmrCYfCqbdO4GGLQTlVhwA
0LAqsCWo6aZPPYeoJRCy4SGIW428+qOVx2AkopT6SlCi9mypuvar07JoN0aNBj0C
+9n0/WBQ5BmY1+vpFYyos7JOcopGg1ownF1nZ0IccZhyBgTk0S7E+rYh2ovzzy7K
1PRh/zo+bWKJmBKhClAgr+104AM0oVCfUdG8G+JY2ckoInA8ticv8a4JMYHnxCRD
ZJ/5TpBw4YLimgBRLj9iDOf5V9HeIb7weUp+q8NZ2BEjG9ODX4/kcVVxWQPJS8Ig
u0dBl+T61nq7Tg45PhE0nzyEoGGPL2xC5QayF5/eAhFtGUXpPVAE52AuCLrT5KKB
ksfiIbqq6gKrK0clNZFgn7TyadGZL6NKdlb3Gk0ZY+F7/E23ayjEJ28GgHo+yLXz
p8oGMn1F2IuzQH+H1vNhb0iFDDcE6lalq6TOhtGE/sxkll4JXTosAJIJ2gimpNOJ
18qHpB8cbl/X2haKbURLTKppLqxSJYAwhttJ37oeq5t88u1W481bHZKlOD7OZ1l5
r7BWFUY7nFBYVmNixWeda/EBuFQth+XZ1hfKO5M2F6F2jcLbkElbST30Fj1aU/pA
Yq0PBW01sq1EdlxRszjCEtYjFePmXvDpOKW9mqvAKHYNY/U2vUS3go5BYwXOpK3Z
ijWBuwJLisbFSzxC7eCWu7S8y4W96lENOz+Xf4aD0n3rjYeCVmj4VXsF1Tcl9H2k
S4p9SP4OC/IMK8TmFWTy3Ilp3la3IJnFAqDezaJeLA32w756QbQ6ziJKgGUZYWhg
RYM/8ha06LxOLhRA4qvSCEs6NOii3stKB4pQTBcTqYp5jZFnoR9YhpV++rKwyQC7
8vQO+MZnJ3mBuwoXA01VBI9spmvTC8w7S6z1nIr7mjuItluYZHZOXpwaiL0kB+0O
Ttv8l/uRlT85Q0vwpqk9tY8uNKdqOidrLHuv2ICHUeTU7AylDzZrWvK0/NNd+Sfx
2/Mqu6jbuEUJwsFGHB3zPJoT8v9+jHiTi0upv/OEqu/LXfw7/Arhbup4ujyymrpu
aD9i/5vu042XXzM1IcF4FrXITb36vRvBOfFIgsdIXstvXXxyLYZ8orO1kreVQB1J
TRbWDOk+/IDQMnYef/p84y+DKHiCVUYIbYQe0x7synYrtlzSjf6SOrMKiFlZWZhl
Du0YAMRnxHp6CiFVc+Zpt8hFC2GEgeffL+qUgfJyGH1R0hknSWyBN2b/84kcs9wJ
YIqAzzPz1HSE80MVCbv5lJt1I0PLNam5AwEeTqcntZuu/4LWlbYSFZ/R4kF/qaAb
EzE6PQeLBNIzNEemQWD7b2XvludB8cAiMa/HNvpjOaWqZZLdDR8VLYb2WzPjWuO/
gFc9z95G0AZM8aUadt3w/bgoc4QgZX3sZmBiWBtKI6XNnhVD54w04gAirrXYjxF0
pb79pb2w0IVCAdgm2Ybd8QB83EU2bwAXL4e9infojHpRZMk/j7TKHnaJOpr0xTsQ
CY+D6+3EkbM+jl3OdtaMORY2fFik2Ujf6DctQG5IR8LB0z7xp2HbedGNI5x2eY5i
13veBE+U/Z9d6uzs9Vgxk1maKLQXu7+h8IN+jMjVJsZ/WkeERcbbt8nihBS+66er
JFjFGRejvWEuBdhKl8rSWf8OxNoajZyWcmUgxNvg/6o3xGYdj6Wu8VT/t2juoC4F
fY2yvlIq7+Zx4q5IuSW+Qm5wROtHvKLvBwDSzAoJ4ai/Yyse3USEOZCv7rS+59Si
p3/rlnipLAs29R8SYAt8Q2ntSmZOkfWaz+/IV1KeEoH5qUmI/rKDueNjPnKNEmLX
9Q3oDEvNmsYuAaCN2wvFDDpwkmhjepxUR5FewlCwbHVNfF0KVdlX9XmvVIQXfbF6
uRlmBalqiXW1xyd9agQnV3+t8Mvuddn+KKEX9nqZ4teloVByfyNE9gAatBUcT33V
0xGW1CIRzkYykT3HSbf/irfzXHU090M//P8bX6z430Jrg3DxiVUQZiAo2NoVetFH
x/h68BZ6/j8O0F36V2W+0sE+qN8Wrh1lDxBykzexJAWS063xe5oVbINbZplsO5tK
ghdr7RWGVApmwaXBhKIxtapSQUMLFhBKEGLL4iDw+jwPAlcBQl3lw+xEOu/No+aE
XPwFoojhSS9XuE3M8Sc9gKvLcZbdKAegMnYz+py5OwdJ8RiaoaetCTJist08FUg2
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

//
// These certificates were generated by following the steps outlined in this tutorial
// for creating the Root CA, Intermediate CA and end-user cert:
// https://raymii.org/s/tutorials/ \
// OpenSSL_command_line_Root_and_Intermediate_CA_including_OCSP_CRL%20and_revocation.html
//
// The parts relating to the OSCP and CRL were omitted.
//
// | serverCert TRUSTS intermediateCA TRUSTS rootCA |
//
// The 'cert_file' here contains the serverCert and intermediateCA.
// The 'ca_cert_file' contains the rootCA and the same intermediateCA.
// This was added to test KUDU-2091 and KUDU-2220.
Status CreateTestSSLCertSignedByChain(const string& dir,
                                      string* cert_file,
                                      string* key_file,
                                      string* ca_cert_file) {
  const char* kCert = R"(
-----BEGIN CERTIFICATE-----
MIIFizCCA3OgAwIBAgICEAAwDQYJKoZIhvcNAQEFBQAwUTEXMBUGA1UEAwwOSW50
ZXJtZWRpYXRlQ0ExCzAJBgNVBAgMAkNBMQswCQYDVQQGEwJVUzENMAsGA1UECgwE
QWNtZTENMAsGA1UECwwES3VkdTAeFw0xNzA4MTEyMTM4MDZaFw00NDEyMjYyMTM4
MDZaMEwxEjAQBgNVBAMMCWxvY2FsaG9zdDELMAkGA1UECAwCQ0ExCzAJBgNVBAYT
AlVTMQ0wCwYDVQQKDARBY21lMQ0wCwYDVQQLDARLdWR1MIICIjANBgkqhkiG9w0B
AQEFAAOCAg8AMIICCgKCAgEAqevNYH73n4kARZtMsHRucdKmqVd/xxztMlK5VOor
ERUBhKVVOw3kpDrN9z80ldIkpOrtrfE7Ame/nA9v4k6P3minPEm1qCA/kvaAodtT
4HjAkrPc+fto6VO6+aUV6l+ckAV/79lOuc7AutNlvvPtBQQcgOKvlNUSRKwM7ndy
dO4ZAa+uP9Wtsd0gl8b5F3P8vwevD3a0+iDvwSd3pi2s/BeVgRwvOxJzud8ipZ/A
ZmZN8Df9nHw5lsqLdNnqHXjTVCNXLnYXQC4gKU56fzyZL595liuefyQxiGY+dCCn
CpqlSsHboJVC/F3OaQi3xVRTB5l2Nwb149EIadwCF0OulZCuYljJ5y9H2bECXEjP
e5aOdz9d8W3/T7p9vBKWctToeCpqKXUd+8RPudh0D0sUHuwQ4u4S1K6X+eK+gGhT
HOnPwt+P8ytG0M463z5Gh9feW9ZDIYoiFckheFBAHxsgDWhjYpFmYireLLXMbyaM
s5v/AxPNRAsx3vAAd0M0vGOpdgEJ9V1MsKmxkPO/tDC3zmnv6uJhtJfrOAKxwiGC
fDe4IoSC6H5fTxeAgw6BG5onS1UPLADL8NA/M1y8qiSCZS/5S0cHoJp5AxDfZSSR
O49ispjqcONRwckcRJ5Pbl0IA+wGyg2DuI9LaqS5kKWp5AE8VCLPz7yepDkRnnjO
3m8CAwEAAaNyMHAwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQUZBZLZZaUfyIK/8B7
GIIWDqeEvDgwHwYDVR0jBBgwFoAU8KctfaqAq0887CHqDsIC0Rkg7oQwCwYDVR0P
BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA0GCSqGSIb3DQEBBQUAA4ICAQA3
XJXk9CbzdZUQugPI43LY88g+WjbTJfc/KtPSkHN3GjBBh8C0He7A2tp6Xj/LELmx
crq62FzcFBnq8/iSdFITaYWRo0V/mXlpv2cpPebtwqbARCXUHGvF4/dGk/kw7uK/
ohZJbeNySuQmQ5SQyfTdVA30Z0OSZ4jp24jC8uME7L8XOcFDgCRw01QNOISpi/5J
BqeuFihmu/odYMHiEJdCXqe+4qIFfTh0mbgQ57l/geZm0K8uCEiOdTzSMoO8YdO2
tm6EGNnc4yrVywjIHIvSy6YtNzd4ZM1a1CkEfPvGwe/wI1DI/zl3aJ721kcMPken
rgEA4xXTPh6gZNMELIGZfu/mOTCFObe8rrh4QSaW4L+xa/VrLEnQRxuXAYGnmDWF
e79aA+uXdS4+3OysNgEf4qDBt/ZquS/31DBdfJ59VfXWxp2yxMcGhcfiOdnx2Jy5
KO8wdpXJA/7uwTJzsjLrIgfZnserOiBwE4luaHhDmKDGNVQvhkMq5tdtMdzuwn3/
n6P1UwbFPiRGIzEAo0SSC1PRT8phv+5y0B1+gcj/peFymZVE+gRcrv9irVQqUpAY
Lo9xrClAJ2xx4Ouz1GprKPoHdVyqtgcLXN4Oyi8Tehu96Zf6GytSEfTXsbQp+GgR
TGRhKnDySjPhLp/uObfVwioyuAyA5mVCwjsZ/cvUUA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
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
Wd40Cr+wAdHKN6t/oransoxu0EZ3HcSOI1umFg==
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
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
QSLJ/7+Bn9NwsoDIaegA7vbs2BrKqg==
-----END PRIVATE KEY-----
)";
  const char* kCaChainCert = R"(
-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
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
Wd40Cr+wAdHKN6t/oransoxu0EZ3HcSOI1umFg==
-----END CERTIFICATE-----
)";

  *cert_file = JoinPathSegments(dir, "test.cert");
  *key_file = JoinPathSegments(dir, "test.key");
  *ca_cert_file = JoinPathSegments(dir, "testchainca.cert");

  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kCert, *cert_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kKey, *key_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kCaChainCert, *ca_cert_file));
  return Status::OK();
}

//
// These certificates were generated by following the steps outlined in this tutorial
// for creating the Root CA, Intermediate CA and end-user cert:
// https://raymii.org/s/tutorials/ \
// OpenSSL_command_line_Root_and_Intermediate_CA_including_OCSP_CRL%20and_revocation.html
//
// The parts relating to the OSCP and CRL were omitted.
//
// | serverCert TRUSTS intermediateCA TRUSTS rootCA |
//
// The 'cert_file' here contains the serverCert and intermediateCA.
// The 'ca_cert_file' contains only the rootCA.
// This was added to test KUDU-2041.
Status CreateTestSSLCertWithChainSignedByRoot(const string& dir,
                                              string* cert_file,
                                              string* key_file,
                                              string* ca_cert_file) {
  const char* kCert = R"(
-----BEGIN CERTIFICATE-----
MIIFizCCA3OgAwIBAgICEAAwDQYJKoZIhvcNAQEFBQAwUTEXMBUGA1UEAwwOSW50
ZXJtZWRpYXRlQ0ExCzAJBgNVBAgMAkNBMQswCQYDVQQGEwJVUzENMAsGA1UECgwE
QWNtZTENMAsGA1UECwwES3VkdTAeFw0xNzA4MTEyMTM4MDZaFw00NDEyMjYyMTM4
MDZaMEwxEjAQBgNVBAMMCWxvY2FsaG9zdDELMAkGA1UECAwCQ0ExCzAJBgNVBAYT
AlVTMQ0wCwYDVQQKDARBY21lMQ0wCwYDVQQLDARLdWR1MIICIjANBgkqhkiG9w0B
AQEFAAOCAg8AMIICCgKCAgEAqevNYH73n4kARZtMsHRucdKmqVd/xxztMlK5VOor
ERUBhKVVOw3kpDrN9z80ldIkpOrtrfE7Ame/nA9v4k6P3minPEm1qCA/kvaAodtT
4HjAkrPc+fto6VO6+aUV6l+ckAV/79lOuc7AutNlvvPtBQQcgOKvlNUSRKwM7ndy
dO4ZAa+uP9Wtsd0gl8b5F3P8vwevD3a0+iDvwSd3pi2s/BeVgRwvOxJzud8ipZ/A
ZmZN8Df9nHw5lsqLdNnqHXjTVCNXLnYXQC4gKU56fzyZL595liuefyQxiGY+dCCn
CpqlSsHboJVC/F3OaQi3xVRTB5l2Nwb149EIadwCF0OulZCuYljJ5y9H2bECXEjP
e5aOdz9d8W3/T7p9vBKWctToeCpqKXUd+8RPudh0D0sUHuwQ4u4S1K6X+eK+gGhT
HOnPwt+P8ytG0M463z5Gh9feW9ZDIYoiFckheFBAHxsgDWhjYpFmYireLLXMbyaM
s5v/AxPNRAsx3vAAd0M0vGOpdgEJ9V1MsKmxkPO/tDC3zmnv6uJhtJfrOAKxwiGC
fDe4IoSC6H5fTxeAgw6BG5onS1UPLADL8NA/M1y8qiSCZS/5S0cHoJp5AxDfZSSR
O49ispjqcONRwckcRJ5Pbl0IA+wGyg2DuI9LaqS5kKWp5AE8VCLPz7yepDkRnnjO
3m8CAwEAAaNyMHAwDAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQUZBZLZZaUfyIK/8B7
GIIWDqeEvDgwHwYDVR0jBBgwFoAU8KctfaqAq0887CHqDsIC0Rkg7oQwCwYDVR0P
BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA0GCSqGSIb3DQEBBQUAA4ICAQA3
XJXk9CbzdZUQugPI43LY88g+WjbTJfc/KtPSkHN3GjBBh8C0He7A2tp6Xj/LELmx
crq62FzcFBnq8/iSdFITaYWRo0V/mXlpv2cpPebtwqbARCXUHGvF4/dGk/kw7uK/
ohZJbeNySuQmQ5SQyfTdVA30Z0OSZ4jp24jC8uME7L8XOcFDgCRw01QNOISpi/5J
BqeuFihmu/odYMHiEJdCXqe+4qIFfTh0mbgQ57l/geZm0K8uCEiOdTzSMoO8YdO2
tm6EGNnc4yrVywjIHIvSy6YtNzd4ZM1a1CkEfPvGwe/wI1DI/zl3aJ721kcMPken
rgEA4xXTPh6gZNMELIGZfu/mOTCFObe8rrh4QSaW4L+xa/VrLEnQRxuXAYGnmDWF
e79aA+uXdS4+3OysNgEf4qDBt/ZquS/31DBdfJ59VfXWxp2yxMcGhcfiOdnx2Jy5
KO8wdpXJA/7uwTJzsjLrIgfZnserOiBwE4luaHhDmKDGNVQvhkMq5tdtMdzuwn3/
n6P1UwbFPiRGIzEAo0SSC1PRT8phv+5y0B1+gcj/peFymZVE+gRcrv9irVQqUpAY
Lo9xrClAJ2xx4Ouz1GprKPoHdVyqtgcLXN4Oyi8Tehu96Zf6GytSEfTXsbQp+GgR
TGRhKnDySjPhLp/uObfVwioyuAyA5mVCwjsZ/cvUUA==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIHmDCCA4CgAwIBAgICEAAwDQYJKoZIhvcNAQEFBQAwVjELMAkGA1UEBhMCVVMx
CzAJBgNVBAgMAkNBMQswCQYDVQQHDAJTRjENMAsGA1UECgwEQWNtZTENMAsGA1UE
CwwES3VkdTEPMA0GA1UEAwwGUk9PVENBMB4XDTE3MDgxMTIxMzUzNVoXDTQ0MTIy
NzIxMzUzNVowUTEXMBUGA1UEAwwOSW50ZXJtZWRpYXRlQ0ExCzAJBgNVBAgMAkNB
MQswCQYDVQQGEwJVUzENMAsGA1UECgwEQWNtZTENMAsGA1UECwwES3VkdTCCAiIw
DQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAM1X35LT/eBWBt0Uqqh3DSUyY3K8
HLIlX3ZXg2Nx6y8yqhw5UGVFZl0uYBDo2DSlTl4sey+AxLIbpQI9ArRA+xqmFynV
jheB9otudnA8hVwi/e9o+m+VSjG+HPRjSS5hwdPgpJG8DCPSmGyUUFtf3v0NxkUq
Is+fB5qhQ36aQkI+MwQsSlHR+YrrKKVnE3f911wr9OScQP5KHjrZLQex8OmpWD9G
v4P9jfVSUwmNEXXjmXDhNG/1R4ofX6HogZR6lBmRNGbcjjWRZQmPrOe9YcdkMLD0
CdaUyKikqqW6Ilxs7scfuCGqwBWqh66tY18MBMHnt0bL26atTPduKYqulJ1pijio
DUrzqtAzm7PirqPZ4aOJ9PNjdQs9zH3Zad3pcjfjpdKj4a/asX0st631J5jE6MLB
LcbAerb/Csr/+tD0TOxwWlA+p/6wPb8ECflQLkvDDEY5BrRGdqYDpEOdm1F9DWQh
y0RB8rWJMkxC/tTqYHfeaphzCxndLRsZQKVcPiqWCT7b431umIjPaDhsykNlcU3N
f0V7V/fLY6wwuACngS0BLQuMrXy5FyhmWnUBeWwHfAeTxCkHlF+cVT6wHmeOuGbC
c1piq7O7puKdC3UjO7Nn+WoOb2B6Qm/dajHpj5myxYJa5tGQGeUnWPwjjMQR557k
HzugGAzkuG1ASQrhAgMBAAGjdTBzMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0OBBYE
FPCnLX2qgKtPPOwh6g7CAtEZIO6EMB8GA1UdIwQYMBaAFE/9XKaDey5kC8f3bCeU
HW46abboMAsGA1UdDwQEAwIBpjATBgNVHSUEDDAKBggrBgEFBQcDATANBgkqhkiG
9w0BAQUFAAOCBAEAIaD2yzjTFdn61A4Qi+ek3fBJaDNQZytd0rHb49v3T+mdj/MI
yShI1qezDFkg2FP1LfNgjuQl/T+g0BloXatAhZ/dj20Y8oN6bmilV+r2YLJbvbTn
3hI+MxNf3Ue3FmIrwKK3QdkWcDBURpyYaDO71oxPl9QNfdhWCGHB/oWKU2y4Qt/O
aPy+CmBAZEclX+hsdUBDJG5vuujpv4myCFwpLgFKNQX3XqCPLc4SRjfyla2YmeZv
j7KKYh8XOWBbBF0BnWD94WzUDIBmFlUfS32aJTvd7tVaWXwH8rGwDfLN8i05UD9G
zc3uuFH+UdzWVymk/4svKIPlB2nw9vPV8hvRRah0yFN3EQqAF0vQtwVJF/VwtZdg
ahH0DykYTf7cKtFXE40xB7YgwDLXd3UiXfo3USW28uKqsrO52xYuUTBn+xkilds1
tNKwtpXFWP2PUk92ficxoqi1cJnHxIIt5HKskFPgfIpzkpR8IM/vsom1a5fn4TT1
aJbO5FsZTXQMxFLYWiSOMhTZMp3iNduxMYPosngjjKPEIkTQHKkedpF+CAGIMOKE
BVa0vHyF34laKMMDT8d9yxwBJLqjlBohNsLLZa/Y90ThaMw+QYn/GZATB+7ng+ip
VdGAQrghsGSxP+47HZ6WgBrlRdUWN1d1tlN2NBMHLucpbra5THGzl5MlaSVBYZb6
yXI+2lwcTnnEkKv2zoA4ZHWdtLn/b1y4NKNg205TA+sOZcl6B1BgMe/rFuXdZe9Q
/b6Tjz65qL4y1ByBVBJNhQQairw6cypHzwzC3w6ub1ZXtFqnTlU8fFcHGeOyydYS
NfoepF0w2v0ounqD+6rN1CH/ERVb4FCEN19HQ3z+rAj19z2h6m/l5QEKI7bz8ghD
8yxyqJz+L9XpfOo1yZfHQJckilY6BBIGWyeetJBmvkwv2WPt+3pX1u7h5LkvNRj2
3fItf486zqtzUi+i/E//rS4gD/rRr4a85U8GSfp3LSAbtmfC0LNYUYA9Dcc0LSpl
9alNuEpBHSHXlCVh4bcOb0L9n5XNdMcUYBo14hQdP0K1G7TounuAXFKYIQeyNyoi
OAZ+eb7Y2xNnkY/ps/kyhsZgOJyiDZhdcruK3FIUGYlg5aVjQTB8H0c3/5SZnSky
6779yMKztFXj9ctYU0YyJXWdF0xP/vi1gjQx/hJnDfXFfIOmeJdQSC08BGyK/PeC
8zAS380bgzOza/eBL6IK0RqytbWgdoLrUQQfa1+f7AQxDDdoOkUenM0HSWjKfCuG
m1/N7KUDHtnjVIHWqRefTPg1/tQjVY8/zgxN8MyAy+D95y4rawjsJf1dL6c0+zGv
Wd40Cr+wAdHKN6t/oransoxu0EZ3HcSOI1umFg==
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
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
QSLJ/7+Bn9NwsoDIaegA7vbs2BrKqg==
-----END PRIVATE KEY-----
)";
  const char* kRootCaCert = R"(
-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----
)";

  *cert_file = JoinPathSegments(dir, "test.cert");
  *key_file = JoinPathSegments(dir, "test.key");
  *ca_cert_file = JoinPathSegments(dir, "testchainca.cert");

  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kCert, *cert_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kKey, *key_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kRootCaCert, *ca_cert_file));
  return Status::OK();
}

} // namespace security
} // namespace kudu
