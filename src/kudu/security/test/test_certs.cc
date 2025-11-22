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

// This uses a similar process as kCaCert_, except it specifies
// RSASSA-PSS via '-sigopt rsa_padding_mode:pss -sigopt rsa_pss_saltlen:64'
const char  kCaRsassaPssCert[] = R"***(
-----BEGIN CERTIFICATE-----
MIID3zCCApegAwIBAgIUPm3WcVM4xPYuHt88YFMJaTxadMUwPQYJKoZIhvcNAQEK
MDCgDTALBglghkgBZQMEAgGhGjAYBgkqhkiG9w0BAQgwCwYJYIZIAWUDBAIBogMC
AUAwTjELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAkNBMQswCQYDVQQHDAJTRjERMA8G
A1UECgwIQ2xvdWRlcmExEjAQBgNVBAMMCWxvY2FsaG9zdDAgFw0yNTA1MTYyMTIx
MjdaGA8yMDUyMTAwMTIxMjEyN1owTjELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAkNB
MQswCQYDVQQHDAJTRjERMA8GA1UECgwIQ2xvdWRlcmExEjAQBgNVBAMMCWxvY2Fs
aG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKSUIlO7untRydez
wAmpzDHMWE0wa/2RU+UDUDvVyyY6CyoDWTkNnAPWQKDgxDE2TboakKKivuiDseRV
ztZjUZE93O0oJZtRgEsvH5UTsdxD1ggv3q5cbN7lKOpjd56p1rWrmW6lBghxt1ID
eW9YDqZUQJH4ea36WEAlZiQwDexHhq1ed7yA3Ht7p+fyJvEy0/amF1n4rh00nxht
ofzGdMzTQ5dqf8I9qr9mqKcs5hslc2Vs4+zXckUncbmAB2m/mVItyCev7Uy/FfG/
k6nc+AaXQcTn7xQwD4MJRnAgDDybdNCZ9Idobml1YCmrVuE6BD7qsT8SoEoAr5Ri
Lw6J+YsCAwEAAaNTMFEwHQYDVR0OBBYEFLGZLVEQNR1qeA3fN3TgzgAF75cmMB8G
A1UdIwQYMBaAFLGZLVEQNR1qeA3fN3TgzgAF75cmMA8GA1UdEwEB/wQFMAMBAf8w
PQYJKoZIhvcNAQEKMDCgDTALBglghkgBZQMEAgGhGjAYBgkqhkiG9w0BAQgwCwYJ
YIZIAWUDBAIBogMCAUADggEBAIuAX+VP1+4H3POcQYtegEkDOw+mV5JrYpu1K2Qw
K9AMtfdfafWfIGGYBPZ/tcPsfXxjy7KX9l5B8RAfCiqbCbQ1N5MbukqgsKkw2eNr
vgFzMcRfy9tif3bP5ytPLhoPla8gsa5x0Lng2P5MaWeA9pJdO8OIbhtLUwCC/KTe
Z84T2499hu3RJgDIfYi0QIFKVEltQRH4+5zP8Ay7buJICjMCkmKYDoHJAVDYlArI
VHqg4qNVcY76E7urByeLx3NJ45RIT01u6GbC/V7Rhq+lSa2ON0xAsyhqHj5Hq+1Q
j3Om2w+ByZEDJE0e/YKeFHk7mWjyFtigMO388sMgU007PXE=
-----END CERTIFICATE-----
)***";

// See comment on kCaRsassaPssCert
const char  kCaRsassaPssPrivateKey[] = R"***(
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEApJQiU7u6e1HJ17PACanMMcxYTTBr/ZFT5QNQO9XLJjoLKgNZ
OQ2cA9ZAoODEMTZNuhqQoqK+6IOx5FXO1mNRkT3c7Sglm1GASy8flROx3EPWCC/e
rlxs3uUo6mN3nqnWtauZbqUGCHG3UgN5b1gOplRAkfh5rfpYQCVmJDAN7EeGrV53
vIDce3un5/Im8TLT9qYXWfiuHTSfGG2h/MZ0zNNDl2p/wj2qv2aopyzmGyVzZWzj
7NdyRSdxuYAHab+ZUi3IJ6/tTL8V8b+Tqdz4BpdBxOfvFDAPgwlGcCAMPJt00Jn0
h2huaXVgKatW4ToEPuqxPxKgSgCvlGIvDon5iwIDAQABAoIBAEi3vzcaInpsl+97
16UtZjC2pmlstLp0JQpyXVgizcEVMmuc0SZ5Ue8MEsBCr81CvjM1m6SQniOkVMyb
8WketyKin+QVshAfgb02lBDNg+/b9UzmwdBuvBf8TwjJbEgpqNnaeU+/EJxYinRt
XpGI6egqH+GfVTw++hFVtPzWUsCL4D5XnwC+ggTTIL022IYM7oRcsWREN0lNSj0I
EIivp9I7+t6hfObo49KJlKuvcx+4DytaLzF5J0Om6aChYVRgbMrfv+QXOd8w5GWy
vZlpM3uEK9FfA/qYJMCev9PFj2BT+dfDjGz2SyP6PdH6JxjXYqrSX9+4aUAjl7kh
ZLXFLvECgYEA2tIdZVetwoUQ4zCoZUg64e/5LEcJzz4Ds+qcHoc3pMIByPFfy1Zs
L6vMgRx0G9uKNUchbw8ATkxgisfxNOA1Q3h/hfZmOllFikX4/N2XsGNXoRfTYmuS
eERvSG5X3+YFADiggGGo61BJN8BXnRplVJUkqH/kyetee1doE+OUxgMCgYEAwIqw
rHmfTArLJ03rZu3MVEmtXw4uJbv5N676cODWiGFsaU59P7q0pRDrfKzoYZT8/IIG
BFGWqur5nmRc89DXvjQHHlPGPz/McIifuQTYEBXTtxUcNhOK7xHxV9mvmoVTSopv
lNX43p+nXRLSMzQkSIaa2+EiNk3ULoVNOk2kC9kCgYAhBYhOHNcp/a64ukUPU8Ef
C3nMxsOiNLeWVRdOPBWXlXdzfYl5RAd7gi+QZFzZP14yABP5kIf5SOlgyB+MXTFs
hyinbLGsqIAoB7s1XbNgeP1mYBQUTCuEXr90bMJyFWI30FPYS+ST7j++XBZcrPkR
tJgdnX9HQW+2qVAZgESZRQKBgCts3E36HEhtQsZ5l3ceePAlsdl3fEb8b0f0yf09
aIVX27iggDUoaee0ujfjU4H2tVxKAwtkT2P7HRNxNVm0J4R5fYWEhXjsbbKPzd5P
zl9KXPa05yj3HWWwGUukCCwEl/V+5Y2e+MNVJM0kGo572xcUbMbcrveqdAmN/Q4C
RtZ5AoGBAI5vyaO13QgVzsAfuX4uhWLa/X16ht1FV7prDAZ13T+wfNzHGf4LFd8D
HI3kpN7QlkdSGHa5Izr5d3Nv/N6AEDB2RIVxV/p3HXSQYxFWyPhHVR5/8i5V/JTj
dN0jgPAyvyrffMc9J0mGh5Gmbg9HiuT51fMRz65Qpp+BIDkztR2z
-----END RSA PRIVATE KEY-----
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
aguDYzI0vhxbixQQW9MDhpBZMs3u0VjIjjY=
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN ENCRYPTED PRIVATE KEY-----
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
4o5SENtYBbXaRMaKKSUBNDa5V5FGvv0H
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
// The below certs has been generated using OpenSSL3, with the following changes
// to the above article:
//    * -aes256 is omitted
//    * in 'openssl req' commands the '-sha256' switch is omitted
//    * the commonName in 'openssl ca' is set to 'localhost'
//    * in ca.conf [alt_names] DNS.0 is set to 'localhost'
//    * validity in ca.conf is set to 7200
//
// Note about the cert nomenclature:
//
// article      <-> this file
// -------          ---------
// root         <-> root
// intermediate <-> intermediate
// enduser      <-> server
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
MIIJoTCCBYmgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwgZ4xFzAVBgNVBAMMDklu
dGVybWVkaWF0ZUNBMRMwEQYDVQQIDApDYWxpZm9ybmlhMQswCQYDVQQGEwJVUzEi
MCAGCSqGSIb3DQEJARYTZGV2QGt1ZHUuYXBhY2hlLm9yZzEjMCEGA1UECgwaQXBh
Y2hlIFNvZnR3YXJlIEZvdW5kYXRpb24xGDAWBgNVBAsMD0ludGVybWVkaWF0ZSBD
QTAeFw0yNDA3MjMxOTU4MTlaFw00NDA0MDkxOTU4MTlaMIGOMRIwEAYDVQQDDAkx
MjcuMC4wLjExEzARBgNVBAgMCkNhbGlmb3JuaWExCzAJBgNVBAYTAlVTMSIwIAYJ
KoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFjaGUub3JnMSMwIQYDVQQKDBpBcGFjaGUg
U29mdHdhcmUgRm91bmRhdGlvbjENMAsGA1UECwwES3VkdTCCAiIwDQYJKoZIhvcN
AQEBBQADggIPADCCAgoCggIBAI/O8ezskBVo/BzldVjkvzJwvDhz6JZe26NU4qGu
bevZiyYKhCBeCJ0bgvEP0tkQ1DCTIPSyWTGNzVrm9a3oJbudsocWGTscdZT7LFPm
oaoMJbVtNVT35iy6TVuBKugrzz4azrrhnsdJnG/Px937yhARDs6AnCB5LNoBgQMd
4cJ41H6xwexqkjgdNQfRAtyHZT0nyjrONTEoZMfNTAbdawYptTz7ft7W8NIDrzAl
lXnTxJc9ycnfF0pbNao1Msw1y0pjuRcE/EKdPQ1VK/B3MdQOlRFSl2BI/B9Jzfe9
o+WQG984kprb767JWiO4W8DKsT64IgOTCa/1A4ExKAbIqngM4+RrznFhDLxrHvsL
CSH9dCL6VAxQeLxcrQpkekwj9k6qQTlY4J337LHBy3izWmfj4kiVH+EN+Jg8HqIt
1CVP+pMb9eoOXwur98wsYX/1He5/XAaFSrJ486OLz2wYQ5KMYc+xzjG1q8zMhtLA
R8Vn/uPRg0SJATphvs6hRSAbRkz6fdU+HpuAdlqfdOrlQRtL397BTMzA0ksJP/HZ
VhCifsNcXWACr8ue9Seyd1Nrq3dw2oKfQf+oyS18RAf2gpMEGt0gScVWPfsbuFb7
Twntxiakb1ozfqDpuoOjOgxEQkIWluw/Cm/uYxw7KinAJoZQ6h1NstxDuIczsWyk
F2YhAgMBAAGjggH1MIIB8TAMBgNVHRMBAf8EAjAAMB0GA1UdDgQWBBR9hKckR8a1
YQEVfScXpaH1V32UeDAfBgNVHSMEGDAWgBTwqgsXpPvzYO7RyI+CKtW4z0byiDAL
BgNVHQ8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwfgYDVR0fBHcwdTA7oDmg
N4Y1aHR0cDovL3BraS5zcGFya2xpbmdjYS5jb20vU3BhcmtsaW5nSW50ZXJtaWRp
YXRlNS5jcmwwNqA0oDKGMGh0dHA6Ly9wa2kuYmFja3VwLmNvbS9TcGFya2xpbmdJ
bnRlcm1pZGlhdGU1LmNybDAUBgNVHREEDTALgglsb2NhbGhvc3QwgegGCCsGAQUF
BwEBBIHbMIHYMEEGCCsGAQUFBzAChjVodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNv
bS9TcGFya2xpbmdJbnRlcm1lZGlhdGU1LmNydDA8BggrBgEFBQcwAoYwaHR0cDov
L3BraS5iYWNrdXAuY29tL1NwYXJrbGluZ0ludGVybWVkaWF0ZTUuY3J0MCwGCCsG
AQUFBzABhiBodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNvbS9vY3NwLzAnBggrBgEF
BQcwAYYbaHR0cDovL3BraS5iYWNrdXAuY29tL29jc3AvMA0GCSqGSIb3DQEBCwUA
A4IEAQCbWm3Q5suIXBYVkzbU4ClxvkHASayoBoxx5pH5xeC+9IXYHGTwtBRl06NM
E3Koj0vqNTdg0cQnqaqJBjaXNdCRnDSlcs+K1xh/hzgjTLNb7PnVzBg8lauu86CE
eEKsrolAmSTsAEUfwrxiQ8MPJTJLLXN6r15lqCIeFdgKtiM2EPg3rBtTxKIwvpQB
efAAbGI+gp4MlaZULf2695V0GHPdsenydYfYFTc/cCcSJlJi7QSvp3gAIqK5o2A1
HmoGHQArVcHBAloAbLLQ1nM4l2O1E5J5DZ4mRwhQiE/ShBMVOAEFqSNn+n/q6Z9t
gCy1lgGuWG1/614OvOXROoLhNI6yaDZy5N+LfMKCMP+F2hwLx+id2T9qh2tCBOuO
xG2MxF+sE/4B2oAnQ2S/FFdIarSiKEInyvamg7wtnsEYsG5yL62kwjMcxOhhm1K9
suAjp8Rq5wZC2Z0esZ5hmQEigtnh815NSILHBH4BNye+b4UTVX3lkg92Q5snsRkn
PMRL728DrIeFOcXiCsVcn/5iPtz+y58Xb8LYoDfNkyTh3EKC3JbheN6Xr+mfiw8u
0AWUCPGFB99X6wgCfKCug2BZ5bT2a4Zc8+PMWRNOG5kd/UqeRLo5zNSZIGPSjYSB
QQYgs31IoRL74sUnfKGdx2iSua7T0946Iph9DU8mQK+NudtrB8UVzgUWzRbx0/wp
SC1zbqCWvrPY8en2w2B+kPJWtRW3sqR4TRJiegjgEdeJu8NrikC+A0hr1Obgckro
NXkwLt4df0wHtj5Vg0w3c+twwlESKErkON4wWaUGKEM8ckdRuHp3K+Hhgo2Y2iUx
Ydd6zOe+X95Zt4bOdPIN8SjhKpc40UIBIaVhma94nEjNXZCbovlSxjP3q59QXXzn
qfMru778lBpC2TZMqmErJ35C6gIvKbM8WbfIOcyj0S17yFVwcjxeLslQXAXvOPZB
lpXF1qy4czCWLI9GTZMV3y6XK5gb55vw8lHze271EPds2nb9E/ntYcULs5uDSwKh
Yni0MtWZN6LY4gIle5L/AstNkk70CeGUYDt3pPB38C/GbZYo1mVEayOQXSW7Ml4K
NFqkf7UW0wfKx2Ki8hR6s6W46iUS8PNtCs3gabZHisJM6FcTIF1FiXWlLZeoEZJW
QIirrEyCZUsgqGedAcyHU4SoHIdGqVpPHol33yr3tNYImS7mViHpusPlgg5bta4d
6NrckJQlrs/zD+rM37xVKIvC8GnH/h3+du9JWCgb2Cn0gyOF1VcZyqJ+xBF2AFY3
vb0cYv2ddTb+nMIbiARDaEIwG2QYC9uTrtUEBuoob7ObS7ddjNlLhzeGp3NpE9q4
92AnEo0pFET79a8mYFtcagFruEJ7
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIILcDCCB1igAwIBAgICEAQwDQYJKoZIhvcNAQELBQAwfzELMAkGA1UEBhMCVVMx
EzARBgNVBAgMClNvbWUtU3RhdGUxIzAhBgNVBAoMGkFwYWNoZSBTb2Z0d2FyZSBG
b3VuZGF0aW9uMRIwEAYDVQQDDAkxMjcuMC4wLjExIjAgBgkqhkiG9w0BCQEWE2Rl
dkBrdWR1LmFwYWNoZS5vcmcwHhcNMjQwNzIzMTk1NTM0WhcNNDQwNzE4MTk1NTM0
WjCBnjEXMBUGA1UEAwwOSW50ZXJtZWRpYXRlQ0ExEzARBgNVBAgMCkNhbGlmb3Ju
aWExCzAJBgNVBAYTAlVTMSIwIAYJKoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFjaGUu
b3JnMSMwIQYDVQQKDBpBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbjEYMBYGA1UE
CwwPSW50ZXJtZWRpYXRlIENBMIIEIjANBgkqhkiG9w0BAQEFAAOCBA8AMIIECgKC
BAEAwpQU6QayDO4u4/oYTO99GBRomJC2vC7NnuTmcg6Y/vYv76cU4I12adZsbQHZ
kCWIwqQqpJeEPfzmr6HwX5FfSmUVGj6eTNpTM+ZzojAlEe2hUteU+NHW6yYrlyqO
+EMTRgdd4zirJa7TcYj+GyEL9Hk0sEKtD98wU1WUVgyu9HyxR2A6iAGLf0wraU/7
XbO0tMRLG829kQp+TgKIfs/96/HeUNS6++H7SxdWR8fCZzwDy+s5Tah25hmwVumU
eZ2zPES8zGyp9v69bJD9CtJxPWKnMcXYDZ18Cag+WVWjTWrV+1VdpPCD8Jnv8eDU
hw2xOi0xStDB3+XP4d1l0IQHlTPTp/ViELJmHiGXZBh0lYGrDYyqx6Y6wGKv72Pr
OvKzXtWrfoIDjSu2QBKm4Lj8MAbEe6tvtBAUXtMMYFCpfHZHc7IF1eF3ffThsS//
LTHiEG/rU7MSoU0i7CMVO52v7TdWIwMJJQBao+u5duaFlCu8CpllxhLmOjyNUYCO
7QyvxAm+60DEDYj3yh16tnwKj8lNEyBrr2yFL0G8YFYC0ILy2Sp1qoXifAFWOY9p
nFudkPSmsbXTn9wZI2iXQQF6K7DdjsWxo6m+ovRg9MDRjquGSo1Ag39fp7E+OcRO
9q+8eq91mBGRTcYucga3erbkQgaeCka2Dlbo6QWZ2KQ4G9E1E139oBUOykBqaiU+
LoDF/7AA5LUT/aMkM1YEWeubGBI9d9AEAb5JQqP/aIiivujB7gfmsFJZeKEgXs21
rbKg7ouvMIfSDeElJAP+DIpKNOVVq6xm8Pyhc5Ua6JddW9uozS8IEIzv8J05E461
MO+fqbExLM1FsN+cwDzt13CEcz88e3z121xwrEwWVsqLjwxnicA71vHpMJTbl/lS
+KiE7dOSN0jFKfULnRnn0I8bNSvZg1H/vASBqRmSGLRBhnUsgFhcuYmBeQmJNVIn
+h2AiCWOjp7xQBgt1EVwxp55nf7LsF0SYL+IKDy94d0pkzSfFEEkEpKY0K1GaIdX
WEqGK/SbRY3I9CeFwtApI9D1Rbo4tPedfviyajZJXDUctGbiu94eoMSC3HSfaOmv
H+tTizhncLKoeiAUZMMli9OF07DrhQZcQWAMUEwKL0/dmomUVdDMS+QOyNnUbJ4i
l2GqnM81BtyKGGjZkyMwk6W61vyfa+bMBpyXD3BzlBtzPmI/DtpOpFjSfaJPP7cK
eR/DYq0887TIfA+ayph4YISdJgzfdI/c2QFvCZToM7ZvXUNpGNMuc9u5clEqu3T+
EUKMZAbiGjLPgcuusPJtT0P3RxP5haTaqZhfU0+MbFSFl7zlS6uE5DEvgEflRpcO
uY1kw+vT4H7G8N7GbvIUqACm7wIDAQABo4IB1DCCAdAwDwYDVR0TAQH/BAUwAwEB
/zAdBgNVHQ4EFgQU8KoLF6T782Du0ciPgirVuM9G8ogwHwYDVR0jBBgwFoAUmUUY
9ijGZnIWADolnZMk/zKzhxkwCwYDVR0PBAQDAgGmMBMGA1UdJQQMMAoGCCsGAQUF
BwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29t
L1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtpLmJhY2t1cC5jb20v
U3BhcmtsaW5nUm9vdC5jcmwwFAYDVR0RBA0wC4IJbG9jYWxob3N0MIHWBggrBgEF
BQcBAQSByTCBxjA4BggrBgEFBQcwAoYsaHR0cDovL3BraS5zcGFya2xpbmdjYS5j
b20vU3BhcmtsaW5nUm9vdC5jcnQwMwYIKwYBBQUHMAKGJ2h0dHA6Ly9wa2kuYmFj
a3VwLmNvbS9TcGFya2xpbmdSb290LmNydDAsBggrBgEFBQcwAYYgaHR0cDovL3Br
aS5zcGFya2xpbmdjYS5jb20vb2NzcC8wJwYIKwYBBQUHMAGGG2h0dHA6Ly9wa2ku
YmFja3VwLmNvbS9vY3NwLzANBgkqhkiG9w0BAQsFAAOCBAEAMOckms25gifFNM4X
O1T77aU/JiaK2Wp2baS7+iRN1d/5ajxFCWBg7i8J8lWw1harx3ri5htOC4hA8luI
EtKZjPtwfCvFl1vfG4huM1VPhqCV341N7liIyHidm0EAlg+yKGr8ncrjjFJ0fvVt
3o0K+d6gYZ4KP6j1bxB5HFC+vMIyee4eAe1m8lTZZFB8ZkjtJroIjZxfqUwBqWTS
snSARFttQr5U+Xu+oqcbwpIyLfWT3xIDjSupXkbu86Dte3dHatQxBspd9IxrRQpb
gU4KGQVtxbakHU2xsjbUmHlUZR1A77AsQYBHGp1o3wSb81WT765i96oGno3IHE+q
M36VwE9phS450eUfOVYySGaBGn5aDP3hVnX+R7xnCI9Gs9+dzR5xDo+tETvCqSvo
m+foGkjK8a3wkF9kVD/Vwm1rYpNQ0aTrZuWszSbs6V6uqnf2HOFJOoWr7QoMCTTp
MQEj/hAXH03ueq2M+BC0nits2+h8XkjuL6Ss34vlzECAs+XScLk2gjUUH+XwaoRN
H3hgEXp481yfW5m6jjc/nwVza6c8vfcgF9f4atGFkOF+kp5MJWP5z3bveGxqsfTY
q17gRGwtmInlO5GB1e8SWuOtM177SUoykiC0O6mtDg82ptEHdpQf0KAthpId0ulU
lg4n0uXFVweqAsVUGi+Ii0GcNyWlfxEikf19ERBbjrtCfYgeJjcjjDwjQhff3InZ
cOMdZtIXSiahlhBBE6aXSkCn46C15rPqO+8aUrTAkWeIWVt8OiqzRsbXwrjj/yhK
xktRpD+v6ZLviudhxU1wnGzPMADyVend1LzfOvD9kFvqgebldj4+oDg3hyH1cqXe
/lEOO+epB2yo14tsGqzOnSmS2F3rFfOUVZdj6yUMLiSyKnrJ/76VKh0LBexPFbvb
GNunZDGHQ8Y++zRjsskhMcY729ABMK17x7kBeOqaJ1o8yi1HrKufOXiKdHILBZ+K
0cpU/r3YaK7OdcY3P5ne4w1XtqY17FT+LuNCiSJDI0SX4gg4PVTFCn6no91USCaw
aTyx9C0abcSJCUXZIZG4Dxj0qxJr/jzH/RshLTOsn0WVQowkaTMTVTArHi3e0oGL
HdhgN+CRdfE8RK+HrW9DuMCfXzWHDvhbi2lx+jSi76PRhBCK1XDmLG1nVaFwr1o9
CzH8e7OJB7E7E+r21ecGzvc9CuKcvLLTkhaLhsoCLBe9EmfvMX1gtwbUHdYAx+Y7
Q8FP3DaDfUmF1TGgncXBoQpab7MzEeIGtFYtFteKWrAgVCwpX2R1CJVYXCgkaDmx
iWoTwpGpXm8usNpvIyIMwqU14NcCmZEzCZT62UFzfvB7nivLhoZTl2VbDB10xf8p
TnPf5g==
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
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
VN9C85/NOQXjpMb0hB6sL9R1HBjcAl8=
-----END PRIVATE KEY-----
)";
  const char* kCaChainCert = R"(
-----BEGIN CERTIFICATE-----
MIILcDCCB1igAwIBAgICEAQwDQYJKoZIhvcNAQELBQAwfzELMAkGA1UEBhMCVVMx
EzARBgNVBAgMClNvbWUtU3RhdGUxIzAhBgNVBAoMGkFwYWNoZSBTb2Z0d2FyZSBG
b3VuZGF0aW9uMRIwEAYDVQQDDAkxMjcuMC4wLjExIjAgBgkqhkiG9w0BCQEWE2Rl
dkBrdWR1LmFwYWNoZS5vcmcwHhcNMjQwNzIzMTk1NTM0WhcNNDQwNzE4MTk1NTM0
WjCBnjEXMBUGA1UEAwwOSW50ZXJtZWRpYXRlQ0ExEzARBgNVBAgMCkNhbGlmb3Ju
aWExCzAJBgNVBAYTAlVTMSIwIAYJKoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFjaGUu
b3JnMSMwIQYDVQQKDBpBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbjEYMBYGA1UE
CwwPSW50ZXJtZWRpYXRlIENBMIIEIjANBgkqhkiG9w0BAQEFAAOCBA8AMIIECgKC
BAEAwpQU6QayDO4u4/oYTO99GBRomJC2vC7NnuTmcg6Y/vYv76cU4I12adZsbQHZ
kCWIwqQqpJeEPfzmr6HwX5FfSmUVGj6eTNpTM+ZzojAlEe2hUteU+NHW6yYrlyqO
+EMTRgdd4zirJa7TcYj+GyEL9Hk0sEKtD98wU1WUVgyu9HyxR2A6iAGLf0wraU/7
XbO0tMRLG829kQp+TgKIfs/96/HeUNS6++H7SxdWR8fCZzwDy+s5Tah25hmwVumU
eZ2zPES8zGyp9v69bJD9CtJxPWKnMcXYDZ18Cag+WVWjTWrV+1VdpPCD8Jnv8eDU
hw2xOi0xStDB3+XP4d1l0IQHlTPTp/ViELJmHiGXZBh0lYGrDYyqx6Y6wGKv72Pr
OvKzXtWrfoIDjSu2QBKm4Lj8MAbEe6tvtBAUXtMMYFCpfHZHc7IF1eF3ffThsS//
LTHiEG/rU7MSoU0i7CMVO52v7TdWIwMJJQBao+u5duaFlCu8CpllxhLmOjyNUYCO
7QyvxAm+60DEDYj3yh16tnwKj8lNEyBrr2yFL0G8YFYC0ILy2Sp1qoXifAFWOY9p
nFudkPSmsbXTn9wZI2iXQQF6K7DdjsWxo6m+ovRg9MDRjquGSo1Ag39fp7E+OcRO
9q+8eq91mBGRTcYucga3erbkQgaeCka2Dlbo6QWZ2KQ4G9E1E139oBUOykBqaiU+
LoDF/7AA5LUT/aMkM1YEWeubGBI9d9AEAb5JQqP/aIiivujB7gfmsFJZeKEgXs21
rbKg7ouvMIfSDeElJAP+DIpKNOVVq6xm8Pyhc5Ua6JddW9uozS8IEIzv8J05E461
MO+fqbExLM1FsN+cwDzt13CEcz88e3z121xwrEwWVsqLjwxnicA71vHpMJTbl/lS
+KiE7dOSN0jFKfULnRnn0I8bNSvZg1H/vASBqRmSGLRBhnUsgFhcuYmBeQmJNVIn
+h2AiCWOjp7xQBgt1EVwxp55nf7LsF0SYL+IKDy94d0pkzSfFEEkEpKY0K1GaIdX
WEqGK/SbRY3I9CeFwtApI9D1Rbo4tPedfviyajZJXDUctGbiu94eoMSC3HSfaOmv
H+tTizhncLKoeiAUZMMli9OF07DrhQZcQWAMUEwKL0/dmomUVdDMS+QOyNnUbJ4i
l2GqnM81BtyKGGjZkyMwk6W61vyfa+bMBpyXD3BzlBtzPmI/DtpOpFjSfaJPP7cK
eR/DYq0887TIfA+ayph4YISdJgzfdI/c2QFvCZToM7ZvXUNpGNMuc9u5clEqu3T+
EUKMZAbiGjLPgcuusPJtT0P3RxP5haTaqZhfU0+MbFSFl7zlS6uE5DEvgEflRpcO
uY1kw+vT4H7G8N7GbvIUqACm7wIDAQABo4IB1DCCAdAwDwYDVR0TAQH/BAUwAwEB
/zAdBgNVHQ4EFgQU8KoLF6T782Du0ciPgirVuM9G8ogwHwYDVR0jBBgwFoAUmUUY
9ijGZnIWADolnZMk/zKzhxkwCwYDVR0PBAQDAgGmMBMGA1UdJQQMMAoGCCsGAQUF
BwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29t
L1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtpLmJhY2t1cC5jb20v
U3BhcmtsaW5nUm9vdC5jcmwwFAYDVR0RBA0wC4IJbG9jYWxob3N0MIHWBggrBgEF
BQcBAQSByTCBxjA4BggrBgEFBQcwAoYsaHR0cDovL3BraS5zcGFya2xpbmdjYS5j
b20vU3BhcmtsaW5nUm9vdC5jcnQwMwYIKwYBBQUHMAKGJ2h0dHA6Ly9wa2kuYmFj
a3VwLmNvbS9TcGFya2xpbmdSb290LmNydDAsBggrBgEFBQcwAYYgaHR0cDovL3Br
aS5zcGFya2xpbmdjYS5jb20vb2NzcC8wJwYIKwYBBQUHMAGGG2h0dHA6Ly9wa2ku
YmFja3VwLmNvbS9vY3NwLzANBgkqhkiG9w0BAQsFAAOCBAEAMOckms25gifFNM4X
O1T77aU/JiaK2Wp2baS7+iRN1d/5ajxFCWBg7i8J8lWw1harx3ri5htOC4hA8luI
EtKZjPtwfCvFl1vfG4huM1VPhqCV341N7liIyHidm0EAlg+yKGr8ncrjjFJ0fvVt
3o0K+d6gYZ4KP6j1bxB5HFC+vMIyee4eAe1m8lTZZFB8ZkjtJroIjZxfqUwBqWTS
snSARFttQr5U+Xu+oqcbwpIyLfWT3xIDjSupXkbu86Dte3dHatQxBspd9IxrRQpb
gU4KGQVtxbakHU2xsjbUmHlUZR1A77AsQYBHGp1o3wSb81WT765i96oGno3IHE+q
M36VwE9phS450eUfOVYySGaBGn5aDP3hVnX+R7xnCI9Gs9+dzR5xDo+tETvCqSvo
m+foGkjK8a3wkF9kVD/Vwm1rYpNQ0aTrZuWszSbs6V6uqnf2HOFJOoWr7QoMCTTp
MQEj/hAXH03ueq2M+BC0nits2+h8XkjuL6Ss34vlzECAs+XScLk2gjUUH+XwaoRN
H3hgEXp481yfW5m6jjc/nwVza6c8vfcgF9f4atGFkOF+kp5MJWP5z3bveGxqsfTY
q17gRGwtmInlO5GB1e8SWuOtM177SUoykiC0O6mtDg82ptEHdpQf0KAthpId0ulU
lg4n0uXFVweqAsVUGi+Ii0GcNyWlfxEikf19ERBbjrtCfYgeJjcjjDwjQhff3InZ
cOMdZtIXSiahlhBBE6aXSkCn46C15rPqO+8aUrTAkWeIWVt8OiqzRsbXwrjj/yhK
xktRpD+v6ZLviudhxU1wnGzPMADyVend1LzfOvD9kFvqgebldj4+oDg3hyH1cqXe
/lEOO+epB2yo14tsGqzOnSmS2F3rFfOUVZdj6yUMLiSyKnrJ/76VKh0LBexPFbvb
GNunZDGHQ8Y++zRjsskhMcY729ABMK17x7kBeOqaJ1o8yi1HrKufOXiKdHILBZ+K
0cpU/r3YaK7OdcY3P5ne4w1XtqY17FT+LuNCiSJDI0SX4gg4PVTFCn6no91USCaw
aTyx9C0abcSJCUXZIZG4Dxj0qxJr/jzH/RshLTOsn0WVQowkaTMTVTArHi3e0oGL
HdhgN+CRdfE8RK+HrW9DuMCfXzWHDvhbi2lx+jSi76PRhBCK1XDmLG1nVaFwr1o9
CzH8e7OJB7E7E+r21ecGzvc9CuKcvLLTkhaLhsoCLBe9EmfvMX1gtwbUHdYAx+Y7
Q8FP3DaDfUmF1TGgncXBoQpab7MzEeIGtFYtFteKWrAgVCwpX2R1CJVYXCgkaDmx
iWoTwpGpXm8usNpvIyIMwqU14NcCmZEzCZT62UFzfvB7nivLhoZTl2VbDB10xf8p
TnPf5g==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIJ4TCCBcmgAwIBAgIUEDOKKRnqgWhlJ5Wuuw6V1PNjCxEwDQYJKoZIhvcNAQEL
BQAwfzELMAkGA1UEBhMCVVMxEzARBgNVBAgMClNvbWUtU3RhdGUxIzAhBgNVBAoM
GkFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uMRIwEAYDVQQDDAkxMjcuMC4wLjEx
IjAgBgkqhkiG9w0BCQEWE2RldkBrdWR1LmFwYWNoZS5vcmcwIBcNMjQwNzIzMTc1
MDExWhgPMjEyNDA2MjkxNzUwMTFaMH8xCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApT
b21lLVN0YXRlMSMwIQYDVQQKDBpBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbjES
MBAGA1UEAwwJMTI3LjAuMC4xMSIwIAYJKoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFj
aGUub3JnMIIEIjANBgkqhkiG9w0BAQEFAAOCBA8AMIIECgKCBAEA5LSzTp5Jg1OK
uFkAsAofiOxnoUPzH/e1hadFKgxM4gtix3nC4TVUz+xUJKxRUS6ka+Ft2i1wfPH0
VEKROUQUK3UufWK1GvTpUclKoQmZ7/aNBn7Qj8ZsPi2ZWq6yW6aESVlwUS+DBz4S
pPXOBlxmCnSFL8ettCYuu76pUjrdSkWl6yNvv+zOGwdtIBadJCITZ6dhENK/vZs1
15IWfHhToAGBu57rsWl6qA09IHGRpvfe6Laju8QlzmoPUtpwhB3PtLZw4rHDt4s5
tEWG4LIxLyVwG55Odo2dsG3xJQFkwS8l8gsyK9kSvC5J/Qji3disfmmdXRObKK/4
QQGABBeQB0UOM7pE/GhCV9UGpJK6BN8BXK8Or3jJBkdbwAWBTZ1mzhGzykZG2eg4
5RbS4sRYOkuD5EZH/0kGBiL8mitTZjlI81+1hDVwr2SN3EPUqWjiw5yC6sVof1IX
6LPGdkAsY3F4Lm8QCL4k8i6dZJQSK11vcV6d/rAqEeUYWIQb231I5URps3Rw8XSN
TFuW88Pgmk7saTRrLVtVwHHnaNutBV0VS+7rRPdzLa8q23es7eawMhn2ZNWFPHy5
mzXaJV942HGUGo4Q1gQ+HskCAXmLGgBD1pde291cXjSNmuNMDi/YeuGkMG86C+Aq
V6e7KdAdD4akLu0U30tgqt6l3EpkKp/QL/81p5b4/gWuHO5NzmFFxnJHMDjEXi22
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
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
// The below certs has been generated using OpenSSL3, with the following changes
// to the above article:
//    * -aes256 is omitted
//    * in 'openssl req' commands the '-sha256' switch is omitted
//    * the commonName in 'openssl ca' is set to 'localhost'
//    * in ca.conf [alt_names] DNS.0 is set to 'localhost'
//    * validity in ca.conf is set to 7200
//
// Note about the cert nomenclature:
//
// article      <-> this file
// -------          ---------
// root         <-> root
// intermediate <-> intermediate
// enduser      <-> server
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
MIIJoTCCBYmgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwgZ4xFzAVBgNVBAMMDklu
dGVybWVkaWF0ZUNBMRMwEQYDVQQIDApDYWxpZm9ybmlhMQswCQYDVQQGEwJVUzEi
MCAGCSqGSIb3DQEJARYTZGV2QGt1ZHUuYXBhY2hlLm9yZzEjMCEGA1UECgwaQXBh
Y2hlIFNvZnR3YXJlIEZvdW5kYXRpb24xGDAWBgNVBAsMD0ludGVybWVkaWF0ZSBD
QTAeFw0yNDA3MjMxOTU4MTlaFw00NDA0MDkxOTU4MTlaMIGOMRIwEAYDVQQDDAkx
MjcuMC4wLjExEzARBgNVBAgMCkNhbGlmb3JuaWExCzAJBgNVBAYTAlVTMSIwIAYJ
KoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFjaGUub3JnMSMwIQYDVQQKDBpBcGFjaGUg
U29mdHdhcmUgRm91bmRhdGlvbjENMAsGA1UECwwES3VkdTCCAiIwDQYJKoZIhvcN
AQEBBQADggIPADCCAgoCggIBAI/O8ezskBVo/BzldVjkvzJwvDhz6JZe26NU4qGu
bevZiyYKhCBeCJ0bgvEP0tkQ1DCTIPSyWTGNzVrm9a3oJbudsocWGTscdZT7LFPm
oaoMJbVtNVT35iy6TVuBKugrzz4azrrhnsdJnG/Px937yhARDs6AnCB5LNoBgQMd
4cJ41H6xwexqkjgdNQfRAtyHZT0nyjrONTEoZMfNTAbdawYptTz7ft7W8NIDrzAl
lXnTxJc9ycnfF0pbNao1Msw1y0pjuRcE/EKdPQ1VK/B3MdQOlRFSl2BI/B9Jzfe9
o+WQG984kprb767JWiO4W8DKsT64IgOTCa/1A4ExKAbIqngM4+RrznFhDLxrHvsL
CSH9dCL6VAxQeLxcrQpkekwj9k6qQTlY4J337LHBy3izWmfj4kiVH+EN+Jg8HqIt
1CVP+pMb9eoOXwur98wsYX/1He5/XAaFSrJ486OLz2wYQ5KMYc+xzjG1q8zMhtLA
R8Vn/uPRg0SJATphvs6hRSAbRkz6fdU+HpuAdlqfdOrlQRtL397BTMzA0ksJP/HZ
VhCifsNcXWACr8ue9Seyd1Nrq3dw2oKfQf+oyS18RAf2gpMEGt0gScVWPfsbuFb7
Twntxiakb1ozfqDpuoOjOgxEQkIWluw/Cm/uYxw7KinAJoZQ6h1NstxDuIczsWyk
F2YhAgMBAAGjggH1MIIB8TAMBgNVHRMBAf8EAjAAMB0GA1UdDgQWBBR9hKckR8a1
YQEVfScXpaH1V32UeDAfBgNVHSMEGDAWgBTwqgsXpPvzYO7RyI+CKtW4z0byiDAL
BgNVHQ8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwfgYDVR0fBHcwdTA7oDmg
N4Y1aHR0cDovL3BraS5zcGFya2xpbmdjYS5jb20vU3BhcmtsaW5nSW50ZXJtaWRp
YXRlNS5jcmwwNqA0oDKGMGh0dHA6Ly9wa2kuYmFja3VwLmNvbS9TcGFya2xpbmdJ
bnRlcm1pZGlhdGU1LmNybDAUBgNVHREEDTALgglsb2NhbGhvc3QwgegGCCsGAQUF
BwEBBIHbMIHYMEEGCCsGAQUFBzAChjVodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNv
bS9TcGFya2xpbmdJbnRlcm1lZGlhdGU1LmNydDA8BggrBgEFBQcwAoYwaHR0cDov
L3BraS5iYWNrdXAuY29tL1NwYXJrbGluZ0ludGVybWVkaWF0ZTUuY3J0MCwGCCsG
AQUFBzABhiBodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNvbS9vY3NwLzAnBggrBgEF
BQcwAYYbaHR0cDovL3BraS5iYWNrdXAuY29tL29jc3AvMA0GCSqGSIb3DQEBCwUA
A4IEAQCbWm3Q5suIXBYVkzbU4ClxvkHASayoBoxx5pH5xeC+9IXYHGTwtBRl06NM
E3Koj0vqNTdg0cQnqaqJBjaXNdCRnDSlcs+K1xh/hzgjTLNb7PnVzBg8lauu86CE
eEKsrolAmSTsAEUfwrxiQ8MPJTJLLXN6r15lqCIeFdgKtiM2EPg3rBtTxKIwvpQB
efAAbGI+gp4MlaZULf2695V0GHPdsenydYfYFTc/cCcSJlJi7QSvp3gAIqK5o2A1
HmoGHQArVcHBAloAbLLQ1nM4l2O1E5J5DZ4mRwhQiE/ShBMVOAEFqSNn+n/q6Z9t
gCy1lgGuWG1/614OvOXROoLhNI6yaDZy5N+LfMKCMP+F2hwLx+id2T9qh2tCBOuO
xG2MxF+sE/4B2oAnQ2S/FFdIarSiKEInyvamg7wtnsEYsG5yL62kwjMcxOhhm1K9
suAjp8Rq5wZC2Z0esZ5hmQEigtnh815NSILHBH4BNye+b4UTVX3lkg92Q5snsRkn
PMRL728DrIeFOcXiCsVcn/5iPtz+y58Xb8LYoDfNkyTh3EKC3JbheN6Xr+mfiw8u
0AWUCPGFB99X6wgCfKCug2BZ5bT2a4Zc8+PMWRNOG5kd/UqeRLo5zNSZIGPSjYSB
QQYgs31IoRL74sUnfKGdx2iSua7T0946Iph9DU8mQK+NudtrB8UVzgUWzRbx0/wp
SC1zbqCWvrPY8en2w2B+kPJWtRW3sqR4TRJiegjgEdeJu8NrikC+A0hr1Obgckro
NXkwLt4df0wHtj5Vg0w3c+twwlESKErkON4wWaUGKEM8ckdRuHp3K+Hhgo2Y2iUx
Ydd6zOe+X95Zt4bOdPIN8SjhKpc40UIBIaVhma94nEjNXZCbovlSxjP3q59QXXzn
qfMru778lBpC2TZMqmErJ35C6gIvKbM8WbfIOcyj0S17yFVwcjxeLslQXAXvOPZB
lpXF1qy4czCWLI9GTZMV3y6XK5gb55vw8lHze271EPds2nb9E/ntYcULs5uDSwKh
Yni0MtWZN6LY4gIle5L/AstNkk70CeGUYDt3pPB38C/GbZYo1mVEayOQXSW7Ml4K
NFqkf7UW0wfKx2Ki8hR6s6W46iUS8PNtCs3gabZHisJM6FcTIF1FiXWlLZeoEZJW
QIirrEyCZUsgqGedAcyHU4SoHIdGqVpPHol33yr3tNYImS7mViHpusPlgg5bta4d
6NrckJQlrs/zD+rM37xVKIvC8GnH/h3+du9JWCgb2Cn0gyOF1VcZyqJ+xBF2AFY3
vb0cYv2ddTb+nMIbiARDaEIwG2QYC9uTrtUEBuoob7ObS7ddjNlLhzeGp3NpE9q4
92AnEo0pFET79a8mYFtcagFruEJ7
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIILcDCCB1igAwIBAgICEAQwDQYJKoZIhvcNAQELBQAwfzELMAkGA1UEBhMCVVMx
EzARBgNVBAgMClNvbWUtU3RhdGUxIzAhBgNVBAoMGkFwYWNoZSBTb2Z0d2FyZSBG
b3VuZGF0aW9uMRIwEAYDVQQDDAkxMjcuMC4wLjExIjAgBgkqhkiG9w0BCQEWE2Rl
dkBrdWR1LmFwYWNoZS5vcmcwHhcNMjQwNzIzMTk1NTM0WhcNNDQwNzE4MTk1NTM0
WjCBnjEXMBUGA1UEAwwOSW50ZXJtZWRpYXRlQ0ExEzARBgNVBAgMCkNhbGlmb3Ju
aWExCzAJBgNVBAYTAlVTMSIwIAYJKoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFjaGUu
b3JnMSMwIQYDVQQKDBpBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbjEYMBYGA1UE
CwwPSW50ZXJtZWRpYXRlIENBMIIEIjANBgkqhkiG9w0BAQEFAAOCBA8AMIIECgKC
BAEAwpQU6QayDO4u4/oYTO99GBRomJC2vC7NnuTmcg6Y/vYv76cU4I12adZsbQHZ
kCWIwqQqpJeEPfzmr6HwX5FfSmUVGj6eTNpTM+ZzojAlEe2hUteU+NHW6yYrlyqO
+EMTRgdd4zirJa7TcYj+GyEL9Hk0sEKtD98wU1WUVgyu9HyxR2A6iAGLf0wraU/7
XbO0tMRLG829kQp+TgKIfs/96/HeUNS6++H7SxdWR8fCZzwDy+s5Tah25hmwVumU
eZ2zPES8zGyp9v69bJD9CtJxPWKnMcXYDZ18Cag+WVWjTWrV+1VdpPCD8Jnv8eDU
hw2xOi0xStDB3+XP4d1l0IQHlTPTp/ViELJmHiGXZBh0lYGrDYyqx6Y6wGKv72Pr
OvKzXtWrfoIDjSu2QBKm4Lj8MAbEe6tvtBAUXtMMYFCpfHZHc7IF1eF3ffThsS//
LTHiEG/rU7MSoU0i7CMVO52v7TdWIwMJJQBao+u5duaFlCu8CpllxhLmOjyNUYCO
7QyvxAm+60DEDYj3yh16tnwKj8lNEyBrr2yFL0G8YFYC0ILy2Sp1qoXifAFWOY9p
nFudkPSmsbXTn9wZI2iXQQF6K7DdjsWxo6m+ovRg9MDRjquGSo1Ag39fp7E+OcRO
9q+8eq91mBGRTcYucga3erbkQgaeCka2Dlbo6QWZ2KQ4G9E1E139oBUOykBqaiU+
LoDF/7AA5LUT/aMkM1YEWeubGBI9d9AEAb5JQqP/aIiivujB7gfmsFJZeKEgXs21
rbKg7ouvMIfSDeElJAP+DIpKNOVVq6xm8Pyhc5Ua6JddW9uozS8IEIzv8J05E461
MO+fqbExLM1FsN+cwDzt13CEcz88e3z121xwrEwWVsqLjwxnicA71vHpMJTbl/lS
+KiE7dOSN0jFKfULnRnn0I8bNSvZg1H/vASBqRmSGLRBhnUsgFhcuYmBeQmJNVIn
+h2AiCWOjp7xQBgt1EVwxp55nf7LsF0SYL+IKDy94d0pkzSfFEEkEpKY0K1GaIdX
WEqGK/SbRY3I9CeFwtApI9D1Rbo4tPedfviyajZJXDUctGbiu94eoMSC3HSfaOmv
H+tTizhncLKoeiAUZMMli9OF07DrhQZcQWAMUEwKL0/dmomUVdDMS+QOyNnUbJ4i
l2GqnM81BtyKGGjZkyMwk6W61vyfa+bMBpyXD3BzlBtzPmI/DtpOpFjSfaJPP7cK
eR/DYq0887TIfA+ayph4YISdJgzfdI/c2QFvCZToM7ZvXUNpGNMuc9u5clEqu3T+
EUKMZAbiGjLPgcuusPJtT0P3RxP5haTaqZhfU0+MbFSFl7zlS6uE5DEvgEflRpcO
uY1kw+vT4H7G8N7GbvIUqACm7wIDAQABo4IB1DCCAdAwDwYDVR0TAQH/BAUwAwEB
/zAdBgNVHQ4EFgQU8KoLF6T782Du0ciPgirVuM9G8ogwHwYDVR0jBBgwFoAUmUUY
9ijGZnIWADolnZMk/zKzhxkwCwYDVR0PBAQDAgGmMBMGA1UdJQQMMAoGCCsGAQUF
BwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29t
L1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtpLmJhY2t1cC5jb20v
U3BhcmtsaW5nUm9vdC5jcmwwFAYDVR0RBA0wC4IJbG9jYWxob3N0MIHWBggrBgEF
BQcBAQSByTCBxjA4BggrBgEFBQcwAoYsaHR0cDovL3BraS5zcGFya2xpbmdjYS5j
b20vU3BhcmtsaW5nUm9vdC5jcnQwMwYIKwYBBQUHMAKGJ2h0dHA6Ly9wa2kuYmFj
a3VwLmNvbS9TcGFya2xpbmdSb290LmNydDAsBggrBgEFBQcwAYYgaHR0cDovL3Br
aS5zcGFya2xpbmdjYS5jb20vb2NzcC8wJwYIKwYBBQUHMAGGG2h0dHA6Ly9wa2ku
YmFja3VwLmNvbS9vY3NwLzANBgkqhkiG9w0BAQsFAAOCBAEAMOckms25gifFNM4X
O1T77aU/JiaK2Wp2baS7+iRN1d/5ajxFCWBg7i8J8lWw1harx3ri5htOC4hA8luI
EtKZjPtwfCvFl1vfG4huM1VPhqCV341N7liIyHidm0EAlg+yKGr8ncrjjFJ0fvVt
3o0K+d6gYZ4KP6j1bxB5HFC+vMIyee4eAe1m8lTZZFB8ZkjtJroIjZxfqUwBqWTS
snSARFttQr5U+Xu+oqcbwpIyLfWT3xIDjSupXkbu86Dte3dHatQxBspd9IxrRQpb
gU4KGQVtxbakHU2xsjbUmHlUZR1A77AsQYBHGp1o3wSb81WT765i96oGno3IHE+q
M36VwE9phS450eUfOVYySGaBGn5aDP3hVnX+R7xnCI9Gs9+dzR5xDo+tETvCqSvo
m+foGkjK8a3wkF9kVD/Vwm1rYpNQ0aTrZuWszSbs6V6uqnf2HOFJOoWr7QoMCTTp
MQEj/hAXH03ueq2M+BC0nits2+h8XkjuL6Ss34vlzECAs+XScLk2gjUUH+XwaoRN
H3hgEXp481yfW5m6jjc/nwVza6c8vfcgF9f4atGFkOF+kp5MJWP5z3bveGxqsfTY
q17gRGwtmInlO5GB1e8SWuOtM177SUoykiC0O6mtDg82ptEHdpQf0KAthpId0ulU
lg4n0uXFVweqAsVUGi+Ii0GcNyWlfxEikf19ERBbjrtCfYgeJjcjjDwjQhff3InZ
cOMdZtIXSiahlhBBE6aXSkCn46C15rPqO+8aUrTAkWeIWVt8OiqzRsbXwrjj/yhK
xktRpD+v6ZLviudhxU1wnGzPMADyVend1LzfOvD9kFvqgebldj4+oDg3hyH1cqXe
/lEOO+epB2yo14tsGqzOnSmS2F3rFfOUVZdj6yUMLiSyKnrJ/76VKh0LBexPFbvb
GNunZDGHQ8Y++zRjsskhMcY729ABMK17x7kBeOqaJ1o8yi1HrKufOXiKdHILBZ+K
0cpU/r3YaK7OdcY3P5ne4w1XtqY17FT+LuNCiSJDI0SX4gg4PVTFCn6no91USCaw
aTyx9C0abcSJCUXZIZG4Dxj0qxJr/jzH/RshLTOsn0WVQowkaTMTVTArHi3e0oGL
HdhgN+CRdfE8RK+HrW9DuMCfXzWHDvhbi2lx+jSi76PRhBCK1XDmLG1nVaFwr1o9
CzH8e7OJB7E7E+r21ecGzvc9CuKcvLLTkhaLhsoCLBe9EmfvMX1gtwbUHdYAx+Y7
Q8FP3DaDfUmF1TGgncXBoQpab7MzEeIGtFYtFteKWrAgVCwpX2R1CJVYXCgkaDmx
iWoTwpGpXm8usNpvIyIMwqU14NcCmZEzCZT62UFzfvB7nivLhoZTl2VbDB10xf8p
TnPf5g==
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
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
VN9C85/NOQXjpMb0hB6sL9R1HBjcAl8=
-----END PRIVATE KEY-----
)";
  const char* kRootCaCert = R"(
-----BEGIN CERTIFICATE-----
MIIJ4TCCBcmgAwIBAgIUEDOKKRnqgWhlJ5Wuuw6V1PNjCxEwDQYJKoZIhvcNAQEL
BQAwfzELMAkGA1UEBhMCVVMxEzARBgNVBAgMClNvbWUtU3RhdGUxIzAhBgNVBAoM
GkFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uMRIwEAYDVQQDDAkxMjcuMC4wLjEx
IjAgBgkqhkiG9w0BCQEWE2RldkBrdWR1LmFwYWNoZS5vcmcwIBcNMjQwNzIzMTc1
MDExWhgPMjEyNDA2MjkxNzUwMTFaMH8xCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApT
b21lLVN0YXRlMSMwIQYDVQQKDBpBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbjES
MBAGA1UEAwwJMTI3LjAuMC4xMSIwIAYJKoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFj
aGUub3JnMIIEIjANBgkqhkiG9w0BAQEFAAOCBA8AMIIECgKCBAEA5LSzTp5Jg1OK
uFkAsAofiOxnoUPzH/e1hadFKgxM4gtix3nC4TVUz+xUJKxRUS6ka+Ft2i1wfPH0
VEKROUQUK3UufWK1GvTpUclKoQmZ7/aNBn7Qj8ZsPi2ZWq6yW6aESVlwUS+DBz4S
pPXOBlxmCnSFL8ettCYuu76pUjrdSkWl6yNvv+zOGwdtIBadJCITZ6dhENK/vZs1
15IWfHhToAGBu57rsWl6qA09IHGRpvfe6Laju8QlzmoPUtpwhB3PtLZw4rHDt4s5
tEWG4LIxLyVwG55Odo2dsG3xJQFkwS8l8gsyK9kSvC5J/Qji3disfmmdXRObKK/4
QQGABBeQB0UOM7pE/GhCV9UGpJK6BN8BXK8Or3jJBkdbwAWBTZ1mzhGzykZG2eg4
5RbS4sRYOkuD5EZH/0kGBiL8mitTZjlI81+1hDVwr2SN3EPUqWjiw5yC6sVof1IX
6LPGdkAsY3F4Lm8QCL4k8i6dZJQSK11vcV6d/rAqEeUYWIQb231I5URps3Rw8XSN
TFuW88Pgmk7saTRrLVtVwHHnaNutBV0VS+7rRPdzLa8q23es7eawMhn2ZNWFPHy5
mzXaJV942HGUGo4Q1gQ+HskCAXmLGgBD1pde291cXjSNmuNMDi/YeuGkMG86C+Aq
V6e7KdAdD4akLu0U30tgqt6l3EpkKp/QL/81p5b4/gWuHO5NzmFFxnJHMDjEXi22
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

// These certificates are the same as used in CreateTestSSLCertWithChainSignedByRoot,
// except the ca_cert, which is replaced with an expired version.
Status CreateTestSSLExpiredCertWithChainSignedByRoot(const string& dir,
                                              string* cert_file,
                                              string* key_file,
                                              string* expired_ca_cert_file) {

   const char* kCert = R"(
-----BEGIN CERTIFICATE-----
MIIJoTCCBYmgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwgZ4xFzAVBgNVBAMMDklu
dGVybWVkaWF0ZUNBMRMwEQYDVQQIDApDYWxpZm9ybmlhMQswCQYDVQQGEwJVUzEi
MCAGCSqGSIb3DQEJARYTZGV2QGt1ZHUuYXBhY2hlLm9yZzEjMCEGA1UECgwaQXBh
Y2hlIFNvZnR3YXJlIEZvdW5kYXRpb24xGDAWBgNVBAsMD0ludGVybWVkaWF0ZSBD
QTAeFw0yNDA3MjMxOTU4MTlaFw00NDA0MDkxOTU4MTlaMIGOMRIwEAYDVQQDDAkx
MjcuMC4wLjExEzARBgNVBAgMCkNhbGlmb3JuaWExCzAJBgNVBAYTAlVTMSIwIAYJ
KoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFjaGUub3JnMSMwIQYDVQQKDBpBcGFjaGUg
U29mdHdhcmUgRm91bmRhdGlvbjENMAsGA1UECwwES3VkdTCCAiIwDQYJKoZIhvcN
AQEBBQADggIPADCCAgoCggIBAI/O8ezskBVo/BzldVjkvzJwvDhz6JZe26NU4qGu
bevZiyYKhCBeCJ0bgvEP0tkQ1DCTIPSyWTGNzVrm9a3oJbudsocWGTscdZT7LFPm
oaoMJbVtNVT35iy6TVuBKugrzz4azrrhnsdJnG/Px937yhARDs6AnCB5LNoBgQMd
4cJ41H6xwexqkjgdNQfRAtyHZT0nyjrONTEoZMfNTAbdawYptTz7ft7W8NIDrzAl
lXnTxJc9ycnfF0pbNao1Msw1y0pjuRcE/EKdPQ1VK/B3MdQOlRFSl2BI/B9Jzfe9
o+WQG984kprb767JWiO4W8DKsT64IgOTCa/1A4ExKAbIqngM4+RrznFhDLxrHvsL
CSH9dCL6VAxQeLxcrQpkekwj9k6qQTlY4J337LHBy3izWmfj4kiVH+EN+Jg8HqIt
1CVP+pMb9eoOXwur98wsYX/1He5/XAaFSrJ486OLz2wYQ5KMYc+xzjG1q8zMhtLA
R8Vn/uPRg0SJATphvs6hRSAbRkz6fdU+HpuAdlqfdOrlQRtL397BTMzA0ksJP/HZ
VhCifsNcXWACr8ue9Seyd1Nrq3dw2oKfQf+oyS18RAf2gpMEGt0gScVWPfsbuFb7
Twntxiakb1ozfqDpuoOjOgxEQkIWluw/Cm/uYxw7KinAJoZQ6h1NstxDuIczsWyk
F2YhAgMBAAGjggH1MIIB8TAMBgNVHRMBAf8EAjAAMB0GA1UdDgQWBBR9hKckR8a1
YQEVfScXpaH1V32UeDAfBgNVHSMEGDAWgBTwqgsXpPvzYO7RyI+CKtW4z0byiDAL
BgNVHQ8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwfgYDVR0fBHcwdTA7oDmg
N4Y1aHR0cDovL3BraS5zcGFya2xpbmdjYS5jb20vU3BhcmtsaW5nSW50ZXJtaWRp
YXRlNS5jcmwwNqA0oDKGMGh0dHA6Ly9wa2kuYmFja3VwLmNvbS9TcGFya2xpbmdJ
bnRlcm1pZGlhdGU1LmNybDAUBgNVHREEDTALgglsb2NhbGhvc3QwgegGCCsGAQUF
BwEBBIHbMIHYMEEGCCsGAQUFBzAChjVodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNv
bS9TcGFya2xpbmdJbnRlcm1lZGlhdGU1LmNydDA8BggrBgEFBQcwAoYwaHR0cDov
L3BraS5iYWNrdXAuY29tL1NwYXJrbGluZ0ludGVybWVkaWF0ZTUuY3J0MCwGCCsG
AQUFBzABhiBodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNvbS9vY3NwLzAnBggrBgEF
BQcwAYYbaHR0cDovL3BraS5iYWNrdXAuY29tL29jc3AvMA0GCSqGSIb3DQEBCwUA
A4IEAQCbWm3Q5suIXBYVkzbU4ClxvkHASayoBoxx5pH5xeC+9IXYHGTwtBRl06NM
E3Koj0vqNTdg0cQnqaqJBjaXNdCRnDSlcs+K1xh/hzgjTLNb7PnVzBg8lauu86CE
eEKsrolAmSTsAEUfwrxiQ8MPJTJLLXN6r15lqCIeFdgKtiM2EPg3rBtTxKIwvpQB
efAAbGI+gp4MlaZULf2695V0GHPdsenydYfYFTc/cCcSJlJi7QSvp3gAIqK5o2A1
HmoGHQArVcHBAloAbLLQ1nM4l2O1E5J5DZ4mRwhQiE/ShBMVOAEFqSNn+n/q6Z9t
gCy1lgGuWG1/614OvOXROoLhNI6yaDZy5N+LfMKCMP+F2hwLx+id2T9qh2tCBOuO
xG2MxF+sE/4B2oAnQ2S/FFdIarSiKEInyvamg7wtnsEYsG5yL62kwjMcxOhhm1K9
suAjp8Rq5wZC2Z0esZ5hmQEigtnh815NSILHBH4BNye+b4UTVX3lkg92Q5snsRkn
PMRL728DrIeFOcXiCsVcn/5iPtz+y58Xb8LYoDfNkyTh3EKC3JbheN6Xr+mfiw8u
0AWUCPGFB99X6wgCfKCug2BZ5bT2a4Zc8+PMWRNOG5kd/UqeRLo5zNSZIGPSjYSB
QQYgs31IoRL74sUnfKGdx2iSua7T0946Iph9DU8mQK+NudtrB8UVzgUWzRbx0/wp
SC1zbqCWvrPY8en2w2B+kPJWtRW3sqR4TRJiegjgEdeJu8NrikC+A0hr1Obgckro
NXkwLt4df0wHtj5Vg0w3c+twwlESKErkON4wWaUGKEM8ckdRuHp3K+Hhgo2Y2iUx
Ydd6zOe+X95Zt4bOdPIN8SjhKpc40UIBIaVhma94nEjNXZCbovlSxjP3q59QXXzn
qfMru778lBpC2TZMqmErJ35C6gIvKbM8WbfIOcyj0S17yFVwcjxeLslQXAXvOPZB
lpXF1qy4czCWLI9GTZMV3y6XK5gb55vw8lHze271EPds2nb9E/ntYcULs5uDSwKh
Yni0MtWZN6LY4gIle5L/AstNkk70CeGUYDt3pPB38C/GbZYo1mVEayOQXSW7Ml4K
NFqkf7UW0wfKx2Ki8hR6s6W46iUS8PNtCs3gabZHisJM6FcTIF1FiXWlLZeoEZJW
QIirrEyCZUsgqGedAcyHU4SoHIdGqVpPHol33yr3tNYImS7mViHpusPlgg5bta4d
6NrckJQlrs/zD+rM37xVKIvC8GnH/h3+du9JWCgb2Cn0gyOF1VcZyqJ+xBF2AFY3
vb0cYv2ddTb+nMIbiARDaEIwG2QYC9uTrtUEBuoob7ObS7ddjNlLhzeGp3NpE9q4
92AnEo0pFET79a8mYFtcagFruEJ7
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIILcDCCB1igAwIBAgICEAQwDQYJKoZIhvcNAQELBQAwfzELMAkGA1UEBhMCVVMx
EzARBgNVBAgMClNvbWUtU3RhdGUxIzAhBgNVBAoMGkFwYWNoZSBTb2Z0d2FyZSBG
b3VuZGF0aW9uMRIwEAYDVQQDDAkxMjcuMC4wLjExIjAgBgkqhkiG9w0BCQEWE2Rl
dkBrdWR1LmFwYWNoZS5vcmcwHhcNMjQwNzIzMTk1NTM0WhcNNDQwNzE4MTk1NTM0
WjCBnjEXMBUGA1UEAwwOSW50ZXJtZWRpYXRlQ0ExEzARBgNVBAgMCkNhbGlmb3Ju
aWExCzAJBgNVBAYTAlVTMSIwIAYJKoZIhvcNAQkBFhNkZXZAa3VkdS5hcGFjaGUu
b3JnMSMwIQYDVQQKDBpBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbjEYMBYGA1UE
CwwPSW50ZXJtZWRpYXRlIENBMIIEIjANBgkqhkiG9w0BAQEFAAOCBA8AMIIECgKC
BAEAwpQU6QayDO4u4/oYTO99GBRomJC2vC7NnuTmcg6Y/vYv76cU4I12adZsbQHZ
kCWIwqQqpJeEPfzmr6HwX5FfSmUVGj6eTNpTM+ZzojAlEe2hUteU+NHW6yYrlyqO
+EMTRgdd4zirJa7TcYj+GyEL9Hk0sEKtD98wU1WUVgyu9HyxR2A6iAGLf0wraU/7
XbO0tMRLG829kQp+TgKIfs/96/HeUNS6++H7SxdWR8fCZzwDy+s5Tah25hmwVumU
eZ2zPES8zGyp9v69bJD9CtJxPWKnMcXYDZ18Cag+WVWjTWrV+1VdpPCD8Jnv8eDU
hw2xOi0xStDB3+XP4d1l0IQHlTPTp/ViELJmHiGXZBh0lYGrDYyqx6Y6wGKv72Pr
OvKzXtWrfoIDjSu2QBKm4Lj8MAbEe6tvtBAUXtMMYFCpfHZHc7IF1eF3ffThsS//
LTHiEG/rU7MSoU0i7CMVO52v7TdWIwMJJQBao+u5duaFlCu8CpllxhLmOjyNUYCO
7QyvxAm+60DEDYj3yh16tnwKj8lNEyBrr2yFL0G8YFYC0ILy2Sp1qoXifAFWOY9p
nFudkPSmsbXTn9wZI2iXQQF6K7DdjsWxo6m+ovRg9MDRjquGSo1Ag39fp7E+OcRO
9q+8eq91mBGRTcYucga3erbkQgaeCka2Dlbo6QWZ2KQ4G9E1E139oBUOykBqaiU+
LoDF/7AA5LUT/aMkM1YEWeubGBI9d9AEAb5JQqP/aIiivujB7gfmsFJZeKEgXs21
rbKg7ouvMIfSDeElJAP+DIpKNOVVq6xm8Pyhc5Ua6JddW9uozS8IEIzv8J05E461
MO+fqbExLM1FsN+cwDzt13CEcz88e3z121xwrEwWVsqLjwxnicA71vHpMJTbl/lS
+KiE7dOSN0jFKfULnRnn0I8bNSvZg1H/vASBqRmSGLRBhnUsgFhcuYmBeQmJNVIn
+h2AiCWOjp7xQBgt1EVwxp55nf7LsF0SYL+IKDy94d0pkzSfFEEkEpKY0K1GaIdX
WEqGK/SbRY3I9CeFwtApI9D1Rbo4tPedfviyajZJXDUctGbiu94eoMSC3HSfaOmv
H+tTizhncLKoeiAUZMMli9OF07DrhQZcQWAMUEwKL0/dmomUVdDMS+QOyNnUbJ4i
l2GqnM81BtyKGGjZkyMwk6W61vyfa+bMBpyXD3BzlBtzPmI/DtpOpFjSfaJPP7cK
eR/DYq0887TIfA+ayph4YISdJgzfdI/c2QFvCZToM7ZvXUNpGNMuc9u5clEqu3T+
EUKMZAbiGjLPgcuusPJtT0P3RxP5haTaqZhfU0+MbFSFl7zlS6uE5DEvgEflRpcO
uY1kw+vT4H7G8N7GbvIUqACm7wIDAQABo4IB1DCCAdAwDwYDVR0TAQH/BAUwAwEB
/zAdBgNVHQ4EFgQU8KoLF6T782Du0ciPgirVuM9G8ogwHwYDVR0jBBgwFoAUmUUY
9ijGZnIWADolnZMk/zKzhxkwCwYDVR0PBAQDAgGmMBMGA1UdJQQMMAoGCCsGAQUF
BwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29t
L1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtpLmJhY2t1cC5jb20v
U3BhcmtsaW5nUm9vdC5jcmwwFAYDVR0RBA0wC4IJbG9jYWxob3N0MIHWBggrBgEF
BQcBAQSByTCBxjA4BggrBgEFBQcwAoYsaHR0cDovL3BraS5zcGFya2xpbmdjYS5j
b20vU3BhcmtsaW5nUm9vdC5jcnQwMwYIKwYBBQUHMAKGJ2h0dHA6Ly9wa2kuYmFj
a3VwLmNvbS9TcGFya2xpbmdSb290LmNydDAsBggrBgEFBQcwAYYgaHR0cDovL3Br
aS5zcGFya2xpbmdjYS5jb20vb2NzcC8wJwYIKwYBBQUHMAGGG2h0dHA6Ly9wa2ku
YmFja3VwLmNvbS9vY3NwLzANBgkqhkiG9w0BAQsFAAOCBAEAMOckms25gifFNM4X
O1T77aU/JiaK2Wp2baS7+iRN1d/5ajxFCWBg7i8J8lWw1harx3ri5htOC4hA8luI
EtKZjPtwfCvFl1vfG4huM1VPhqCV341N7liIyHidm0EAlg+yKGr8ncrjjFJ0fvVt
3o0K+d6gYZ4KP6j1bxB5HFC+vMIyee4eAe1m8lTZZFB8ZkjtJroIjZxfqUwBqWTS
snSARFttQr5U+Xu+oqcbwpIyLfWT3xIDjSupXkbu86Dte3dHatQxBspd9IxrRQpb
gU4KGQVtxbakHU2xsjbUmHlUZR1A77AsQYBHGp1o3wSb81WT765i96oGno3IHE+q
M36VwE9phS450eUfOVYySGaBGn5aDP3hVnX+R7xnCI9Gs9+dzR5xDo+tETvCqSvo
m+foGkjK8a3wkF9kVD/Vwm1rYpNQ0aTrZuWszSbs6V6uqnf2HOFJOoWr7QoMCTTp
MQEj/hAXH03ueq2M+BC0nits2+h8XkjuL6Ss34vlzECAs+XScLk2gjUUH+XwaoRN
H3hgEXp481yfW5m6jjc/nwVza6c8vfcgF9f4atGFkOF+kp5MJWP5z3bveGxqsfTY
q17gRGwtmInlO5GB1e8SWuOtM177SUoykiC0O6mtDg82ptEHdpQf0KAthpId0ulU
lg4n0uXFVweqAsVUGi+Ii0GcNyWlfxEikf19ERBbjrtCfYgeJjcjjDwjQhff3InZ
cOMdZtIXSiahlhBBE6aXSkCn46C15rPqO+8aUrTAkWeIWVt8OiqzRsbXwrjj/yhK
xktRpD+v6ZLviudhxU1wnGzPMADyVend1LzfOvD9kFvqgebldj4+oDg3hyH1cqXe
/lEOO+epB2yo14tsGqzOnSmS2F3rFfOUVZdj6yUMLiSyKnrJ/76VKh0LBexPFbvb
GNunZDGHQ8Y++zRjsskhMcY729ABMK17x7kBeOqaJ1o8yi1HrKufOXiKdHILBZ+K
0cpU/r3YaK7OdcY3P5ne4w1XtqY17FT+LuNCiSJDI0SX4gg4PVTFCn6no91USCaw
aTyx9C0abcSJCUXZIZG4Dxj0qxJr/jzH/RshLTOsn0WVQowkaTMTVTArHi3e0oGL
HdhgN+CRdfE8RK+HrW9DuMCfXzWHDvhbi2lx+jSi76PRhBCK1XDmLG1nVaFwr1o9
CzH8e7OJB7E7E+r21ecGzvc9CuKcvLLTkhaLhsoCLBe9EmfvMX1gtwbUHdYAx+Y7
Q8FP3DaDfUmF1TGgncXBoQpab7MzEeIGtFYtFteKWrAgVCwpX2R1CJVYXCgkaDmx
iWoTwpGpXm8usNpvIyIMwqU14NcCmZEzCZT62UFzfvB7nivLhoZTl2VbDB10xf8p
TnPf5g==
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
MIIJQwIBADANBgkqhkiG9w0BAQEFAASCCS0wggkpAgEAAoICAQCPzvHs7JAVaPwc
5XVY5L8ycLw4c+iWXtujVOKhrm3r2YsmCoQgXgidG4LxD9LZENQwkyD0slkxjc1a
5vWt6CW7nbKHFhk7HHWU+yxT5qGqDCW1bTVU9+Ysuk1bgSroK88+Gs664Z7HSZxv
z8fd+8oQEQ7OgJwgeSzaAYEDHeHCeNR+scHsapI4HTUH0QLch2U9J8o6zjUxKGTH
zUwG3WsGKbU8+37e1vDSA68wJZV508SXPcnJ3xdKWzWqNTLMNctKY7kXBPxCnT0N
VSvwdzHUDpURUpdgSPwfSc33vaPlkBvfOJKa2++uyVojuFvAyrE+uCIDkwmv9QOB
MSgGyKp4DOPka85xYQy8ax77Cwkh/XQi+lQMUHi8XK0KZHpMI/ZOqkE5WOCd9+yx
wct4s1pn4+JIlR/hDfiYPB6iLdQlT/qTG/XqDl8Lq/fMLGF/9R3uf1wGhUqyePOj
i89sGEOSjGHPsc4xtavMzIbSwEfFZ/7j0YNEiQE6Yb7OoUUgG0ZM+n3VPh6bgHZa
n3Tq5UEbS9/ewUzMwNJLCT/x2VYQon7DXF1gAq/LnvUnsndTa6t3cNqCn0H/qMkt
fEQH9oKTBBrdIEnFVj37G7hW+08J7cYmpG9aM36g6bqDozoMREJCFpbsPwpv7mMc
OyopwCaGUOodTbLcQ7iHM7FspBdmIQIDAQABAoICAADBuJcIkez0Dwr9mPgx6/MR
bV5o9t9/HpJ4BeFFoo8C3lO4od+kA5pyUlQR1Ygy4HitiPKS9/bl7rHYC5Ex2GNF
NOC3YejkSk2IY63lV+4l/1uoLyvx3EkQiecZE8K4/yZ3FRNnPj5cgIOpiRiP8A0e
KdxampxJImC2ueI1dTAXW0AJtI/QfFGg/JK8hxuc0sdFCFTRvhsOHVvupp96n++M
FKM1Od33dm/Nnbs8jj/yJi72skVrGRtrWAU71m4FO5aenyVIuJMZKUnwYsupa8Mh
nRV7G8cYf8siWtv2A8npvaTyxUK4fiBiL23QHkANvrUFfwYJypuLR/EnhVmKpViy
7IEbtZPdkHUu9vtU/rc9tBfXn4K4oR6g5bR0GX1N2qZ9mXc+47fsdhYC4P69V97R
XyRCJgm+sjxCHMCU2QcjixQcs/skKK4uHS1cq5022CaPINFTQDHyRTNGmeQ91OYM
qvCHcE6qF+tMuaEOD0ojWm3dTAKvKU0AT3hmEtmYo27sngLq51WaummTeBAu5k5t
4+HsJNQiWJzewG9p0EQMeMeimDQRg2ipHUhJp/z+RCQCMtChfhW+sIXCHjbFbakK
4HrAHzUl2Rr8GKIP9UoyMeNN3EHtsa5yDXFn3DYtuL5duV2jqpBjZnE8SVVcQMgG
guCPlOal4tEDM3D+0rmJAoIBAQDHMbLmzDFz/qiWL74MxWiUudcpzGsQwFcpPwds
WoRvDi/NinIF3j4mx5V2lzrSa/C+TIcJJRE2S6xLQqwoRvTfp4wLX9olR6o1QQ5V
fkSZu4xa1sCpB3jM1sfkRfqKdTiMYWyAXFUd4VIIKUQPW5YfT0cotTpjVrxKr89H
4nG16T6oshgHzVN3n+MJukvVfQ/jlFm2bOsZpzSuyUH89o+fU88zl0sqH/LVSbud
MlgFjbkdJjZxsX3O5aZbNt8WHOOgczg3FIS0XmxghaKkWcXyCA+jvvaOlfeJDLqF
8sjXLwehz2IlgWui9kLI9huaUHHZBgRmOcckBnusIxoeN63nAoIBAQC40cWdvTW/
AJWjmwcjWIvr34m+xRghgjm1aMRVfR7w9rYdAu4WDuuqv5cEdx+ChSVqi9mdya9F
WALNHPvVRYK7dQG2YE4ps3GhL5nlbxa/inz6WhfI5M1esd/U6ELi281HKvqPoYG6
/WivUJIagLeb1KxYg2VTs7RAR4ZeZa784l1k04H+j69HZMUWhup9RKYXd5ANSUyG
llp72qx+pxUk6Yx8CQktydUBfkPvvFWQHi0AYB22eK6qLkcGWRD2jx5XSLCN02HI
tgE4Mb0FW5fuh72vW37UEldIKJE2zUeBY1XkwMz+Bhzso093gfHSYJWGEVOGu4Ke
FACzH3ZiBnq3AoIBAD4rDrDlrdGL+0XUZLZLrlYsojCTch7qSMnAuCpjHc4jWVwH
B4s5T6B+zfETRfKCacKa09JQ1Uxi1cUva9euPaLIAfdS1Rypfz/brAOWwwZP+IAp
Yjve7x7PMdfW11j7OMjnZxuwtYf72MRfTLSWWwYukgDsfuqeGWod8M14qRCjWUEB
RGq69H/zUMKqeByLSbg3hsBewglhnBmpCoVO1ohded+aKoVdL0bOGlX3bm1dTBcC
5B8bzC4UkpUJTRrzrT9YdoUKTFiW17BQRPCSbCsbxPXQFdduirElXdmMao5uSS2B
MhqOh+92eGWMGhVRYhbfv+O/yJ0/wL16vx4LNqUCggEBAKOIkehaFGV2WfHS6gT/
g4dpW1OeyBRWS0PWeOr/9FHYqxmOjyabezGxpym/UfVGZO4a4qp4XArqrkfZ3oc+
o0fFZ4d9PYwuiFvMZ8sYebNwEdffwe3zbjjdASY9gXmEbeYMBHr0uHrBYVDG5RBo
Vo6fJwSG9zCR8OtznlSGesiub18Yu4yIjNqKMs4VAQVoqeJX2/G1qu3nuhafTkQf
CVmJAdJ6mFGYpk5U8D3/kmIcIUuUwJXFwR86JYRbaOQRjRJEclx77qkPSpLzh8Jm
4k+gclnohpeVxv9FTgCEsUGuqMDpTDl1JUDJ3DXtmBDDf2qKiDLOVewT3O6h5zkj
bI8CggEBAIsSArOnQvKtEEFUP7qu1RvlCq4cT3n/maTgK/QhlktlcqpUg460mbH4
CVwTPnMH9G3p/r7HQ0VhqI8bkDEdAn61IXS8qbt41eXuxa93zKfJIskYd1qkFppQ
vKe22s0Sxi0aXc67JeyHezV1EovD1Wl9cubzaRhg7bUk/f6S+ACyNqHWgN6XCj32
XHLPry/u7LMiwr8u2opNmL0FOzbR6HGJsxi1KNppJXZrXQgi0UQzN/UjUk4+Or9v
vUJZNex9Iro2sEHGtsvSv07D3XFpb8VYxSKp2o86CIvR+SAaRtS31nUgx6vOVLA/
VN9C85/NOQXjpMb0hB6sL9R1HBjcAl8=
-----END PRIVATE KEY-----
)";

  *cert_file = JoinPathSegments(dir, "test.cert");
  *key_file = JoinPathSegments(dir, "test.key");
  *expired_ca_cert_file = JoinPathSegments(dir, "testchainca.cert");

  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kCert, *cert_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kKey, *key_file));
  RETURN_NOT_OK(WriteStringToFile(Env::Default(), kCaExpiredCert, *expired_ca_cert_file));

  return Status::OK();
}

} // namespace security
} // namespace kudu
