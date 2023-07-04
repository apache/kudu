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
MIIJbTCCBVWgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwdTESMBAGA1UEAwwJbG9j
YWxob3N0MRAwDgYDVQQIDAdzYWRmamFzMQswCQYDVQQGEwJBUzEZMBcGCSqGSIb3
DQEJARYKc2RmbGtkc2FqZjETMBEGA1UECgwKbHNrZGZqbHNhZDEQMA4GA1UECwwH
c2xkZmtqYTAeFw0yMzA3MDUxMzA1NTdaFw0yNDA3MDQxMzA1NTdaMHgxEjAQBgNV
BAMMCWxvY2FsaG9zdDERMA8GA1UECAwIc2FramRmaGExCzAJBgNVBAYTAkFTMRkw
FwYJKoZIhvcNAQkBFgpzamRoZmtzZGFmMRQwEgYDVQQKDAtsa3NhamRmaGFsczER
MA8GA1UECwwIanNkaGZrYXMwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoIC
AQCkqsJ6lPQ1cbocrp60iUUOj2JBAAc6XJ7PcT0HszSGI8xjWFPByrNKbh9CUF1h
ulch6QbI/u+3pkBb0HkpvsjU2yPP6Qgn0/SxE1GNbARE9tyDWU/NBIqvq40rqfXe
OygQNkOYRHv5bql2Mk1U/xbtLOkYE8yf03PuChkgm5OQmY1YFBtmPlTwSutbeUG4
KxsQITbnIFnsNC8cZzhGlaTk55xDWMUsGw1f6CT5OwNuSBxmpBp2HnwvEfpKEqBD
OOAa7N6gzn4CfDIJGDC3/s5WiLGagDgVjTyHyGN+KZd02U1Pnk5sG9OFitDSGa3d
aKuOGOAuphfYvqgaZKptQJ2heBjDPWYxRD3x/I60VpCOj0o+syCZhcgcIMDbjA+C
5lYRzJZkO9QQ0bEHlTq+ks7hZCGpx7sE9KDD0AsYB6fapcMg/rTHNsu8nfaDWWD4
xesQrchvFZYgTOmH/I2JUkxqVmv1uRIx0akANhn5GdqfJ1Uif1OwMikNKaA0M2Ve
KKKI/y5/kFEMsew2j0Zh6RaM4k0fWB6MQHFAvwsF5ptoXnBq1K01w4jvdTnUb8gm
NUPjg+bQANxDzYxruH9KtgVw3zfm00Exz2R9CXKaB7gPtKzFFxqesimH+BbP3rDP
MU5rzv8HWJ0Hn97Njw+C9g0pDXrZ97HI5ieUqTN8EgjbkQIDAQABo4ICAjCCAf4w
DAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQUSWPWrk7iay2FSnQm2gBh877P/VMwHwYD
VR0jBBgwFoAU244rpvoJhKafs083DFvHum9HwWUwCwYDVR0PBAQDAgWgMBMGA1Ud
JQQMMAoGCCsGAQUFBwMBMH4GA1UdHwR3MHUwO6A5oDeGNWh0dHA6Ly9wa2kuc3Bh
cmtsaW5nY2EuY29tL1NwYXJrbGluZ0ludGVybWlkaWF0ZTEuY3JsMDagNKAyhjBo
dHRwOi8vcGtpLmJhY2t1cC5jb20vU3BhcmtsaW5nSW50ZXJtaWRpYXRlMS5jcmww
IQYDVR0RBBowGIIJbG9jYWxob3N0ggtleGFtcGxlLm9yZzCB6AYIKwYBBQUHAQEE
gdswgdgwQQYIKwYBBQUHMAKGNWh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29tL1Nw
YXJrbGluZ0ludGVybWVkaWF0ZTEuY3J0MDwGCCsGAQUFBzAChjBodHRwOi8vcGtp
LmJhY2t1cC5jb20vU3BhcmtsaW5nSW50ZXJtZWRpYXRlMS5jcnQwLAYIKwYBBQUH
MAGGIGh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29tL29jc3AvMCcGCCsGAQUFBzAB
hhtodHRwOi8vcGtpLmJhY2t1cC5jb20vb2NzcC8wDQYJKoZIhvcNAQELBQADggQB
AGgLQs+32qSCMY7LqLQZbsn0l989NWrHl4Nn60ftLe/VyQ5w9pP1QnQ+epoRwiY7
Gd6U+2fFtzeJOD3N4ckAzw3hJQh0uSDgGglI/S3zZLNOm80QE5+aJr8/YaHH3BIR
cYkhMHiGg4/Dijq1hCKsnqTmZE1JLmVWFezopmKJYOABw9lgbMceFiUtxsAhaLJA
rW98SkznrFos7KovtDkX8tZpuJqkqThyvOhZAbE0LkIXug3s7ShB1id7jfDrPAFd
iph0BkcU9iWLmochuZITSe9yRMbbhS38PYwnXQ35B1PPtc+wCKAMKP6RT357GqZq
HTPpD9vembZm6Db/IW4j4wLMrk12DPNQbh70cCsW16fO/CpNL+l7fP4lakvC4iu4
zFQ93XuoAtwp3VSjH3l3dHr9CjSERdnBBAXBjVIHR3bu3iFlueDIzT7Gb/G4VNCn
ylJllZ8D7vcKdPyBYNV9ako1TFIHdFT5g/XvyPotPgBZa8E3FciMU/MfVds0hdmG
Yizx0T5atH3/IdYW7EXhgsBjWD6Tlscyxptb1e+JWPL1N3E7IexFH5xp+jfvrHLn
xTuabhLjsIeaEKWV5DXO3yQrFcrq4JhMZ/A+cQuOUrHfiB6eqDq2yXSTflQh07qB
j/GWHgu6o5bHVFsv2sVIBbM+vP6Upa4trUHZNsgPOwnD7vb8GHNAjxxx41goy9gI
X3TQ1P8zpE/msKCVDOscwObx2h8OTQ6jgWYRp2EK+Qp2FnKNxrdr0N7q2U2gzyao
fkApVgFmG36N2v7Gi3Dn3Lfsc1bFfd7wGjDx8s8CIBf3GxaL1RsN2rzvZoJIl7d5
8a4qXHlYcshgJTG6oMLRR+c3Cq3bkMtfU7Y5VYcPH9NcvtYnDhzLxuWQZPXZJ4Wl
F9z3Uhy4/Z7DHiR18H7fTCNv5zA0xhOUiMhRUuji5y6ip0t4KK6HtbTsjA7ozzTJ
MHYuuB7fXxfY0hkL02gysnhjRN0KghajJwKZFGFqGjj7N1StI9lPgE6Jqs3UyszV
JqtrJrAdIrySwFncyZMYll4SujNPS/IOL5/DcOI2JeKt/QlZv3+zfNaC+HOJcbdy
wU9SReth3cXZCUPu59wZNFIVhVFolRQCQwLFx01AeQ9IDV0pgvDrfF/BiSScZrSo
b9eFhBWOy05GV3Fln2FIEWuqdWX2iQ6bpkpi5QauTp0c4RR1ZXKBtjkqUo98s20R
QEPLaPxSCusmpszGiSzyrOFStUo769/Yk+WPHpoLM3KsAz1Cs936rcwRPICqlGn/
eblcwS7LxM1UuYuD9FtZKJNXZENosOchV2bNxWCKNkrKqwcNA/S4UFgeOQCdgyUL
aMGMNFdwc2dmKcFTNJySMc0=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIILaTCCB1GgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwgYQxCzAJBgNVBAYTAkFT
MRIwEAYDVQQIDAlBU0Rma2pkaGYxDzANBgNVBAcMBmxza2RqZjEQMA4GA1UECgwH
c2FsZGZqYTEOMAwGA1UECwwFc2tkamYxEjAQBgNVBAMMCWxvY2FsaG9zdDEaMBgG
CSqGSIb3DQEJARYLc2FkamZsa2Fkc2owHhcNMjMwNzA1MTMwMzU5WhcNMjUwNzA0
MTMwMzU5WjB1MRIwEAYDVQQDDAlsb2NhbGhvc3QxEDAOBgNVBAgMB3NhZGZqYXMx
CzAJBgNVBAYTAkFTMRkwFwYJKoZIhvcNAQkBFgpzZGZsa2RzYWpmMRMwEQYDVQQK
DApsc2tkZmpsc2FkMRAwDgYDVQQLDAdzbGRma2phMIIEIjANBgkqhkiG9w0BAQEF
AAOCBA8AMIIECgKCBAEApwUqxdXFSIg463ke4OHwkOFp6vDX8el4iCJDQYHl5+Mj
1apFN0PCqSggF/mkwdXmsMYHHlP8+3RiVsd2I+cNB6NEUj368OCgIgbvzCOlaLCx
LTZE2dLlRgSPPXPFTlNMnCP7tnrIZYAm/xnZgvfoHa3YsU9kRAbKwbKahdVvo7V4
TWWfvokmR92lF2YMEOrMTaUgKcW6DxF5zdC1Phvepx3uXnAs/Hg8pOVp6DZ+RUqd
0JRBUhEJk0wM5hnRni/E/ZKe7OO/OGjXR2SAs8BInjCZtWBxEjbTCdEkvjsYRetg
CeopWzeHGN/BYnKVQi7hyL8jutFvGNBqX5gyvi9f8rlNvCZdsXyQ8HC1AslUw25/
qnwIRmBJKv5WfWUMsyzID+QkDgM3/Z4kd41uQwq/jWnEWtqLw6xcV/aQxyw5mhmJ
vqK4Vx6uxa8Eif2FDFxftX7setiiIW88S1b/rSGQTcxxh0tGgNkM1nZi6D2eCzqM
riLzou6Pywhy+49i78by8T19wZsy5FrITx7x2s9dhWG9dxD7SSmMjtQqqeinX7Eg
AO2fjv+pyqre20tN40IPaJhc7dl7T33VL7pdTKjLgxcTJLORXgHDnd8mbU9SXLM2
fjQecBuzgIrgUk1FDRz7pDMPsOsWvOwa9JLhykL+5USyHr0WVFEoYQNZ3UHShhtv
6EQk+/y1QFWEsjzVReuHuVGbWfRSuL58nz1HNOECXiP+lrB6dQEWzuQbaapiXnV4
kRxFFjxdF9BwI+gRf4uGX92P8cBue7MKIcIq7j0gks6YXEPgdooFOIJj171XbtvY
j3jxWLbQVXuLyhMf4ftrLNGM/ku2Edp6nRd1m2dDeVHsCT27BkmvUUa3xVqv3/gO
V9mF9KtjNYrTXX8OTBnjDGAYWA+lxS4/OsejS3D9W9k0RAhlTorsF71gZhi5T78F
3voIPlQG2PR5bbB+kaNwsc+AtY6K4RgaiXLil39S+7dKG/N9NDqbuCVgpQzvzCWs
lliGpSr2oTWbcIbXK1UQjNTEy/hLR8WxmUcWfzI2vk+mwwRG2p55CwOD1G9KTrDv
6wPfXKZceqFmH9jkCt/iPPRVVrMbtNwzz7Q2sEh3fi6DRzA3gjErl3jMbcfU4YJx
9HfC1mNCJJuLvjhrQRjrcrocEPSgv2EPJkRo8XNZgVwi5syEY+3cRKsIOFRj2IAe
6nppW0Pe1uwpqqdoURGI9BE0r5D0lIY19qTO2M4qMGBW0oedHO4rnQt4y3MBQzi8
sLb4Z5A+KjSB6XECgEq8m+Wwj4K6olMm7YluTspyzE8Nqwg2kH2SXY4wf98bQDSf
gVg5u4JsC3lPQRAtan/1yffgFO3e+shp15tuWshknwIDAQABo4IB8TCCAe0wDwYD
VR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU244rpvoJhKafs083DFvHum9HwWUwHwYD
VR0jBBgwFoAUyIG2Gvtv1hr/8jrdJvVXLG6x+TAwCwYDVR0PBAQDAgGmMBMGA1Ud
JQQMMAoGCCsGAQUFBwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3Bh
cmtsaW5nY2EuY29tL1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtp
LmJhY2t1cC5jb20vU3BhcmtsaW5nUm9vdC5jcmwwMQYDVR0RBCowKIIJbG9jYWxo
b3N0ghtTcGFya2xpbmcgQ0EgSW50ZXJtaWRpYXRlIDEwgdYGCCsGAQUFBwEBBIHJ
MIHGMDgGCCsGAQUFBzAChixodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNvbS9TcGFy
a2xpbmdSb290LmNydDAzBggrBgEFBQcwAoYnaHR0cDovL3BraS5iYWNrdXAuY29t
L1NwYXJrbGluZ1Jvb3QuY3J0MCwGCCsGAQUFBzABhiBodHRwOi8vcGtpLnNwYXJr
bGluZ2NhLmNvbS9vY3NwLzAnBggrBgEFBQcwAYYbaHR0cDovL3BraS5iYWNrdXAu
Y29tL29jc3AvMA0GCSqGSIb3DQEBCwUAA4IEAQAZQf83aVYyirjmTwV8Rsi9iLzs
izFSwMUNoGNEn+5lb3VzCLDBFXRG8vymhVG7wTxGIKjbpajyJgfDzns1lgBrrqK8
+5DH2fA98OEDMmwlq41QcXZHxn6Q6futJifPz4+c7GRq3Z0XNlOyqExX4feVry1g
LcIM3vS6Q6UC47KCFrhJMNiQUPdQe7puitrUt22pdnkhAMPYjIpPUGD/H6jKWdiJ
BJhsdPjzsODlcNXUb5xQxRfd+C5D7HXIRotjl8L0r388HGZ/VEcETaxJdeAzAa3t
L6Rd5T0zMlYxhWfQiekr2xIO379T6oh/LD/FV+8sQD1vWrwVkmYxINiKsiJnVBa3
lDmT8iDqdfvmfvBqg/Nf7lAcAwlAFUExHYfKj9QdOFK080I4qZbajW9s4xd/xj8K
LmcpWRqSEPHEicc3OJ0QJ8Du1vvKVkzBvx7/PScAd/NZilrI1ZXTCQt+VNlmnYSD
ZFM/I9P1GCjR1orXS/t75ktyyEMiCVT7fo0ppB0d7S+xBcLkzUV5D6y3KnQI3t2C
U48e2QbeEutswSm3v7JysY4uEDBnQqftiFwTTIN+PDnkvnntw3yAAowVqY2hhOvs
JEmlre5xRWZMq7w7nCDx/p8ciUJZvfeCwFlSwTA8aSWcmuudQ1vfJdx9Z++zO/qH
h/7Aw3aBSaMtHA7Bdg7Z3993vLwE8p2TyPsOOzFEAXFX0iFuWoDCO/gu+c3mIDDv
1+pB4WORTuzb+UUr1CFyDhjtFOCbGnLpSn1Jl2WMY8uRfviW49nwwPL7oxiDdMBr
TialbjmwRDOPSgS9OoCV3gqZp/ygEBi0S+xHJ2EfGB/TrgZjQcDZI+fPqTnGxf9Z
e17ROC3M3uZWwD2KMw2cIFyL1g3FeUyyhFm3BMn05muKhsAucy/mpcgPsjVO29h7
lHaW4VsFF+zJLEDOpURKPQbjmSP27VLT0W0V/YTDmhtiuhgi91+QeOgI6RPbxo96
ubabcoPbkVaG8+HEXZd+rBBYX9fEvQ8HXzgzPcWqApfnQrbm0r4HVzKM4ekEaxwD
gime6wkeJfbcMmiT8Y/dTyxARWlPwNigU+e5xj8ckRlumK/jLOE94extC9fUINPs
lwTrp+mnDyC97h+G5B5gszJ+S6XkneyYWqgGLtwyI/mmI4AaVNKIvsK0v1916tjD
Ky3hFhCJfXGiuqZGu81SmUIcFnz6EgSDOpXFePbl/IMyW1zkihrh3brSEHr+PYym
++7QNAh8RsheF3aX6l3K1iuGmsDBM/bLRAL2qTklBy0zi7hwFs2KjDmoFYZ6f6Po
uzsEffG9Yyns4Qia47NitM2/tnMm/QLc+C32qCJUQ68xDkG7E4Ay98CCUNBd
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
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
+WDMk0mCnqqcusrqoAu117IxdSwh
-----END PRIVATE KEY-----
)";
  const char* kCaChainCert = R"(
-----BEGIN CERTIFICATE-----
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
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIILaTCCB1GgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwgYQxCzAJBgNVBAYTAkFT
MRIwEAYDVQQIDAlBU0Rma2pkaGYxDzANBgNVBAcMBmxza2RqZjEQMA4GA1UECgwH
c2FsZGZqYTEOMAwGA1UECwwFc2tkamYxEjAQBgNVBAMMCWxvY2FsaG9zdDEaMBgG
CSqGSIb3DQEJARYLc2FkamZsa2Fkc2owHhcNMjMwNzA1MTMwMzU5WhcNMjUwNzA0
MTMwMzU5WjB1MRIwEAYDVQQDDAlsb2NhbGhvc3QxEDAOBgNVBAgMB3NhZGZqYXMx
CzAJBgNVBAYTAkFTMRkwFwYJKoZIhvcNAQkBFgpzZGZsa2RzYWpmMRMwEQYDVQQK
DApsc2tkZmpsc2FkMRAwDgYDVQQLDAdzbGRma2phMIIEIjANBgkqhkiG9w0BAQEF
AAOCBA8AMIIECgKCBAEApwUqxdXFSIg463ke4OHwkOFp6vDX8el4iCJDQYHl5+Mj
1apFN0PCqSggF/mkwdXmsMYHHlP8+3RiVsd2I+cNB6NEUj368OCgIgbvzCOlaLCx
LTZE2dLlRgSPPXPFTlNMnCP7tnrIZYAm/xnZgvfoHa3YsU9kRAbKwbKahdVvo7V4
TWWfvokmR92lF2YMEOrMTaUgKcW6DxF5zdC1Phvepx3uXnAs/Hg8pOVp6DZ+RUqd
0JRBUhEJk0wM5hnRni/E/ZKe7OO/OGjXR2SAs8BInjCZtWBxEjbTCdEkvjsYRetg
CeopWzeHGN/BYnKVQi7hyL8jutFvGNBqX5gyvi9f8rlNvCZdsXyQ8HC1AslUw25/
qnwIRmBJKv5WfWUMsyzID+QkDgM3/Z4kd41uQwq/jWnEWtqLw6xcV/aQxyw5mhmJ
vqK4Vx6uxa8Eif2FDFxftX7setiiIW88S1b/rSGQTcxxh0tGgNkM1nZi6D2eCzqM
riLzou6Pywhy+49i78by8T19wZsy5FrITx7x2s9dhWG9dxD7SSmMjtQqqeinX7Eg
AO2fjv+pyqre20tN40IPaJhc7dl7T33VL7pdTKjLgxcTJLORXgHDnd8mbU9SXLM2
fjQecBuzgIrgUk1FDRz7pDMPsOsWvOwa9JLhykL+5USyHr0WVFEoYQNZ3UHShhtv
6EQk+/y1QFWEsjzVReuHuVGbWfRSuL58nz1HNOECXiP+lrB6dQEWzuQbaapiXnV4
kRxFFjxdF9BwI+gRf4uGX92P8cBue7MKIcIq7j0gks6YXEPgdooFOIJj171XbtvY
j3jxWLbQVXuLyhMf4ftrLNGM/ku2Edp6nRd1m2dDeVHsCT27BkmvUUa3xVqv3/gO
V9mF9KtjNYrTXX8OTBnjDGAYWA+lxS4/OsejS3D9W9k0RAhlTorsF71gZhi5T78F
3voIPlQG2PR5bbB+kaNwsc+AtY6K4RgaiXLil39S+7dKG/N9NDqbuCVgpQzvzCWs
lliGpSr2oTWbcIbXK1UQjNTEy/hLR8WxmUcWfzI2vk+mwwRG2p55CwOD1G9KTrDv
6wPfXKZceqFmH9jkCt/iPPRVVrMbtNwzz7Q2sEh3fi6DRzA3gjErl3jMbcfU4YJx
9HfC1mNCJJuLvjhrQRjrcrocEPSgv2EPJkRo8XNZgVwi5syEY+3cRKsIOFRj2IAe
6nppW0Pe1uwpqqdoURGI9BE0r5D0lIY19qTO2M4qMGBW0oedHO4rnQt4y3MBQzi8
sLb4Z5A+KjSB6XECgEq8m+Wwj4K6olMm7YluTspyzE8Nqwg2kH2SXY4wf98bQDSf
gVg5u4JsC3lPQRAtan/1yffgFO3e+shp15tuWshknwIDAQABo4IB8TCCAe0wDwYD
VR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU244rpvoJhKafs083DFvHum9HwWUwHwYD
VR0jBBgwFoAUyIG2Gvtv1hr/8jrdJvVXLG6x+TAwCwYDVR0PBAQDAgGmMBMGA1Ud
JQQMMAoGCCsGAQUFBwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3Bh
cmtsaW5nY2EuY29tL1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtp
LmJhY2t1cC5jb20vU3BhcmtsaW5nUm9vdC5jcmwwMQYDVR0RBCowKIIJbG9jYWxo
b3N0ghtTcGFya2xpbmcgQ0EgSW50ZXJtaWRpYXRlIDEwgdYGCCsGAQUFBwEBBIHJ
MIHGMDgGCCsGAQUFBzAChixodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNvbS9TcGFy
a2xpbmdSb290LmNydDAzBggrBgEFBQcwAoYnaHR0cDovL3BraS5iYWNrdXAuY29t
L1NwYXJrbGluZ1Jvb3QuY3J0MCwGCCsGAQUFBzABhiBodHRwOi8vcGtpLnNwYXJr
bGluZ2NhLmNvbS9vY3NwLzAnBggrBgEFBQcwAYYbaHR0cDovL3BraS5iYWNrdXAu
Y29tL29jc3AvMA0GCSqGSIb3DQEBCwUAA4IEAQAZQf83aVYyirjmTwV8Rsi9iLzs
izFSwMUNoGNEn+5lb3VzCLDBFXRG8vymhVG7wTxGIKjbpajyJgfDzns1lgBrrqK8
+5DH2fA98OEDMmwlq41QcXZHxn6Q6futJifPz4+c7GRq3Z0XNlOyqExX4feVry1g
LcIM3vS6Q6UC47KCFrhJMNiQUPdQe7puitrUt22pdnkhAMPYjIpPUGD/H6jKWdiJ
BJhsdPjzsODlcNXUb5xQxRfd+C5D7HXIRotjl8L0r388HGZ/VEcETaxJdeAzAa3t
L6Rd5T0zMlYxhWfQiekr2xIO379T6oh/LD/FV+8sQD1vWrwVkmYxINiKsiJnVBa3
lDmT8iDqdfvmfvBqg/Nf7lAcAwlAFUExHYfKj9QdOFK080I4qZbajW9s4xd/xj8K
LmcpWRqSEPHEicc3OJ0QJ8Du1vvKVkzBvx7/PScAd/NZilrI1ZXTCQt+VNlmnYSD
ZFM/I9P1GCjR1orXS/t75ktyyEMiCVT7fo0ppB0d7S+xBcLkzUV5D6y3KnQI3t2C
U48e2QbeEutswSm3v7JysY4uEDBnQqftiFwTTIN+PDnkvnntw3yAAowVqY2hhOvs
JEmlre5xRWZMq7w7nCDx/p8ciUJZvfeCwFlSwTA8aSWcmuudQ1vfJdx9Z++zO/qH
h/7Aw3aBSaMtHA7Bdg7Z3993vLwE8p2TyPsOOzFEAXFX0iFuWoDCO/gu+c3mIDDv
1+pB4WORTuzb+UUr1CFyDhjtFOCbGnLpSn1Jl2WMY8uRfviW49nwwPL7oxiDdMBr
TialbjmwRDOPSgS9OoCV3gqZp/ygEBi0S+xHJ2EfGB/TrgZjQcDZI+fPqTnGxf9Z
e17ROC3M3uZWwD2KMw2cIFyL1g3FeUyyhFm3BMn05muKhsAucy/mpcgPsjVO29h7
lHaW4VsFF+zJLEDOpURKPQbjmSP27VLT0W0V/YTDmhtiuhgi91+QeOgI6RPbxo96
ubabcoPbkVaG8+HEXZd+rBBYX9fEvQ8HXzgzPcWqApfnQrbm0r4HVzKM4ekEaxwD
gime6wkeJfbcMmiT8Y/dTyxARWlPwNigU+e5xj8ckRlumK/jLOE94extC9fUINPs
lwTrp+mnDyC97h+G5B5gszJ+S6XkneyYWqgGLtwyI/mmI4AaVNKIvsK0v1916tjD
Ky3hFhCJfXGiuqZGu81SmUIcFnz6EgSDOpXFePbl/IMyW1zkihrh3brSEHr+PYym
++7QNAh8RsheF3aX6l3K1iuGmsDBM/bLRAL2qTklBy0zi7hwFs2KjDmoFYZ6f6Po
uzsEffG9Yyns4Qia47NitM2/tnMm/QLc+C32qCJUQ68xDkG7E4Ay98CCUNBd
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
MIIJbTCCBVWgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwdTESMBAGA1UEAwwJbG9j
YWxob3N0MRAwDgYDVQQIDAdzYWRmamFzMQswCQYDVQQGEwJBUzEZMBcGCSqGSIb3
DQEJARYKc2RmbGtkc2FqZjETMBEGA1UECgwKbHNrZGZqbHNhZDEQMA4GA1UECwwH
c2xkZmtqYTAeFw0yMzA3MDUxMzA1NTdaFw0yNDA3MDQxMzA1NTdaMHgxEjAQBgNV
BAMMCWxvY2FsaG9zdDERMA8GA1UECAwIc2FramRmaGExCzAJBgNVBAYTAkFTMRkw
FwYJKoZIhvcNAQkBFgpzamRoZmtzZGFmMRQwEgYDVQQKDAtsa3NhamRmaGFsczER
MA8GA1UECwwIanNkaGZrYXMwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoIC
AQCkqsJ6lPQ1cbocrp60iUUOj2JBAAc6XJ7PcT0HszSGI8xjWFPByrNKbh9CUF1h
ulch6QbI/u+3pkBb0HkpvsjU2yPP6Qgn0/SxE1GNbARE9tyDWU/NBIqvq40rqfXe
OygQNkOYRHv5bql2Mk1U/xbtLOkYE8yf03PuChkgm5OQmY1YFBtmPlTwSutbeUG4
KxsQITbnIFnsNC8cZzhGlaTk55xDWMUsGw1f6CT5OwNuSBxmpBp2HnwvEfpKEqBD
OOAa7N6gzn4CfDIJGDC3/s5WiLGagDgVjTyHyGN+KZd02U1Pnk5sG9OFitDSGa3d
aKuOGOAuphfYvqgaZKptQJ2heBjDPWYxRD3x/I60VpCOj0o+syCZhcgcIMDbjA+C
5lYRzJZkO9QQ0bEHlTq+ks7hZCGpx7sE9KDD0AsYB6fapcMg/rTHNsu8nfaDWWD4
xesQrchvFZYgTOmH/I2JUkxqVmv1uRIx0akANhn5GdqfJ1Uif1OwMikNKaA0M2Ve
KKKI/y5/kFEMsew2j0Zh6RaM4k0fWB6MQHFAvwsF5ptoXnBq1K01w4jvdTnUb8gm
NUPjg+bQANxDzYxruH9KtgVw3zfm00Exz2R9CXKaB7gPtKzFFxqesimH+BbP3rDP
MU5rzv8HWJ0Hn97Njw+C9g0pDXrZ97HI5ieUqTN8EgjbkQIDAQABo4ICAjCCAf4w
DAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQUSWPWrk7iay2FSnQm2gBh877P/VMwHwYD
VR0jBBgwFoAU244rpvoJhKafs083DFvHum9HwWUwCwYDVR0PBAQDAgWgMBMGA1Ud
JQQMMAoGCCsGAQUFBwMBMH4GA1UdHwR3MHUwO6A5oDeGNWh0dHA6Ly9wa2kuc3Bh
cmtsaW5nY2EuY29tL1NwYXJrbGluZ0ludGVybWlkaWF0ZTEuY3JsMDagNKAyhjBo
dHRwOi8vcGtpLmJhY2t1cC5jb20vU3BhcmtsaW5nSW50ZXJtaWRpYXRlMS5jcmww
IQYDVR0RBBowGIIJbG9jYWxob3N0ggtleGFtcGxlLm9yZzCB6AYIKwYBBQUHAQEE
gdswgdgwQQYIKwYBBQUHMAKGNWh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29tL1Nw
YXJrbGluZ0ludGVybWVkaWF0ZTEuY3J0MDwGCCsGAQUFBzAChjBodHRwOi8vcGtp
LmJhY2t1cC5jb20vU3BhcmtsaW5nSW50ZXJtZWRpYXRlMS5jcnQwLAYIKwYBBQUH
MAGGIGh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29tL29jc3AvMCcGCCsGAQUFBzAB
hhtodHRwOi8vcGtpLmJhY2t1cC5jb20vb2NzcC8wDQYJKoZIhvcNAQELBQADggQB
AGgLQs+32qSCMY7LqLQZbsn0l989NWrHl4Nn60ftLe/VyQ5w9pP1QnQ+epoRwiY7
Gd6U+2fFtzeJOD3N4ckAzw3hJQh0uSDgGglI/S3zZLNOm80QE5+aJr8/YaHH3BIR
cYkhMHiGg4/Dijq1hCKsnqTmZE1JLmVWFezopmKJYOABw9lgbMceFiUtxsAhaLJA
rW98SkznrFos7KovtDkX8tZpuJqkqThyvOhZAbE0LkIXug3s7ShB1id7jfDrPAFd
iph0BkcU9iWLmochuZITSe9yRMbbhS38PYwnXQ35B1PPtc+wCKAMKP6RT357GqZq
HTPpD9vembZm6Db/IW4j4wLMrk12DPNQbh70cCsW16fO/CpNL+l7fP4lakvC4iu4
zFQ93XuoAtwp3VSjH3l3dHr9CjSERdnBBAXBjVIHR3bu3iFlueDIzT7Gb/G4VNCn
ylJllZ8D7vcKdPyBYNV9ako1TFIHdFT5g/XvyPotPgBZa8E3FciMU/MfVds0hdmG
Yizx0T5atH3/IdYW7EXhgsBjWD6Tlscyxptb1e+JWPL1N3E7IexFH5xp+jfvrHLn
xTuabhLjsIeaEKWV5DXO3yQrFcrq4JhMZ/A+cQuOUrHfiB6eqDq2yXSTflQh07qB
j/GWHgu6o5bHVFsv2sVIBbM+vP6Upa4trUHZNsgPOwnD7vb8GHNAjxxx41goy9gI
X3TQ1P8zpE/msKCVDOscwObx2h8OTQ6jgWYRp2EK+Qp2FnKNxrdr0N7q2U2gzyao
fkApVgFmG36N2v7Gi3Dn3Lfsc1bFfd7wGjDx8s8CIBf3GxaL1RsN2rzvZoJIl7d5
8a4qXHlYcshgJTG6oMLRR+c3Cq3bkMtfU7Y5VYcPH9NcvtYnDhzLxuWQZPXZJ4Wl
F9z3Uhy4/Z7DHiR18H7fTCNv5zA0xhOUiMhRUuji5y6ip0t4KK6HtbTsjA7ozzTJ
MHYuuB7fXxfY0hkL02gysnhjRN0KghajJwKZFGFqGjj7N1StI9lPgE6Jqs3UyszV
JqtrJrAdIrySwFncyZMYll4SujNPS/IOL5/DcOI2JeKt/QlZv3+zfNaC+HOJcbdy
wU9SReth3cXZCUPu59wZNFIVhVFolRQCQwLFx01AeQ9IDV0pgvDrfF/BiSScZrSo
b9eFhBWOy05GV3Fln2FIEWuqdWX2iQ6bpkpi5QauTp0c4RR1ZXKBtjkqUo98s20R
QEPLaPxSCusmpszGiSzyrOFStUo769/Yk+WPHpoLM3KsAz1Cs936rcwRPICqlGn/
eblcwS7LxM1UuYuD9FtZKJNXZENosOchV2bNxWCKNkrKqwcNA/S4UFgeOQCdgyUL
aMGMNFdwc2dmKcFTNJySMc0=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIILaTCCB1GgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwgYQxCzAJBgNVBAYTAkFT
MRIwEAYDVQQIDAlBU0Rma2pkaGYxDzANBgNVBAcMBmxza2RqZjEQMA4GA1UECgwH
c2FsZGZqYTEOMAwGA1UECwwFc2tkamYxEjAQBgNVBAMMCWxvY2FsaG9zdDEaMBgG
CSqGSIb3DQEJARYLc2FkamZsa2Fkc2owHhcNMjMwNzA1MTMwMzU5WhcNMjUwNzA0
MTMwMzU5WjB1MRIwEAYDVQQDDAlsb2NhbGhvc3QxEDAOBgNVBAgMB3NhZGZqYXMx
CzAJBgNVBAYTAkFTMRkwFwYJKoZIhvcNAQkBFgpzZGZsa2RzYWpmMRMwEQYDVQQK
DApsc2tkZmpsc2FkMRAwDgYDVQQLDAdzbGRma2phMIIEIjANBgkqhkiG9w0BAQEF
AAOCBA8AMIIECgKCBAEApwUqxdXFSIg463ke4OHwkOFp6vDX8el4iCJDQYHl5+Mj
1apFN0PCqSggF/mkwdXmsMYHHlP8+3RiVsd2I+cNB6NEUj368OCgIgbvzCOlaLCx
LTZE2dLlRgSPPXPFTlNMnCP7tnrIZYAm/xnZgvfoHa3YsU9kRAbKwbKahdVvo7V4
TWWfvokmR92lF2YMEOrMTaUgKcW6DxF5zdC1Phvepx3uXnAs/Hg8pOVp6DZ+RUqd
0JRBUhEJk0wM5hnRni/E/ZKe7OO/OGjXR2SAs8BInjCZtWBxEjbTCdEkvjsYRetg
CeopWzeHGN/BYnKVQi7hyL8jutFvGNBqX5gyvi9f8rlNvCZdsXyQ8HC1AslUw25/
qnwIRmBJKv5WfWUMsyzID+QkDgM3/Z4kd41uQwq/jWnEWtqLw6xcV/aQxyw5mhmJ
vqK4Vx6uxa8Eif2FDFxftX7setiiIW88S1b/rSGQTcxxh0tGgNkM1nZi6D2eCzqM
riLzou6Pywhy+49i78by8T19wZsy5FrITx7x2s9dhWG9dxD7SSmMjtQqqeinX7Eg
AO2fjv+pyqre20tN40IPaJhc7dl7T33VL7pdTKjLgxcTJLORXgHDnd8mbU9SXLM2
fjQecBuzgIrgUk1FDRz7pDMPsOsWvOwa9JLhykL+5USyHr0WVFEoYQNZ3UHShhtv
6EQk+/y1QFWEsjzVReuHuVGbWfRSuL58nz1HNOECXiP+lrB6dQEWzuQbaapiXnV4
kRxFFjxdF9BwI+gRf4uGX92P8cBue7MKIcIq7j0gks6YXEPgdooFOIJj171XbtvY
j3jxWLbQVXuLyhMf4ftrLNGM/ku2Edp6nRd1m2dDeVHsCT27BkmvUUa3xVqv3/gO
V9mF9KtjNYrTXX8OTBnjDGAYWA+lxS4/OsejS3D9W9k0RAhlTorsF71gZhi5T78F
3voIPlQG2PR5bbB+kaNwsc+AtY6K4RgaiXLil39S+7dKG/N9NDqbuCVgpQzvzCWs
lliGpSr2oTWbcIbXK1UQjNTEy/hLR8WxmUcWfzI2vk+mwwRG2p55CwOD1G9KTrDv
6wPfXKZceqFmH9jkCt/iPPRVVrMbtNwzz7Q2sEh3fi6DRzA3gjErl3jMbcfU4YJx
9HfC1mNCJJuLvjhrQRjrcrocEPSgv2EPJkRo8XNZgVwi5syEY+3cRKsIOFRj2IAe
6nppW0Pe1uwpqqdoURGI9BE0r5D0lIY19qTO2M4qMGBW0oedHO4rnQt4y3MBQzi8
sLb4Z5A+KjSB6XECgEq8m+Wwj4K6olMm7YluTspyzE8Nqwg2kH2SXY4wf98bQDSf
gVg5u4JsC3lPQRAtan/1yffgFO3e+shp15tuWshknwIDAQABo4IB8TCCAe0wDwYD
VR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU244rpvoJhKafs083DFvHum9HwWUwHwYD
VR0jBBgwFoAUyIG2Gvtv1hr/8jrdJvVXLG6x+TAwCwYDVR0PBAQDAgGmMBMGA1Ud
JQQMMAoGCCsGAQUFBwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3Bh
cmtsaW5nY2EuY29tL1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtp
LmJhY2t1cC5jb20vU3BhcmtsaW5nUm9vdC5jcmwwMQYDVR0RBCowKIIJbG9jYWxo
b3N0ghtTcGFya2xpbmcgQ0EgSW50ZXJtaWRpYXRlIDEwgdYGCCsGAQUFBwEBBIHJ
MIHGMDgGCCsGAQUFBzAChixodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNvbS9TcGFy
a2xpbmdSb290LmNydDAzBggrBgEFBQcwAoYnaHR0cDovL3BraS5iYWNrdXAuY29t
L1NwYXJrbGluZ1Jvb3QuY3J0MCwGCCsGAQUFBzABhiBodHRwOi8vcGtpLnNwYXJr
bGluZ2NhLmNvbS9vY3NwLzAnBggrBgEFBQcwAYYbaHR0cDovL3BraS5iYWNrdXAu
Y29tL29jc3AvMA0GCSqGSIb3DQEBCwUAA4IEAQAZQf83aVYyirjmTwV8Rsi9iLzs
izFSwMUNoGNEn+5lb3VzCLDBFXRG8vymhVG7wTxGIKjbpajyJgfDzns1lgBrrqK8
+5DH2fA98OEDMmwlq41QcXZHxn6Q6futJifPz4+c7GRq3Z0XNlOyqExX4feVry1g
LcIM3vS6Q6UC47KCFrhJMNiQUPdQe7puitrUt22pdnkhAMPYjIpPUGD/H6jKWdiJ
BJhsdPjzsODlcNXUb5xQxRfd+C5D7HXIRotjl8L0r388HGZ/VEcETaxJdeAzAa3t
L6Rd5T0zMlYxhWfQiekr2xIO379T6oh/LD/FV+8sQD1vWrwVkmYxINiKsiJnVBa3
lDmT8iDqdfvmfvBqg/Nf7lAcAwlAFUExHYfKj9QdOFK080I4qZbajW9s4xd/xj8K
LmcpWRqSEPHEicc3OJ0QJ8Du1vvKVkzBvx7/PScAd/NZilrI1ZXTCQt+VNlmnYSD
ZFM/I9P1GCjR1orXS/t75ktyyEMiCVT7fo0ppB0d7S+xBcLkzUV5D6y3KnQI3t2C
U48e2QbeEutswSm3v7JysY4uEDBnQqftiFwTTIN+PDnkvnntw3yAAowVqY2hhOvs
JEmlre5xRWZMq7w7nCDx/p8ciUJZvfeCwFlSwTA8aSWcmuudQ1vfJdx9Z++zO/qH
h/7Aw3aBSaMtHA7Bdg7Z3993vLwE8p2TyPsOOzFEAXFX0iFuWoDCO/gu+c3mIDDv
1+pB4WORTuzb+UUr1CFyDhjtFOCbGnLpSn1Jl2WMY8uRfviW49nwwPL7oxiDdMBr
TialbjmwRDOPSgS9OoCV3gqZp/ygEBi0S+xHJ2EfGB/TrgZjQcDZI+fPqTnGxf9Z
e17ROC3M3uZWwD2KMw2cIFyL1g3FeUyyhFm3BMn05muKhsAucy/mpcgPsjVO29h7
lHaW4VsFF+zJLEDOpURKPQbjmSP27VLT0W0V/YTDmhtiuhgi91+QeOgI6RPbxo96
ubabcoPbkVaG8+HEXZd+rBBYX9fEvQ8HXzgzPcWqApfnQrbm0r4HVzKM4ekEaxwD
gime6wkeJfbcMmiT8Y/dTyxARWlPwNigU+e5xj8ckRlumK/jLOE94extC9fUINPs
lwTrp+mnDyC97h+G5B5gszJ+S6XkneyYWqgGLtwyI/mmI4AaVNKIvsK0v1916tjD
Ky3hFhCJfXGiuqZGu81SmUIcFnz6EgSDOpXFePbl/IMyW1zkihrh3brSEHr+PYym
++7QNAh8RsheF3aX6l3K1iuGmsDBM/bLRAL2qTklBy0zi7hwFs2KjDmoFYZ6f6Po
uzsEffG9Yyns4Qia47NitM2/tnMm/QLc+C32qCJUQ68xDkG7E4Ay98CCUNBd
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
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
+WDMk0mCnqqcusrqoAu117IxdSwh
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

// These certificates are the same as used in CreateTestSSLCertWithChainSignedByRoot,
// except the ca_cert, which is replaced with an expired version.
Status CreateTestSSLExpiredCertWithChainSignedByRoot(const string& dir,
                                              string* cert_file,
                                              string* key_file,
                                              string* expired_ca_cert_file) {

   const char* kCert = R"(
-----BEGIN CERTIFICATE-----
MIIJbTCCBVWgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwdTESMBAGA1UEAwwJbG9j
YWxob3N0MRAwDgYDVQQIDAdzYWRmamFzMQswCQYDVQQGEwJBUzEZMBcGCSqGSIb3
DQEJARYKc2RmbGtkc2FqZjETMBEGA1UECgwKbHNrZGZqbHNhZDEQMA4GA1UECwwH
c2xkZmtqYTAeFw0yMzA3MDUxMzA1NTdaFw0yNDA3MDQxMzA1NTdaMHgxEjAQBgNV
BAMMCWxvY2FsaG9zdDERMA8GA1UECAwIc2FramRmaGExCzAJBgNVBAYTAkFTMRkw
FwYJKoZIhvcNAQkBFgpzamRoZmtzZGFmMRQwEgYDVQQKDAtsa3NhamRmaGFsczER
MA8GA1UECwwIanNkaGZrYXMwggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoIC
AQCkqsJ6lPQ1cbocrp60iUUOj2JBAAc6XJ7PcT0HszSGI8xjWFPByrNKbh9CUF1h
ulch6QbI/u+3pkBb0HkpvsjU2yPP6Qgn0/SxE1GNbARE9tyDWU/NBIqvq40rqfXe
OygQNkOYRHv5bql2Mk1U/xbtLOkYE8yf03PuChkgm5OQmY1YFBtmPlTwSutbeUG4
KxsQITbnIFnsNC8cZzhGlaTk55xDWMUsGw1f6CT5OwNuSBxmpBp2HnwvEfpKEqBD
OOAa7N6gzn4CfDIJGDC3/s5WiLGagDgVjTyHyGN+KZd02U1Pnk5sG9OFitDSGa3d
aKuOGOAuphfYvqgaZKptQJ2heBjDPWYxRD3x/I60VpCOj0o+syCZhcgcIMDbjA+C
5lYRzJZkO9QQ0bEHlTq+ks7hZCGpx7sE9KDD0AsYB6fapcMg/rTHNsu8nfaDWWD4
xesQrchvFZYgTOmH/I2JUkxqVmv1uRIx0akANhn5GdqfJ1Uif1OwMikNKaA0M2Ve
KKKI/y5/kFEMsew2j0Zh6RaM4k0fWB6MQHFAvwsF5ptoXnBq1K01w4jvdTnUb8gm
NUPjg+bQANxDzYxruH9KtgVw3zfm00Exz2R9CXKaB7gPtKzFFxqesimH+BbP3rDP
MU5rzv8HWJ0Hn97Njw+C9g0pDXrZ97HI5ieUqTN8EgjbkQIDAQABo4ICAjCCAf4w
DAYDVR0TAQH/BAIwADAdBgNVHQ4EFgQUSWPWrk7iay2FSnQm2gBh877P/VMwHwYD
VR0jBBgwFoAU244rpvoJhKafs083DFvHum9HwWUwCwYDVR0PBAQDAgWgMBMGA1Ud
JQQMMAoGCCsGAQUFBwMBMH4GA1UdHwR3MHUwO6A5oDeGNWh0dHA6Ly9wa2kuc3Bh
cmtsaW5nY2EuY29tL1NwYXJrbGluZ0ludGVybWlkaWF0ZTEuY3JsMDagNKAyhjBo
dHRwOi8vcGtpLmJhY2t1cC5jb20vU3BhcmtsaW5nSW50ZXJtaWRpYXRlMS5jcmww
IQYDVR0RBBowGIIJbG9jYWxob3N0ggtleGFtcGxlLm9yZzCB6AYIKwYBBQUHAQEE
gdswgdgwQQYIKwYBBQUHMAKGNWh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29tL1Nw
YXJrbGluZ0ludGVybWVkaWF0ZTEuY3J0MDwGCCsGAQUFBzAChjBodHRwOi8vcGtp
LmJhY2t1cC5jb20vU3BhcmtsaW5nSW50ZXJtZWRpYXRlMS5jcnQwLAYIKwYBBQUH
MAGGIGh0dHA6Ly9wa2kuc3BhcmtsaW5nY2EuY29tL29jc3AvMCcGCCsGAQUFBzAB
hhtodHRwOi8vcGtpLmJhY2t1cC5jb20vb2NzcC8wDQYJKoZIhvcNAQELBQADggQB
AGgLQs+32qSCMY7LqLQZbsn0l989NWrHl4Nn60ftLe/VyQ5w9pP1QnQ+epoRwiY7
Gd6U+2fFtzeJOD3N4ckAzw3hJQh0uSDgGglI/S3zZLNOm80QE5+aJr8/YaHH3BIR
cYkhMHiGg4/Dijq1hCKsnqTmZE1JLmVWFezopmKJYOABw9lgbMceFiUtxsAhaLJA
rW98SkznrFos7KovtDkX8tZpuJqkqThyvOhZAbE0LkIXug3s7ShB1id7jfDrPAFd
iph0BkcU9iWLmochuZITSe9yRMbbhS38PYwnXQ35B1PPtc+wCKAMKP6RT357GqZq
HTPpD9vembZm6Db/IW4j4wLMrk12DPNQbh70cCsW16fO/CpNL+l7fP4lakvC4iu4
zFQ93XuoAtwp3VSjH3l3dHr9CjSERdnBBAXBjVIHR3bu3iFlueDIzT7Gb/G4VNCn
ylJllZ8D7vcKdPyBYNV9ako1TFIHdFT5g/XvyPotPgBZa8E3FciMU/MfVds0hdmG
Yizx0T5atH3/IdYW7EXhgsBjWD6Tlscyxptb1e+JWPL1N3E7IexFH5xp+jfvrHLn
xTuabhLjsIeaEKWV5DXO3yQrFcrq4JhMZ/A+cQuOUrHfiB6eqDq2yXSTflQh07qB
j/GWHgu6o5bHVFsv2sVIBbM+vP6Upa4trUHZNsgPOwnD7vb8GHNAjxxx41goy9gI
X3TQ1P8zpE/msKCVDOscwObx2h8OTQ6jgWYRp2EK+Qp2FnKNxrdr0N7q2U2gzyao
fkApVgFmG36N2v7Gi3Dn3Lfsc1bFfd7wGjDx8s8CIBf3GxaL1RsN2rzvZoJIl7d5
8a4qXHlYcshgJTG6oMLRR+c3Cq3bkMtfU7Y5VYcPH9NcvtYnDhzLxuWQZPXZJ4Wl
F9z3Uhy4/Z7DHiR18H7fTCNv5zA0xhOUiMhRUuji5y6ip0t4KK6HtbTsjA7ozzTJ
MHYuuB7fXxfY0hkL02gysnhjRN0KghajJwKZFGFqGjj7N1StI9lPgE6Jqs3UyszV
JqtrJrAdIrySwFncyZMYll4SujNPS/IOL5/DcOI2JeKt/QlZv3+zfNaC+HOJcbdy
wU9SReth3cXZCUPu59wZNFIVhVFolRQCQwLFx01AeQ9IDV0pgvDrfF/BiSScZrSo
b9eFhBWOy05GV3Fln2FIEWuqdWX2iQ6bpkpi5QauTp0c4RR1ZXKBtjkqUo98s20R
QEPLaPxSCusmpszGiSzyrOFStUo769/Yk+WPHpoLM3KsAz1Cs936rcwRPICqlGn/
eblcwS7LxM1UuYuD9FtZKJNXZENosOchV2bNxWCKNkrKqwcNA/S4UFgeOQCdgyUL
aMGMNFdwc2dmKcFTNJySMc0=
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIILaTCCB1GgAwIBAgICEAAwDQYJKoZIhvcNAQELBQAwgYQxCzAJBgNVBAYTAkFT
MRIwEAYDVQQIDAlBU0Rma2pkaGYxDzANBgNVBAcMBmxza2RqZjEQMA4GA1UECgwH
c2FsZGZqYTEOMAwGA1UECwwFc2tkamYxEjAQBgNVBAMMCWxvY2FsaG9zdDEaMBgG
CSqGSIb3DQEJARYLc2FkamZsa2Fkc2owHhcNMjMwNzA1MTMwMzU5WhcNMjUwNzA0
MTMwMzU5WjB1MRIwEAYDVQQDDAlsb2NhbGhvc3QxEDAOBgNVBAgMB3NhZGZqYXMx
CzAJBgNVBAYTAkFTMRkwFwYJKoZIhvcNAQkBFgpzZGZsa2RzYWpmMRMwEQYDVQQK
DApsc2tkZmpsc2FkMRAwDgYDVQQLDAdzbGRma2phMIIEIjANBgkqhkiG9w0BAQEF
AAOCBA8AMIIECgKCBAEApwUqxdXFSIg463ke4OHwkOFp6vDX8el4iCJDQYHl5+Mj
1apFN0PCqSggF/mkwdXmsMYHHlP8+3RiVsd2I+cNB6NEUj368OCgIgbvzCOlaLCx
LTZE2dLlRgSPPXPFTlNMnCP7tnrIZYAm/xnZgvfoHa3YsU9kRAbKwbKahdVvo7V4
TWWfvokmR92lF2YMEOrMTaUgKcW6DxF5zdC1Phvepx3uXnAs/Hg8pOVp6DZ+RUqd
0JRBUhEJk0wM5hnRni/E/ZKe7OO/OGjXR2SAs8BInjCZtWBxEjbTCdEkvjsYRetg
CeopWzeHGN/BYnKVQi7hyL8jutFvGNBqX5gyvi9f8rlNvCZdsXyQ8HC1AslUw25/
qnwIRmBJKv5WfWUMsyzID+QkDgM3/Z4kd41uQwq/jWnEWtqLw6xcV/aQxyw5mhmJ
vqK4Vx6uxa8Eif2FDFxftX7setiiIW88S1b/rSGQTcxxh0tGgNkM1nZi6D2eCzqM
riLzou6Pywhy+49i78by8T19wZsy5FrITx7x2s9dhWG9dxD7SSmMjtQqqeinX7Eg
AO2fjv+pyqre20tN40IPaJhc7dl7T33VL7pdTKjLgxcTJLORXgHDnd8mbU9SXLM2
fjQecBuzgIrgUk1FDRz7pDMPsOsWvOwa9JLhykL+5USyHr0WVFEoYQNZ3UHShhtv
6EQk+/y1QFWEsjzVReuHuVGbWfRSuL58nz1HNOECXiP+lrB6dQEWzuQbaapiXnV4
kRxFFjxdF9BwI+gRf4uGX92P8cBue7MKIcIq7j0gks6YXEPgdooFOIJj171XbtvY
j3jxWLbQVXuLyhMf4ftrLNGM/ku2Edp6nRd1m2dDeVHsCT27BkmvUUa3xVqv3/gO
V9mF9KtjNYrTXX8OTBnjDGAYWA+lxS4/OsejS3D9W9k0RAhlTorsF71gZhi5T78F
3voIPlQG2PR5bbB+kaNwsc+AtY6K4RgaiXLil39S+7dKG/N9NDqbuCVgpQzvzCWs
lliGpSr2oTWbcIbXK1UQjNTEy/hLR8WxmUcWfzI2vk+mwwRG2p55CwOD1G9KTrDv
6wPfXKZceqFmH9jkCt/iPPRVVrMbtNwzz7Q2sEh3fi6DRzA3gjErl3jMbcfU4YJx
9HfC1mNCJJuLvjhrQRjrcrocEPSgv2EPJkRo8XNZgVwi5syEY+3cRKsIOFRj2IAe
6nppW0Pe1uwpqqdoURGI9BE0r5D0lIY19qTO2M4qMGBW0oedHO4rnQt4y3MBQzi8
sLb4Z5A+KjSB6XECgEq8m+Wwj4K6olMm7YluTspyzE8Nqwg2kH2SXY4wf98bQDSf
gVg5u4JsC3lPQRAtan/1yffgFO3e+shp15tuWshknwIDAQABo4IB8TCCAe0wDwYD
VR0TAQH/BAUwAwEB/zAdBgNVHQ4EFgQU244rpvoJhKafs083DFvHum9HwWUwHwYD
VR0jBBgwFoAUyIG2Gvtv1hr/8jrdJvVXLG6x+TAwCwYDVR0PBAQDAgGmMBMGA1Ud
JQQMMAoGCCsGAQUFBwMBMGwGA1UdHwRlMGMwMqAwoC6GLGh0dHA6Ly9wa2kuc3Bh
cmtsaW5nY2EuY29tL1NwYXJrbGluZ1Jvb3QuY3JsMC2gK6AphidodHRwOi8vcGtp
LmJhY2t1cC5jb20vU3BhcmtsaW5nUm9vdC5jcmwwMQYDVR0RBCowKIIJbG9jYWxo
b3N0ghtTcGFya2xpbmcgQ0EgSW50ZXJtaWRpYXRlIDEwgdYGCCsGAQUFBwEBBIHJ
MIHGMDgGCCsGAQUFBzAChixodHRwOi8vcGtpLnNwYXJrbGluZ2NhLmNvbS9TcGFy
a2xpbmdSb290LmNydDAzBggrBgEFBQcwAoYnaHR0cDovL3BraS5iYWNrdXAuY29t
L1NwYXJrbGluZ1Jvb3QuY3J0MCwGCCsGAQUFBzABhiBodHRwOi8vcGtpLnNwYXJr
bGluZ2NhLmNvbS9vY3NwLzAnBggrBgEFBQcwAYYbaHR0cDovL3BraS5iYWNrdXAu
Y29tL29jc3AvMA0GCSqGSIb3DQEBCwUAA4IEAQAZQf83aVYyirjmTwV8Rsi9iLzs
izFSwMUNoGNEn+5lb3VzCLDBFXRG8vymhVG7wTxGIKjbpajyJgfDzns1lgBrrqK8
+5DH2fA98OEDMmwlq41QcXZHxn6Q6futJifPz4+c7GRq3Z0XNlOyqExX4feVry1g
LcIM3vS6Q6UC47KCFrhJMNiQUPdQe7puitrUt22pdnkhAMPYjIpPUGD/H6jKWdiJ
BJhsdPjzsODlcNXUb5xQxRfd+C5D7HXIRotjl8L0r388HGZ/VEcETaxJdeAzAa3t
L6Rd5T0zMlYxhWfQiekr2xIO379T6oh/LD/FV+8sQD1vWrwVkmYxINiKsiJnVBa3
lDmT8iDqdfvmfvBqg/Nf7lAcAwlAFUExHYfKj9QdOFK080I4qZbajW9s4xd/xj8K
LmcpWRqSEPHEicc3OJ0QJ8Du1vvKVkzBvx7/PScAd/NZilrI1ZXTCQt+VNlmnYSD
ZFM/I9P1GCjR1orXS/t75ktyyEMiCVT7fo0ppB0d7S+xBcLkzUV5D6y3KnQI3t2C
U48e2QbeEutswSm3v7JysY4uEDBnQqftiFwTTIN+PDnkvnntw3yAAowVqY2hhOvs
JEmlre5xRWZMq7w7nCDx/p8ciUJZvfeCwFlSwTA8aSWcmuudQ1vfJdx9Z++zO/qH
h/7Aw3aBSaMtHA7Bdg7Z3993vLwE8p2TyPsOOzFEAXFX0iFuWoDCO/gu+c3mIDDv
1+pB4WORTuzb+UUr1CFyDhjtFOCbGnLpSn1Jl2WMY8uRfviW49nwwPL7oxiDdMBr
TialbjmwRDOPSgS9OoCV3gqZp/ygEBi0S+xHJ2EfGB/TrgZjQcDZI+fPqTnGxf9Z
e17ROC3M3uZWwD2KMw2cIFyL1g3FeUyyhFm3BMn05muKhsAucy/mpcgPsjVO29h7
lHaW4VsFF+zJLEDOpURKPQbjmSP27VLT0W0V/YTDmhtiuhgi91+QeOgI6RPbxo96
ubabcoPbkVaG8+HEXZd+rBBYX9fEvQ8HXzgzPcWqApfnQrbm0r4HVzKM4ekEaxwD
gime6wkeJfbcMmiT8Y/dTyxARWlPwNigU+e5xj8ckRlumK/jLOE94extC9fUINPs
lwTrp+mnDyC97h+G5B5gszJ+S6XkneyYWqgGLtwyI/mmI4AaVNKIvsK0v1916tjD
Ky3hFhCJfXGiuqZGu81SmUIcFnz6EgSDOpXFePbl/IMyW1zkihrh3brSEHr+PYym
++7QNAh8RsheF3aX6l3K1iuGmsDBM/bLRAL2qTklBy0zi7hwFs2KjDmoFYZ6f6Po
uzsEffG9Yyns4Qia47NitM2/tnMm/QLc+C32qCJUQ68xDkG7E4Ay98CCUNBd
-----END CERTIFICATE-----
)";
  const char* kKey = R"(
-----BEGIN PRIVATE KEY-----
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
+WDMk0mCnqqcusrqoAu117IxdSwh
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
