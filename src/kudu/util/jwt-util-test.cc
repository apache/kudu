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
//
// Copied from Impala and adapted to Kudu.

#include "kudu/util/jwt-util.h"

#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/kazuho-picojson/defaults.h>
#include <jwt-cpp/traits/kazuho-picojson/traits.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/server/webserver.h"
#include "kudu/server/webserver_options.h"
#include "kudu/util/env.h"
#include "kudu/util/jwt-util-internal.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/web_callback_registry.h"

namespace kudu {

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

const std::string kRsaPrivKeyPem = R"(-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC4ZtdaIrd1BPIJ
tfnF0TjIK5inQAXZ3XlCrUlJdP+XHwIRxdv1FsN12XyMYO/6ymLmo9ryoQeIrsXB
XYqlET3zfAY+diwCb0HEsVvhisthwMU4gZQu6TYW2s9LnXZB5rVtcBK69hcSlA2k
ZudMZWxZcj0L7KMfO2rIvaHw/qaVOE9j0T257Z8Kp2CLF9MUgX0ObhIsdumFRLaL
DvDUmBPr2zuh/34j2XmWwn1yjN/WvGtdfhXW79Ki1S40HcWnygHgLV8sESFKUxxQ
mKvPUTwDOIwLFL5WtE8Mz7N++kgmDcmWMCHc8kcOIu73Ta/3D4imW7VbKgHZo9+K
3ESFE3RjAgMBAAECggEBAJTEIyjMqUT24G2FKiS1TiHvShBkTlQdoR5xvpZMlYbN
tVWxUmrAGqCQ/TIjYnfpnzCDMLhdwT48Ab6mQJw69MfiXwc1PvwX1e9hRscGul36
ryGPKIVQEBsQG/zc4/L2tZe8ut+qeaK7XuYrPp8bk/X1e9qK5m7j+JpKosNSLgJj
NIbYsBkG2Mlq671irKYj2hVZeaBQmWmZxK4fw0Istz2WfN5nUKUeJhTwpR+JLUg4
ELYYoB7EO0Cej9UBG30hbgu4RyXA+VbptJ+H042K5QJROUbtnLWuuWosZ5ATldwO
u03dIXL0SH0ao5NcWBzxU4F2sBXZRGP2x/jiSLHcqoECgYEA4qD7mXQpu1b8XO8U
6abpKloJCatSAHzjgdR2eRDRx5PMvloipfwqA77pnbjTUFajqWQgOXsDTCjcdQui
wf5XAaWu+TeAVTytLQbSiTsBhrnoqVrr3RoyDQmdnwHT8aCMouOgcC5thP9vQ8Us
rVdjvRRbnJpg3BeSNimH+u9AHgsCgYEA0EzcbOltCWPHRAY7B3Ge/AKBjBQr86Kv
TdpTlxePBDVIlH+BM6oct2gaSZZoHbqPjbq5v7yf0fKVcXE4bSVgqfDJ/sZQu9Lp
PTeV7wkk0OsAMKk7QukEpPno5q6tOTNnFecpUhVLLlqbfqkB2baYYwLJR3IRzboJ
FQbLY93E8gkCgYB+zlC5VlQbbNqcLXJoImqItgQkkuW5PCgYdwcrSov2ve5r/Acz
FNt1aRdSlx4176R3nXyibQA1Vw+ztiUFowiP9WLoM3PtPZwwe4bGHmwGNHPIfwVG
m+exf9XgKKespYbLhc45tuC08DATnXoYK7O1EnUINSFJRS8cezSI5eHcbQKBgQDC
PgqHXZ2aVftqCc1eAaxaIRQhRmY+CgUjumaczRFGwVFveP9I6Gdi+Kca3DE3F9Pq
PKgejo0SwP5vDT+rOGHN14bmGJUMsX9i4MTmZUZ5s8s3lXh3ysfT+GAhTd6nKrIE
kM3Nh6HWFhROptfc6BNusRh1kX/cspDplK5x8EpJ0QKBgQDWFg6S2je0KtbV5PYe
RultUEe2C0jYMDQx+JYxbPmtcopvZQrFEur3WKVuLy5UAy7EBvwMnZwIG7OOohJb
vkSpADK6VPn9lbqq7O8cTedEHttm6otmLt8ZyEl3hZMaL3hbuRj6ysjmoFKx6CrX
rK0/Ikt5ybqUzKCMJZg2VKGTxg==
-----END PRIVATE KEY-----)";
const std::string kRsaPubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuGbXWiK3dQTyCbX5xdE4
yCuYp0AF2d15Qq1JSXT/lx8CEcXb9RbDddl8jGDv+spi5qPa8qEHiK7FwV2KpRE9
83wGPnYsAm9BxLFb4YrLYcDFOIGULuk2FtrPS512Qea1bXASuvYXEpQNpGbnTGVs
WXI9C+yjHztqyL2h8P6mlThPY9E9ue2fCqdgixfTFIF9Dm4SLHbphUS2iw7w1JgT
69s7of9+I9l5lsJ9cozf1rxrXX4V1u/SotUuNB3Fp8oB4C1fLBEhSlMcUJirz1E8
AziMCxS+VrRPDM+zfvpIJg3JljAh3PJHDiLu902v9w+Iplu1WyoB2aPfitxEhRN0
YwIDAQAB
-----END PUBLIC KEY-----)";
// The public keys in JWK format were converted from PEM formatted crypto keys with
// pem-to-jwk tool at https://hub.docker.com/r/danedmunds/pem-to-jwk/
const std::string kRsaPubKeyJwkN =
    "uGbXWiK3dQTyCbX5xdE4yCuYp0AF2d15Qq1JSXT_lx8CEcXb9RbDddl8jGDv-sp"
    "i5qPa8qEHiK7FwV2KpRE983wGPnYsAm9BxLFb4YrLYcDFOIGULuk2FtrPS512Qe"
    "a1bXASuvYXEpQNpGbnTGVsWXI9C-yjHztqyL2h8P6mlThPY9E9ue2fCqdgixfTF"
    "IF9Dm4SLHbphUS2iw7w1JgT69s7of9-I9l5lsJ9cozf1rxrXX4V1u_SotUuNB3F"
    "p8oB4C1fLBEhSlMcUJirz1E8AziMCxS-VrRPDM-zfvpIJg3JljAh3PJHDiLu902"
    "v9w-Iplu1WyoB2aPfitxEhRN0Yw";
const std::string kRsaPubKeyJwkE = "AQAB";
const std::string kRsaInvalidPubKeyJwkN =
    "xzYuc22QSst_dS7geYYK5l5kLxU0tayNdixkEQ17ix-CUcUbKIsnyftZxaCYT46"
    "rQtXgCaYRdJcbB3hmyrOavkhTpX79xJZnQmfuamMbZBqitvscxW9zRR9tBUL6vd"
    "i_0rpoUwPMEh8-Bw7CgYR0FK0DhWYBNDfe9HKcyZEv3max8Cdq18htxjEsdYO0i"
    "wzhtKRXomBWTdhD5ykd_fACVTr4-KEY-IeLvubHVmLUhbE5NgWXxrRpGasDqzKh"
    "CTmsa2Ysf712rl57SlH0Wz_Mr3F7aM9YpErzeYLrl0GhQr9BVJxOvXcVd4kmY-X"
    "kiCcrkyS1cnghnllh-LCwQu1sYw";

const std::string kRsa512PrivKeyPem = R"(-----BEGIN RSA PRIVATE KEY-----
MIICWwIBAAKBgQDdlatRjRjogo3WojgGHFHYLugdUWAY9iR3fy4arWNA1KoS8kVw
33cJibXr8bvwUAUparCwlvdbH6dvEOfou0/gCFQsHUfQrSDv+MuSUMAe8jzKE4qW
+jK+xQU9a03GUnKHkkle+Q0pX/g6jXZ7r1/xAK5Do2kQ+X5xK9cipRgEKwIDAQAB
AoGAD+onAtVye4ic7VR7V50DF9bOnwRwNXrARcDhq9LWNRrRGElESYYTQ6EbatXS
3MCyjjX2eMhu/aF5YhXBwkppwxg+EOmXeh+MzL7Zh284OuPbkglAaGhV9bb6/5Cp
uGb1esyPbYW+Ty2PC0GSZfIXkXs76jXAu9TOBvD0ybc2YlkCQQDywg2R/7t3Q2OE
2+yo382CLJdrlSLVROWKwb4tb2PjhY4XAwV8d1vy0RenxTB+K5Mu57uVSTHtrMK0
GAtFr833AkEA6avx20OHo61Yela/4k5kQDtjEf1N0LfI+BcWZtxsS3jDM3i1Hp0K
Su5rsCPb8acJo5RO26gGVrfAsDcIXKC+bQJAZZ2XIpsitLyPpuiMOvBbzPavd4gY
6Z8KWrfYzJoI/Q9FuBo6rKwl4BFoToD7WIUS+hpkagwWiz+6zLoX1dbOZwJACmH5
fSSjAkLRi54PKJ8TFUeOP15h9sQzydI8zJU+upvDEKZsZc/UhT/SySDOxQ4G/523
Y0sz/OZtSWcol/UMgQJALesy++GdvoIDLfJX5GBQpuFgFenRiRDabxrE9MNUZ2aP
FaFp+DyAe+b4nDwuJaW2LURbr8AEZga7oQj0uYxcYw==
-----END RSA PRIVATE KEY-----)";
const std::string kRsa512PubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDdlatRjRjogo3WojgGHFHYLugd
UWAY9iR3fy4arWNA1KoS8kVw33cJibXr8bvwUAUparCwlvdbH6dvEOfou0/gCFQs
HUfQrSDv+MuSUMAe8jzKE4qW+jK+xQU9a03GUnKHkkle+Q0pX/g6jXZ7r1/xAK5D
o2kQ+X5xK9cipRgEKwIDAQAB
-----END PUBLIC KEY-----)";
const std::string kRsa512PubKeyJwkN =
    "3ZWrUY0Y6IKN1qI4BhxR2C7oHVFgGPYkd38uGq1jQNSqEvJFcN93CYm16_G78FA"
    "FKWqwsJb3Wx-nbxDn6LtP4AhULB1H0K0g7_jLklDAHvI8yhOKlvoyvsUFPWtNxl"
    "Jyh5JJXvkNKV_4Oo12e69f8QCuQ6NpEPl-cSvXIqUYBCs";
const std::string kRsa512PubKeyJwkE = "AQAB";
const std::string kRsa512InvalidPubKeyJwkN =
    "xzYuc22QSst_dS7geYYK5l5kLxU0tayNdixkEQ17ix-CUcUbKIsnyftZxaCYT46"
    "rQtXgCaYRdJcbB3hmyrOavkhTpX79xJZnQmfuamMbZBqitvscxW9zRR9tBUL6vd"
    "i_0rpoUwPMEh8-Bw7CgYR0FK0DhWYBNDfe9HKcyZEv3max8Cdq18htxjEsdYO0i"
    "wzhtKRXomBWTdhD5ykd_fACVTr4-KEY-IeLvubHVmLUhbE5NgWXxrRpGasDqzKh"
    "CTmsa2Ysf712rl57SlH0Wz_Mr3F7aM9YpErzeYLrl0GhQr9BVJxOvXcVd4kmY-X"
    "kiCcrkyS1cnghnllh-LCwQu1sYw";

const std::string kRsa1024PrivKeyPem = R"(-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDT+6sb2SvN69NB+6Zg78B7mdke0tC91CTfixzCSn7wS8JUvvZK
AO1uMgnrCQdDr2TNeRYr6urawIOCDB1Ybz1+cBSNxouVdt/aT9+cw27kzVQE59NA
PMpQyLtXaAOR6rD8xzyIgAV12QFmc1kHFl7Sjobwmsu5ZWRqYTwdXvFXIQIDAQAB
AoGBAJKDLxBgWVZJ2AmS1LvK+U50VwxmyL9rENEwZQAkXPfYZMgN9EvRuEihbRl1
c//kCde6CQjxpMDsrfgER4QH3odypQWT9A5uXKcdfu/z+xKNtB813rSrew3Q9pXe
wlOb0q7EcS7XHMrcPxj4gvn2yKqB40vF3TIY6oiSeZbFLUvBAkEA9NaTrGB1+FZj
+3lIAs7UtYbxNggX53OEcXlstDbqhG3O9SzAHiccMbGu2lDBcAAghmtg9poT0Uo6
V3VCJcnfNwJBAN2lppZFVWAXOLD2k8OMCp4jc9pRHIUtPU6kWoflU8O6kuDNNamD
AeNMhdHX+Ed/Js3ig75eAGxsd9q+CFp/uGcCQQDFfGb0/YFqZFSVPMhm62oLWeMq
T/DoEfdciDK0Ui9rzh7HB+eW6rkFJGsDUWwV6SRTCD3X64PcpuDUNpK6ZFCVAkEA
oaBgAAiDH1UPpAvK6LfALl0P6E1pjLvWjvhOg/Z4xKvS21cJIJlF0ShGFSV2CTzx
YQUiqLkHegkGxV353XRxVQJAZaW5O2BI5jKy2hK0EoAx3pSnp2X4CmkWrXsSeOgC
Zz+jDkn8QzPbRwb8cyks/IHc2CBvaFStLFKO2VQj1THDhw==
-----END RSA PRIVATE KEY-----)";
const std::string kRsa1024PubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDT+6sb2SvN69NB+6Zg78B7mdke
0tC91CTfixzCSn7wS8JUvvZKAO1uMgnrCQdDr2TNeRYr6urawIOCDB1Ybz1+cBSN
xouVdt/aT9+cw27kzVQE59NAPMpQyLtXaAOR6rD8xzyIgAV12QFmc1kHFl7Sjobw
msu5ZWRqYTwdXvFXIQIDAQAB
-----END PUBLIC KEY-----)";
const std::string kRsa1024PubKeyJwkN =
    "0_urG9krzevTQfumYO_Ae5nZHtLQvdQk34scwkp-8EvCVL72SgDtbjIJ6wkHQ69"
    "kzXkWK-rq2sCDggwdWG89fnAUjcaLlXbf2k_fnMNu5M1UBOfTQDzKUMi7V2gDke"
    "qw_Mc8iIAFddkBZnNZBxZe0o6G8JrLuWVkamE8HV7xVyE";
const std::string kRsa1024PubKeyJwkE = "AQAB";

const std::string kRsa2048PrivKeyPem = R"(-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEA0jCHomsNIaRYVlsemWg9yBx3od1B9Fd9RUslk9IVE7IU+QYZ
+T4NvRVPAMjpzuurvPnN4uBVPREycXOgEcWHiJDDQEhlQD4F69W8MFE7SXpdcBih
zcj5qPYtTFP/52s6Vg7Y8SAUkBDyr0B442ONR1SBD8qEMAxpiLMH1Q/Yap+etvIj
D1r2zQkQke53An9LvVl7OKkM8KGOcE/0tJRmc7x8ZlLqogPczkXvW6T+YAkwA8Xw
ginZw0xBzfpoOEnajqm4Yikck0gJ0HwdlYFIp72ih7uozne7PYLVGb9X97cL0H1X
DiA/SXJiFKo1AKXihcOdIRiw49eo9rzsoWPygQIDAQABAoIBAADe2BT1XgojYNqc
s9P9UUeEof80Mst6WEe4RQknb9RozVBEX55Ut4sEAqjVbC3MnpBgtXhTfFmNem4W
BUCa7DyFzZ/fcjc8T9sh7mQB1h3FXraHN5ZUrH9auPsjBuvfBGW/rSjUfJlQefzS
psgu950Rwxtnt+PuDTrWc6QaKx0ylvESKPIaVoticc11Kcts5Fe/RQ2Az2epDDM7
ptZamvtzptozPPq5YUIvpSnKCJfzOczAQT4omVewJV/7nbo/MdCALExrqHcIqXFp
2uMpHV1QhqZ160Bzf1O+iDRCxT3rd4OZ5Y68x/fYV8dRqrqPA5BFep6ukf17cnWM
svDqsaUCgYEA+Z5RbadUKteAM3v1Deu9RG7TucnxyoNSofpEuwMoVxo3+z+dS44v
UpC7/MJhx1FBf15yKSPIgtjt5o/LanApcJEZVyghucsNvqy11db027P63NkIL/ic
AgB04odLvxpgLHNv/qEWy7zHBLHhcazajzDHW+a/xBXrtJa3i2G+poUCgYEA15Ap
OJPafAx/BPMbrYthpd5pVX5AMExXTur7rMIPi4/wh0O0vqGtulwgX3FiS0X4bAzK
tNJ23/V2RR0F16IAIVZQqt16pIvmhx52iC55EPp3bZWkGhZ33/8Dxzkbe+rlwECa
wRK4dOyA9hwsnlRuEb8OHva6sr+EusOxmeN6Us0CgYEAg4O/QTe057GM0RNRJFl8
6a4+jRdx9hHEmqTCS4m5WlLtBcoZdLJgCm9JLD25yIruKE45daVtwkrK5PwD33ti
yfUY1cvGIR5zim9yikzry0mDNZJ/ds7UW1WkP6mq5e/elezoJ871tLgsXzPdJMg+
iszXbHshtA0cl5QE9kG0cgUCgYEAzZf3WLjbxzh75RKhMVIgnfyU5i91tRr6opBH
3atw/CEavUf8GV1GvtmjHqSbpUNk/ljs9K1PJ6eLV7uomNMv4JvccDqxAENWaUTK
tHPukBzyzxfL3f3T81XcGqUC65tL6aM0djUOrKXtEc4pWBEasd5Q74NO6bD0PNTs
jOODBXkCgYEAhaD3gZUCWU+ZA6QmxPotfe9L0tzjmUjsLo0QUgIHJa2VaoHzdnWC
ClvP3tFFkv2dlD6UW+g0JJFTVWcv+HEiC9WUnD/C6dXK/qA3fRvBhRKy8FTwvOis
zSVeYds6mvDJwFe+2mk0KQiKnxlx22B4PcYbbN7mZ2ClBFTFrp0+Id4=
-----END RSA PRIVATE KEY-----)";
const std::string kRsa2048PubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0jCHomsNIaRYVlsemWg9
yBx3od1B9Fd9RUslk9IVE7IU+QYZ+T4NvRVPAMjpzuurvPnN4uBVPREycXOgEcWH
iJDDQEhlQD4F69W8MFE7SXpdcBihzcj5qPYtTFP/52s6Vg7Y8SAUkBDyr0B442ON
R1SBD8qEMAxpiLMH1Q/Yap+etvIjD1r2zQkQke53An9LvVl7OKkM8KGOcE/0tJRm
c7x8ZlLqogPczkXvW6T+YAkwA8XwginZw0xBzfpoOEnajqm4Yikck0gJ0HwdlYFI
p72ih7uozne7PYLVGb9X97cL0H1XDiA/SXJiFKo1AKXihcOdIRiw49eo9rzsoWPy
gQIDAQAB
-----END PUBLIC KEY-----)";
const std::string kRsa2048PubKeyJwkN =
    "0jCHomsNIaRYVlsemWg9yBx3od1B9Fd9RUslk9IVE7IU-QYZ-T4NvRVPAMjpzuu"
    "rvPnN4uBVPREycXOgEcWHiJDDQEhlQD4F69W8MFE7SXpdcBihzcj5qPYtTFP_52"
    "s6Vg7Y8SAUkBDyr0B442ONR1SBD8qEMAxpiLMH1Q_Yap-etvIjD1r2zQkQke53A"
    "n9LvVl7OKkM8KGOcE_0tJRmc7x8ZlLqogPczkXvW6T-YAkwA8XwginZw0xBzfpo"
    "OEnajqm4Yikck0gJ0HwdlYFIp72ih7uozne7PYLVGb9X97cL0H1XDiA_SXJiFKo"
    "1AKXihcOdIRiw49eo9rzsoWPygQ";
const std::string kRsa2048PubKeyJwkE = "AQAB";

const std::string kRsa4096PrivKeyPem = R"(-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----)";
const std::string kRsa4096PubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAtxmYsvs6ZfhTCFKCHQBW
/W3iRfh8wZN+/XPXaOiIx9SXYSFrb/WRaTn8UOvflYuRnPYMaRGr5gVTS6/WFVvt
NuZVIDOQBgEOBt5MQ0BeM0yPiM6qacP15couRwxbJx45ODQNyh5jNF4SdzqThNFT
CFHtWakL1qrkGNSKdowIMaM59dm58liMHxp9h9yTmqM4ZgiZkoF6Vy4KYrg9ChVq
UZze4KMiyow8Xv6ESM7Eg2ncTbRevuedbtYv7OVlotyozt1geFkWm/8ZUA6Z68lf
tYMySq0/yjAmjql0DXP1+vPL9k5sKGr5lpIUlB7a9JWbXdepNjvy1vslFuLjcM50
9d1E8e70C5VF61XthKlk3rRIBWK80XupWR7o6clJsMYKxeF+ImBbzDbWrIkMxe0v
TbikS6S4CLPVlYx4sMWAWu4UBpxZquw4cVxyiKoZ2j5yTMAD1xiHI/b2/psFzW3q
XcgQWTY7dpIP1BInhepCzHlcLSEiHtmMoCXNC3+i9ZXxiJ1u8avYfGjH8RrJW8dv
Vw7BS7zlH9s7rCn001VBJCJcXtkGaykw9Zd1E+Jh7IKQJn8gydsQ0enlMmtwsJO/
tEvYBojFXbl4XecMWADTiExjXobX1y7u9ZTn0KRNkPpX9GTgY3oR0ei+rwOr4d+k
2CrUdkMTGfjnfcDHKjHh3LMCAwEAAQ==
-----END PUBLIC KEY-----)";
const std::string kRsa4096PubKeyJwkN =
    "txmYsvs6ZfhTCFKCHQBW_W3iRfh8wZN-_XPXaOiIx9SXYSFrb_WRaTn8UOvflYu"
    "RnPYMaRGr5gVTS6_WFVvtNuZVIDOQBgEOBt5MQ0BeM0yPiM6qacP15couRwxbJx"
    "45ODQNyh5jNF4SdzqThNFTCFHtWakL1qrkGNSKdowIMaM59dm58liMHxp9h9yTm"
    "qM4ZgiZkoF6Vy4KYrg9ChVqUZze4KMiyow8Xv6ESM7Eg2ncTbRevuedbtYv7OVl"
    "otyozt1geFkWm_8ZUA6Z68lftYMySq0_yjAmjql0DXP1-vPL9k5sKGr5lpIUlB7"
    "a9JWbXdepNjvy1vslFuLjcM509d1E8e70C5VF61XthKlk3rRIBWK80XupWR7o6c"
    "lJsMYKxeF-ImBbzDbWrIkMxe0vTbikS6S4CLPVlYx4sMWAWu4UBpxZquw4cVxyi"
    "KoZ2j5yTMAD1xiHI_b2_psFzW3qXcgQWTY7dpIP1BInhepCzHlcLSEiHtmMoCXN"
    "C3-i9ZXxiJ1u8avYfGjH8RrJW8dvVw7BS7zlH9s7rCn001VBJCJcXtkGaykw9Zd"
    "1E-Jh7IKQJn8gydsQ0enlMmtwsJO_tEvYBojFXbl4XecMWADTiExjXobX1y7u9Z"
    "Tn0KRNkPpX9GTgY3oR0ei-rwOr4d-k2CrUdkMTGfjnfcDHKjHh3LM";
const std::string kRsa4096PubKeyJwkE = "AQAB";

const std::string kEcdsa521PrivKeyPem = R"(-----BEGIN EC PRIVATE KEY-----
MIHcAgEBBEIAuZxTZjLIZM5hxgZX+JRrqt5FKpAEg/meZ7m9aSE3XbRITqtfz1Uy
h2Srn7o8+4j/jQpwHTTHZThy10u5jMjaR+mgBwYFK4EEACOhgYkDgYYABAFFah0k
6m4ddp/tUN/ObrKKwSCp4QUZdiAMaC9eY1HyNBPuuEsH5qCfeY5lmeJwSUpzCosn
rgW8M2hQ4Kr5V9OXrgHLA5WVtH6//sSkUY2/xYuqc7/Ln8gI5ddtr1qG64Xtgs05
/CNajSjFZeLm76llakvYiBTTH/ii8hIfrwukW9IP7Q==
-----END EC PRIVATE KEY-----)";
const std::string kEcdsa521PubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MIGbMBAGByqGSM49AgEGBSuBBAAjA4GGAAQBRWodJOpuHXaf7VDfzm6yisEgqeEF
GXYgDGgvXmNR8jQT7rhLB+agn3mOZZnicElKcwqLJ64FvDNoUOCq+VfTl64BywOV
lbR+v/7EpFGNv8WLqnO/y5/ICOXXba9ahuuF7YLNOfwjWo0oxWXi5u+pZWpL2IgU
0x/4ovISH68LpFvSD+0=
-----END PUBLIC KEY-----)";
const std::string kEcdsa521PubKeyJwkX =
    "AUVqHSTqbh12n-1Q385usorBIKnhBRl2IAxoL15jUfI0E-64SwfmoJ95jmWZ4nB"
    "JSnMKiyeuBbwzaFDgqvlX05eu";
const std::string kEcdsa521PubKeyJwkY =
    "AcsDlZW0fr_-xKRRjb_Fi6pzv8ufyAjl122vWobrhe2CzTn8I1qNKMVl4ubvqWV"
    "qS9iIFNMf-KLyEh-vC6Rb0g_t";

const std::string kEcdsa384PrivKeyPem = R"(-----BEGIN EC PRIVATE KEY-----
MIGkAgEBBDCrPXJDgQDtNRpM0qNUW/zN1vrCvOVH1CsItVZ+1NeGB+w/2whnIXJQ
K7U5C1ETPHagBwYFK4EEACKhZANiAAR0JjvVJXc3u1I/7vt5mxzPtAIi1VIqxCwN
wgISZVySTYZQzyicW2GfhMlFCow28LzqTwH/eCymAvnTAmpK/P1hXhNcnxDBZNOU
WMbMLFcQrg2wwpIb/k/IXobNwjNPRBo=
-----END EC PRIVATE KEY-----)";
const std::string kEcdsa384PubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEdCY71SV3N7tSP+77eZscz7QCItVSKsQs
DcICEmVckk2GUM8onFthn4TJRQqMNvC86k8B/3gspgL50wJqSvz9YV4TXJ8QwWTT
lFjGzCxXEK4NsMKSG/5PyF6GzcIzT0Qa
-----END PUBLIC KEY-----)";
const std::string kEcdsa384PubKeyJwkX =
    "dCY71SV3N7tSP-77eZscz7QCItVSKsQsDcICEmVckk2GUM8onFthn4TJRQqMNvC8";
const std::string kEcdsa384PubKeyJwkY =
    "6k8B_3gspgL50wJqSvz9YV4TXJ8QwWTTlFjGzCxXEK4NsMKSG_5PyF6GzcIzT0Qa";

const std::string kEcdsa256PrivKeyPem = R"(-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgPGJGAm4X1fvBuC1z
SpO/4Izx6PXfNMaiKaS5RUkFqEGhRANCAARCBvmeksd3QGTrVs2eMrrfa7CYF+sX
sjyGg+Bo5mPKGH4Gs8M7oIvoP9pb/I85tdebtKlmiCZHAZE5w4DfJSV6
-----END PRIVATE KEY-----)";
const std::string kEcdsa256PubKeyPem = R"(-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEQgb5npLHd0Bk61bNnjK632uwmBfr
F7I8hoPgaOZjyhh+BrPDO6CL6D/aW/yPObXXm7SpZogmRwGROcOA3yUleg==
-----END PUBLIC KEY-----)";
const std::string kEcdsa256PubKeyJwkX = "Qgb5npLHd0Bk61bNnjK632uwmBfrF7I8hoPgaOZjyhg";
const std::string kEcdsa256PubKeyJwkY = "fgazwzugi-g_2lv8jzm115u0qWaIJkcBkTnDgN8lJXo";

const std::string kKid1 = "public:c424b67b-fe28-45d7-b015-f79da50b5b21";
const std::string kKid2 = "public:9b9d0b47-b9ed-4ba6-9180-52fc5b161a3a";

const std::string kJwksHsFileFormat = R"(
{
  "keys": [
    { "kty": "oct", "kid": "$0", "alg": "$1", "k": "$2" }
  ]
})";

const std::string kJwksRsaFileFormat = R"(
{
  "keys": [
    { "kty": "RSA", "kid": "$0", "alg": "$1", "n": "$2", "e": "$3" },
    { "kty": "RSA", "kid": "$4", "alg": "$5", "n": "$6", "e": "$7" }
  ]
})";

const std::string kJwksEcFileFormat = R"(
{
  "keys": [
    { "kty": "EC", "kid": "$0", "crv": "$1", "x": "$2", "y": "$3" }
  ]
})";

/// Utility class for creating a file that will be automatically deleted upon test
/// completion.
class TempTestDataFile {
 public:
  // Creates a temporary file with the specified contents.
  explicit TempTestDataFile(const std::string& contents);

  /// Returns the absolute path to the file.
  const std::string& Filename() const { return name_; }

 private:
  std::string name_;
  std::unique_ptr<WritableFile> tmp_file_;
};

TempTestDataFile::TempTestDataFile(const std::string& contents)
  : name_("/tmp/jwks_XXXXXX") {
  string created_filename;
  WritableFileOptions opts;
  opts.is_sensitive = false;
  Status status;
  status = Env::Default()->NewTempWritableFile(opts, &name_[0], &created_filename, &tmp_file_);
  if (!status.ok()) {
    std::cerr << Substitute("Error creating temp file: $0", created_filename);
  }

  status = WriteStringToFile(Env::Default(), contents, created_filename);
  if (!status.ok()) {
    std::cerr << Substitute("Error writing contents to temp file: $0", created_filename);
  }

  name_ = created_filename;
}

TEST(JwtUtilTest, LoadJwksFile) {
  // Load JWKS from file.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_FALSE(jwks->IsEmpty());
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ("rs256", key1->get_algorithm());
  ASSERT_EQ(kRsaPubKeyPem, key1->get_key());

  std::string non_existing_kid("public:c424b67b-fe28-45d7-b015-f79da5-xxxxx");
  const JWTPublicKey* key3 = jwks->LookupRSAPublicKey(non_existing_kid);
  ASSERT_FALSE(key3 != nullptr);
}

TEST(JwtUtilTest, LoadInvalidJwksFiles) {
  // JWK without kid.
  std::unique_ptr<TempTestDataFile> jwks_file(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "    }"
      "  ]"
      "}"));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "parsing key #0")
      << " Actual error: " << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "'kid' property is required")
      << "actual error: " << status.ToString();

  // Invalid JSON format, missing "]" and "}".
  jwks_file.reset(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"kid\": \"public:c424b67b-fe28-45d7-b015-f79da50b5b21\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "}"));
  status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "Missing a comma or ']' after an array element")
      << " Actual error: " << status.ToString();

  // JWKS with empty key id.
  jwks_file.reset(new TempTestDataFile(
      Substitute(kJwksRsaFileFormat, "", "RS256", kRsaPubKeyJwkN, kRsaPubKeyJwkE,
          "", "RS256", kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE)));
  status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "parsing key #0")
      << " Actual error: " << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "'kid' property must be a non-empty string")
      << " Actual error: " << status.ToString();

  // JWKS with empty key value.
  jwks_file.reset(new TempTestDataFile(
      Substitute(kJwksRsaFileFormat, kKid1, "RS256", "", "", kKid2, "RS256", "", "")));
  status = jwt_helper.Init(jwks_file->Filename(), true);
  ASSERT_FALSE(status.ok());
  ASSERT_STR_CONTAINS(status.ToString(), "parsing key #0")
      << " Actual error: " << status.ToString();
  ASSERT_STR_CONTAINS(status.ToString(), "'n' and 'e' properties must be a non-empty string")
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtHS256) {
  // Cryptographic algorithm: HS256.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "Yx57JSBzhGFDgDj19CabRpH/+kiaKqI6UZI6lDunQKw=";
  TempTestDataFile jwks_file(
      Substitute(kJwksHsFileFormat, kKid1, "HS256", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS256.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS256")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs256(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtHS384) {
  // Cryptographic algorithm: HS384.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret =
      "TlqmKRc2PNQJXTC3Go7eAadwPxA7x9byyXCi5I8tSvxrE77tYbuF5pfZAyswrkou";
  TempTestDataFile jwks_file(
      Substitute(kJwksHsFileFormat, kKid1, "HS384", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS384.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS384")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs384(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtHS512) {
  // Cryptographic algorithm: HS512.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "ywc6DN7+iRw1E5HOqzvrsYodykSLFutT28KN3bJnLZcZpPCNjn0b6gbMfXPcxeY"
                         "VyuWWGDxh6gCDwPMejbuEEg==";
  TempTestDataFile jwks_file(
      Substitute(kJwksHsFileFormat, kKid1, "HS512", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS512.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS512")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs512(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS256) {
  // Cryptographic algorithm: RS256.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsaPubKeyPem, key1->get_key());

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  ASSERT_EQ(
      "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1mNzlkYTUwYj"
      "ViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoiaW1wYWxhIn0.OW5H2SClL"
      "lsotsCarTHYEbqlbRh43LFwOyo9WubpNTwE7hTuJDsnFoVrvHiWI02W69TZNat7DYcC86A_ogLMfNXagHj"
      "lMFJaRnvG5Ekag8NRuZNJmHVqfX-qr6x7_8mpOdU554kc200pqbpYLhhuK4Qf7oT7y9mOrtNrUKGDCZ0Q2"
      "y_mizlbY6SMg4RWqSz0RQwJbRgXIWSgcbZd0GbD_MQQ8x7WRE4nluU-5Fl4N2Wo8T9fNTuxALPiuVeIczO"
      "25b5n4fryfKasSgaZfmk0CoOJzqbtmQxqiK9QNSJAiH2kaqMwLNgAdgn8fbd-lB1RAEGeyPH8Px8ipqcKs"
      "Pk0bg",
      token);

  // Verify the JWT token with jwt-cpp APIs directly.
  auto jwt_decoded_token = jwt::decode(token);
  auto verifier = jwt::verify()
                      .allow_algorithm(jwt::algorithm::rs256(kRsaPubKeyPem, "", "", ""))
                      .with_issuer("auth0");
  verifier.verify(jwt_decoded_token);

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS384) {
  // Cryptographic algorithm: RS384.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS384",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS384", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsaPubKeyPem, key1->get_key());

  // Create a JWT token and sign it with RS384.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS384")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs384(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS512) {
  // Cryptographic algorithm: RS512.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS512",
      kRsa512PubKeyJwkN, kRsa512PubKeyJwkE, kKid2, "RS512",
      kRsa512InvalidPubKeyJwkN, kRsa512PubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa512PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with RS512.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs512(kRsa512PubKeyPem, kRsa512PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtPS256) {
  // Cryptographic algorithm: PS256.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "PS256",
      kRsa1024PubKeyJwkN, kRsa1024PubKeyJwkE, kKid2, "PS256",
      kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa1024PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with PS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS256")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::ps256(kRsa1024PubKeyPem, kRsa1024PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtPS384) {
  // Cryptographic algorithm: PS384.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "PS384",
      kRsa2048PubKeyJwkN, kRsa2048PubKeyJwkE, kKid2, "PS384",
      kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa2048PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with PS384.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS384")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::ps384(kRsa2048PubKeyPem, kRsa2048PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtPS512) {
  // Cryptographic algorithm: PS512.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "PS512",
      kRsa4096PubKeyJwkN, kRsa4096PubKeyJwkE, kKid2, "PS512",
      kRsaInvalidPubKeyJwkN, kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kRsa4096PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with PS512.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("PS512")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::ps512(kRsa4096PubKeyPem, kRsa4096PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtES256) {
  // Cryptographic algorithm: ES256.
  TempTestDataFile jwks_file(Substitute(kJwksEcFileFormat, kKid1, "P-256",
      kEcdsa256PubKeyJwkX, kEcdsa256PubKeyJwkY));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kEcdsa256PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with ES256.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES256")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::es256(
                       kEcdsa256PubKeyPem, kEcdsa256PrivKeyPem, "", ""));

  // Verify the JWT token with jwt-cpp APIs directly.
  auto jwt_decoded_token = jwt::decode(token);
  auto verifier =
      jwt::verify()
          .allow_algorithm(jwt::algorithm::es256(kEcdsa256PubKeyPem, "", "", ""))
          .with_issuer("auth0");
  verifier.verify(jwt_decoded_token);

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtES384) {
  // Cryptographic algorithm: ES384.
  TempTestDataFile jwks_file(Substitute(kJwksEcFileFormat, kKid1, "P-384",
      kEcdsa384PubKeyJwkX, kEcdsa384PubKeyJwkY));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kEcdsa384PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with ES384.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES384")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::es384(
                       kEcdsa384PubKeyPem, kEcdsa384PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtES512) {
  // Cryptographic algorithm: ES512.
  TempTestDataFile jwks_file(Substitute(kJwksEcFileFormat, kKid1, "P-521",
      kEcdsa521PubKeyJwkX, kEcdsa521PubKeyJwkY));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);
  JWKSSnapshotPtr jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(1, jwks->GetECPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupECPublicKey(kKid1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(kEcdsa521PubKeyPem, key1->get_key());

  // Create a JWT token and sign it with ES512.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("ES512")
                   .set_key_id(kKid1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::es512(
                       kEcdsa521PubKeyPem, kEcdsa521PrivKeyPem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtNotVerifySignature) {
  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));

  // Do not verify signature.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  Status status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtFailMismatchingAlgorithms) {
  // JWT algorithm is not matching with algorithm in JWK.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token, but set mismatching algorithm.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kKid1)
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  // Failed to verify the token due to mismatching algorithms.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find(
                  "JWT algorithm 'rs512' is not matching with JWK algorithm 'rs256'")
      != std::string::npos)
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtFailKeyNotFound) {
  // The key cannot be found in JWKS.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token with a key ID which can not be found in JWKS.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id("unfound-key-id")
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  // Failed to verify the token since key is not found in JWKS.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(
      status.ToString().find("Invalid JWK ID in the JWT token") != std::string::npos)
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  // Verify the token by trying each key in JWK set and there is one matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .sign(jwt::algorithm::rs512(kRsa512PubKeyPem, kRsa512PrivKeyPem, "", ""));
  // Verify the token by trying each key in JWK set, but there is no matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutSignature) {
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token without signature.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").sign(jwt::algorithm::none{});
  // Failed to verify the unsigned token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("Unsecured JWT") != std::string::npos)
      << " Actual error: " << status.ToString();
}

TEST(JwtUtilTest, VerifyJwtFailExpiredToken) {
  // Sign JWT token with RS256.
  TempTestDataFile jwks_file(Substitute(kJwksRsaFileFormat, kKid1, "RS256",
      kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
      kRsaPubKeyJwkE));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename(), true);
  EXPECT_OK(status);

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kKid1)
          .set_issued_at(std::chrono::system_clock::now())
          .set_expires_at(std::chrono::system_clock::now() - std::chrono::seconds{10})
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));

  // Verify the token, including expiring time.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.ToString().find("Verification failed, error: token expired")
      != std::string::npos)
      << " Actual error: " << status.ToString();
}

namespace {

void JWKSHandler(const Webserver::WebRequest& /*req*/,
                 Webserver::PrerenderedWebResponse* resp) {
  resp->output <<
      Substitute(kJwksRsaFileFormat, kKid1, "RS256",
          kRsaPubKeyJwkN, kRsaPubKeyJwkE, kKid2, "RS256", kRsaInvalidPubKeyJwkN,
          kRsaPubKeyJwkE);
  resp->status_code = HttpStatusCode::Ok;
}

class JWKSMockServer {
 public:
  Status Start() {
    WebserverOptions opts;
    opts.port = 0;
    opts.bind_interface = "127.0.0.1";
    webserver_.reset(new Webserver(opts));
    webserver_->RegisterPrerenderedPathHandler("/jwks", "JWKS", JWKSHandler,
                                               /*is_styled*/false, /*is_on_nav_bar*/false);
    RETURN_NOT_OK(webserver_->Start());
    vector<Sockaddr> addrs;
    RETURN_NOT_OK(webserver_->GetBoundAddresses(&addrs));
    url_ = Substitute("http://$0/jwks", addrs[0].ToString());
    return Status::OK();
  }

  const string& url() const {
    return url_;
  }
 private:
  unique_ptr<Webserver> webserver_;
  string url_;
};

} // anonymous namespace

TEST(JwtUtilTest, VerifyJWKSUrl) {
  JWKSMockServer jwks_server;
  ASSERT_OK(jwks_server.Start());

  JWTHelper jwt_helper;
  ASSERT_OK(jwt_helper.Init(jwks_server.url(), false));
  auto encoded_token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kKid1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(kRsaPubKeyPem, kRsaPrivKeyPem, "", ""));
  ASSERT_EQ(
      "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1mNzlkYTUwYj"
      "ViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoiaW1wYWxhIn0.OW5H2SClL"
      "lsotsCarTHYEbqlbRh43LFwOyo9WubpNTwE7hTuJDsnFoVrvHiWI02W69TZNat7DYcC86A_ogLMfNXagHj"
      "lMFJaRnvG5Ekag8NRuZNJmHVqfX-qr6x7_8mpOdU554kc200pqbpYLhhuK4Qf7oT7y9mOrtNrUKGDCZ0Q2"
      "y_mizlbY6SMg4RWqSz0RQwJbRgXIWSgcbZd0GbD_MQQ8x7WRE4nluU-5Fl4N2Wo8T9fNTuxALPiuVeIczO"
      "25b5n4fryfKasSgaZfmk0CoOJzqbtmQxqiK9QNSJAiH2kaqMwLNgAdgn8fbd-lB1RAEGeyPH8Px8ipqcKs"
      "Pk0bg",
      encoded_token);
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  ASSERT_OK(jwt_helper.Decode(encoded_token, decoded_token));
  ASSERT_OK(jwt_helper.Verify(decoded_token.get()));
}

} // namespace kudu
