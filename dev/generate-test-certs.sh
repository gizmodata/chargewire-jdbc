#!/usr/bin/env bash
# Regenerates the test certificates in src/test/resources/certs (valid for 100 years).
# They secure nothing: the keys are public, and only the tests trust this CA.
set -euo pipefail
cd "$(dirname "$0")/../src/test/resources/certs"
openssl version | grep -q "^OpenSSL 3" || { echo "needs OpenSSL 3" >&2; exit 1; }
days=36500

openssl req -x509 -newkey rsa:2048 -nodes -days "$days" -subj "/CN=warp-sql-jdbc test CA" \
  -keyout ca-key.pem -out ca.pem
openssl req -x509 -newkey rsa:2048 -nodes -days "$days" -subj "/CN=untrusted test CA" \
  -keyout other-ca-key.pem -out other-ca.pem
rm other-ca-key.pem

openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server-key.pem -out server.csr
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days "$days" \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth") \
  -out server.pem

openssl req -newkey rsa:2048 -nodes -subj "/CN=wrong.example" -keyout wrong-host-key.pem \
  -out wrong-host.csr
openssl x509 -req -in wrong-host.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days "$days" \
  -extfile <(printf "subjectAltName=DNS:wrong.example\nextendedKeyUsage=serverAuth") \
  -out wrong-host.pem

openssl req -newkey rsa:2048 -nodes -subj "/CN=warp" -keyout client-key.pem -out client.csr
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days "$days" \
  -extfile <(printf "extendedKeyUsage=clientAuth") -out client.pem
openssl pkey -in client-key.pem -traditional -out client-key-pkcs1.pem
openssl ecparam -name prime256v1 -genkey -noout -out ec-key-sec1.pem
openssl pkcs8 -topk8 -nocrypt -in ec-key-sec1.pem -out ec-key-pkcs8.pem

rm -f server.csr wrong-host.csr client.csr ca.srl ca-key.pem
rm -f truststore.p12
keytool -importcert -noprompt -alias test-ca -file ca.pem -keystore truststore.p12 \
  -storetype PKCS12 -storepass changeit
