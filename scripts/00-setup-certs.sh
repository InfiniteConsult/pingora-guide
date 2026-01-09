#!/usr/bin/env bash
set -e

# Configuration
KEYS_DIR="$(pwd)/conf/keys"
mkdir -p "$KEYS_DIR"

echo "🔐 Generating Certificates for Pingora City..."

# --- 1. Root CA (The Trust Anchor) ---
if [ ! -f "$KEYS_DIR/ca.crt" ]; then
    echo "--- Creating Root CA ---"
    openssl genrsa -out "$KEYS_DIR/ca.key" 2048
    openssl req -x509 -new -nodes -key "$KEYS_DIR/ca.key" \
        -sha256 -days 3650 -out "$KEYS_DIR/ca.crt" \
        -subj "/C=US/ST=PingoraCity/O=CityGov/CN=Pingora Root CA"
else
    echo "--- Root CA exists ---"
fi

# --- 2. Pingora Server Cert (Wildcard) ---
# Used by your Rust examples to serve traffic to Clients 1 & 2.
if [ ! -f "$KEYS_DIR/server.crt" ]; then
    echo "--- Creating Server Certificates (*.pingora.local) ---"
    openssl genrsa -out "$KEYS_DIR/server.key" 2048

    cat > "$KEYS_DIR/server.conf" <<EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = req_ext

[dn]
C=US
ST=PingoraCity
O=Pingora Proxy
CN=*.pingora.local

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = *.pingora.local
DNS.2 = dev.pingora.local
DNS.3 = localhost
IP.1 = 127.0.0.1
IP.2 = 172.28.0.10
EOF

    openssl req -new -key "$KEYS_DIR/server.key" -out "$KEYS_DIR/server.csr" -config "$KEYS_DIR/server.conf"
    openssl x509 -req -in "$KEYS_DIR/server.csr" \
        -CA "$KEYS_DIR/ca.crt" -CAkey "$KEYS_DIR/ca.key" \
        -CAcreateserial -out "$KEYS_DIR/server.crt" \
        -days 365 -sha256 -extfile "$KEYS_DIR/server.conf" -extensions req_ext

    rm "$KEYS_DIR/server.csr" "$KEYS_DIR/server.conf"
fi

# --- 3. Upstream Cert (For Nginx) ---
# Used by Nginx to prove its identity to Pingora (Upstream TLS).
if [ ! -f "$KEYS_DIR/upstream.crt" ]; then
    echo "--- Creating Upstream Certificates (advanced.pingora.local) ---"
    openssl genrsa -out "$KEYS_DIR/upstream.key" 2048

    cat > "$KEYS_DIR/upstream.conf" <<EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = req_ext

[dn]
C=US
ST=PingoraCity
O=Upstream Corp
CN=advanced.pingora.local

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = advanced.pingora.local
IP.1 = 172.28.0.22
EOF

    openssl req -new -key "$KEYS_DIR/upstream.key" -out "$KEYS_DIR/upstream.csr" -config "$KEYS_DIR/upstream.conf"
    openssl x509 -req -in "$KEYS_DIR/upstream.csr" \
        -CA "$KEYS_DIR/ca.crt" -CAkey "$KEYS_DIR/ca.key" \
        -CAcreateserial -out "$KEYS_DIR/upstream.crt" \
        -days 365 -sha256 -extfile "$KEYS_DIR/upstream.conf" -extensions req_ext

    rm "$KEYS_DIR/upstream.csr" "$KEYS_DIR/upstream.conf"
fi

# --- 4. Client Cert (For mTLS) ---
# Used by Pingora to prove its identity to Nginx (mTLS).
if [ ! -f "$KEYS_DIR/client.crt" ]; then
    echo "--- Creating Client Certificate (PingoraClient) ---"
    openssl genrsa -out "$KEYS_DIR/client.key" 2048
    openssl req -new -key "$KEYS_DIR/client.key" -out "$KEYS_DIR/client.csr" \
        -subj "/C=US/ST=PingoraCity/O=Pingora Proxy/CN=PingoraClient"

    openssl x509 -req -in "$KEYS_DIR/client.csr" \
        -CA "$KEYS_DIR/ca.crt" -CAkey "$KEYS_DIR/ca.key" \
        -CAcreateserial -out "$KEYS_DIR/client.crt" \
        -days 365 -sha256

    rm "$KEYS_DIR/client.csr"
fi

echo "✅ All certificates generated in conf/keys/"