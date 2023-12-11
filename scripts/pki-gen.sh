#!/bin/bash -x

# 集群由多个节点注册，这里假设3个节点，
export NAME1=etcd01
export ADDRESS1=192.168.0.55

export NAME2=etcd02
export ADDRESS2=192.168.0.55

export NAME3=etcd03
export ADDRESS3=192.168.0.55

days=3650

cat > openssl.conf << EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[ v3_req ]
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = $NAME1
DNS.2 = $NAME2
DNS.3 = $NAME3
IP.1 = 127.0.0.1
IP.2 = $ADDRESS1
IP.3 = $ADDRESS2
IP.4 = $ADDRESS3
EOF


# 准备 CA 证书
[ -f ca.key ] || openssl genrsa -out ca.key 2048
[ -f ca.crt ] || openssl req -x509 -new -nodes -key ca.key -subj "/CN=etcd-ca" -days ${days} -out ca.crt

# 创建 etcd client 证书
[ -f client.key ] || openssl genrsa -out client.key 2048
[ -f client.csr ] || openssl req -new -key client.key -subj "/CN=etcd-client" -out client.csr -config openssl.conf
[ -f client.crt ] || openssl x509 -req -sha256 -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days ${days} -extensions v3_req  -extfile openssl.conf

# 创建 etcd 集群 peer 间证书
[ -f peer.key ] || openssl genrsa -out peer.key 2048
[ -f peer.csr ] || openssl req -new -key peer.key -subj "/CN=etcd-peer" -out peer.csr -config openssl.conf
[ -f peer.crt ] || openssl x509 -req -sha256 -in peer.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out peer.crt -days ${days} -extensions v3_req  -extfile openssl.conf
