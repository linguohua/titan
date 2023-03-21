package rsa

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"hash"
)

type Rsa struct {
	code crypto.Hash
	hash hash.Hash
}

func New(code crypto.Hash, hash hash.Hash) *Rsa {
	return &Rsa{code, hash}
}

// GeneratePrivateKey Generate PrivateKey
func GeneratePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

// VerifyRsaSign Verify Rsa Sign
func (r *Rsa) VerifySign(publicKey *rsa.PublicKey, sign []byte, content []byte) error {
	r.hash.Write(content)
	hashSum := r.hash.Sum(nil)
	r.hash.Reset()

	err := rsa.VerifyPKCS1v15(publicKey, r.code, hashSum, sign)
	if err != nil {
		return err
	}
	return nil
}

// RsaSign Rsa Sign
func (r *Rsa) Sign(privateKey *rsa.PrivateKey, content []byte) ([]byte, error) {
	r.hash.Write(content)
	sum := r.hash.Sum(nil)
	r.hash.Reset()

	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, r.code, sum)
	if err != nil {
		return nil, err
	}
	return signature, nil
}

// Pem2PublicKey  Pem to PublicKey
func Pem2PublicKey(publicPem []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(publicPem)
	if block == nil {
		return nil, fmt.Errorf("failed to decode public key")
	}

	pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key, %s", err.Error())
	}

	return pub, nil
}

// Pem2PrivateKey Pem to PrivateKey
func Pem2PrivateKey(privateKeyStr []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(privateKeyStr)
	if block == nil {
		return nil, fmt.Errorf("failed to decode private key")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key")
	}

	return privateKey, nil
}

// PrivateKey2Pem PrivateKey to Pem
func PrivateKey2Pem(privateKey *rsa.PrivateKey) []byte {
	if privateKey == nil {
		return nil
	}

	private := x509.MarshalPKCS1PrivateKey(privateKey)

	privateKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: private,
		},
	)

	return privateKeyBytes
}

// PublicKey2Pem PublicKey to Pem
func PublicKey2Pem(publicKey *rsa.PublicKey) []byte {
	if publicKey == nil {
		return nil
	}

	public := x509.MarshalPKCS1PublicKey(publicKey)

	publicKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PUBLIC KEY",
			Bytes: public,
		},
	)

	return publicKeyBytes
}

// Encrypt encrypts data with public key
func (r *Rsa) Encrypt(msg []byte, pub *rsa.PublicKey) ([]byte, error) {
	ciphertext, err := rsa.EncryptOAEP(r.hash, rand.Reader, pub, msg, nil)
	if err != nil {
		return nil, err
	}
	return ciphertext, nil
}

// Decrypt decrypts data with private key
func (r *Rsa) Decrypt(ciphertext []byte, priv *rsa.PrivateKey) ([]byte, error) {
	plaintext, err := rsa.DecryptOAEP(r.hash, rand.Reader, priv, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
