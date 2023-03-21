package rsa

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"github.com/opentracing/opentracing-go/log"
)

// GeneratePrivateKey Generate PrivateKey
func GeneratePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

// VerifyRsaSign Verify Rsa Sign
func VerifyRsaSign(publicKey *rsa.PublicKey, sign []byte, content string) error {
	hash := sha256.New()
	_, err := hash.Write([]byte(content))
	if err != nil {
		return err
	}

	hashSum := hash.Sum(nil)

	err = rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hashSum, sign)
	if err != nil {
		return err
	}
	return nil
}

// RsaSign Rsa Sign
func RsaSign(privateKey *rsa.PrivateKey, content string) ([]byte, error) {
	msgHash := sha256.New()
	_, err := msgHash.Write([]byte(content))
	if err != nil {
		return nil, err
	}
	msgHashSum := msgHash.Sum(nil)

	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, msgHashSum)
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

// EncryptWithPublicKey encrypts data with public key
func EncryptWithPublicKey(msg []byte, pub *rsa.PublicKey) []byte {
	hash := sha512.New()
	ciphertext, err := rsa.EncryptOAEP(hash, rand.Reader, pub, msg, nil)
	if err != nil {
		log.Error(err)
	}
	return ciphertext
}

// DecryptWithPrivateKey decrypts data with private key
func DecryptWithPrivateKey(ciphertext []byte, priv *rsa.PrivateKey) []byte {
	hash := sha512.New()
	plaintext, err := rsa.DecryptOAEP(hash, rand.Reader, priv, ciphertext, nil)
	if err != nil {
		log.Error(err)
	}
	return plaintext
}
