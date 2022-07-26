package rsa

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
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
		return nil, nil
	}
	msgHashSum := msgHash.Sum(nil)

	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, msgHashSum)
	if err != nil {
		return nil, err
	}
	return signature, nil
}

// Pem2PublicKey  Pem to PublicKey
func Pem2PublicKey(publicPem string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(publicPem))
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
func Pem2PrivateKey(privateKeyStr string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(privateKeyStr))
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
func PrivateKey2Pem(privateKey *rsa.PrivateKey) string {
	if privateKey == nil {
		return ""
	}

	private := x509.MarshalPKCS1PrivateKey(privateKey)

	privateKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: private,
		},
	)

	return string(privateKeyBytes)
}

// PublicKey2Pem PublicKey to Pem
func PublicKey2Pem(publicKey *rsa.PublicKey) string {
	if publicKey == nil {
		return ""
	}

	public := x509.MarshalPKCS1PublicKey(publicKey)

	publicKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PUBLIC KEY",
			Bytes: public,
		},
	)

	return string(publicKeyBytes)
}
