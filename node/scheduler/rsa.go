package scheduler

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

func generatePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func verifyRsaSign(publicKey *rsa.PublicKey, sign []byte, content string) (bool, error) {
	hash := sha256.New()
	_, err := hash.Write([]byte(content))
	if err != nil {
		return false, err
	}

	hashSum := hash.Sum(nil)

	err = rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hashSum, sign)
	if err != nil {
		return false, err
	}
	return true, nil
}

func rsaSign(privateKey *rsa.PrivateKey, digest string) ([]byte, error) {
	msgHash := sha256.New()
	_, err := msgHash.Write([]byte(digest))
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

func pem2PublicKey(publicPem string) (*rsa.PublicKey, error) {
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

func pem2PrivateKey(privateKeyStr string) (*rsa.PrivateKey, error) {
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

func privateKey2Pem(privateKey *rsa.PrivateKey) string {
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

func publicKey2Pem(publicKey *rsa.PublicKey) string {
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
