package scheduler

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

func verifyRsaSign(publicKeyStr, sign, digest string) (bool, error) {
	publicKey, err := pem2PublicKey(publicKeyStr)
	if err != nil {
		return false, err
	}

	err = rsa.VerifyPSS(publicKey, crypto.SHA256, []byte(digest), []byte(sign), nil)
	if err != nil {
		fmt.Println("could not verify signature: ", err)
		return false, err
	}
	return true, nil
}

func rsaSign(privateKeyStr, digest string) (string, error) {
	privateKey, err := pem2PrivateKey(privateKeyStr)
	if err != nil {
		return "", nil
	}

	signature, err := rsa.SignPSS(rand.Reader, privateKey, crypto.SHA256, []byte(digest), nil)
	if err != nil {
		return "", err
	}
	return string(signature), nil
}

func pem2PublicKey(publicKeyStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(publicKeyStr))
	if block == nil {
		return nil, fmt.Errorf("failed to decode public key")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key")
	}

	return pub.(*rsa.PublicKey), nil
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
