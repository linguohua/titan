package token

import (
	"github.com/golang-jwt/jwt"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("p2p")

func GenerateToken(key string, expireAt int64) (string, error) {
	claims := &jwt.StandardClaims{}
	claims.ExpiresAt = expireAt

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString([]byte(key))
	if err != nil {
		return "", err
	}

	return signedToken, nil
}

func parseToken(signedToken, key string) (*jwt.Token, error) {
	claims := &jwt.StandardClaims{}

	token, err := jwt.ParseWithClaims(signedToken, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(key), nil
	})

	if err != nil {
		return nil, err
	}

	return token, nil
}

func ValidToken(signedToken, key string) bool {
	token, err := parseToken(signedToken, key)
	if err != nil {
		log.Infof("ValidToken failed:%v", err)
		return false
	}

	return token.Valid
}
