package scheduler

import (
	"github.com/dgrijalva/jwt-go"
)

// nowDate = time.Now().Format("2006-01-02 15")
// secret  = fmt.Sprintf("%v%v", nowDate, "xxxx")

var secretKey = "00112233"

func generateToken(deviceID, secret string) (string, error) {
	dict := make(jwt.MapClaims)
	dict["deviceID"] = deviceID
	// dict["secret"] = secret

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, dict)
	return token.SignedString([]byte(secretKey))
}

func parseToken(token string) (string, string, error) {
	claim, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
		return []byte(secretKey), nil
	})
	if err != nil {
		return "", "", err
	}

	deviceID := claim.Claims.(jwt.MapClaims)["deviceID"].(string)
	secret := claim.Claims.(jwt.MapClaims)["secret"].(string)

	return deviceID, secret, nil
}
