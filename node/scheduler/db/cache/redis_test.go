package cache

import "testing"

func TestRedisDB_GetValidatorsWithList(t *testing.T) {
	err := InitRedis("127.0.0.1:6379")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	list, err := GetValidatorsWithList()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	t.Log(list)
}
