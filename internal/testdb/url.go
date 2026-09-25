package testdb

import (
	"net/url"
	"os"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// dbURLWithName 把 postgres://.../oldname?params 形式连接串中的库名替换掉。
func dbURLWithName(raw, name string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}
