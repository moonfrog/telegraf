package caching

import (
	"github.com/patrickmn/go-cache"
	"time"
)

// cache is an object for storing anything in a cache.
type Caching struct {
	cac *cache.Cache
}

// Create a cache with a default expiration time of 5 minutes, and which
// purges expired items every 30 seconds
func Newcache() *Caching {
	return &Caching{
		cac: cache.New(5*time.Minute, 30*time.Second),
	}
}

// set the cache key value.
func (c *Caching) SetWithExpiration(key, value string) {
	c.cac.Set(key, value, cache.DefaultExpiration)
}

// set the cache key value.
func (c *Caching) SetWithNoExpiration(key, value string) {
	c.cac.Set(key, value, cache.NoExpiration)
}

func (c *Caching) GetKey(key string) (string, bool) {
	value, found := c.cac.Get(key)
	if found {
		return value.(string), found
	}
	return "", found
}
