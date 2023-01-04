package repository

import (
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
)

type RedisHealth struct {
	db redis.UniversalClient
}

func NewRedisHealth(db redis.UniversalClient) *RedisHealth {
	return &RedisHealth{db: db}
}

func (r *RedisHealth) Check() error {
	_, err := r.db.Ping().Result()
	return errors.WithStack(err)
}
