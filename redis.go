package redigo_lock

import (
	"context"
	"errors"
	"github.com/gomodule/redigo/redis"
	"strings"
	"time"
)

// LockClient 对外暴露的接口
// SetNEX 方法，语义是 set with expire time only if key not exist. 用于支持分布式锁的加锁操作
// Eval 方法，用以执行 lua 脚本，后续用来支持分布式锁的解锁操作
type LockClient interface {
	SetNX(ctx context.Context, key, value string, expireSeconds int64) (int64, error)
	Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error)
}

// Client Redis客户端抽象
type Client struct {
	ClientOptions
	pool *redis.Pool
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	client := Client{
		ClientOptions: ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}

	// 将额外参数注入到ClientOption中
	for _, opt := range opts {
		opt(&client.ClientOptions)
	}

	setClientArgs(&client.ClientOptions)

	pool := client.getRedisPool()
	return &Client{
		pool: pool,
	}
}

// getRedisPool 获取连接池
func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:     c.maxIdle,
		MaxActive:   c.maxActive,
		IdleTimeout: time.Duration(c.idleTimeoutSeconds) * time.Second,
		Wait:        c.wait,
	}
}

func (c *Client) getRedisConn() (redis.Conn, error) {
	if c.address == "" {
		panic("ERROR: Empty redis address!")
	}

	// 配置redis连接参数
	var opts []redis.DialOption
	if len(c.password) > 0 {
		opts = append(opts, redis.DialPassword(c.password))
	}

	conn, err := redis.DialContext(context.Background(), c.network, c.address, opts...)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (c *Client) SetNX(ctx context.Context, key, value string, expireSeconds int64) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("ERROR: redis key or value may be empty!")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	reply, err := conn.Do("SET", key, value, "NX")
	if err != nil {
		return -1, err
	}

	if resp, ok := reply.(string); ok && strings.ToLower(resp) == "ok" {
		return 1, nil
	}

	// 存在空指针异常：https://github.com/xiaoxuxiansheng/redis_lock/issues/2
	r, _ := reply.(int64)
	return r, err
}

func (c *Client) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	// Redis Eval 命令使用 Lua 解释器执行脚本
	return conn.Do("EVAL", args...)
}

// 其他命令的实现
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", errors.New("ERROR: redis GET key can't be empty!")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	return redis.String(conn.Do("GET", key))
}

func (c *Client) Set(ctx context.Context, key, value string) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("ERROR: redis SET key or value can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	resp, err := conn.Do("SET", key, value)
	if err != nil {
		return -1, err
	}

	if respStr, ok := resp.(string); ok && strings.ToLower(respStr) == "ok" {
		return 1, nil
	}

	r, _ := resp.(int64)
	return r, err
}

func (c *Client) SetNEX(ctx context.Context, key, value string, expireSeconds int64) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("ERROR: redis SET keyNX or value can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	reply, err := conn.Do("SET", key, value, "EX", expireSeconds, "NX")
	if err != nil {
		return -1, err
	}

	if respStr, ok := reply.(string); ok && strings.ToLower(respStr) == "ok" {
		return 1, nil
	}

	r, _ := reply.(int64)
	return r, err
}

func (c *Client) Del(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("ERROR: redis DEL key can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("DEL", key)
	return err
}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return -1, errors.New("ERROR: redis INCR key can't be empty")
	}

	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	return redis.Int64(conn.Do("INCR", key))
}
