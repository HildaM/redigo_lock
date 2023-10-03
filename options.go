package redigo_lock

import "time"

const (
	// 默认连接池超过 10 s 释放连接
	DefaultIdleTimeoutSeconds = 10
	// 默认最大激活连接数
	DefaultMaxActive = 100
	// 默认最大空闲连接数
	DefaultMaxIdle = 20

	// 默认的分布式锁过期时间
	DefaultLockExpireSeconds = 30
	// 看门狗工作时间间隙
	WatchDogWorkStepSeconds = 10
)

// 相关配置信息
type ClientOptions struct {
	maxIdle            int
	idleTimeoutSeconds int
	maxActive          int
	wait               bool
	// 必填参数
	network  string
	address  string
	password string
}

// 定义处理ClientOptions的匿名函数
// 定义匿名函数方便后续进行“链式调用”
type ClientOption func(c *ClientOptions)

// setClientArgs 设置默认参数
func setClientArgs(c *ClientOptions) {
	if c.maxIdle < 0 {
		c.maxIdle = DefaultMaxIdle
	}
	if c.idleTimeoutSeconds < 0 {
		c.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}
	if c.maxActive < 0 {
		c.maxActive = DefaultMaxActive
	}
}

// 参数设置函数
func WithMaxIdle(maxIdle int) ClientOption {
	return func(c *ClientOptions) {
		c.maxIdle = maxIdle
	}
}
func WithIdleTimeoutSeconds(idleTimeoutSeconds int) ClientOption {
	return func(c *ClientOptions) {
		c.idleTimeoutSeconds = idleTimeoutSeconds
	}
}
func WithMaxActive(maxActive int) ClientOption {
	return func(c *ClientOptions) {
		c.maxActive = maxActive
	}
}
func WithWaitMode() ClientOption {
	return func(c *ClientOptions) {
		c.wait = true
	}
}

// LockOptions 分布式锁配置
type LockOptions struct {
	isBlock             bool
	blockWaitingSeconds int64
	expireSeconds       int64
	watchDogMode        bool
}

type LockOption func(*LockOptions)

func setLockOptions(lock *LockOptions) {
	if lock.isBlock && lock.blockWaitingSeconds <= 0 {
		// 默认阻塞等待时间上限是5秒
		lock.blockWaitingSeconds = 5
	}

	// 倘若未设置分布式锁的过期时间，则会启动 watchdog
	if lock.expireSeconds > 0 {
		return
	}

	// 用户未显式指定锁的过期时间，则此时会启动看门狗
	lock.expireSeconds = DefaultLockExpireSeconds
	lock.watchDogMode = true
}

// LockOptions 配置函数
func WithBlock() LockOption {
	return func(o *LockOptions) {
		o.isBlock = true
	}
}
func WithBlockWaitingSeconds(waitingSeconds int64) LockOption {
	return func(o *LockOptions) {
		o.blockWaitingSeconds = waitingSeconds
	}
}
func WithExpireSeconds(expireSeconds int64) LockOption {
	return func(o *LockOptions) {
		o.expireSeconds = expireSeconds
	}
}

// RedLockOptions 分布式锁实现
type RedLockOptions struct {
	singleNodesTimeout time.Duration
	expireDuration     time.Duration
}

type RedLockOption func(options *RedLockOptions)

func WithSingleNodesTimeout(singleNodesTimeout time.Duration) RedLockOption {
	return func(o *RedLockOptions) {
		o.singleNodesTimeout = singleNodesTimeout
	}
}

func WithRedLockExpireDuration(expireDuration time.Duration) RedLockOption {
	return func(o *RedLockOptions) {
		o.expireDuration = expireDuration
	}
}

func setRedLockOption(r *RedLockOptions) {
	if r.singleNodesTimeout <= 0 {
		r.singleNodesTimeout = DefaultSingleLockTimeout
	}
}

type SingleNodeConf struct {
	Network  string
	Address  string
	Password string
	Opts     []ClientOption
}
