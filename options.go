package redigo_lock

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
