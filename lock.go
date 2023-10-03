package redigo_lock

import (
	"context"
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"redigo_lock/utils"
	"sync/atomic"
	"time"
)

const RedisLockKeyPrefix = "REDIS_LOCK_PREFIX_"

var ErrLockAcquiredByOthers = errors.New("This lock is acquired by others")
var ErrNil = redis.ErrNil

// IsRetryableErr 不可重入错误
func IsRetryableErr(err error) bool {
	return errors.Is(err, ErrLockAcquiredByOthers)
}

type RedisLock struct {
	LockOptions
	key    string
	token  string
	client LockClient

	// watchdog标志
	runningDog int32
	// watchdog终止
	stopDog context.CancelFunc
}

func (r *RedisLock) getLockKey() string {
	return RedisLockKeyPrefix + r.key
}

func NewRedisLock(key string, client LockClient, opts ...LockOption) *RedisLock {
	lock := RedisLock{
		key:    key,
		token:  utils.GetProcessAndGoroutineIDStr(),
		client: client,
	}

	for _, opt := range opts {
		opt(&lock.LockOptions)
	}

	setLockOptions(&lock.LockOptions)
	return &lock
}

// Lock 加锁
func (r *RedisLock) Lock(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			return
		}
		// 加锁成功的情况下，会启动看门狗
		// 关于该锁本身是不可重入的，所以不会出现同一把锁下看门狗重复启动的情况
		r.watchDog(ctx)
	}()

	// 先获取一次锁，然后再分”阻塞模式“和”非阻塞模式“分别处理
	err = r.tryLock(ctx)
	if err == nil {
		return nil
	}

	// 1. 非阻塞模式获取锁失败直接返回
	if !r.isBlock {
		return err
	}
	// 2. 判断是否允许重试
	if !IsRetryableErr(err) {
		return err
	}

	// 3. 阻塞模式下，继续轮询获取锁
	err = r.tryGetBlockingLock(ctx)
	return
}

func (r *RedisLock) tryLock(ctx context.Context) error {
	reply, err := r.client.SetNEX(ctx, r.getLockKey(), r.token, r.expireSeconds)
	if err != nil {
		return err
	}
	if reply != 1 {
		return fmt.Errorf("reply: %d, err: %w", reply, ErrLockAcquiredByOthers)
	}

	return nil
}

// watchDog 启动看门狗
func (r *RedisLock) watchDog(ctx context.Context) {
	if !r.watchDogMode {
		return
	}

	// 确保之前启动的看门狗已经正常回收
	for !atomic.CompareAndSwapInt32(&r.runningDog, 0, 1) {
		// CAS空转
	}

	// 启动看门狗
	ctx, r.stopDog = context.WithCancel(ctx)
	go func() {
		defer func() {
			atomic.StoreInt32(&r.runningDog, 0)
		}()
		r.runWatchDog(ctx)
	}()
}

func (r *RedisLock) runWatchDog(ctx context.Context) {
	ticker := time.NewTicker(WatchDogWorkStepSeconds * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			return
		default:

		}

		// 看门狗负责在用户未显示解锁时，持续为分布式锁进行续约
		// 通过 lua 脚本，延期之前会确保保证锁仍然属于自己
		// 为避免因为网络延迟而导致锁被提前释放的问题，watch dog 续约时需要把锁的过期时长额外增加 5 s
		_ = r.DelayExpire(ctx, WatchDogWorkStepSeconds+5)
	}
}

// DelayExpire 延长锁的过期实践，通过lua脚本实现操作原子性
func (r *RedisLock) DelayExpire(ctx context.Context, expireSeconds int64) error {
	keysAndArgs := []interface{}{
		r.getLockKey(),
		r.token,
		expireSeconds,
	}
	// 执行lua脚本
	reply, err := r.client.Eval(ctx, LuaCheckAndExpireDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if code, _ := reply.(int64); code != 1 {
		return errors.New("ERROR: Can not expire lock without ownership of lock")
	}

	return nil
}

func (r *RedisLock) tryGetBlockingLock(ctx context.Context) error {
	// 阻塞模式等待时间上限
	timeoutCh := time.After(time.Duration(r.blockWaitingSeconds) * time.Second)
	// 轮询ticker，每隔50ms尝试获取锁一次
	ticker := time.NewTicker(time.Duration(50) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		// ctx终止
		case <-ctx.Done():
			return fmt.Errorf("ERROR: Lock failed, ctx timeout, err: %w", ctx.Err())
		case <-timeoutCh:
			return fmt.Errorf("ERROR: Block waiting time out, err: %w", ErrLockAcquiredByOthers)
		default:
			// 放行
		}

		err := r.tryLock(ctx)
		if err == nil {
			// 加锁成功
			return nil
		}

		// 不可重试类型错误，直接返回
		if !IsRetryableErr(err) {
			return err
		}
	}

	// 不可达
	return nil
}

func (r *RedisLock) Unlock(ctx context.Context) error {
	defer func() {
		if r.stopDog != nil {
			r.stopDog()
		}
	}()

	keysAndArgs := []interface{}{
		r.getLockKey(),
		r.token,
	}
	reply, err := r.client.Eval(ctx, LuaCheckAndDeleteDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}

	if code, _ := reply.(int64); code != 1 {
		return errors.New("ERROR: Can not unlock without ownership of lock")
	}

	return nil
}
