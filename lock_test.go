package redigo_lock

import (
	"context"
	"errors"
	"sync"
	"testing"
)

const (
	addr   = "ip:port"
	passwd = "password"
)

func Test_tryGetBlockingLock(t *testing.T) {
	client := NewClient("tcp", addr, passwd)
	lock1 := NewRedisLock("test_key", client, WithExpireSeconds(1))
	lock2 := NewRedisLock("test_key", client, WithBlock(), WithBlockWaitingSeconds(2))

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := lock1.Lock(ctx); err != nil {
			t.Error(err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		//time.Sleep(10)
		if err := lock2.Lock(ctx); err != nil {
			t.Error(err)
			return
		}
	}()

	wg.Wait()

	t.Log("success")
}

func Test_nonblockingLock(t *testing.T) {
	client := NewClient("tcp", addr, passwd)
	lock1 := NewRedisLock("test_key", client, WithExpireSeconds(1))
	lock2 := NewRedisLock("test_key", client)

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := lock1.Lock(ctx); err != nil {
			t.Error(err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := lock2.Lock(ctx); err == nil || !errors.Is(err, ErrLockAcquiredByOthers) {
			t.Errorf("got err: %v, expect: %v", err, ErrLockAcquiredByOthers)
			return
		}
	}()

	wg.Wait()
	t.Log("success")
}
