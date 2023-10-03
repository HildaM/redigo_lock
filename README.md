# redigo_lock
## 简介
基于 redigo，使用golang实现的 redis 分布式锁

## 项目结构
- option.go: redis相关配置文件的封装
- redis.go: redigo sdk的封装，方便lock.go和redlock.go的使用
- lock.go: 分布式锁的实现