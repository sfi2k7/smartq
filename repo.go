package smartq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var ErrChannelPaused = errors.New("channel is paused")

func newconn() *redis.Client {
tryagain:

	newconn := redis.NewClient(&redis.Options{
		Addr:     redisurl,
		Password: redispassword, // no password set
		DB:       0,             // use default DB
	})

	if err := newconn.Ping(context.Background()).Err(); err != nil {
		time.Sleep(1 * time.Second)
		goto tryagain
	}

	return newconn
}

func newrepo() *repo {
	return &repo{
		conn:    newconn(),
		timeout: time.Second,
	}
}

var cachedrepo *repo

func getcachedrepo() *repo {
	if cachedrepo == nil {
		cachedrepo = newrepo()
	}
	return cachedrepo
}

type repo struct {
	conn    *redis.Client
	timeout time.Duration
}

func (r *repo) Close() error {
	if r.conn == nil {
		return nil
	}

	if err := r.conn.Close(); err != nil {
		return err
	}

	return nil
}

func (r *repo) R() *redis.Client {
	if err := r.conn.Ping(context.Background()).Err(); err != nil {
		return newconn()
	}

	return r.conn
}

func (r *repo) loadobjectfromhash(key string) map[string]string {
	c := r.R()
	if c == nil {
		return nil
	}

	if v, err := c.HGetAll(context.Background(), key).Result(); err != nil {
		return nil
	} else {
		return v
	}
}

func (r *repo) sethash(key string, keyvals ...any) error {

	c := r.R()
	if c == nil {
		return errors.New("redis is nil. How?")
	}

	if err := c.HSet(context.Background(), key, keyvals...).Err(); err != nil {
		return err
	}

	return nil
}

func (r *repo) pushzset(key, id string) error {
	c := r.R()
	if c == nil {
		return errors.New("redis is nil. How?")
	}

	if err := c.ZAdd(context.Background(), key, &redis.Z{Member: id, Score: 9}).Err(); err != nil {
		return err
	}

	return nil
}

func (r *repo) rmzset(key string, id string) error {
	c := r.R()
	if r == nil {
		return errors.New("connection to redis is nil: how?")
	}

	_, err := c.ZRem(context.Background(), key, id).Result()

	return err
}

func (r *repo) popzset(key string) (string, error) {
	c := r.R()
	if r == nil {
		fmt.Println("Error in R", r)
		return "", errors.New("connection to redis is nil: how?")
	}

	z, err := c.BZPopMin(context.Background(), r.timeout, key).Result()
	if err != nil {
		return "", err
	}

	return z.Member.(string), nil
}

func (r *repo) ensurechannelstatus(channel string) error {
	c := r.R()
	if r == nil {
		fmt.Println("Error in R", r)
		return errors.New("connection to redis is nil: how?")
	}

	exists, err := c.Exists(context.Background(), channelStatusKey(channel)).Result()
	if err != nil {
		fmt.Println("call to exists", err)
		return err
	}

	// fmt.Println("exists", exists, err)

	if exists == 1 {
		return nil
	}

	return c.HSet(context.Background(), channelStatusKey(channel), "name", channel, "appended", "0", "routed", "0", "created", fmt.Sprint(time.Now().Unix()), "is_paused", "false").Err()
}

func (r *repo) tranx(fn func(redis.Pipeliner) error) error {
	c := r.R()
	if c == nil {
		return errors.New("connection to redis is nil: how?")
	}

	pipe := c.Pipeline()
	defer pipe.Exec(context.Background())

	if err := fn(pipe); err != nil {
		return err
	}

	_, err := pipe.Exec(context.Background())

	return err
}

func (r *repo) popfromchannel(channel, workingset string, count int) ([]string, error) {
	channelstatus := r.loadobjectfromhash(channelStatusKey(channel))
	if channelstatus == nil {
		return nil, errors.New("channel does not exist")
	}

	ispaused, ok := channelstatus["is_paused"]
	if !ok {
		return nil, errors.New("channel status is missing")
	}

	if ispaused == "true" {
		return nil, ErrChannelPaused
	}

	c := r.R()
	if c == nil {
		return nil, errors.New("connection to redis is nil: how?")
	}

	//TODO: Pull items from workingset first - in case process is crashed and now resumes

	items, err := c.ZPopMin(context.Background(), channelKey(channel), int64(count)).Result()
	if err != nil {
		return nil, errors.New("no items in channel")
	}

	var ids []string
	for _, item := range items {
		id := item.Member.(string)
		c.RPush(context.Background(), workingset, id)
		ids = append(ids, id)
	}

	return ids, nil
}

func (r *repo) addtochannel(id, channel string) error {

	c := r.R()
	if c == nil {
		return errors.New("connection to redis is nil: how?")
	}

	channelkey := channelKey(channel)
	jobkey := jobKey(id)

	r.tranx(func(pipe redis.Pipeliner) error {
		pipe.HSet(context.Background(), jobkey, "id", id, "channel", channel, "created", fmt.Sprint(time.Now().Unix()))
		pipe.ZAdd(context.Background(), channelkey, &redis.Z{Member: id, Score: 9})
		pipe.ZAdd(context.Background(), channelsKey, &redis.Z{Member: channel, Score: 9})
		pipe.HIncrBy(context.Background(), channelStatusKey(channel), "appended", 1)
		return nil
	})

	return nil
}

func (r *repo) deletejob(id string) error {
	r.tranx(func(pipe redis.Pipeliner) error {
		pipe.Del(context.Background(), jobKey(id))
		pipe.RPush(context.Background(), storeKey, icc(id, "", "delete"))
		return nil
	})

	return nil
}

func (r *repo) checkzhasmemeber(key, member string) bool {
	c := r.R()
	if c == nil {
		return false
	}

	return c.ZScore(context.Background(), key, member).Err() == nil
}

func (r *repo) routetochannel(id, channel string, hasChanges bool) error {
	c := r.R()
	if c == nil {
		return errors.New("connection to redis is nil: how?")
	}

	return r.tranx(func(pipe redis.Pipeliner) error {
		if hasChanges {
			//send to store
			pipe.RPush(context.Background(), storeKey, icc(id, channel, "sync"))
		} else {
			//route job
			pipe.ZAdd(context.Background(), channelKey(channel), &redis.Z{Member: id, Score: 9}).Err()
		}

		//set current job status to be in target channel
		pipe.HIncrBy(context.Background(), channelStatusKey(channel), "routed", 1)
		return nil
	})
}

// func (r *repo) hinc(key, field string) error {
// 	c := r.R()
// 	if r == nil {
// 		fmt.Println("Error in R", r)
// 		return errors.New("connection to redis is nil: how?")
// 	}

// 	// fmt.Println("adding 1 to ", field, "inside key", key)
// 	return c.HIncrBy(context.Background(), key, field, 1).Err()
// }

func (r *repo) hget(key, field string) string {
	c := r.R()
	if c == nil {
		fmt.Println("Error in R", r)
		return ""
	}

	return c.HGet(context.Background(), key, field).Val()
}

func (r *repo) deletekey(key string) error {
	c := r.R()
	if c == nil {
		return errors.New("connection to redis is nil: how?")
	}

	if err := c.Del(context.Background(), key).Err(); err != nil {
		return err
	}

	return nil
}

func (r *repo) deletelkey(key, id string) error {
	c := r.R()
	if r == nil {
		return errors.New("connection to redis is nil: how?")
	}

	if err := c.LRem(context.Background(), key, 0, id).Err(); err != nil {
		return err
	}

	return nil
}

func (r *repo) pushlist(key, value string) error {
	c := r.R()
	if r == nil {
		return errors.New("connection to redis is nil: how?")
	}

	err := c.RPush(context.Background(), key, value).Err()
	return err
}

func (r *repo) popfromlist(key, workingset string, count int) ([]string, error) {
	var items []string
	c := r.R()
	if r == nil {
		return items, nil
	}

	var err error
	for range count {
		var item string

		if len(workingset) > 0 {
			item, err = c.LMove(context.Background(), key, workingset, "left", "right").Result()
		} else {
			item, err = c.LPop(context.Background(), key).Result()
		}

		if err != nil {
			return items, nil
		}

		if len(item) > 0 {
			items = append(items, item)
		}
	}

	return items, nil
}

func (r *repo) hscan(key string, fn func(string, string) error) error {
	return r._scan(key, false, fn)
}

func (r *repo) zscan(key string, fn func(string) error) error {
	return r._scan(key, true, func(k, v string) error {
		return fn(k)
	})
}

func (r *repo) _scan(key string, iszscan bool, fn func(string, string) error) error {
	c := r.R()
	if c == nil {
		return errors.New("redis is nil: how?")
	}

	var method = c.HScan

	if iszscan {
		method = c.ZScan
	}

	var cursor uint64

	for {
		keys, c, err := method(context.Background(), key, cursor, "", 100).Result()
		if err != nil {
			return err
		}

		cursor = c

		var x = 0
		var k, v string

		for x < len(keys) {
			k = keys[x]

			if !iszscan {
				v = keys[x+1]
				x++
			}

			if fn(k, v) != nil {
				return nil
			}

			x++
		}

		// if iszscan {
		// 	for _, k := range keys {
		// 		if fn(k, "") != nil {
		// 			return nil
		// 		}
		// 	}
		// } else {
		// 	for x := 0; x < len(keys); x += 2 {
		// 		if fn(keys[x], keys[x+1]) != nil {
		// 			return nil
		// 		}
		// 	}
		// }

		if c == 0 {
			return nil
		}
	}
}
