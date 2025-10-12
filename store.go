package smartq

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sfi2k7/blueweb"
	"go.etcd.io/bbolt"
)

type storeresponse map[string]interface{}

type store struct {
	filepath string
	db       *bbolt.DB
	port     int
}

func NewStore(filepath string, port int) *store {
	return &store{
		filepath: filepath,
		port:     port,
	}
}

func (s *store) get(bucket, key string) (string, error) {
	var value string

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("container: not found")
		}

		v := b.Get([]byte(key))
		if len(v) == 0 {
			return errors.New("error: key not found")
		}

		value = string(v)
		// fmt.Println("got", key, "value", value)
		return nil
	})
	return value, err
}

func (s *store) set(bucket, key, value string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		// fmt.Println("setting", key, value)
		return b.Put([]byte(key), []byte(value))
	})
}

func (s *store) del(bucket, key string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		return b.Delete([]byte(key))
	})
}

func (s *store) count(bucket, key string) (int, error) {
	var count int
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("container: not found")
		}

		count = b.Stats().KeyN

		return b.Delete([]byte(key))
	})
	return count, err
}

func (s *store) emptybucket(bucket string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket([]byte(bucket))
	})
}

func (s *store) printcurrentjobsanddata(bucket string) error {
	fmt.Println("printing current list")
	return s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return errors.New("bucket not found")
		}

		cursor := bucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			fmt.Printf("Key: %s, Value: %s\n", k, v)
		}

		fmt.Println("Done printing")
		return nil
	})
}

func (s *store) Start() {
	db, err := bbolt.Open(s.filepath, 0664, nil)
	if err != nil {
		panic(err)
	}
	s.db = db

	LoadConfig()

	ex := make(chan os.Signal, 2)
	signal.Notify(ex, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(ex)

	go func(exitchannnel chan os.Signal) {
		fmt.Println("inner loop started")

		r := newrepo()
		defer r.Close()

		for {
			select {
			case <-exitchannnel:
				fmt.Println("exiting inner loop")
				return
			default:
				var messages []string
				if messages, err = r.popfromlist(storeKey, "", 10); len(messages) == 0 {
					time.Sleep(time.Millisecond * 250)
					continue
				}

				for _, message := range messages {
					id, channel, command := iccparse(message)

					//system commands
					if command == "empty" {
						fmt.Println("emptying bucket")
						fmt.Println("done", s.emptybucket(defautBucket))
						continue
					}

					if command == "scan" {
						r.hscan("__test__hash__", func(k, v string) error {
							fmt.Println("kv", k, v)
							return nil
						})
					}

					if command == storePrintCommand {
						fmt.Println("print: " + id)
						s.printcurrentjobsanddata(defautBucket)
						continue
					}

					//TODO: at this point check if queue is paused

					//channel commands
					if command == deletecommand {
						fmt.Println("delete: " + id)
						s.del(defautBucket, id)
						continue
					}

					jobkey := jobKey(id)

					if command == synccommand {
						r.sethash(jobkey, "channel", channel)

						obj := r.loadobjectfromhash(jobkey)
						if len(obj) > 0 {
							jsoned, _ := json.Marshal(obj)
							s.set(defautBucket, id, string(jsoned))
						}
					}

					fmt.Println("route: " + id)
					r.routetochannel(id, channel, false)
				}
			}
		}
	}(ex)

	if s.port == 0 {
		fmt.Println("web is not started: waiting on exit channel")
		<-ex
		fmt.Println("exiting: store")
		os.Exit(0)
	}

	router := blueweb.NewRouter()

	storeApi := router.Group("/store")

	storeApi.Get("/:key", func(c *blueweb.Context) {
		key := c.Params("key")
		if key == "" {
			c.Json(storeresponse{"error": "error: key not provided"})
			return
		}

		value, err := s.get(defautBucket, key)
		if err != nil {
			c.Json(storeresponse{"error": "error: key not found"})
			return
		}
		if strings.HasPrefix(value, "{") || strings.HasPrefix(value, "[") {
			var i any
			json.Unmarshal([]byte(value), &i)
			c.Json(storeresponse{"success": true, "key": key, "json": true, "value": i})
			return
		}

		c.Json(storeresponse{"success": true, "key": key, "value": value})
	})

	storeApi.Post("/:key/:value", func(c *blueweb.Context) {
		key := c.Params("key")
		value := c.Params("value")

		if key == "" || value == "" {
			c.Json(storeresponse{"error": "error: key and value not provided"})
			return
		}

		err := s.set(defautBucket, key, value)
		if err != nil {
			c.Json(storeresponse{"error": "error: error adding value"})
			return
		}

		c.Json(storeresponse{"success": true})
	})

	storeApi.Delete("/:key", func(c *blueweb.Context) {
		key := c.Params("key")
		if key == "" {
			c.Json(storeresponse{"error": "error: key not provided"})
			return
		}

		err := s.del(defautBucket, key)
		if err != nil {
			c.Json(storeresponse{"error": "error: key not foun or error deleting key"})
			return
		}

		c.Json(storeresponse{"success": true})
	})

	router.Config().SetDev(true).SetPort(s.port).StopOnInterrupt()

	router.StartServer()
}
