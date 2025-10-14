package smartq

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type RouteToken struct {
	cmd   string
	token string
}

type Watch struct {
	name     string
	r        *repo
	ctxtoken string
}

func NewWatch(name string) *Watch {
	return &Watch{
		name:     name,
		r:        newrepo(),
		ctxtoken: ID(),
	}
}

func (w *Watch) Close() error {
	return w.r.Close()
}

func (w *Watch) Name() string {
	return w.name
}

func InitJob(channel, id string) error {
	if len(id) == 0 {
		return errors.New("id cannot be empty")
	}

	if len(channel) == 0 {
		return errors.New("channel cannot be empty")
	}

	r := getcachedrepo()

	return r.addtochannel(id, channel)
}

func (w *Watch) Start(channel string, callback func(*WatchContext) *RouteToken) {
	defer func() {
		fmt.Println(w.name, "stopped watching", channel)
	}()

	ex := make(chan os.Signal, 2)
	signal.Notify(ex, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(ex)

	w.r.pushzset("sq_watches", w.name+"|"+channel)

	defer func() {
		w.r.rmzset("sw_watches", w.name+"|"+channel)
	}()

	workingset := "workingset_" + channel

	var err error
	var ids []string

	for {

		select {
		case <-ex:
			return
		default:
			ids, err = w.r.popfromchannel(channel, workingset, 10)
			if err != nil {
				if err == ErrChannelPaused {
					// fmt.Println("channel is paused:", channel)
				}

				time.Sleep(time.Millisecond * 250)
				continue
			}

			if len(ids) == 0 {
				time.Sleep(time.Millisecond * 250)
				continue
			}

			for _, id := range ids {

				// fmt.Println(w.r.conn.LLen(context.Background(), workingset).Result())
				// w.r.pushlist(workingset, id)

				ctx := &WatchContext{
					ID:      id,
					Channel: channel,
					w:       w,
				}

				var job = w.r.loadobjectfromhash(jobKey(id))

				ctx.Job = job

				nextcommand := callback(ctx)
				if nextcommand == nil {
					w.r.deletelkey(workingset, id)
					continue
				}

				if len(nextcommand.cmd) == 0 || nextcommand.token != w.ctxtoken {
					w.r.deletelkey(workingset, id)
					continue
				}

				id, channel, command := iccparse(nextcommand.cmd)

				w.r.deletelkey(workingset, id)

				if command == deletecommand {
					w.r.deletejob(id)
				}

				if command == routecommand {
					w.r.routetochannel(id, channel, ctx.haschanged)
				}
			}

			ids = []string{}
		}
	}
}
