package smartq

import (
	"encoding/json"
	"fmt"
)

type WatchContext struct {
	ID         string
	Channel    string
	Job        Job
	w          *Watch
	haschanged bool
}

func (wc *WatchContext) Route(channel string, keyvals ...any) *RouteToken {
	if len(keyvals) > 0 {
		err := wc.w.r.sethash(jobKey(wc.ID), keyvalstostring(keyvals...))
		if err != nil {
			fmt.Println("error setting key/val on job using route")
		}

		wc.haschanged = true
	}

	return &RouteToken{
		cmd:   icc(wc.ID, channel, routecommand),
		token: wc.w.ctxtoken,
	}
}

func (wc *WatchContext) NoOp() *RouteToken {
	return &RouteToken{
		cmd:   icc(wc.ID, "", deletecommand),
		token: wc.w.ctxtoken,
	}
}

func (w *WatchContext) SetKV(k string, v any) {
	w.w.r.sethash(jobKey(w.ID), k, v)
}

func (w *WatchContext) SetObj(k string, o any) {
	jsoned, _ := json.Marshal(o)
	w.SetKV(k, string(jsoned))
}

func (w *WatchContext) GetObj(k string, o any) error {
	jsoned := w.w.r.hget(jobKey(w.ID), k)
	if jsoned == "" {
		return fmt.Errorf("key %s not found", k)
	}

	return json.Unmarshal([]byte(jsoned), o)
}
