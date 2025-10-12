package smartq

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const routecommand = "route"
const deletecommand = "delete"
const synccommand = "sync"
const newcommand = "new"

var channelsKey = "sq_channels"

const defautBucket = "__container__"

// const storeDeleteKey = "sq_store_delete"
// const storeSyncKey = "sq_store_sync"
const storeKey = "sq_store_"
const storePrintCommand = "print"

func ID() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}

func channelKey(channel string) string {
	return fmt.Sprintf("sq_channel_%s", channel)
}

func channelStatusKey(channel string) string {
	return fmt.Sprintf("sq_channel_status_%s", channel)
}

func jobKey(jobid string) string {
	return fmt.Sprintf("sq_job_%s", jobid)
}

func icc(id, channel, command string) string {
	return fmt.Sprintf("%s|%s|%s", id, channel, command)
}

func iccparse(i string) (id, channel, command string) {
	splitted := strings.Split(i, "|")
	if len(splitted) == 0 {
		return "", "", ""
	}

	if len(splitted) < 3 {
		fmt.Println("job is moved to trash", i)
		return "trash_job", "trash_channel", ""
	}

	id = splitted[0]
	channel = splitted[1]
	command = splitted[2]

	return id, channel, command
}

func keyvalstostring(keyvals ...any) []string {
	var result []string
	for x := 1; x < len(keyvals); x += 2 {
		result = append(result, keyvals[x-1].(string))
		switch v := keyvals[x].(type) {
		case string:
			result = append(result, fmt.Sprintf("%s", v))
		case int:
			result = append(result, fmt.Sprintf("%d", v))
		case float64:
			result = append(result, fmt.Sprintf("%f", v))
		case bool:
			result = append(result, fmt.Sprintf("%t", v))
		case time.Time:
			result = append(result, fmt.Sprintf("%d", v.Unix()))
		case *any:
			jsoned, _ := json.Marshal(v)
			result = append(result, string(jsoned))
		case any:
			jsoned, _ := json.Marshal(v)
			result = append(result, string(jsoned))
		default:
			result = append(result, fmt.Sprintf("%v", v))
		}
	}
	return result
}
