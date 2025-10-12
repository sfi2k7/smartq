package smartq

type Admin struct {
	r *repo
}

func NewAdmin() *Admin {
	return &Admin{
		r: newrepo(),
	}
}

// func (a *Admin) run(cmd string) (string, error) {
// 	splitted := strings.Split(cmd, "|")
// 	var id, command, channel string
// 	if len(splitted) > 0 {
// 		command = splitted[0]
// 	}

// 	if len(splitted) > 1 {
// 		channel = splitted[1]
// 	}

// 	if len(splitted) > 2 {
// 		id = splitted[2]
// 	}

// 	switch command {
// 	case "push":
// 		return "", a.r.pushzset(channelKey(channel), id)
// 	case "pop":
// 		return a.r.popfromchannel(channelKey(channel))
// 	}

// 	return "", nil
// }
