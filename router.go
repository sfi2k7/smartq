package smartq

// var newjobschannel = "sq_newjobs"

// var keyactivejobs = "sq_active_jobs"

// var jobsrepo = "sq_jobs_repo"

// func routerloop(ex chan os.Signal) {
// 	fmt.Println("starting router loop")
// 	var r = newrepo()
// 	for {
// 		select {
// 		case <-ex:
// 			fmt.Println("intersection: exiting")
// 			return
// 		default:
// 			message, err := r.popzset(newjobschannel)
// 			if err != nil {
// 				time.Sleep(time.Millisecond * 200)
// 				continue
// 			}

// 			if len(message) == 0 {
// 				continue
// 			}

// 			// fmt.Println("got message in intersection", message)

// 			jobid, channel, command := iccparse(message)
// 			if jobid == "" || command == "" {
// 				fmt.Println("invalid message format")
// 				continue
// 			}

// 			if len(channel) > 0 {
// 				//error ignored - for now
// 				r.ensurechannelstatus(channel)
// 			}

// 			jobkey := jobKey(jobid)
// 			channelkey := channelKey(channel)

// 			if command == newcommand {

// 				//set job metadata
// 				r.sethash(jobkey, "id", jobid, "channel", channel, "created", fmt.Sprint(time.Now().Unix()))

// 				//add job to the channel
// 				r.pushzset(channelkey, jobid)

// 				//add channel to zset of channels
// 				r.pushzset(keychannels, channel) //add channel to channels list

// 				r.hinc(channelStatusKey(channel), "appended")

// 				//push job to store for sync
// 				r.pushlist(storekey, icc(jobid, channel, "sync"))
// 				continue
// 			}

// 			if command == routecommand {
// 				//update hash with channel
// 				r.sethash(jobkey, "channel", channel)

// 				//add job to channel
// 				r.pushlist(channelkey, jobid)

// 				//update channel status
// 				r.hinc(channelStatusKey(channel), "routed")

// 				// add job to store for sync
// 				r.pushlist(storekey, icc(jobid, channel, "sync"))
// 			}

// 			if command == synccommand { //this will hit incase original command was route_and_sync
// 				//TODO: Sync job back to disk with metadata if needed
// 				r.pushlist(storekey, icc(jobid, channel, "sync"))
// 			}

// 			if command == deletecommand {
// 				r.pushlist(storekey, icc(jobid, channel, "delete"))
// 				r.deletekey(jobkey)
// 				r.rmzset(keyactivejobs, jobid)
// 				fmt.Println("delete:", jobid)

// 				//TODO: remove job from disk if needed
// 			}
// 		}
// 	}
// }

// func StartRouterLoop() {
// 	LoadConfig()

// 	ex := make(chan os.Signal, 2)
// 	signal.Notify(ex, os.Interrupt, syscall.SIGTERM)

// 	routerloop(ex)
// }
