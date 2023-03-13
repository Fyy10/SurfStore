package surfstore

import (
	"fmt"
	"time"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")

// not used for this project
const (
	// FIXME: choose the correct interval
	HeartBeatInterval = 300 * time.Millisecond
	// FIXME: should be random value
	ElectionTimeout = 300 * time.Millisecond
	// FIXME: should be random value
	FollowerTimeout = 300 * time.Millisecond
)
