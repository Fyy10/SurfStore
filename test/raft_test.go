package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	// setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	leaderIdx := 0
	_, err := test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Error("cannot set leader:", err)
	}
	_, err = test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Error("cannot send heartbeat:", err)
	}

	for i := 0; i < len(test.Clients); i++ {
		isLeader := false
		if i == leaderIdx {
			isLeader = true
		}
		term := int64(1)
		_, err = CheckInternalState(&isLeader, &term, nil, nil, test.Clients[i], test.Context)
		if err != nil {
			t.Error(err)
		}
	}

	// update a file on node 0
	fileMeta := &surfstore.FileMetaData{
		Filename:      "test",
		Version:       1,
		BlockHashList: nil,
	}
	_, err = test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta)
	if err != nil {
		t.Error("cannot update file:", err)
	}
	// replicate log
	_, err = test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Error("cannot replicate log:", err)
	}
	// commit entries
	_, err = test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Error("cannot commit entries:", err)
	}

	// check that all the nodes are in the right state
	// leader and all followers have the log entry
	// all in term 1
	// entries applied to state machine (meta store)
	expectedInfoMap := make(map[string]*surfstore.FileMetaData)
	expectedInfoMap[fileMeta.Filename] = fileMeta

	expectedLog := make([]*surfstore.UpdateOperation, 0)
	expectedLog = append(expectedLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: fileMeta,
	})

	for i := 0; i < len(test.Clients); i++ {
		isLeader := false
		if i == leaderIdx {
			isLeader = true
		}
		term := int64(1)
		_, err = CheckInternalState(&isLeader, &term, expectedLog, expectedInfoMap, test.Clients[i], test.Context)
		if err != nil {
			t.Error(err)
		}
	}
}
