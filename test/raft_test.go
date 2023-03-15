package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"testing"
	"time"

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

func TestRaftBlockRecovery(t *testing.T) {
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

	// majority crash
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// wait for some time and then recover
	go func() {
		time.Sleep(time.Second)
		test.Clients[1].Restore(test.Context, &emptypb.Empty{})
		test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	}()

	// update a file on node 0 (should block and then succeed)
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

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {
	t.Log("leader 0 gets several requests when majority crashed. leader 0 crashes. all other nodes recovered. leader 1 gets a request. leader 0 restored")

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

	// majority crash
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	go func() {
		fileMeta := &surfstore.FileMetaData{
			Filename:      "test",
			Version:       1,
			BlockHashList: nil,
		}
		test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta)
		fileMeta.Version++
		test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta)
		test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	}()

	// recover
	time.Sleep(2 * time.Second)
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	// set node 1 to be the leader
	leaderIdx = 1
	_, err = test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Error("cannot set leader:", err)
	}
	_, err = test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Error("cannot send heartbeat:", err)
	}

	// update a file on node 1 (should succeed)
	fileMeta := &surfstore.FileMetaData{
		Filename:      "test1",
		Version:       1,
		BlockHashList: nil,
	}
	_, err = test.Clients[leaderIdx].UpdateFile(test.Context, fileMeta)
	if err != nil {
		t.Error("cannot update file:", err)
	}
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
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
	// all in term 2
	// entries applied to state machine (meta store)
	expectedInfoMap := make(map[string]*surfstore.FileMetaData)
	expectedInfoMap[fileMeta.Filename] = fileMeta

	expectedLog := make([]*surfstore.UpdateOperation, 0)
	expectedLog = append(expectedLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: fileMeta,
	})

	for i := 0; i < len(test.Clients); i++ {
		isLeader := false
		if i == leaderIdx {
			isLeader = true
		}
		term := int64(2)
		state, _ := test.Clients[i].GetInternalState(test.Context, &emptypb.Empty{})
		t.Log(state)
		_, err = CheckInternalState(&isLeader, &term, expectedLog, expectedInfoMap, test.Clients[i], test.Context)
		if err != nil {
			t.Error(err)
		}
	}
}
