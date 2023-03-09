package surfstore

import (
	"fmt"
	"log"
	"testing"
)

func TestGetResponsibleServer(t *testing.T) {
	serverAddrs := []string{
		"server1addr",
		"server2addr",
		"server3addr",
	}
	hashRing := NewConsistentHashRing(serverAddrs)

	for k, v := range hashRing.ServerMap {
		log.Println(k, v)
	}

	// self-contained
	for _, addr := range serverAddrs {
		responsibleServer := hashRing.GetResponsibleServer(hashRing.Hash(BlockStoreHashPrefix + addr))
		if addr != responsibleServer {
			t.Errorf("expected %s, got %s\n", addr, responsibleServer)
		}
	}

	cnt1 := 0
	cnt2 := 0
	cnt3 := 0
	n := 10000
	for i := 0; i < n; i++ {
		blockId := hashRing.Hash(fmt.Sprint(i))
		responsibleServer := hashRing.GetResponsibleServer(blockId)
		switch responsibleServer {
		case "server1addr":
			cnt1++
		case "server2addr":
			cnt2++
		case "server3addr":
			cnt3++
		default:
			t.Error("unknown responsible server address")
		}
	}

	if cnt1+cnt2+cnt3 != n {
		t.Errorf("total num (%d) != server1 (%d) + server2 (%d) + server3 (%d)\n", n, cnt1, cnt2, cnt3)
	}

	log.Printf("server1: %d, server2: %d, server3: %d\n", cnt1, cnt2, cnt3)
}

func BenchmarkGetResponsibleServer(b *testing.B) {
	serverAddrs := []string{
		"server1145addr",
		"server1419addr",
		"server2333ddr",
	}
	hashRing := NewConsistentHashRing(serverAddrs)

	cnt1 := 0
	cnt2 := 0
	cnt3 := 0
	for i := 0; i < b.N; i++ {
		blockId := hashRing.Hash(fmt.Sprint(i))
		responsibleServer := hashRing.GetResponsibleServer(blockId)
		switch responsibleServer {
		case "server1145addr":
			cnt1++
		case "server1419addr":
			cnt2++
		case "server2333ddr":
			cnt3++
		default:
			b.Error("unknown responsible server address")
		}
	}

	if cnt1+cnt2+cnt3 != b.N {
		b.Errorf("total num (%d) != server1 (%d) + server2 (%d) + server3 (%d)\n", b.N, cnt1, cnt2, cnt3)
	}

	log.Printf("server1: %d, server2: %d, server3: %d\n", cnt1, cnt2, cnt3)
}

func TestEmpty(t *testing.T) {
	serverAddrs := []string{
		"server1addr",
		"server2addr",
		"server3addr",
	}
	hashRing := NewConsistentHashRing(serverAddrs)
	if ans := hashRing.GetResponsibleServer(hashRing.Hash("")); ans != "" {
		t.Errorf("expected %q, got %q\n", "", ans)
	}

	if ans := hashRing.GetResponsibleServer(EMPTYFILE_HASHVALUE); ans != "" {
		t.Errorf("expected %q, got %q\n", "", ans)
	}
}
