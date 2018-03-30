// test_consistentHash
package consistentHash

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"strconv"
	"testing"

	"github.com/GaryBoone/GoStats/stats"
	"github.com/stretchr/testify/assert"
)

var (
	keys [][]byte
)

func init() {
	keys = make([][]byte, 10000)
	for i := 0; i < len(keys); i++ {
		keys[i] = randBytes(10)
	}

}

func randBytes(size int) []byte {
	var bytes = make([]byte, size)
	rand.Read(bytes)
	return bytes
}

// TestVnodeAdd verifies that the correct number of vnodes are added after an Add() call
func TestVnodeAdd(t *testing.T) {
	c := New()
	c.Add("localhost")
	assert.Equal(t, c.vnodeCount, len(c.vnodes))

}

// TestDistribution tests how well keys are distributed across servers
// and how many keys are remapped after a node is removed
// This is informational, not a pass/fail test
func TestDistribution(t *testing.T) {
	c := New()
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	distribution := make(map[string]int)
	keymapping := make(map[string]string)
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		server, _ := c.Get(key)
		keymapping[string(key)] = server
		distribution[server]++
	}
	stat := stats.Stats{}
	for key, count := range distribution {
		stat.Update(float64(count))
		delete(distribution, key)
	}
	t.Logf("Stddev for %d keys mapped across %d servers = %.2f\n", len(keys), serverCount, stat.PopulationStandardDeviation())

	c.Remove("server" + strconv.Itoa(serverCount/2))
	stat = stats.Stats{}
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		server, _ := c.Get(key)
		if keymapping[string(key)] == server {
			delete(keymapping, string(key))
		}
		distribution[server]++
	}
	for key, count := range distribution {
		stat.Update(float64(count))
		delete(distribution, key)
	}
	t.Logf("Stddev for %d keys mapped across %d servers after one server removed = %.2f\n", len(keys), serverCount, stat.PopulationStandardDeviation())
	t.Logf("Number of keys out of %d remapped after removing 1 out of %d servers = %d\n", len(keys), serverCount, len(keymapping))
}

// Benchmark_DefaultLookup tests how fast lookups are if each node has the default number of vnodes
func Benchmark_DefaultLookup(b *testing.B) {
	defer fmt.Printf("called benchmark with %d\n", b.N)
	c := New()
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(keys[i%len(keys)])
	}
}

// Benchmark_SingleVnodeLookup tests how fast lookups are if each node has 1 vnode
// Note that this would have very poor distribution
func Benchmark_SingleVnodeLookup(b *testing.B) {
	c := New()
	c.SetVnodeCount(1)
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(keys[i%len(keys)])
	}
}

// Benchmark_1000VnodeLookup tests how fast lookups are if each node has 1000 vnodes
func Benchmark_1000VnodeLookup(b *testing.B) {
	c := New()
	c.SetVnodeCount(1000)
	serverCount := 10
	for i := 0; i < serverCount; i++ {
		c.Add("server" + strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(keys[i%len(keys)])
	}
}

// TestinsertVnode verifies that vnodes are correctly inserted in the proper order
func TestInsertVnode(t *testing.T) {
	ch := New()
	v1 := vnode{100, "a"}
	v2 := vnode{50, "b"}
	v3 := vnode{1001, "c"}
	v4 := vnode{1000, "d"}
	ch.insertVnode(v1)
	ch.insertVnode(v2)
	ch.insertVnode(v3)
	ch.insertVnode(v4)
	assert.Equal(t, 4, len(ch.vnodes))
	assert.Equal(t, v2, ch.vnodes[0])
	assert.Equal(t, v1, ch.vnodes[1])
	assert.Equal(t, v3, ch.vnodes[3])
	assert.Equal(t, v4, ch.vnodes[2])

}

func TestGet2(t *testing.T) {
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	server1, server2, err := ch.Get2([]byte("testKey"))
	assert.Nil(t, err)
	assert.True(t, (server1 == "server1" && server2 == "server2") || (server1 == "server2" && server2 == "server1"))
}

func TestGetN(t *testing.T) {
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	_, err := ch.GetN([]byte("testKey"), 3)
	assert.Equal(t, err, ErrNotEnoughMembers)
	ch.Add("server3")
	servers, err := ch.GetN([]byte("testKey"), 3)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(servers))
}

// TestRemoveVnode verifies that vnodes are correctly removed
func TestremoveVnode(t *testing.T) {
	ch := New()
	v1 := vnode{100, "a"}
	v2 := vnode{50, "b"}
	v3 := vnode{1001, "c"}
	v4 := vnode{1000, "d"}
	ch.insertVnode(v1)
	ch.insertVnode(v2)
	ch.insertVnode(v3)
	ch.insertVnode(v4)
	ch.removeVnode(50)
	assert.Equal(t, 3, len(ch.vnodes))
	ch.removeVnode(1001)
	ch.removeVnode(100)
	ch.removeVnode(1000)
	assert.Empty(t, ch.vnodes)

}

func Examplebasic() {
	// Output: key=A server=server3
	//key=B server=server3
	//key=C server=server1
	//key=D server=server3
	//key=E server=server2
	//key=F server=server2
	//key=G server=server1
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	ch.Add("server3")
	keys := []string{"A", "B", "C", "D", "E", "F", "G"}
	for _, key := range keys {
		server, err := ch.Get([]byte(key))
		if err != nil {
			panic(err)
		}
		fmt.Printf("key=%s server=%s\n", key, server)
	}
}

func Exampleremove() {
	//Output: 3 servers
	//key=A server=server3
	//key=B server=server3
	//key=C server=server1
	//key=D server=server3
	//key=E server=server2
	//key=F server=server2
	//key=G server=server1
	//Removing server3
	//key=A server=server1
	//key=B server=server2
	//key=C server=server1
	//key=D server=server1
	//key=E server=server2
	//key=F server=server2
	//key=G server=server1
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	ch.Add("server3")
	keys := []string{"A", "B", "C", "D", "E", "F", "G"}
	fmt.Println("3 servers")
	for _, key := range keys {
		server, _ := ch.Get([]byte(key))
		fmt.Printf("key=%s server=%s\n", key, server)
	}
	fmt.Println("Removing server3")
	ch.Remove("server3")
	for _, key := range keys {
		server, _ := ch.Get([]byte(key))
		fmt.Printf("key=%s server=%s\n", key, server)
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func TestSimpleHashRingMemoryUsage(t *testing.T) {
	ch := New()
	ch.Add("server1")
	ch.Add("server2")
	ch.Add("server3")
	ch.Add("server4")
	ch.Add("server4")
	ch.Add("server6")
	ch.Add("server7")

	times := 100000000
	PrintMemUsage()
	for i := 0; i < times; i++ {
		ch.Get([]byte(string(i)))
	}
	PrintMemUsage()
	for i := 0; i < times; i++ {
		ch.Get([]byte(string(i)))
	}
	PrintMemUsage()
}

func TestRemapping(t *testing.T) {
	ch := New()
	ch.SetVnodeCount(200)
	ch.Add("s1")
	ch.Add("s2")
	ch.Add("s3")
	ch.Add("s4")

	times := 200
	var results []string
	for i := 0; i < times; i++ {
		val, _ := ch.Get([]byte(string(i)))
		results = append(results, fmt.Sprintf("%d : %s", i, val))
	}

	var changes int
	for i := 0; i < times; i++ {
		val, _ := ch.Get([]byte(string(i)))
		newResult := fmt.Sprintf("%d : %s", i, val)
		if newResult != results[i] {
			fmt.Printf("%s -> %s\n", results[i], newResult)
		}
	}

	ch2 := New()
	ch2.AddWithNodeCount("s1", 200)
	ch2.AddWithNodeCount("s2", 100)
	ch2.AddWithNodeCount("s2b", 100)
	ch2.AddWithNodeCount("s3", 200)
	ch2.AddWithNodeCount("s4", 200)

	for i := 0; i < times; i++ {
		val, _ := ch2.Get([]byte(string(i)))
		newResult := fmt.Sprintf("%d : %s", i, val)
		if newResult != results[i] {
			changes = changes + 1
			fmt.Printf("%s -> %s\n", results[i], newResult)
		}
	}

	fmt.Printf("%d mappings changed\n", changes)

}

func TestFeature(t *testing.T) {
	Examplebasic()
}
