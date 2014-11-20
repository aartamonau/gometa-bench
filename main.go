package main

import (
	"bytes"
	"code.google.com/p/plotinum/plot"
	"code.google.com/p/plotinum/plotter"
	"code.google.com/p/plotinum/plotutil"
	"flag"
	"fmt"
	"github.com/cheggaaa/pb"
	"github.com/dustin/randbo"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	CLIENT_ATTEMPTS = 10
)

type request struct {
	OpCode string
	Key    string
	Value  []byte
}

type reply struct {
	Result []byte
}

type work struct {
	client *rpc.Client
	req    *request

	addr string
}

type killer struct {
	ch chan bool
	wg sync.WaitGroup
}

var (
	randStream = randbo.New()

	wantedPercentiles = []int{50, 75, 90, 95, 99}

	minNodes    int
	maxNodes    int
	numRequests int
	numWorkers  int
	outputFile  string
)

func newKiller() *killer {
	ch := make(chan bool)
	wg := sync.WaitGroup{}

	wg.Add(1)

	return &killer{
		ch: ch,
		wg: wg,
	}
}

func (k *killer) kill() {
	k.ch <- true
	k.wg.Wait()
}

func spawnServer(nodes int) *killer {
	err := os.RemoveAll("client/")
	if err != nil {
		log.Fatalf("Couldn't cleanup client/ diretory: %s", err.Error())
	}

	killer := newKiller()
	go func() {
		cmd := exec.Command("script/generate-configs.rb", strconv.Itoa(nodes))

		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

		diedch := make(chan error)

		err := cmd.Start()
		if err != nil {
			log.Fatalf("Failed to start the server: %s", err.Error())
		}

		go func() {
			err := cmd.Wait()
			diedch <- err
		}()

		select {
		case <-killer.ch:
			syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			<-diedch
			killer.wg.Done()
		case err := <-diedch:
			log.Printf("Server died unexpectedly")
			if err != nil {
				log.Printf("Error: %s", err.Error())
			}

			log.Printf("Output:\n%s", output.Bytes())

			os.Exit(1)
		}
	}()

	return killer
}

func createClient(host string) *rpc.Client {
	for i := 0; i < CLIENT_ATTEMPTS; i++ {
		client, err := rpc.DialHTTP("tcp", host)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		log.Printf("Created a client to %s after %d attemtps", host, i)
		return client
	}

	log.Fatalf("Failed to create a client to %s after %d attempts",
		host, CLIENT_ATTEMPTS)
	return nil
}

func randBytes(size int) []byte {
	bytes := make([]byte, size)
	randStream.Read(bytes)
	return bytes
}

func benchmark(numNodes int) (latencies []int) {
	clients := make([]*rpc.Client, numNodes)
	addrs := make([]string, numNodes)
	latencies = []int(nil)

	for i := 0; i < numNodes; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 13000+10*i)
		clients[i] = createClient(addr)
		addrs[i] = addr
	}

	workch := make(chan work, numRequests)
	resultch := make(chan time.Duration, numRequests)
	wg := &sync.WaitGroup{}
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(worker int) {
			log.Printf("Worker %d: started", worker)

			for {
				work, ok := <-workch
				if !ok {
					log.Printf("Worker %d: terminating", worker)
					wg.Done()
					break
				}

				var reply *reply
				start := time.Now()
				err := work.client.Call("RequestReceiver.NewRequest",
					work.req, &reply)
				delta := time.Since(start)

				if err != nil {
					log.Printf("Worker %d: client %s error: %s", worker, work.addr, err.Error())
					continue
				}

				resultch <- delta
			}
		}(i)
	}

	log.Printf("Creating requests")

	for i := 0; i < numRequests; i++ {
		opCode := "Set"
		key := string(randBytes(10))
		value := randBytes(50)

		req := &request{
			OpCode: opCode,
			Key:    key,
			Value:  value,
		}

		ix := rand.Int() % numNodes
		client := clients[ix]
		addr := addrs[ix]
		workch <- work{client, req, addr}
	}

	log.Printf("Done creating requests")

	close(workch)
	go func() {
		wg.Wait()
		close(resultch)
	}()

	bar := pb.StartNew(numRequests)

	for {
		delta, ok := <-resultch
		if !ok {
			break
		}

		latencies = append(latencies, int(delta.Nanoseconds()/1000000))
		bar.Increment()
	}

	bar.FinishPrint("Done")

	sort.Ints(latencies)
	return
}

func avg(values []int) int {
	var sum int64
	for _, v := range values {
		sum += int64(v)
	}

	return int(sum / int64(len(values)))
}

func percentile(p int, values []int) int {
	if p > 100 {
		p = 100
	}

	if p < 0 {
		p = 0
	}

	ix := (len(values) - 1) * p / 100
	return values[ix]
}

func addLine(args *[]interface{}, label string, points []int) {
	xy := make(plotter.XYs, len(points))

	for i, y := range points {
		xy[i].X = float64(i + minNodes)
		xy[i].Y = float64(y)
	}

	*args = append(*args, label, xy)
}

func doPlot(avgs []int, percentiles [][]int) {
	p, err := plot.New()
	if err != nil {
		log.Fatalf("failed create a plot: %s", err.Error())
	}

	p.Title.Text = fmt.Sprintf("num-workers = %d", numWorkers)
	p.X.Label.Text = "number of nodes"
	p.Y.Label.Text = "latency (ms)"

	args := []interface{}(nil)

	addLine(&args, "avg", avgs)

	for i, percentile := range wantedPercentiles {
		addLine(&args, strconv.Itoa(percentile), percentiles[i])
	}

	err = plotutil.AddLinePoints(p, args...)
	if err != nil {
		log.Fatalf("failed to add lines to the plot: %s", err.Error())
	}

	err = p.Save(10, 10, outputFile)
	if err != nil {
		log.Fatalf("failed to save the plot: %s", err.Error())
	}
}

func main() {
	flag.IntVar(&minNodes, "min-nodes", 1, "start with this number of nodes")
	flag.IntVar(&maxNodes, "max-nodes", 50, "stop with this number of nodes")
	flag.IntVar(&numRequests, "num-requests", 1000,
		"number of requests to send for each configuration")
	flag.IntVar(&numWorkers, "num-workers", 10,
		"number of concurrent workers")
	flag.StringVar(&outputFile, "output", "bench.png", "output file")
	flag.Parse()

	averages := make([]int, maxNodes-minNodes+1)
	percentiles := make([][]int, len(wantedPercentiles))
	for i := range percentiles {
		percentiles[i] = make([]int, maxNodes-minNodes+1)
	}

	for i := minNodes; i <= maxNodes; i++ {
		log.Printf("NumNodes: %d", i)
		log.Printf("Spawning the server")

		sigch := make(chan os.Signal)
		once := &sync.Once{}
		signal.Notify(sigch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

		killer := spawnServer(i)
		go func() {
			sig, ok := <-sigch
			if !ok {
				return
			}

			log.Printf("Received signal: %v. Terminating gometa", sig)
			once.Do(func() {
				killer.kill()
			})
			os.Exit(1)
		}()

		// work around follower/leader synchronization bugs in gometa
		time.Sleep(5000 * time.Millisecond)
		latencies := benchmark(i)

		averages[i-minNodes] = avg(latencies)
		for j, p := range wantedPercentiles {
			percentiles[j][i-minNodes] = percentile(p, latencies)
		}

		once.Do(func() {
			log.Printf("Killing the server")
			killer.kill()
		})
	}

	doPlot(averages, percentiles)
}
