package p2p

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-bitswap"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("p2p")

func Bootstrap(ctx context.Context, peers []peer.AddrInfo) (exchange.Interface, error) {
	host, err := libp2p.New(libp2p.NoListenAddrs)
	if err != nil {
		return nil, err
	}

	err = bootstrapConnect(ctx, host, peers)
	if err != nil {
		return nil, err
	}

	go traceSwarm(host)

	kad, err := dht.New(ctx, host)
	if err != nil {
		return nil, err
	}

	network := bsnet.NewFromIpfsHost(host, kad)
	bs := blockstore.NewBlockstore(datastore.NewNullDatastore())

	exchange := bitswap.New(ctx, network, bs)

	return exchange, nil
}

func bootstrap(ctx context.Context, ph host.Host, urls []string) error {
	peers := make([]peer.AddrInfo, 0, len(urls))
	for _, url := range urls {
		addr, err := multiaddr.NewMultiaddr(url)
		if err != nil {
			log.Fatalf("failed to multiaddr '%q': %s", os.Args[1], err)
		}
		ai, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			log.Fatal(err)
		}

		peers = append(peers, *ai)
	}

	return bootstrapConnect(ctx, ph, peers)
}

func bootstrapConnect(ctx context.Context, ph host.Host, peers []peer.AddrInfo) error {
	if len(peers) < 1 {
		return fmt.Errorf("peers can not empty")
	}

	errs := make(chan error, len(peers))
	var wg sync.WaitGroup
	for _, p := range peers {

		// performed asynchronously because when performed synchronously, if
		// one `Connect` call hangs, subsequent calls are more likely to
		// fail/abort due to an expiring context.
		// Also, performed asynchronously for dial speed.

		wg.Add(1)
		go func(p peer.AddrInfo) {
			defer wg.Done()
			log.Infof("%s bootstrapping to %s", ph.ID(), p.ID)

			ph.Peerstore().AddAddrs(p.ID, p.Addrs, peerstore.PermanentAddrTTL)
			if err := ph.Connect(ctx, p); err != nil {
				log.Errorf("failed to bootstrap with %v: %s", p.ID, err)
				errs <- err
				return
			}
			log.Infof("bootstrapped with %v", p.ID)
		}(p)
	}
	wg.Wait()

	// our failure condition is when no connection attempt succeeded.
	// So drain the errs channel, counting the results.
	close(errs)
	count := 0
	var err error
	for err = range errs {
		if err != nil {
			count++
		}
	}
	if count == len(peers) {
		return fmt.Errorf("failed to bootstrap. %s", err)
	}
	return nil
}

func traceSwarm(ph host.Host) {
	for {
		time.Sleep(15 * time.Second)

		printSwarmAddrs((ph))

	}
}
func printSwarmAddrs(ph host.Host) {
	conns := ph.Network().Conns()
	log.Infof("peers count %d", len(conns))
}
