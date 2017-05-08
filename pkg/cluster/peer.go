// Package cluster provides an elastic peer discovery and gossip layer.
//
// Ingest and store instances join the same cluster and know about each other.
// Store instances consume segments from each ingest instance, and broadcast
// queries to each store instance. In the future, ingest instances will share
// load information to potentially refuse connections and balance writes.
package cluster

import (
	"io/ioutil"
	"net"
	"strconv"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/pborman/uuid"
)

// Peer represents this node in the cluster.
type Peer struct {
	ml *serf.Serf
}

// PeerType enumerates the types of nodes in the cluster.
type PeerType string

const (
	// PeerTypeIngest serves the ingest API.
	PeerTypeIngest PeerType = "ingest"

	// PeerTypeStore serves the store API.
	PeerTypeStore = "store"

	// PeerTypeIngestStore serves both ingest and store APIs.
	PeerTypeIngestStore = "ingeststore"
)

// NewPeer creates or joins a cluster with the existing peers.
// We will listen for cluster communications on the bind addr:port.
// We advertise a PeerType HTTP API, reachable on apiPort.
//
// If advertiseAddr is not empty, we will advertise ourself as reachable for
// cluster communications on that address; otherwise, memberlist will extract
// the IP from the bound addr:port and advertise on that.
func NewPeer(
	bindAddr string, bindPort int,
	advertiseAddr string, advertisePort int,
	existing []string,
	t PeerType, apiPort int,
	logger log.Logger,
) (*Peer, error) {
	level.Debug(logger).Log("bind_addr", bindAddr, "bind_port", bindPort, "ParseIP", net.ParseIP(bindAddr).String())

	config := serf.DefaultConfig()
	{
		config.NodeName = uuid.New()
		config.MemberlistConfig.BindAddr = bindAddr
		config.MemberlistConfig.BindPort = bindPort
		if advertiseAddr != "" {
			level.Debug(logger).Log("advertise_addr", advertiseAddr, "advertise_port", advertisePort)

			config.MemberlistConfig.AdvertiseAddr = advertiseAddr
			config.MemberlistConfig.AdvertisePort = advertisePort
		}
		config.LogOutput = ioutil.Discard
		config.BroadcastTimeout = time.Second * 10
		config.Tags = encodePeerInfoTag(peerInfo{
			Type:    t,
			APIAddr: bindAddr,
			APIPort: bindPort,
		})
	}

	ml, err := serf.Create(config)
	if err != nil {
		return nil, err
	}

	n, _ := ml.Join(existing, true)
	level.Debug(logger).Log("Join", n)

	if len(existing) > 0 {
		go warnIfAlone(ml.Memberlist(), logger, 5*time.Second)
	}

	return &Peer{
		ml: ml,
	}, nil
}

func warnIfAlone(ml *memberlist.Memberlist, logger log.Logger, d time.Duration) {
	for range time.Tick(d) {
		if n := ml.NumMembers(); n <= 1 {
			level.Warn(logger).Log("NumMembers", n, "msg", "I appear to be alone in the cluster")
		}
	}
}

// Leave the cluster, waiting up to timeout.
func (p *Peer) Leave(timeout time.Duration) error {
	// Ignore this timeout for now, serf uses a config timeout.
	return p.ml.Leave()
}

// Current API host:ports for the given type of node.
func (p *Peer) Current(t PeerType) (res []string) {
	for _, v := range p.ml.Members() {
		if v.Status != serf.StatusAlive {
			continue
		}

		if info, ok := decodePeerInfoTag(v.Tags); ok {
			var (
				matchIngest      = t == PeerTypeIngest && (info.Type == PeerTypeIngest || info.Type == PeerTypeIngestStore)
				matchStore       = t == PeerTypeStore && (info.Type == PeerTypeStore || info.Type == PeerTypeIngestStore)
				matchIngestStore = t == PeerTypeIngestStore && info.Type == PeerTypeIngestStore
			)
			if matchIngest || matchStore || matchIngestStore {
				res = append(res, net.JoinHostPort(info.APIAddr, strconv.Itoa(info.APIPort)))
			}
		}

	}
	return
}

// Name returns unique ID of this peer in the cluster.
func (p *Peer) Name() string {
	return p.ml.Memberlist().LocalNode().Name
}

// ClusterSize returns the total size of the cluster from this node's perspective.
func (p *Peer) ClusterSize() int {
	return p.ml.Memberlist().NumMembers()
}

// State returns a JSON-serializable dump of cluster state.
// Useful for debug.
func (p *Peer) State() map[string]interface{} {
	return map[string]interface{}{
		"self":    p.ml.Memberlist().LocalNode(),
		"members": p.ml.Memberlist().Members(),
		"n":       p.ml.Memberlist().NumMembers(),
	}
}

type peerInfo struct {
	Type    PeerType
	APIAddr string
	APIPort int
}

// encodeTagPeerInfo encodes the peer information for the node tags.
func encodePeerInfoTag(info peerInfo) map[string]string {
	return map[string]string{
		"type":     string(info.Type),
		"api_addr": info.APIAddr,
		"api_port": strconv.Itoa(info.APIPort),
	}
}

// decodePeerInfoTag gets the peer information from the node tags.
func decodePeerInfoTag(m map[string]string) (info peerInfo, valid bool) {
	if peerType, ok := m["type"]; ok {
		info.Type = PeerType(peerType)
	} else {
		return
	}

	if apiPort, ok := m["api_port"]; ok {
		var err error
		info.APIPort, err = strconv.Atoi(apiPort)
		if err != nil {
			return
		}
	} else {
		return
	}

	if info.APIAddr, valid = m["api_addr"]; !valid {
		return
	}

	return
}
