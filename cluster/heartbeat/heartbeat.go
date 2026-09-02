package heartbeat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	incusapi "github.com/lxc/incus/v7/shared/api"
	incustls "github.com/lxc/incus/v7/shared/tls"

	"github.com/cowsql/go-cowsql/cluster/api"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	"github.com/cowsql/go-cowsql/cluster/tls"
)

// Hook represents a function that can be called as the heartbeat hook.
type Hook func(heartbeatData *APIHeartbeat, isLeader bool, unavailableMembers []string)

// Handler represents a function that can be called when a heartbeat request arrives.
type Handler func(w http.ResponseWriter, r *http.Request, isLeader bool, hbData *APIHeartbeat)

type Mode int

const (
	HeartbeatNormal Mode = iota
	HeartbeatImmediate
	HeartbeatInitial
)

func (m *Mode) Name() string {
	switch *m {
	case HeartbeatNormal:
		return "normal"
	case HeartbeatImmediate:
		return "immediate"
	case HeartbeatInitial:
		return "initial"
	default:
		return "unknown"
	}
}

// APIHeartbeatVersion contains max versions for all nodes in cluster.
type APIHeartbeatVersion struct {
	Schema           int
	APIExtensions    int
	MinAPIExtensions int
}

// NewAPIHearbeat returns initialized APIHeartbeat.
func NewAPIHearbeat(dbCluster db.Cluster) *APIHeartbeat {
	return &APIHeartbeat{
		cluster: dbCluster,
	}
}

// APIHeartbeat contains data sent to nodes in heartbeat.
type APIHeartbeat struct {
	sync.Mutex // Used to control access to Members maps.
	cluster    db.Cluster
	Members    map[int64]db.HeartbeatMember
	Version    APIHeartbeatVersion
	Time       time.Time

	// Indicates if heartbeat contains a fresh set of node states.
	// This can be used to indicate to the receiving node that the state is fresh enough to
	// trigger node refresh activities.
	FullStateList bool
}

// Update updates an existing APIHeartbeat struct with the raft and all node states supplied.
// If allNodes provided is an empty set then this is considered a non-full state list.
func (hbState *APIHeartbeat) Update(fullStateList bool, raftNodes []db.RaftNode, allNodes []db.NodeInfo, offlineThreshold time.Duration) {
	var maxSchemaVersion, maxAPIExtensionsVersion, minAPIExtensionsVersion int

	if hbState.Members == nil {
		hbState.Members = make(map[int64]db.HeartbeatMember)
	}

	// If we've been supplied a fresh set of node states, this is a full state list.
	hbState.FullStateList = fullStateList

	// Convert raftNodes to a map keyed on address for lookups later.
	raftNodeMap := make(map[string]db.RaftNode, len(raftNodes))
	for _, raftNode := range raftNodes {
		raftNodeMap[raftNode.Address] = raftNode
	}

	// Add nodes (overwrites any nodes with same ID in map with fresh data).
	for _, node := range allNodes {
		member := db.HeartbeatMember{
			ID:            node.ID,
			Address:       node.Address,
			Name:          node.Name,
			LastHeartbeat: node.Heartbeat,
			Online:        !node.IsOffline(offlineThreshold),
			Roles:         node.Roles,
		}

		raftNode, exists := raftNodeMap[member.Address]
		if exists {
			member.RaftID = raftNode.ID
			member.RaftRole = int(raftNode.Role)
			delete(raftNodeMap, member.Address) // Used to check any remaining later.
		}

		// Add to the members map using the node ID (not the Raft Node ID).
		hbState.Members[node.ID] = member

		// Keep a record of highest APIExtensions and Schema version seen in all nodes.
		if node.APIExtensions > maxAPIExtensionsVersion {
			maxAPIExtensionsVersion = node.APIExtensions
		}

		if minAPIExtensionsVersion == 0 || node.APIExtensions < minAPIExtensionsVersion {
			minAPIExtensionsVersion = node.APIExtensions
		}

		if node.Schema > maxSchemaVersion {
			maxSchemaVersion = node.Schema
		}
	}

	hbState.Version = APIHeartbeatVersion{
		Schema:           maxSchemaVersion,
		APIExtensions:    maxAPIExtensionsVersion,
		MinAPIExtensions: minAPIExtensionsVersion,
	}

	if len(raftNodeMap) > 0 && hbState.cluster != nil {
		_ = transaction.Do(context.TODO(), hbState.cluster, func(ctx context.Context) error {
			tx := hbState.cluster
			for addr, raftNode := range raftNodeMap {
				_, err := tx.GetPendingNodeByAddress(ctx, addr)
				if err != nil {
					slog.Error("Unaccounted raft node(s) not found in 'nodes' table for heartbeat", "id", raftNode.ID, "address", raftNode.Address)
				}
			}

			return nil
		})
	}
}

// Send sends heartbeat requests to the nodes supplied and updates heartbeat state.
func (hbState *APIHeartbeat) Send(ctx context.Context, databaseEndpoint string, networkCert *incustls.CertInfo, serverCert *incustls.CertInfo, localAddress string, nodes []db.NodeInfo, spreadDuration time.Duration) {
	// Find the local member name for warning management.
	var localName string
	for _, node := range nodes {
		if node.Address == localAddress {
			localName = node.Name
			break
		}
	}

	heartbeatsWg := sync.WaitGroup{}
	sendHeartbeat := func(nodeID int64, name string, address string, spreadDuration time.Duration, heartbeatData *APIHeartbeat) {
		defer heartbeatsWg.Done()

		if spreadDuration > 0 {
			// Spread in time by waiting up to 3s less than the interval.
			spreadDurationMs := int(spreadDuration.Milliseconds())
			spreadRange := spreadDurationMs - 3000

			if spreadRange > 0 {
				select {
				case <-time.After(time.Duration(rand.Intn(spreadRange)) * time.Millisecond):
				case <-ctx.Done(): // Proceed immediately to heartbeat of member if asked to.
				}
			}
		}

		// Update timestamp to current, used for time skew detection
		heartbeatData.Time = time.Now().UTC()

		// Don't use ctx here, as we still want to finish off the request if the ctx has been cancelled.
		err := HeartbeatNode(context.Background(), databaseEndpoint, address, networkCert, serverCert, heartbeatData)
		if err == nil {
			heartbeatData.Lock()
			// Ensure only update nodes that exist in Members already.
			hbNode, existing := hbState.Members[nodeID]
			if !existing {
				return
			}

			hbNode.LastHeartbeat = time.Now()
			hbNode.Online = true
			hbNode.Updated = true
			heartbeatData.Members[nodeID] = hbNode
			heartbeatData.Unlock()
			slog.Debug("Successful heartbeat", "remote", address)

			err = hbState.cluster.ResolveOfflineMemberWarning(context.TODO(), nodeID, localName)
			if err != nil {
				slog.Warn("Failed to resolve warning", "err", err)
			}
		} else {
			slog.Warn("Cluster member isn't responding", "name", name, "err", err)

			if ctx.Err() == nil {
				err = hbState.cluster.EmitOfflineMemberWarning(context.TODO(), nodeID, localName, err.Error())
				if err != nil {
					slog.Warn("Failed to create warning", "err", err)
				}
			}
		}
	}

	for _, node := range nodes {
		// Special case for the local member - just record the time now.
		if node.Address == localAddress {
			hbState.Lock()
			hbNode := hbState.Members[node.ID]
			hbNode.LastHeartbeat = time.Now()
			hbNode.Online = true
			hbNode.Updated = true
			hbState.Members[node.ID] = hbNode
			hbState.Unlock()
			continue
		}

		// Parallelize the rest.
		heartbeatsWg.Add(1)
		go sendHeartbeat(node.ID, node.Name, node.Address, spreadDuration, hbState)
	}

	heartbeatsWg.Wait()
}

// HeartbeatNode performs a single heartbeat request against the node with the given address.
func HeartbeatNode(taskCtx context.Context, databaseEndpoint string, address string, networkCert *incustls.CertInfo, serverCert *incustls.CertInfo, heartbeatData *APIHeartbeat) error {
	slog.Debug("Sending heartbeat request", "address", address)

	timeout := 2 * time.Second
	url := fmt.Sprintf("https://%s%s", address, databaseEndpoint)

	transport, cleanup, err := tls.Transport(networkCert, serverCert)
	if err != nil {
		return err
	}

	defer cleanup()

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	buffer := bytes.Buffer{}
	heartbeatData.Lock()
	err = json.NewEncoder(&buffer).Encode(heartbeatData)
	heartbeatData.Unlock()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewReader(buffer.Bytes()))
	if err != nil {
		return err
	}

	api.SetCOWSQLVersionHeader(req)

	// Use 1s later timeout to give HTTP client chance timeout with more useful info.
	ctx, cancel := context.WithTimeout(taskCtx, timeout+time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	req.Close = true // Immediately close the connection after the request is done

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to send heartbeat request: %w", err)
	}

	defer func() {
		err := resp.Body.Close()
		if err != nil {
			slog.Warn("Failed to close response body", "err", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Heartbeat request failed with status: %w", incusapi.StatusErrorf(resp.StatusCode, "%s", resp.Status))
	}

	return nil
}
