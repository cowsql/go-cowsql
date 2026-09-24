//go:build !nosqlite3 && !darwin

package example

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/cowsql/go-cowsql/cluster"
	"github.com/cowsql/go-cowsql/cluster/db"
	"github.com/cowsql/go-cowsql/cluster/db/transaction"
	exampletls "github.com/cowsql/go-cowsql/cluster/example/tls"
	"github.com/cowsql/go-cowsql/cluster/heartbeat"
	"github.com/cowsql/go-cowsql/cluster/membership"
	cowsqltls "github.com/cowsql/go-cowsql/cluster/tls"
)

// ListGet is the response body for /.
type ListGet struct {
	Name          string `json:"name"`
	Address       string `json:"address"`
	Role          string `json:"role"`
	LastHeartbeat string `json:"last_heartbeat"`
	Offline       bool   `json:"offline"`
}

// AssignPost is the request body for /assign.
type AssignPost struct {
	Nodes []db.RaftNode `json:"nodes"`
}

// AcceptResponse is the response body from /accept.
type AcceptResponse struct {
	PublicKey  string `json:"public_key"`
	PrivateKey string `json:"private_key"`

	CurrentMembers []db.RaftNode            `json:"nodes"`
	Certificates   []exampletls.Certificate `json:"certificates"`
}

// AcceptPost is the request body for /accept.
type AcceptPost struct {
	ServerCert     exampletls.Certificate `json:"server_cert"`
	ServerName     string                 `json:"server_name"`
	ClusterAddress string                 `json:"cluster_address"`
}

// JoinPost is the request body for /join.
type JoinPost struct {
	ClusterAddress string `json:"cluster_address"`
}

func (d *Daemon) apiHandlers() *http.ServeMux {
	serveMux := http.NewServeMux()

	serveMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if d.gateway == nil || d.gateway.Cluster() == nil {
			w.WriteHeader(http.StatusTooEarly)
			_, _ = w.Write([]byte("Daemon is initializing"))

			return
		}

		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNotImplemented)
			_, _ = fmt.Fprintf(w, "%q not implemented on %q", r.Method, r.URL.Path)

			return
		}

		var members []db.NodeInfo

		err := transaction.Do(r.Context(), d.gateway.Cluster(), func(ctx context.Context) error {
			var err error

			members, err = d.gateway.Cluster().GetNodes(ctx)

			return err
		})
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		var raftMembers []db.RaftNode

		err = transaction.Do(r.Context(), d.gateway.Node(), func(ctx context.Context) error {
			var err error

			raftMembers, err = d.gateway.Node().GetRaftNodes(ctx)

			return err
		})
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		data := make([]ListGet, 0, len(raftMembers))

		for _, m := range members {
			var role string

			for _, raft := range raftMembers {
				if raft.Address == m.Address {
					role = raft.Role.String()

					break
				}
			}

			data = append(data, ListGet{
				Name:          m.Name,
				Address:       m.Address,
				Role:          role,
				LastHeartbeat: m.Heartbeat.String(),
				Offline:       m.IsOffline(d.gateway.HeartbeatOfflineThreshold()),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(data)
	})

	for p, f := range d.gateway.HandlerFuncs(Authorizer(d.gateway.NetworkCert, d.gateway.ServerCert, d.getTrustedCerts)) {
		serveMux.HandleFunc(p, f)
	}

	serveMux.HandleFunc("/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Method != http.MethodPost {
			errResponse(w, http.StatusNotFound, fmt.Errorf("%s: %s not found", r.Method, r.URL.Path))

			return
		}

		slog.Info("Bootstrap request")

		err := membership.Bootstrap(d.gateway, d.serverName)
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		w.WriteHeader(http.StatusOK)
	})

	serveMux.HandleFunc("/accept", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Method != http.MethodPost {
			errResponse(w, http.StatusNotFound, fmt.Errorf("%s: %s not found", r.Method, r.URL.Path))

			return
		}

		slog.Info("Accept request")

		enabled, err := membership.Enabled(d.gateway.Node())
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		if !enabled {
			errResponse(w, http.StatusBadRequest, errors.New("Clustering is not enabled, can't report on cluster data"))

			return
		}

		var joiner AcceptPost

		err = json.NewDecoder(r.Body).Decode(&joiner)
		if err != nil {
			errResponse(w, http.StatusBadRequest, err)

			return
		}

		// Always use 1/1/1 for schema version, API version, and architectures, since these aren't handled by the example.
		// In practice you will likely want proper versioning to handle backwards compatibility.
		currentNodes, err := membership.Accept(d.gateway, joiner.ServerCert.Certificate, joiner.ServerName, joiner.ClusterAddress, 1, 1, 1)
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		certCache := d.getTrustedCerts()
		info := AcceptResponse{
			PublicKey:      string(d.gateway.NetworkCert().PublicKey()),
			PrivateKey:     string(d.gateway.NetworkCert().PrivateKey()),
			CurrentMembers: currentNodes,
			Certificates:   make([]exampletls.Certificate, 0, len(certCache)),
		}

		for _, c := range certCache {
			info.Certificates = append(info.Certificates, exampletls.Certificate{Certificate: &c})
		}

		w.Header().Set("Content-Type", "application/json")

		err = json.NewEncoder(w).Encode(info) //nolint:gosec
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}
	})

	serveMux.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Method != http.MethodPost {
			errResponse(w, http.StatusNotFound, fmt.Errorf("%s: %s not found", r.Method, r.URL.Path))

			return
		}

		slog.Info("Join request")

		var target JoinPost

		err := json.NewDecoder(r.Body).Decode(&target)
		if err != nil {
			errResponse(w, http.StatusBadRequest, err)

			return
		}

		serverCertx509, err := d.gateway.ServerCert().PublicKeyX509()
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		joinerInfo := AcceptPost{
			ServerCert:     exampletls.Certificate{Certificate: serverCertx509},
			ServerName:     d.serverName,
			ClusterAddress: d.clusterAddress,
		}

		b, err := json.Marshal(joinerInfo)
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, "https://"+target.ClusterAddress+"/accept", bytes.NewBuffer(b))
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		c := http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec

		resp, err := c.Do(req)
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			errResponse(w, http.StatusInternalServerError, fmt.Errorf("Got error code %d from https://%s/accept", resp.StatusCode, target.ClusterAddress))

			return
		}

		var clusterInfo AcceptResponse

		err = json.NewDecoder(resp.Body).Decode(&clusterInfo)
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		cert, err := tls.X509KeyPair([]byte(clusterInfo.PublicKey), []byte(clusterInfo.PrivateKey))
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		d.setCertificateCache(clusterInfo.Certificates)

		err = membership.Join[any](d.gateway, cert, d.serverName, clusterInfo.CurrentMembers)
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		w.WriteHeader(http.StatusOK)
	})

	serveMux.HandleFunc("/assign", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if r.Method != http.MethodPost {
			errResponse(w, http.StatusNotFound, fmt.Errorf("%s: %s not found", r.Method, r.URL.Path))

			return
		}

		slog.Info("Assign request")

		var req AssignPost

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			errResponse(w, http.StatusBadRequest, err)

			return
		}

		// Quick checks.
		if len(req.Nodes) == 0 {
			errResponse(w, http.StatusBadRequest, errors.New("Empty assign request"))

			return
		}

		err = membership.Assign(d.gateway, req.Nodes)
		if err != nil {
			errResponse(w, http.StatusInternalServerError, err)

			return
		}

		w.WriteHeader(http.StatusOK)
	})

	return serveMux
}

func (d *Daemon) heartbeatHook(heartbeatData *heartbeat.APIHeartbeat, isLeader bool, unavailableMembers []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	globalCerts, err := d.getGlobalCertificates(ctx)
	if err != nil {
		slog.Error("Failed to fetch global certificates", slog.Any("error", err))

		return
	}

	for addr, cert := range globalCerts {
		err := d.updateLocalCertificate(ctx, addr, cert)
		if err != nil {
			slog.Error("Failed to update cached local certificates", slog.String("address", addr), slog.Any("error", err))

			return
		}
	}

	d.hbMu.Lock()
	defer d.hbMu.Unlock()

	membership.RebalanceMembersHook(ctx, d.gateway, d.clusterAddress, heartbeatData, isLeader, unavailableMembers, nil, d.changeMemberRoles)
}

// Post a change role request to the member with the given address. The nodes
// slice contains details about all members, including the one being changed.
func (d *Daemon) changeMemberRoles(ctx context.Context, address string, nodes []db.RaftNode) error {
	post := &AssignPost{Nodes: nodes}

	b, err := json.Marshal(post)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+address+"/assign", bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	c := http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec

	resp, err := c.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return err
}

// Authorizer validates that the connection is from a trusted server.
func Authorizer(networkCertFunc, serverCertFunc func() cowsqltls.CertInfo, getTrustedCerts func() map[string]x509.Certificate) func(w http.ResponseWriter, r *http.Request) bool {
	return func(w http.ResponseWriter, r *http.Request) bool {
		certs := getTrustedCerts()
		if !exampletls.CheckCert(r, networkCertFunc(), serverCertFunc(), certs) {
			http.Error(w, "403 invalid client certificate", http.StatusForbidden)

			return false
		}

		return true
	}
}

// ClusterHeartbeatTask triggers a looping heartbeat.
func ClusterHeartbeatTask(ctx context.Context, gateway cluster.Gateway) {
	clusterTask := func(ctx context.Context) {
		if gateway.HeartbeatCancelFunc() == nil {
			ch := make(chan struct{})

			go func() {
				gateway.Heartbeat(ctx, heartbeat.HeartbeatNormal)
				close(ch)
			}()

			select {
			case <-ch:
			case <-ctx.Done():
			}
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		clusterTask(ctx)
		timeoutCtx, cancel := context.WithTimeout(ctx, gateway.HeartbeatInterval())
		<-timeoutCtx.Done()
		cancel()
	}
}

func errResponse(w http.ResponseWriter, code int, err error) {
	var msg string
	if err != nil {
		msg = err.Error()
	}

	slog.Error("API Error", slog.Int("code", code), slog.String("error", msg))
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(code)
	_, _ = w.Write([]byte("API Error: " + msg))
}
