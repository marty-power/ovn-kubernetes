// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	nodecontroller "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/controllers/node"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

const (
	// minBatchDelay is the minimum delay before processing (0 = immediate)
	minBatchDelay = 1 * time.Millisecond
	// maxBatchDelay is the maximum delay when batching under high load
	maxBatchDelay = 100 * time.Millisecond
	// backoffMultiplier is how much to increase the delay when requests arrive in quick succession
	backoffMultiplier = 2
	// backoffThreshold is the time window to consider requests as "quick succession"
	backoffThreshold = 100 * time.Millisecond
)

// NodeAnnotationBatcher batches node annotation updates from multiple networks
// and applies them in a single API call with dynamic backoff. When idle, it processes
// immediately. When requests arrive in quick succession, it backs off to batch them.
// This significantly reduces API server load when multiple UDNs are being created
// or reconciled simultaneously while maintaining low latency for individual updates.
type NodeAnnotationBatcher struct {
	kube       kube.Interface
	nodeLister listers.NodeLister

	// Pending updates per node
	pendingUpdates map[string]*pendingNodeUpdate
	mutex          sync.Mutex

	// Dynamic backoff state
	currentDelay    time.Duration
	lastEnqueueTime time.Time
	triggerChan     chan struct{}

	// Channels for lifecycle management
	stopChan chan struct{}
	wg       *sync.WaitGroup
	stopping bool // Set during shutdown to prevent requeueing failures

	// Cache for parsed node annotations to avoid redundant parsing during retries
	cache *nodecontroller.NodeAnnotationCache
}

// pendingNodeUpdate accumulates annotation updates for a single node
// from potentially multiple networks before batching them together
type pendingNodeUpdate struct {
	// Per-network updates - stored as network name -> value
	hostSubnets map[string][]*net.IPNet // network -> host subnets
	networkIDs  map[string]int          // network -> network ID (types.NoNetworkID means don't update)
	tunnelIDs   map[string]int          // network -> tunnel ID (types.NoTunnelID means don't update)
}

func NewNodeAnnotationBatcher(kube kube.Interface, nodeLister listers.NodeLister, stopChan chan struct{}, wg *sync.WaitGroup) *NodeAnnotationBatcher {
	return &NodeAnnotationBatcher{
		kube:           kube,
		nodeLister:     nodeLister,
		pendingUpdates: make(map[string]*pendingNodeUpdate),
		currentDelay:   minBatchDelay,
		triggerChan:    make(chan struct{}, 1),
		stopChan:       stopChan,
		wg:             wg,
		cache:          nodecontroller.NewNodeAnnotationCache(),
	}
}

func (b *NodeAnnotationBatcher) Start() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()

		for {
			select {
			case <-b.triggerChan:
				// Calculate delay based on whether we're in quick succession
				b.mutex.Lock()
				delay := b.currentDelay
				b.mutex.Unlock()

				// Wait for the calculated delay
				if delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
						// Delay complete, process batch
					case <-b.stopChan:
						timer.Stop()
						// Set stopping flag to prevent requeuing failures during drain
						b.mutex.Lock()
						b.stopping = true
						queueLength := len(b.pendingUpdates)
						b.mutex.Unlock()
						// Drain all pending updates (no retries due to stopping flag)
						for queueLength > 0 {
							b.processBatch()
							b.mutex.Lock()
							queueLength = len(b.pendingUpdates)
							b.mutex.Unlock()
						}
						klog.Info("Node annotation batcher stopped")
						return
					}
				}

				b.processBatch()

			case <-b.stopChan:
				// Set stopping flag to prevent requeuing failures during drain
				b.mutex.Lock()
				b.stopping = true
				queueLength := len(b.pendingUpdates)
				b.mutex.Unlock()
				// Drain all pending updates (no retries due to stopping flag)
				for queueLength > 0 {
					b.processBatch()
					b.mutex.Lock()
					queueLength = len(b.pendingUpdates)
					b.mutex.Unlock()
				}
				klog.Info("Node annotation batcher stopped")
				return
			}
		}
	}()
	klog.Infof("Node annotation batcher started with dynamic backoff (min=%v, max=%v)", minBatchDelay, maxBatchDelay)
}

// EnqueueUpdate queues an annotation update for a node from a specific network.
// Multiple updates for the same node are merged and applied together.
// Uses dynamic backoff: processes immediately when idle, backs off when requests
// arrive in quick succession.
func (b *NodeAnnotationBatcher) EnqueueUpdate(nodeName, networkName string, hostSubnets []*net.IPNet, networkID, tunnelID int) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// Get or create pending update for this node
	pending, ok := b.pendingUpdates[nodeName]
	if !ok {
		pending = &pendingNodeUpdate{
			hostSubnets: make(map[string][]*net.IPNet),
			networkIDs:  make(map[string]int),
			tunnelIDs:   make(map[string]int),
		}
		b.pendingUpdates[nodeName] = pending
	}

	// Store the updates for this network
	// Note: nil or empty subnet slices signal deletion and must be stored
	pending.hostSubnets[networkName] = hostSubnets
	if networkID != types.NoNetworkID {
		pending.networkIDs[networkName] = networkID
	}
	if tunnelID != types.NoTunnelID {
		pending.tunnelIDs[networkName] = tunnelID
	}

	// Update backoff based on request rate
	now := time.Now()
	timeSinceLastEnqueue := now.Sub(b.lastEnqueueTime)

	if timeSinceLastEnqueue < backoffThreshold && !b.lastEnqueueTime.IsZero() {
		// Requests are arriving in quick succession - increase delay
		b.currentDelay = b.currentDelay * backoffMultiplier
		if b.currentDelay > maxBatchDelay {
			b.currentDelay = maxBatchDelay
		}
		klog.V(5).Infof("Increased batch delay to %v due to quick succession (gap=%v)", b.currentDelay, timeSinceLastEnqueue)
	} else {
		// Requests have slowed down or this is the first request - reset to minimum
		b.currentDelay = minBatchDelay
		klog.V(5).Infof("Reset batch delay to %v (gap=%v)", b.currentDelay, timeSinceLastEnqueue)
	}
	b.lastEnqueueTime = now

	klog.V(5).Infof("Enqueued annotation update for node %s network %s (subnets=%v, netID=%v, tunnelID=%v, delay=%v)",
		nodeName, networkName, len(hostSubnets), networkID, tunnelID, b.currentDelay)

	// Trigger batch processing (non-blocking)
	select {
	case b.triggerChan <- struct{}{}:
	default:
		// Already triggered, no need to send again
	}
}

// processBatch processes all pending updates, merging updates for the same node
// and applying them in a single API call per node
func (b *NodeAnnotationBatcher) processBatch() {
	b.mutex.Lock()
	pendingUpdates := b.pendingUpdates
	b.pendingUpdates = make(map[string]*pendingNodeUpdate)
	stopping := b.stopping
	b.mutex.Unlock()

	if len(pendingUpdates) == 0 {
		return
	}

	klog.V(4).Infof("Processing batch of %d node annotation updates", len(pendingUpdates))

	for nodeName, pending := range pendingUpdates {
		if err := b.updateNodeAnnotations(nodeName, pending); err != nil {
			klog.Errorf("Failed to update node %s annotations in batch: %v", nodeName, err)
			if !stopping {
				b.requeueUpdate(nodeName, pending)
			} else {
				klog.Warningf("Dropping failed update for node %s during shutdown", nodeName)
			}
		}
	}
}

// updateNodeAnnotations applies all pending updates for a node using retry logic
func (b *NodeAnnotationBatcher) updateNodeAnnotations(nodeName string, pending *pendingNodeUpdate) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node, err := b.nodeLister.Get(nodeName)
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", nodeName, err)
		}

		cnode := node.DeepCopy()

		existingSubnets, err := b.parseSubnets(cnode)
		if err != nil && !util.IsAnnotationNotSetError(err) {
			return fmt.Errorf("failed to parse existing node-subnets for node %s: %w", nodeName, err)
		} else if existingSubnets == nil {
			existingSubnets = make(map[string][]*net.IPNet)
		}

		existingNetworkIDs, err := b.parseNetworkIDs(cnode)
		if err != nil && !util.IsAnnotationNotSetError(err) {
			return fmt.Errorf("failed to parse existing network-ids for node %s: %w", nodeName, err)
		} else if existingNetworkIDs == nil {
			existingNetworkIDs = make(map[string]int)
		}

		existingTunnelIDs, err := b.parseTunnelIDs(cnode)
		if err != nil && !util.IsAnnotationNotSetError(err) {
			return fmt.Errorf("failed to parse existing tunnel-ids for node %s: %w", nodeName, err)
		} else if existingTunnelIDs == nil {
			existingTunnelIDs = make(map[string]int)
		}

		subnetsUpdated := false
		networkIDsUpdated := false
		tunnelIDsUpdated := false

		// Update host subnets
		for networkName, subnets := range pending.hostSubnets {
			existingSubnets[networkName] = subnets
			subnetsUpdated = true
		}
		if subnetsUpdated {
			cnode.Annotations, err = util.UpdateNodeHostSubnetAnnotationMap(cnode.Annotations, existingSubnets)
			if err != nil {
				return fmt.Errorf("failed to update node-subnets annotation for node %s: %w", nodeName, err)
			}
		}

		// Update network IDs
		for networkName, networkID := range pending.networkIDs {
			if networkID != types.NoNetworkID {
				existingNetworkIDs[networkName] = networkID
				networkIDsUpdated = true
			}
		}
		if networkIDsUpdated {
			cnode.Annotations, err = util.UpdateNetworkIDAnnotationMap(cnode.Annotations, existingNetworkIDs)
			if err != nil {
				return fmt.Errorf("failed to update network-ids annotation for node %s: %w", nodeName, err)
			}
		}

		// Update tunnel IDs
		for networkName, tunnelID := range pending.tunnelIDs {
			if tunnelID != types.NoTunnelID {
				existingTunnelIDs[networkName] = tunnelID
				tunnelIDsUpdated = true
			}
		}
		if tunnelIDsUpdated {
			cnode.Annotations, err = util.UpdateUDNLayer2NodeGRLRPTunnelIDsMap(cnode.Annotations, existingTunnelIDs)
			if err != nil {
				return fmt.Errorf("failed to update tunnel-ids annotation for node %s: %w", nodeName, err)
			}
		}

		klog.V(4).Infof("Updating node %s with batched annotations (%d networks)", nodeName, len(pending.hostSubnets)+len(pending.networkIDs)+len(pending.tunnelIDs))
		return b.kube.UpdateNodeStatus(cnode)
	})
}

func (b *NodeAnnotationBatcher) parseSubnets(node *corev1.Node) (map[string][]*net.IPNet, error) {
	return b.cache.ParseSubnetMapCached(node, types.NodeSubnetsAnnotation, true)
}

func (b *NodeAnnotationBatcher) parseNetworkIDs(node *corev1.Node) (map[string]int, error) {
	networkIDsStrMap, err := b.cache.ParseNetworkMapCached(node, util.OvnNetworkIDs, true)
	if err != nil {
		return nil, err
	}

	// Convert string values to integers
	networkIDsMap := make(map[string]int, len(networkIDsStrMap))
	for netName, idStr := range networkIDsStrMap {
		id, e := strconv.Atoi(idStr)
		if e == nil {
			networkIDsMap[netName] = id
		}
	}

	return networkIDsMap, nil
}

func (b *NodeAnnotationBatcher) parseTunnelIDs(node *corev1.Node) (map[string]int, error) {
	tunnelIDsStrMap, err := b.cache.ParseNetworkMapCached(node, types.UDNLayer2NodeGRLRPTunnelIDAnnotation, true)
	if err != nil {
		return nil, err
	}

	result := make(map[string]int, len(tunnelIDsStrMap))
	for netName, idStr := range tunnelIDsStrMap {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tunnel ID for network %s: %v", netName, err)
		}
		result[netName] = id
	}
	return result, nil
}

// requeueUpdate re-enqueues a failed update for retry in the next batch
func (b *NodeAnnotationBatcher) requeueUpdate(nodeName string, pending *pendingNodeUpdate) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	existing, ok := b.pendingUpdates[nodeName]
	if !ok {
		b.pendingUpdates[nodeName] = pending
	} else {
		// Merge requeued data only where no newer update exists
		for networkName, subnets := range pending.hostSubnets {
			if _, exists := existing.hostSubnets[networkName]; !exists {
				existing.hostSubnets[networkName] = subnets
			}
		}
		for networkName, networkID := range pending.networkIDs {
			if _, exists := existing.networkIDs[networkName]; !exists {
				existing.networkIDs[networkName] = networkID
			}
		}
		for networkName, tunnelID := range pending.tunnelIDs {
			if _, exists := existing.tunnelIDs[networkName]; !exists {
				existing.tunnelIDs[networkName] = tunnelID
			}
		}
	}

	// Notify the worker to retry (non-blocking to avoid deadlock)
	select {
	case b.triggerChan <- struct{}{}:
	default:
		// Channel already has a pending trigger, no need to send again
	}
}
