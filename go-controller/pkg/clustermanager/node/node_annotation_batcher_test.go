// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

var (
	subnet1  = &net.IPNet{IP: net.ParseIP("10.1.0.0"), Mask: net.CIDRMask(24, 32)}
	subnet2  = &net.IPNet{IP: net.ParseIP("10.2.0.0"), Mask: net.CIDRMask(24, 32)}
	subnet3  = &net.IPNet{IP: net.ParseIP("10.3.0.0"), Mask: net.CIDRMask(24, 32)}
	subnet4  = &net.IPNet{IP: net.ParseIP("10.4.0.0"), Mask: net.CIDRMask(24, 32)}
	subnet5  = &net.IPNet{IP: net.ParseIP("10.5.0.0"), Mask: net.CIDRMask(24, 32)}
	subnet99 = &net.IPNet{IP: net.ParseIP("10.99.0.0"), Mask: net.CIDRMask(24, 32)}
)

func newTestNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: map[string]string{},
		},
	}
}

// setupBatcherTest creates the test fixtures needed for batcher tests
func setupBatcherTest(t *testing.T, nodes ...*corev1.Node) (*NodeAnnotationBatcher, *fake.Clientset, chan struct{}, *sync.WaitGroup) {
	t.Helper()
	fakeClient := fake.NewSimpleClientset()

	// Create nodes in the fake client
	for _, node := range nodes {
		_, err := fakeClient.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", node.Name, err)
		}
	}

	kubeInterface := &kube.Kube{KClient: fakeClient}

	stopCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	batcher := NewNodeAnnotationBatcher(kubeInterface, fakeClient, stopCh, wg)

	return batcher, fakeClient, stopCh, wg
}

// TestNodeAnnotationBatcher_BasicBatching tests that multiple updates across multiple nodes are batched
func TestNodeAnnotationBatcher_BasicBatching(t *testing.T) {
	node1 := newTestNode("node1")
	node2 := newTestNode("node2")
	node3 := newTestNode("node3")

	batcher, fakeClient, stopCh, wg := setupBatcherTest(t, node1, node2, node3)
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	batcher.Start()

	// Enqueue updates for multiple nodes with multiple networks each
	batcher.EnqueueUpdate("node1", "network1", []*net.IPNet{subnet1}, 1, types.NoTunnelID)
	batcher.EnqueueUpdate("node1", "network2", []*net.IPNet{subnet2}, 2, types.NoTunnelID)
	batcher.EnqueueUpdate("node2", "network1", []*net.IPNet{subnet3}, 1, types.NoTunnelID)
	batcher.EnqueueUpdate("node2", "network3", []*net.IPNet{subnet4}, 3, types.NoTunnelID)
	batcher.EnqueueUpdate("node3", "network1", []*net.IPNet{subnet5}, 1, types.NoTunnelID)

	time.Sleep(maxBatchDelay + 100*time.Millisecond)

	// Verify node1 annotations
	updatedNode1, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node1: %v", err)
	}
	subnets1, err := util.ParseNodeHostSubnetsAnnotation(updatedNode1)
	if err != nil {
		t.Fatalf("Failed to parse node1 subnet annotation: %v", err)
	}
	if len(subnets1) != 2 {
		t.Fatalf("node1: expected 2 networks, got %d", len(subnets1))
	}
	if _, ok := subnets1["network1"]; !ok {
		t.Error("node1: network1 subnet not found")
	}
	if _, ok := subnets1["network2"]; !ok {
		t.Error("node1: network2 subnet not found")
	}

	networkIDs1, err := util.GetNodeNetworkIDsAnnotationNetworkIDs(updatedNode1)
	if err != nil {
		t.Fatalf("Failed to parse node1 network IDs: %v", err)
	}
	if networkIDs1["network1"] != 1 {
		t.Errorf("node1: expected network1 ID 1, got %d", networkIDs1["network1"])
	}
	if networkIDs1["network2"] != 2 {
		t.Errorf("node1: expected network2 ID 2, got %d", networkIDs1["network2"])
	}

	// Verify node2 annotations
	updatedNode2, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node2: %v", err)
	}
	subnets2, err := util.ParseNodeHostSubnetsAnnotation(updatedNode2)
	if err != nil {
		t.Fatalf("Failed to parse node2 subnet annotation: %v", err)
	}
	if len(subnets2) != 2 {
		t.Fatalf("node2: expected 2 networks, got %d", len(subnets2))
	}
	if _, ok := subnets2["network1"]; !ok {
		t.Error("node2: network1 subnet not found")
	}
	if _, ok := subnets2["network3"]; !ok {
		t.Error("node2: network3 subnet not found")
	}

	networkIDs2, err := util.GetNodeNetworkIDsAnnotationNetworkIDs(updatedNode2)
	if err != nil {
		t.Fatalf("Failed to parse node2 network IDs: %v", err)
	}
	if networkIDs2["network1"] != 1 {
		t.Errorf("node2: expected network1 ID 1, got %d", networkIDs2["network1"])
	}
	if networkIDs2["network3"] != 3 {
		t.Errorf("node2: expected network3 ID 3, got %d", networkIDs2["network3"])
	}

	// Verify node3 annotations
	updatedNode3, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node3", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node3: %v", err)
	}
	subnets3, err := util.ParseNodeHostSubnetsAnnotation(updatedNode3)
	if err != nil {
		t.Fatalf("Failed to parse node3 subnet annotation: %v", err)
	}
	if len(subnets3) != 1 {
		t.Fatalf("node3: expected 1 network, got %d", len(subnets3))
	}
	if _, ok := subnets3["network1"]; !ok {
		t.Error("node3: network1 subnet not found")
	}

	// Verify batching: should have exactly 3 UpdateStatus actions (one per node, not one per network)
	actions := fakeClient.Actions()
	updateCount := 0
	for _, action := range actions {
		if action.GetVerb() == "update" && action.GetResource().Resource == "nodes" && action.GetSubresource() == "status" {
			updateCount++
		}
	}
	if updateCount != 3 {
		t.Errorf("Expected exactly 3 UpdateStatus actions (one per node), got %d", updateCount)
	}
}

// TestNodeAnnotationBatcher_UpdateMerging tests that multiple updates to the same node/network are merged across multiple nodes
func TestNodeAnnotationBatcher_UpdateMerging(t *testing.T) {
	node1 := newTestNode("node1")
	node2 := newTestNode("node2")

	batcher, fakeClient, stopCh, wg := setupBatcherTest(t, node1, node2)
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	batcher.Start()

	// Enqueue two updates for same network on each node - second should override first
	batcher.EnqueueUpdate("node1", "blue", []*net.IPNet{subnet1}, 1, types.NoTunnelID)
	batcher.EnqueueUpdate("node1", "blue", []*net.IPNet{subnet2}, 2, types.NoTunnelID)
	batcher.EnqueueUpdate("node2", "blue", []*net.IPNet{subnet3}, 3, types.NoTunnelID)
	batcher.EnqueueUpdate("node2", "blue", []*net.IPNet{subnet4}, 4, types.NoTunnelID)

	time.Sleep(maxBatchDelay + 100*time.Millisecond)

	// Verify node1 has second subnet (merged/overwritten)
	updatedNode1, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node1: %v", err)
	}
	subnets1, err := util.ParseNodeHostSubnetsAnnotation(updatedNode1)
	if err != nil {
		t.Fatalf("Failed to parse node1 subnet annotation: %v", err)
	}
	blueSubnets1 := subnets1["blue"]
	if len(blueSubnets1) != 1 {
		t.Fatalf("node1: expected 1 subnet for blue, got %d", len(blueSubnets1))
	}
	if blueSubnets1[0].String() != subnet2.String() {
		t.Errorf("node1: expected subnet %s, got %s", subnet2.String(), blueSubnets1[0].String())
	}
	networkIDs1, err := util.GetNodeNetworkIDsAnnotationNetworkIDs(updatedNode1)
	if err != nil {
		t.Fatalf("Failed to parse node1 network IDs: %v", err)
	}
	if networkIDs1["blue"] != 2 {
		t.Errorf("node1: expected blue network ID 2, got %d", networkIDs1["blue"])
	}

	// Verify node2 has second subnet (merged/overwritten)
	updatedNode2, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node2: %v", err)
	}
	subnets2, err := util.ParseNodeHostSubnetsAnnotation(updatedNode2)
	if err != nil {
		t.Fatalf("Failed to parse node2 subnet annotation: %v", err)
	}
	blueSubnets2 := subnets2["blue"]
	if len(blueSubnets2) != 1 {
		t.Fatalf("node2: expected 1 subnet for blue, got %d", len(blueSubnets2))
	}
	if blueSubnets2[0].String() != subnet4.String() {
		t.Errorf("node2: expected subnet %s, got %s", subnet4.String(), blueSubnets2[0].String())
	}
	networkIDs2, err := util.GetNodeNetworkIDsAnnotationNetworkIDs(updatedNode2)
	if err != nil {
		t.Fatalf("Failed to parse node2 network IDs: %v", err)
	}
	if networkIDs2["blue"] != 4 {
		t.Errorf("node2: expected blue network ID 4, got %d", networkIDs2["blue"])
	}

	// Verify batching: 2 UpdateStatus actions (one per node)
	actions := fakeClient.Actions()
	updateCount := 0
	for _, action := range actions {
		if action.GetVerb() == "update" && action.GetResource().Resource == "nodes" && action.GetSubresource() == "status" {
			updateCount++
		}
	}
	if updateCount != 2 {
		t.Errorf("Expected exactly 2 UpdateStatus actions (one per node), got %d", updateCount)
	}
}

// TestNodeAnnotationBatcher_StopProcessesPendingUpdates tests that stopping the batcher processes remaining updates across multiple nodes
func TestNodeAnnotationBatcher_StopProcessesPendingUpdates(t *testing.T) {
	node1 := newTestNode("node1")
	node2 := newTestNode("node2")

	batcher, fakeClient, stopCh, wg := setupBatcherTest(t, node1, node2)

	// Enqueue updates for multiple nodes before starting
	batcher.EnqueueUpdate("node1", "blue", []*net.IPNet{subnet1}, 1, types.NoTunnelID)
	batcher.EnqueueUpdate("node2", "red", []*net.IPNet{subnet2}, 2, types.NoTunnelID)

	batcher.Start()

	// Stop immediately without waiting for normal batch processing
	close(stopCh)
	wg.Wait()

	// Verify both nodes got their updates
	updatedNode1, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node1: %v", err)
	}
	subnets1, err := util.ParseNodeHostSubnetsAnnotation(updatedNode1)
	if err != nil {
		t.Fatalf("Failed to parse node1 subnet annotation: %v", err)
	}
	if _, ok := subnets1["blue"]; !ok {
		t.Error("node1: expected blue network subnet to be present after immediate stop")
	}

	updatedNode2, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node2: %v", err)
	}
	subnets2, err := util.ParseNodeHostSubnetsAnnotation(updatedNode2)
	if err != nil {
		t.Fatalf("Failed to parse node2 subnet annotation: %v", err)
	}
	if _, ok := subnets2["red"]; !ok {
		t.Error("node2: expected red network subnet to be present after immediate stop")
	}
}

// TestNodeAnnotationBatcher_TunnelIDAnnotation tests tunnel ID annotation updates across multiple nodes
func TestNodeAnnotationBatcher_TunnelIDAnnotation(t *testing.T) {
	node1 := newTestNode("node1")
	node2 := newTestNode("node2")

	batcher, fakeClient, stopCh, wg := setupBatcherTest(t, node1, node2)
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	batcher.Start()

	// Enqueue updates with tunnel IDs for multiple nodes
	batcher.EnqueueUpdate("node1", "blue", []*net.IPNet{subnet1}, 1, 100)
	batcher.EnqueueUpdate("node1", "red", []*net.IPNet{subnet2}, 2, 200)
	batcher.EnqueueUpdate("node2", "blue", []*net.IPNet{subnet3}, 1, 300)

	time.Sleep(maxBatchDelay + 100*time.Millisecond)

	// Verify node1 tunnel IDs
	updatedNode1, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node1: %v", err)
	}
	tunnelID1Blue, err := util.ParseUDNLayer2NodeGRLRPTunnelIDs(updatedNode1, "blue")
	if err != nil {
		t.Fatalf("Failed to parse node1 blue tunnel ID: %v", err)
	}
	if tunnelID1Blue != 100 {
		t.Errorf("node1: expected blue tunnel ID 100, got %d", tunnelID1Blue)
	}
	tunnelID1Red, err := util.ParseUDNLayer2NodeGRLRPTunnelIDs(updatedNode1, "red")
	if err != nil {
		t.Fatalf("Failed to parse node1 red tunnel ID: %v", err)
	}
	if tunnelID1Red != 200 {
		t.Errorf("node1: expected red tunnel ID 200, got %d", tunnelID1Red)
	}

	// Verify node2 tunnel ID
	updatedNode2, err := fakeClient.CoreV1().Nodes().Get(context.Background(), "node2", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get node2: %v", err)
	}
	tunnelID2Blue, err := util.ParseUDNLayer2NodeGRLRPTunnelIDs(updatedNode2, "blue")
	if err != nil {
		t.Fatalf("Failed to parse node2 blue tunnel ID: %v", err)
	}
	if tunnelID2Blue != 300 {
		t.Errorf("node2: expected blue tunnel ID 300, got %d", tunnelID2Blue)
	}
}

// TestNodeAnnotationBatcher_RequeueUpdate tests that requeueUpdate merges failed updates back into pending
func TestNodeAnnotationBatcher_RequeueUpdate(t *testing.T) {
	node1 := newTestNode("node1")
	node2 := newTestNode("node2")
	node3 := newTestNode("node3")

	batcher, _, stopCh, wg := setupBatcherTest(t, node1, node2, node3)
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	// Seed pending updates for node1 and node2
	batcher.mutex.Lock()
	batcher.pendingUpdates["node1"] = &pendingNodeUpdate{
		hostSubnets: map[string][]*net.IPNet{"red": {subnet3}},
		networkIDs:  map[string]int{"red": 3},
		tunnelIDs:   map[string]int{"red": 300},
	}
	batcher.pendingUpdates["node2"] = &pendingNodeUpdate{
		hostSubnets: map[string][]*net.IPNet{"green": {subnet4}},
		networkIDs:  map[string]int{"green": 4},
		tunnelIDs:   map[string]int{"green": 400},
	}
	batcher.mutex.Unlock()

	// Requeue failed updates for node1 with different networks
	failed := &pendingNodeUpdate{
		hostSubnets: map[string][]*net.IPNet{
			"blue":  {subnet1},
			"green": {subnet2},
		},
		networkIDs: map[string]int{
			"blue":  1,
			"green": 2,
		},
		tunnelIDs: map[string]int{
			"blue":  100,
			"green": 200,
		},
	}
	batcher.requeueUpdate("node1", failed)

	// Verify node1 has all three networks merged
	batcher.mutex.Lock()
	pending1 := batcher.pendingUpdates["node1"]
	batcher.mutex.Unlock()

	if len(pending1.hostSubnets) != 3 {
		t.Fatalf("node1: expected 3 networks in hostSubnets, got %d", len(pending1.hostSubnets))
	}
	for _, net := range []string{"blue", "green", "red"} {
		if _, ok := pending1.hostSubnets[net]; !ok {
			t.Errorf("node1: missing network %s in merged hostSubnets", net)
		}
	}
	if pending1.networkIDs["blue"] != 1 || pending1.networkIDs["green"] != 2 || pending1.networkIDs["red"] != 3 {
		t.Errorf("node1: unexpected networkIDs after merge: %v", pending1.networkIDs)
	}
	if pending1.tunnelIDs["blue"] != 100 || pending1.tunnelIDs["green"] != 200 || pending1.tunnelIDs["red"] != 300 {
		t.Errorf("node1: unexpected tunnelIDs after merge: %v", pending1.tunnelIDs)
	}

	// Verify node2 is untouched
	batcher.mutex.Lock()
	pending2 := batcher.pendingUpdates["node2"]
	batcher.mutex.Unlock()

	if len(pending2.hostSubnets) != 1 {
		t.Fatalf("node2: expected 1 network, got %d", len(pending2.hostSubnets))
	}
	if _, ok := pending2.hostSubnets["green"]; !ok {
		t.Error("node2: green network missing")
	}

	// Test requeue into empty node (node3)
	newUpdate := &pendingNodeUpdate{
		hostSubnets: map[string][]*net.IPNet{"blue": {subnet1}},
		networkIDs:  map[string]int{"blue": 1},
		tunnelIDs:   map[string]int{"blue": 100},
	}
	batcher.requeueUpdate("node3", newUpdate)

	batcher.mutex.Lock()
	pending3 := batcher.pendingUpdates["node3"]
	batcher.mutex.Unlock()

	if pending3 == nil {
		t.Fatal("Expected node3 in pendingUpdates after requeue")
	}
	if pending3.networkIDs["blue"] != 1 {
		t.Errorf("node3: expected blue networkID 1, got %d", pending3.networkIDs["blue"])
	}

	// Test that requeuing stale data does not overwrite newer pending updates
	batcher.mutex.Lock()
	batcher.pendingUpdates["node1"] = &pendingNodeUpdate{
		hostSubnets: map[string][]*net.IPNet{"blue": {subnet99}},
		networkIDs:  map[string]int{"blue": 99},
		tunnelIDs:   map[string]int{"blue": 999},
	}
	batcher.mutex.Unlock()

	stale := &pendingNodeUpdate{
		hostSubnets: map[string][]*net.IPNet{"blue": {subnet1}},
		networkIDs:  map[string]int{"blue": 1},
		tunnelIDs:   map[string]int{"blue": 100},
	}
	batcher.requeueUpdate("node1", stale)

	batcher.mutex.Lock()
	pending4 := batcher.pendingUpdates["node1"]
	batcher.mutex.Unlock()

	if pending4.hostSubnets["blue"][0].String() != subnet99.String() {
		t.Errorf("node1: requeue overwrote newer subnet: got %s, want %s", pending4.hostSubnets["blue"][0].String(), subnet99.String())
	}
	if pending4.networkIDs["blue"] != 99 {
		t.Errorf("node1: requeue overwrote newer networkID: got %d, want 99", pending4.networkIDs["blue"])
	}
	if pending4.tunnelIDs["blue"] != 999 {
		t.Errorf("node1: requeue overwrote newer tunnelID: got %d, want 999", pending4.tunnelIDs["blue"])
	}
}
