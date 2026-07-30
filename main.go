package main

import (
	"errors"
	"fmt"
	"sync"
)

// NodeState represents the state of a node in the cluster.
type NodeState string

const (
	NodeStateJoining NodeState = "JOINING"
	NodeStateNormal  NodeState = "NORMAL"
	NodeStateLeaving NodeState = "LEAVING"
)

// TabletState represents the state of a tablet.
type TabletState string

const (
	TabletStateMoving TabletState = "MOVING"
	TabletStateActive TabletState = "ACTIVE"
)

// Tablet represents a database tablet.
type Tablet struct {
	ID          string
	OwnerNodeID string
	State       TabletState
	DataSize    int64 // Simulated data size (e.g., number of rows/bytes)
}

// Node represents a cluster node.
type Node struct {
	ID           string
	State        NodeState
	DurableStore map[string]int64 // tabletID -> durably written data size
	mu           sync.RWMutex
}

// StreamSession represents a data streaming session for a tablet.
type StreamSession struct {
	TabletID   string
	SourceNode string
	TargetNode string
	TotalBytes int64
	SentBytes  int64
	Completed  bool
	Failed     bool
	mu         sync.Mutex
}

// TabletTransfer represents the state of a tablet ownership transfer.
type TabletTransfer struct {
	TabletID      string
	SourceNodeID  string
	TargetNodeID  string
	StreamSession *StreamSession
	Acknowledged  bool // Target node acknowledged durable integration of all streamed SSTables
	Finalized     bool
	Aborted       bool
}

// Cluster represents the ScyllaDB cluster simulation.
type Cluster struct {
	Nodes        map[string]*Node
	Tablets      map[string]*Tablet
	RaftLeaderID string
	Transfers    map[string]*TabletTransfer // tabletID -> transfer
	mu           sync.RWMutex
}

func NewCluster() *Cluster {
	return &Cluster{
		Nodes:     make(map[string]*Node),
		Tablets:   make(map[string]*Tablet),
		Transfers: make(map[string]*TabletTransfer),
	}
}

func (c *Cluster) AddNode(id string, state NodeState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Nodes[id] = &Node{
		ID:           id,
		State:        state,
		DurableStore: make(map[string]int64),
	}
}

func (c *Cluster) AddTablet(id string, ownerNodeID string, dataSize int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Tablets[id] = &Tablet{
		ID:          id,
		OwnerNodeID: ownerNodeID,
		State:       TabletStateActive,
		DataSize:    dataSize,
	}
	if node, ok := c.Nodes[ownerNodeID]; ok {
		node.mu.Lock()
		node.DurableStore[id] = dataSize
		node.mu.Unlock()
	}
}

// StartTabletTransfer initiates the transfer of a tablet from source to target.
func (c *Cluster) StartTabletTransfer(tabletID, sourceNodeID, targetNodeID string) (*TabletTransfer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tablet, ok := c.Tablets[tabletID]
	if !ok {
		return nil, fmt.Errorf("tablet %s not found", tabletID)
	}

	if tablet.OwnerNodeID != sourceNodeID {
		return nil, fmt.Errorf("source node %s does not own tablet %s", sourceNodeID, tabletID)
	}

	tablet.State = TabletStateMoving

	session := &StreamSession{
		TabletID:   tabletID,
		SourceNode: sourceNodeID,
		TargetNode: targetNodeID,
		TotalBytes: tablet.DataSize,
	}

	transfer := &TabletTransfer{
		TabletID:      tabletID,
		SourceNodeID:  sourceNodeID,
		TargetNodeID:  targetNodeID,
		StreamSession: session,
	}

	c.Transfers[tabletID] = transfer
	return transfer, nil
}

// SimulateStreaming simulates the streaming of data.
func (c *Cluster) SimulateStreaming(tabletID string, fail bool) {
	c.mu.RLock()
	transfer, ok := c.Transfers[tabletID]
	c.mu.RUnlock()

	if !ok {
		return
	}

	session := transfer.StreamSession
	session.mu.Lock()
	if fail {
		session.Failed = true
		session.mu.Unlock()
		return
	}

	// Stream data
	session.SentBytes = session.TotalBytes
	session.Completed = true
	session.mu.Unlock()

	// Target node durably writes the data
	c.mu.Lock()
	targetNode := c.Nodes[transfer.TargetNodeID]
	targetNode.mu.Lock()
	targetNode.DurableStore[tabletID] = session.TotalBytes
	targetNode.mu.Unlock()
	c.mu.Unlock()
}

// AcknowledgeDurableIntegration is called by the target node to acknowledge durable integration.
func (c *Cluster) AcknowledgeDurableIntegration(tabletID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	transfer, ok := c.Transfers[tabletID]
	if !ok {
		return fmt.Errorf("no active transfer for tablet %s", tabletID)
	}

	if transfer.Aborted {
		return fmt.Errorf("transfer for tablet %s was aborted", tabletID)
	}

	// Verify target node actually has the durable data
	targetNode := c.Nodes[transfer.TargetNodeID]
	targetNode.mu.RLock()
	durableBytes, ok := targetNode.DurableStore[tabletID]
	targetNode.mu.RUnlock()

	if !ok || durableBytes < transfer.StreamSession.TotalBytes {
		return fmt.Errorf("target node has not durably integrated all data for tablet %s", tabletID)
	}

	transfer.Acknowledged = true
	return nil
}

// TryFinalizeTransfer attempts to finalize the transfer on the current Raft leader.
func (c *Cluster) TryFinalizeTransfer(tabletID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	transfer, ok := c.Transfers[tabletID]
	if !ok {
		return fmt.Errorf("no active transfer for tablet %s", tabletID)
	}

	if transfer.Finalized {
		return nil
	}

	if transfer.Aborted {
		return fmt.Errorf("cannot finalize aborted transfer")
	}

	// CRITICAL: Durable Integration Barrier check
	if !transfer.Acknowledged {
		return errors.New("cannot finalize transfer: target node has not acknowledged durable integration of streamed SSTables")
	}

	// Finalize ownership transition in Raft metadata
	tablet := c.Tablets[tabletID]
	tablet.OwnerNodeID = transfer.TargetNodeID
	tablet.State = TabletStateActive
	transfer.Finalized = true

	// Update node states if bootstrap/decommission is complete
	targetNode := c.Nodes[transfer.TargetNodeID]
	if targetNode.State == NodeStateJoining {
		targetNode.State = NodeStateNormal
	}

	return nil
}

// AbortTransfer aborts the transfer and rolls back state.
func (c *Cluster) AbortTransfer(tabletID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	transfer, ok := c.Transfers[tabletID]
	if !ok || transfer.Finalized {
		return
	}

	transfer.Aborted = true
	tablet := c.Tablets[tabletID]
	tablet.State = TabletStateActive // Revert to active on source

	// Clean up target node's incomplete data to prevent silent data corruption
	targetNode := c.Nodes[transfer.TargetNodeID]
	targetNode.mu.Lock()
	delete(targetNode.DurableStore, tabletID)
	targetNode.mu.Unlock()
}

// HandleLeaderElection simulates a Raft leader election.
// The new leader must inspect pending transfers and ensure they are not prematurely finalized.
func (c *Cluster) HandleLeaderElection(newLeaderID string) {
	c.mu.Lock()
	c.RaftLeaderID = newLeaderID
	fmt.Printf("[Raft] Node %s elected as new leader\n", newLeaderID)
	c.mu.Unlock()

	// The new leader audits pending transfers
	c.mu.RLock()
	defer c.mu.RUnlock()

	for tabletID, transfer := range c.Transfers {
		if transfer.Finalized || transfer.Aborted {
			continue
		}
		fmt.Printf("[Raft Leader %s] Auditing pending transfer for tablet %s\n", newLeaderID, tabletID)
		if transfer.Acknowledged {
			fmt.Printf("[Raft Leader %s] Transfer for tablet %s is already acknowledged. Safe to finalize.\n", newLeaderID, tabletID)
		} else {
			fmt.Printf("[Raft Leader %s] Transfer for tablet %s is NOT acknowledged. Waiting for streaming completion.\n", newLeaderID, tabletID)
		}
	}
}

func main() {
	fmt.Println("Starting ScyllaDB Tablet Ownership Transfer Simulation...")

	// Test Case 1: Successful transfer with Raft leader election mid-streaming
	fmt.Println("\n--- Test Case 1: Raft Leader Election Mid-Streaming (Success Path) ---")
	cluster := NewCluster()
	cluster.AddNode("node1", NodeStateNormal)
	cluster.AddNode("node2", NodeStateJoining)
	cluster.AddTablet("tablet1", "node1", 1000)
	cluster.RaftLeaderID = "node1"

	_, err := cluster.StartTabletTransfer("tablet1", "node1", "node2")
	if err != nil {
		panic(err)
	}

	// Simulate Raft leader election mid-streaming (before streaming completes)
	cluster.HandleLeaderElection("node3") // node3 becomes leader

	// Try to finalize prematurely (should fail because streaming is not complete and not acknowledged)
	err = cluster.TryFinalizeTransfer("tablet1")
	if err == nil {
		panic("Expected error when finalizing unacknowledged transfer, but got nil")
	}
	fmt.Printf("Expected failure: %v\n", err)

	// Complete streaming and acknowledge
	cluster.SimulateStreaming("tablet1", false)
	err = cluster.AcknowledgeDurableIntegration("tablet1")
	if err != nil {
		panic(err)
	}
	fmt.Println("Streaming completed and acknowledged by target node.")

	// Now finalize should succeed
	err = cluster.TryFinalizeTransfer("tablet1")
	if err != nil {
		panic(err)
	}
	fmt.Println("Transfer finalized successfully after acknowledgment.")

	// Verify state
	cluster.mu.RLock()
	tablet := cluster.Tablets["tablet1"]
	targetNode := cluster.Nodes["node2"]
	if tablet.OwnerNodeID != "node2" || tablet.State != TabletStateActive || targetNode.State != NodeStateNormal {
		panic("Invalid cluster state after successful transfer")
	}
	cluster.mu.RUnlock()
	fmt.Println("Test Case 1 Passed: Tablet ownership safely transferred without data loss.")

	// Test Case 2: Streaming failure and rollback
	fmt.Println("\n--- Test Case 2: Streaming Failure and Rollback ---")
	cluster2 := NewCluster()
	cluster2.AddNode("node1", NodeStateNormal)
	cluster2.AddNode("node2", NodeStateJoining)
	cluster2.AddTablet("tablet1", "node1", 1000)
	cluster2.RaftLeaderID = "node1"

	_, err = cluster2.StartTabletTransfer("tablet1", "node1", "node2")
	if err != nil {
		panic(err)
	}

	// Simulate streaming failure
	cluster2.SimulateStreaming("tablet1", true)
	fmt.Println("Streaming failed.")

	// Abort and rollback
	cluster2.AbortTransfer("tablet1")
	fmt.Println("Transfer aborted and rolled back.")

	// Verify rollback state
	cluster2.mu.RLock()
	tablet2 := cluster2.Tablets["tablet1"]
	targetNode2 := cluster2.Nodes["node2"]
	if tablet2.OwnerNodeID != "node1" || tablet2.State != TabletStateActive || targetNode2.State != NodeStateJoining {
		panic("Invalid cluster state after rollback")
	}
	if _, exists := targetNode2.DurableStore["tablet1"]; exists {
		panic("Target node still has incomplete data after rollback")
	}
	cluster2.mu.RUnlock()
	fmt.Println("Test Case 2 Passed: Rollback successful, no incomplete data registered.")
}
