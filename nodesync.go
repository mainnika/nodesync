package sync

import (
	"path"

	"github.com/go-zookeeper/zk"
)

// NodeSync structure
type NodeSync struct {
	Zk *zk.Conn

	rootPath string
}

// Inside private environment keep everything open
var defaultAcl = zk.WorldACL(zk.PermAll)

// The length of a postfix sequence, see http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#Sequence+Nodes+--+Unique+Naming
var sequenceLen = 10

// Name is used by default to create keys
var defaultKey = "sync"

// New creates a new nodesync instance
func New(zkconn *zk.Conn, rootPath string) (nodeSync *NodeSync, err error) {

	nodeSync = &NodeSync{Zk: zkconn, rootPath: rootPath}

	err = nodeSync.createRecursively(rootPath, defaultAcl)
	if err != nil {
		nodeSync = nil
	}

	return
}

// Fetch returns node data
func (s *NodeSync) Fetch(where string, key string) (current []byte, err error) {

	workingPath := path.Join(s.rootPath, where, key)
	current, _, err = s.Zk.Get(workingPath)

	return
}

// FetchAndSet replaces zookeeper key with new value bytes and returns old if exists
func (s *NodeSync) FetchAndSet(where string, key string, value []byte) (old []byte, err error) {

	workingPath := path.Join(s.rootPath, where, key)
	err = s.createRecursively(workingPath, defaultAcl)
	if err != nil {
		return
	}

	var ver int32 = -1

	old, stat, err := s.Zk.Get(workingPath)
	if err == nil {
		ver = stat.Version
	}

	_, err = s.Zk.Set(workingPath, value, ver)

	return old, err
}

// Lock creates zookeeper ephemeral key to ensure itself in the node list.
// This is used by the locking mechanism to emulate mutex in a cluster.
func (s *NodeSync) Lock(where string) (iam string, err error) {

	workingPath := path.Join(s.rootPath, where)

	err = s.createRecursively(workingPath, defaultAcl)
	if err != nil {
		return
	}

	created, err := s.Zk.CreateProtectedEphemeralSequential(path.Join(workingPath, defaultKey), nil, defaultAcl)
	if err != nil {
		return
	}

	_, iam = path.Split(created)

	return
}

// Unlock waits for the previous worker be unlocked, then removes itself
func (s *NodeSync) Unlock(where string, iam string) (err error) {

	workingPath := path.Join(s.rootPath, where)

	children, _, err := s.Zk.Children(workingPath)
	if err != nil {
		return err
	}
	if len(children) == 0 {
		return
	}

	children = sortChildren(children)

	locked := children[0]
	if locked == iam {
		err = s.Zk.Delete(path.Join(workingPath, iam), 0)
		return
	}

	for i := 1; i < len(children); i++ {
		if children[i] == iam {
			break
		}
		locked = children[i]
	}

	for {
		var exists bool
		var ev <-chan zk.Event

		exists, _, ev, err = s.Zk.ExistsW(path.Join(workingPath, locked))
		if err != nil {
			return
		}
		if !exists {
			break
		}

		change := <-ev

		if change.Type == zk.EventNodeDeleted {
			break
		}
	}

	err = s.Zk.Delete(path.Join(workingPath, iam), 0)

	return
}