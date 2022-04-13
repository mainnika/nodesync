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