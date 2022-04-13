package sync

import (
	"github.com/go-zookeeper/zk"
)

// NodeSync structure
type NodeSync struct {
	Zk *zk.Conn

	rootPath string
}

// Inside private environment keep everything open
var defaultAcl = zk.WorldACL(zk.PermAll)

// New creates a new nodesync instance
func New(zkconn *zk.Conn, rootPath string) (nodeSync *NodeSync, err error) {

	nodeSync = &NodeSync{Zk: zkconn, rootPath: rootPath}

	err = nodeSync.createRecursively(rootPath, defaultAcl)
	if err != nil {
		nodeSync = nil
	}

	return
}
