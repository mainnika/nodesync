package sync

import (
	"github.com/go-zookeeper/zk"

	"fmt"
	"strings"
)

// createRecursively creates zookeeper path recursively
func (s *NodeSync) createRecursively(path string, acl []zk.ACL) error {

	parts := strings.Split(path, "/")

	if len(parts) < 1 {
		return fmt.Errorf("path does not contain valid path")
	}

	for i := range parts {
	Ensuring:
		for {
			child := fmt.Sprintf("%s", strings.Join(parts[:i+1], "/"))

			if len(child) == 0 {
				break
			}

			exists, _, err := s.Zk.Exists(child)
			switch true {
			case err == zk.ErrSessionExpired:
				return err
			case err != nil:
				continue
			case exists:
				break Ensuring
			default:
			}

			if _, err = s.Zk.Create(child, nil, 0, acl); err != nil {
				continue
			}

			break
		}
	}

	return nil
}
