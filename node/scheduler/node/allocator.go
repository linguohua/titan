package node

import (
	"fmt"
	"github.com/linguohua/titan/api"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

var secretRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// RegisterNode Register a Node
func (m *Manager) Allocate(nodeType api.NodeType) (api.NodeAllocateInfo, error) {
	info := api.NodeAllocateInfo{}

	nodeID, err := newNodeID(nodeType)
	if err != nil {
		return info, err
	}

	secret := newSecret()

	err = m.CarfileDB.BindNodeAllocateInfo(secret, nodeID, nodeType)
	if err != nil {
		return info, err
	}

	info.NodeID = nodeID
	info.Secret = secret

	return info, nil
}

func newNodeID(nodeType api.NodeType) (string, error) {
	u2 := uuid.New()

	s := strings.Replace(u2.String(), "-", "", -1)
	switch nodeType {
	case api.NodeEdge:
		s = fmt.Sprintf("e_%s", s)
		return s, nil
	case api.NodeCandidate:
		s = fmt.Sprintf("c_%s", s)
		return s, nil
	}

	return "", xerrors.Errorf("nodetype err:%d", nodeType)
}

func newSecret() string {
	uStr := uuid.NewString()

	return strings.Replace(uStr, "-", "", -1)
}
