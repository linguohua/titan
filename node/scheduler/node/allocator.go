package node

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

var secretRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// Allocate Allocate a Node
// func (m *Manager) Allocate(nodeType types.NodeType) (*types.NodeAllocateInfo, error) {
// 	info := &types.NodeAllocateInfo{}

// 	nodeID, err := newNodeID(nodeType)
// 	if err != nil {
// 		return info, err
// 	}

// 	secret := newSecret()

// 	err = m.NodeMgrDB.BindNodeAllocateInfo(secret, nodeID, nodeType)
// 	if err != nil {
// 		return info, err
// 	}

// 	info.NodeID = nodeID
// 	info.PublicKey = secret

// 	return info, nil
// }

func newNodeID(nodeType types.NodeType) (string, error) {
	u2 := uuid.New()

	s := strings.Replace(u2.String(), "-", "", -1)
	switch nodeType {
	case types.NodeEdge:
		s = fmt.Sprintf("e_%s", s)
		return s, nil
	case types.NodeCandidate:
		s = fmt.Sprintf("c_%s", s)
		return s, nil
	}

	return "", xerrors.Errorf("nodetype err:%d", nodeType)
}

func newSecret() string {
	uStr := uuid.NewString()

	return strings.Replace(uStr, "-", "", -1)
}
