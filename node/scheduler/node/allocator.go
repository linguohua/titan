package node

import (
	"fmt"
	"strings"

	"github.com/linguohua/titan/api/types"

	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

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

	return "", xerrors.Errorf("node type err:%d", nodeType)
}

func newSecret() string {
	uStr := uuid.NewString()

	return strings.Replace(uStr, "-", "", -1)
}
