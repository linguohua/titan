package cli

import (
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"

	"github.com/linguohua/titan/api"
	cliutil "github.com/linguohua/titan/cli/util"
)

var log = logging.Logger("cli")

// custom CLI error

type ErrCmdFailed struct {
	msg string
}

func (e *ErrCmdFailed) Error() string {
	return e.msg
}

func NewCliError(s string) error {
	return &ErrCmdFailed{s}
}

// ApiConnector returns API instance
type ApiConnector func() api.Scheduler

var (
	GetAPIInfo = cliutil.GetAPIInfo
	GetRawAPI  = cliutil.GetRawAPI
	GetAPI     = cliutil.GetCommonAPI
)

var (
	DaemonContext = cliutil.DaemonContext
	ReqContext    = cliutil.ReqContext
)

var (
	GetSchedulerAPI = cliutil.GetSchedulerAPI
	GetCandidateAPI = cliutil.GetCandidateAPI
	GetEdgeAPI      = cliutil.GetEdgeAPI
	GetLocatorAPI   = cliutil.GetLocatorAPI
)

var CommonCommands = []*cli.Command{
	AuthCmd,
	LogCmd,
	PprofCmd,
	VersionCmd,
}

var Commands = []*cli.Command{
	// WithCategory("basic", sendCmd),
	// WithCategory("basic", walletCmd),
	// WithCategory("basic", clientCmd),
	// WithCategory("basic", multisigCmd),
	// WithCategory("basic", filplusCmd),
	// WithCategory("basic", paychCmd),
	WithCategory("developer", AuthCmd),
	WithCategory("developer", LogCmd),
	// WithCategory("network", NetCmd),
	// WithCategory("network", SyncCmd),
	// WithCategory("status", StatusCmd),
	PprofCmd,
	VersionCmd,
}

func WithCategory(cat string, cmd *cli.Command) *cli.Command {
	cmd.Category = strings.ToUpper(cat)
	return cmd
}
