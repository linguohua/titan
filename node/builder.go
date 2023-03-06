package node

import (
	"context"
	"errors"
	"github.com/gbrlsnchs/jwt/v3"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/journal"
	"github.com/linguohua/titan/journal/alerting"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/modules"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/secret"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

var log = logging.Logger("builder")

// special is a type used to give keys to modules which
//
//	can't really be identified by the returned type
type special struct{ id int }

type invoke int

// Invokes are called in the order they are defined.
//
//nolint:golint
const (
	// InitJournal at position 0 initializes the journal global var as soon as
	// the system starts, so that it's available for all other components.
	InitJournalKey = invoke(iota)

	ExtractApiKey

	_nInvokes // keep this last
)

type Settings struct {
	// modules is a map of constructors for DI
	//
	// In most cases the index will be a reflect. Type of element returned by
	// the constructor, but for some 'constructors' it's hard to specify what's
	// the return type should be (or the constructor returns fx group)
	modules map[interface{}]fx.Option

	// invokes are separate from modules as they can't be referenced by return
	// type, and must be applied in correct order
	invokes []fx.Option

	nodeType repo.RepoType

	Base   bool // Base option applied
	Config bool // Config option applied
}

// Basic services
func defaults() []Option {
	return []Option{
		// global system journal.
		Override(new(journal.DisabledEvents), journal.EnvDisabledEvents),
		Override(new(*alerting.Alerting), alerting.NewAlertingSystem),
	}
}

func Repo(r repo.Repo) Option {
	return func(settings *Settings) error {
		lr, err := r.Lock(settings.nodeType)
		if err != nil {
			return err
		}
		c, err := lr.Config()
		if err != nil {
			return err
		}
		return Options(
			Override(new(repo.LockedRepo), modules.LockedRepo(lr)), // module handles closing
			Override(new(*jwt.HMACSHA), secret.APISecret),
			Override(new(dtypes.ServerID), modules.NewServerID),
			Override(new(*common.CommonAPI), common.NewCommonAPI),
			Override(new(dtypes.SessionCallbackFunc), modules.DefaultSessionCallback),
			Override(new(dtypes.PermissionWriteToken), modules.GenerateTokenWithPermission(api.ReadWritePerms)),
			Override(new(dtypes.PermissionAdminToken), modules.GenerateTokenWithPermission(api.AllPermissions)),
			ApplyIf(IsType(repo.Scheduler), ConfigScheduler(c)),
			ApplyIf(IsType(repo.Locator), ConfigLocator(c)),
			ApplyIf(IsType(repo.Edge), ConfigEdge(c)),
			ApplyIf(IsType(repo.Candidate), ConfigCandidate(c)),
		)(settings)
	}
}

type StopFunc func(context.Context) error

// New builds and starts new Titan node
func New(ctx context.Context, opts ...Option) (StopFunc, error) {
	settings := Settings{
		modules: map[interface{}]fx.Option{},
		invokes: make([]fx.Option, _nInvokes),
	}

	// apply module options in the right order
	if err := Options(Options(defaults()...), Options(opts...))(&settings); err != nil {
		return nil, xerrors.Errorf("applying node options failed: %w", err)
	}

	// gather constructors for fx.Options
	ctors := make([]fx.Option, 0, len(settings.modules))
	for _, opt := range settings.modules {
		ctors = append(ctors, opt)
	}

	// fill holes in invokes for use in fx.Options
	for i, opt := range settings.invokes {
		if opt == nil {
			settings.invokes[i] = fx.Options()
		}
	}

	app := fx.New(
		fx.Options(ctors...),
		fx.Options(settings.invokes...),

		fx.NopLogger,
	)

	// TODO: we probably should have a 'firewall' for Closing signal
	//  on this context, and implement closing logic through lifecycles
	//  correctly
	if err := app.Start(ctx); err != nil {
		// comment fx.NopLogger few lines above for easier debugging
		return nil, xerrors.Errorf("starting node: %w", err)
	}

	return app.Stop, nil
}

func IsType(t repo.RepoType) func(s *Settings) bool {
	return func(s *Settings) bool { return s.nodeType == t }
}

func Base() Option {
	return Options(
		func(s *Settings) error { s.Base = true; return nil }, // mark Base as applied
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Base() option must be set before Config option")),
		),
	)
}
