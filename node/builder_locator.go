package node

import (
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/locator"
	"github.com/linguohua/titan/node/modules"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/region"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

func Locator(out *api.Locator) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Locator option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Locator
			return nil
		},

		func(s *Settings) error {
			resAPI := &locator.Locator{}
			s.invokes[ExtractAPIKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}

func ConfigLocator(c interface{}) Option {
	cfg, ok := c.(*config.LocatorCfg)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}
	log.Info("start to config locator")

	return Options(
		Override(new(dtypes.ServerID), modules.NewServerID),
		Override(new(*sqlx.DB), modules.NewDB),
		Override(new(*locator.SqlDB), locator.NewSQLDB),
		Override(new(region.Region), modules.NewRegion),
		Override(new(*locator.AccessPointMgr), modules.NewAccessPointManager),
		Override(new(dtypes.SessionCallbackFunc), func() dtypes.SessionCallbackFunc {
			return func(s string, s2 string) error { return nil }
		}),
		Override(new(dtypes.DatabaseAddress), func() dtypes.DatabaseAddress {
			return dtypes.DatabaseAddress(cfg.DatabaseAddress)
		}),
		Override(new(dtypes.LocatorUUID), func() dtypes.LocatorUUID {
			return dtypes.LocatorUUID(cfg.UUID)
		}),
		Override(new(dtypes.GeoDBPath), func() dtypes.GeoDBPath {
			return dtypes.GeoDBPath(cfg.GeoDBPath)
		}),
	)
}
