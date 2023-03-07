package storage

import (
	"fmt"
	"github.com/filecoin-project/go-statemachine"
)

func (m *Manager) handleGetCarfile(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler get carfile, %s", carfile.CarfileCID)
	return ctx.Send(CarfileGetCarfile{})
}

func (m *Manager) handleCandidateCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler candidate  Caching, %s", carfile.CarfileCID)
	fmt.Println("handleCandidateCaching")
	return ctx.Send(CarfileCandidateCaching{})
}

func (m *Manager) handleEdgeCaching(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler carfile , %s", carfile.CarfileCID)
	return ctx.Send(CarfileEdgeCaching{})
}

func (m *Manager) handleFinalize(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler carfile finalize, %s", carfile.CarfileCID)
	return ctx.Send(CarfileFinalize{})
}

func (m *Manager) handleGetCarfileFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler get carfile failed, %s", carfile.CarfileCID)
	return ctx.Send(CarfileGetCarfile{})
}

func (m *Manager) handleCandidateCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler candidate  Caching failed, %s", carfile.CarfileCID)
	return ctx.Send(CarfileCandidateCaching{})
}

func (m *Manager) handlerEdgeCachingFailed(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler edge  Caching failed, %s", carfile.CarfileCID)
	return ctx.Send(CarfileEdgeCaching{})
}
