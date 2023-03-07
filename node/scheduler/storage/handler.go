package storage

import (
	"fmt"
	"github.com/filecoin-project/go-statemachine"
)

func (m *Manager) handlePreparing(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler preparing, %s", carfile.CarfileCID)
	fmt.Println("handlePreparing")
	return ctx.Send(CarfileStart{})
}

func (m *Manager) handleCarfileStart(ctx statemachine.Context, carfile CarfileInfo) error {
	log.Info("handler carfile start, %s", carfile.CarfileCID)
	fmt.Println("handleCarfileStart")
	return ctx.Send(CarfileFinished{})
}

func (m *Manager) handlerCarfileFinished(ctx statemachine.Context, carfile CarfileInfo) error {
	fmt.Println("handlerCarfileFinished")
	log.Info("handler carfile finished, %s", carfile.CarfileCID)
	return nil
}
