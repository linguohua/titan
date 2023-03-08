package storage

func (m *Manager) ListCarfiles() ([]CarfileInfo, error) {
	var sectors []CarfileInfo
	if err := m.carfiles.List(&sectors); err != nil {
		return nil, err
	}
	return sectors, nil
}

func (m *Manager) GetCarfileInfo(cid CarfileID) (CarfileInfo, error) {
	var out CarfileInfo
	err := m.carfiles.Get(cid).Get(&out)
	return out, err
}
