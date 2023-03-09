package storage

func (m *Manager) ListCarfiles() ([]CarfileInfo, error) {
	var carfiles []CarfileInfo
	if err := m.carfiles.List(&carfiles); err != nil {
		return nil, err
	}
	return carfiles, nil
}

func (m *Manager) GetCarfileInfo(cid CarfileID) (CarfileInfo, error) {
	var out CarfileInfo
	err := m.carfiles.Get(cid).Get(&out)
	return out, err
}
