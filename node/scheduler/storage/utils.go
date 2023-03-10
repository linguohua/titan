package storage

// ListCarfiles load carfiles from db
func (m *Manager) ListCarfiles() ([]CarfileInfo, error) {
	var carfiles []CarfileInfo
	if err := m.carfiles.List(&carfiles); err != nil {
		return nil, err
	}
	return carfiles, nil
}

// GetCarfileInfo load a carfile from db
func (m *Manager) GetCarfileInfo(cid CarfileHash) (CarfileInfo, error) {
	var out CarfileInfo
	err := m.carfiles.Get(cid).Get(&out)
	return out, err
}
