package assets

// AssetState asset pull state
type AssetState string

const (
	// UndefinedState Undefined
	UndefinedState AssetState = ""
	// FindFirstCandidate find first candidate to pull seed
	FindFirstCandidate AssetState = "FindFirstCandidate"
	// AssetSeedPulling asset seed pulling
	AssetSeedPulling AssetState = "AssetSeedPulling "
	// FindCandidatesToPull find candidates to pull asset
	FindCandidatesToPull AssetState = "FindCandidatesToPull"
	// CandidatesPulling candidates pulling
	CandidatesPulling AssetState = "CandidatesPulling"
	// FindEdgesToPull find edges to pull asset
	FindEdgesToPull AssetState = "FindEdgesToPull"
	// EdgesPulling edges pulling
	EdgesPulling AssetState = "EdgesPulling"
	// Finished finished
	Finished AssetState = "Finished"
	// SeedPullFailed asset seed pull failed
	SeedPullFailed AssetState = "SeedPullFailed"
	// CandidatesPullFailed candidates pull asset failed
	CandidatesPullFailed AssetState = "CandidatesPullFailed"
	// EdgesPullFailed  edge pull asset failed
	EdgesPullFailed AssetState = "EdgesPullFailed"
	// Remove remove
	Remove AssetState = "Remove"
)

func (s AssetState) String() string {
	return string(s)
}

var (
	// FailedStates asset pull failed states
	FailedStates = []string{
		SeedPullFailed.String(),
		CandidatesPullFailed.String(),
		EdgesPullFailed.String(),
	}

	// PullingStates asset pulling states
	PullingStates = []string{
		FindFirstCandidate.String(),
		AssetSeedPulling.String(),
		FindCandidatesToPull.String(),
		CandidatesPulling.String(),
		FindEdgesToPull.String(),
		EdgesPulling.String(),
	}
)
