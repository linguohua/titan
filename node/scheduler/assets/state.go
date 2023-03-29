package assets

// AssetState asset pull state
type AssetState string

const (
	// UndefinedState Undefined
	UndefinedState AssetState = ""
	// SeedSelect select first candidate to pull seed asset
	SeedSelect AssetState = "SeedSelect"
	// SeedPulling Waiting for candidate nodes to pull seed asset
	SeedPulling AssetState = "SeedPulling "
	// CandidatesSelect select candidates to pull asset
	CandidatesSelect AssetState = "CandidatesSelect"
	// CandidatesPulling candidate nodes pulling asset
	CandidatesPulling AssetState = "CandidatesPulling"
	// EdgesSelect select edges to pull asset
	EdgesSelect AssetState = "EdgesSelect"
	// EdgesPulling edge nodes pulling asset
	EdgesPulling AssetState = "EdgesPulling"
	// Servicing Asset cache completed and in service
	Servicing AssetState = "Servicing"
	// SeedFailed Unable to select candidate nodes or failed to pull seed asset
	SeedFailed AssetState = "SeedFailed"
	// CandidatesFailed Unable to select candidate nodes or failed to pull asset
	CandidatesFailed AssetState = "CandidatesFailed"
	// EdgesFailed  Unable to select edge nodes or failed to pull asset
	EdgesFailed AssetState = "EdgesFailed"
	// Remove remove
	Remove AssetState = "Remove"
)

func (s AssetState) String() string {
	return string(s)
}

var (
	// FailedStates asset pull failed states
	FailedStates = []string{
		SeedFailed.String(),
		CandidatesFailed.String(),
		EdgesFailed.String(),
	}

	// PullingStates asset pulling states
	PullingStates = []string{
		SeedSelect.String(),
		SeedPulling.String(),
		CandidatesSelect.String(),
		CandidatesPulling.String(),
		EdgesSelect.String(),
		EdgesPulling.String(),
	}
)
