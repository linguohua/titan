package api

type DataSync interface {
	// simple to check carfile table
	CheckSummary(susseedCarfilesHash, failedCarfilesHash string) (*CheckSummaryResult, error)
}

type CheckSummaryResult struct {
	IsSusseedCarfilesOk   bool
	IsUnsusseedCarfilesOk bool
}
