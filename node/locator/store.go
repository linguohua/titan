package locator

type storage interface {
	GetSchedulerURLs(areaID string) ([]string, error)
}
