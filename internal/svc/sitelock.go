package svc

import "sync"

// siteRun is an in-process lock so one site cannot sync and clean at the same time.
// It does not coordinate multiple replicas. Run a single writer, or keep replicas
// from calling /sync and /clean for the same site concurrently.
var siteRun sync.Map

// TryLockSite records that site is busy. The caller must UnlockSite.
func TryLockSite(site string) bool {
	_, loaded := siteRun.LoadOrStore(site, struct{}{})
	return !loaded
}

// UnlockSite releases a lock taken by TryLockSite.
func UnlockSite(site string) {
	siteRun.Delete(site)
}
