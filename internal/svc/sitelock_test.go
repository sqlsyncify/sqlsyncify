package svc

import "testing"

func TestSiteLockExclusive(t *testing.T) {
	const site = "lock-test"
	t.Cleanup(func() { UnlockSite(site) })
	if !TryLockSite(site) {
		t.Fatal("first lock should succeed")
	}
	if TryLockSite(site) {
		t.Fatal("second lock should fail while the site is busy")
	}
	UnlockSite(site)
	if !TryLockSite(site) {
		t.Fatal("lock should succeed after unlock")
	}
}
