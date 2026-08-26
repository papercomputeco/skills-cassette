package server

import "time"

// StubExternalFilterProbeTimeout shortens the per-view probe deadline for
// tests and returns a func restoring the previous value.
func StubExternalFilterProbeTimeout(d time.Duration) (restore func()) {
	previous := externalFilterProbeTimeout
	externalFilterProbeTimeout = d
	return func() { externalFilterProbeTimeout = previous }
}
