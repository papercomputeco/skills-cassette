package server

import "time"

// StubExternalFilterProbeTimeout shortens the per-view probe deadline for
// tests and returns a func restoring the previous value.
func StubExternalFilterProbeTimeout(d time.Duration) (restore func()) {
	previous := externalFilterProbeTimeout
	externalFilterProbeTimeout = d
	return func() { externalFilterProbeTimeout = previous }
}

// StubExternalFilterReprobeInterval shortens the background re-probe cadence
// for tests and returns a func restoring the previous value.
func StubExternalFilterReprobeInterval(d time.Duration) (restore func()) {
	previous := externalFilterReprobeInterval
	externalFilterReprobeInterval = d
	return func() { externalFilterReprobeInterval = previous }
}
