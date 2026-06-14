// Package jobs implements the background data jobs: channel/peer/forward/payment
// sync, autofees, auto-maxhtlc, probe, and the main job loop.
package jobs

import (
	"fmt"
	"time"
)

// cTimeLayout is the timestamp format used in [Data] log lines
// (e.g. "Wed Jun 11 14:30:05 2025").
const cTimeLayout = "Mon Jan _2 15:04:05 2006"

// dataLogAt writes a [Data] log line with an explicit timestamp to stdout.
func dataLogAt(now time.Time, msg string) {
	fmt.Printf("%s : [Data] : %s\n", now.Format(cTimeLayout), msg)
}

// dataLog writes a [Data] log line timestamped with time.Now().
func dataLog(msg string) {
	dataLogAt(time.Now(), msg)
}
