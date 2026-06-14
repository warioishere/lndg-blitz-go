package rebalancer

import (
	"fmt"
	"time"
)

// cTimeLayout is the timestamp format used in rebalancer log lines.
const cTimeLayout = "Mon Jan _2 15:04:05 2006"

// rebalLogAt writes a [Rebalancer] log line with the given timestamp.
func rebalLogAt(now time.Time, msg string) {
	fmt.Printf("%s : [Rebalancer] : %s\n", now.Format(cTimeLayout), msg)
}

// rebalLog writes a [Rebalancer] log line using the current time.
func rebalLog(msg string) {
	rebalLogAt(time.Now(), msg)
}
