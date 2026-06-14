package web

import (
	"strconv"
	"strings"
	"time"
)

// djangoMonthsAP are the AP-style month abbreviations used by the 'N' format specifier.
// March through July are spelled out in full.
var djangoMonthsAP = [...]string{
	"", "Jan.", "Feb.", "March", "April", "May", "June",
	"July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec.",
}

// djangoMonthsF are the full month names used by the 'F' format specifier.
var djangoMonthsF = [...]string{
	"", "January", "February", "March", "April", "May", "June",
	"July", "August", "September", "October", "November", "December",
}

// djangoMonthsM are the short month names used by the 'M' format specifier.
var djangoMonthsM = [...]string{
	"", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
	"Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
}

var djangoWeekdaysD = [...]string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"}
var djangoWeekdaysL = [...]string{"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

// djangoDate formats a time value according to Django's date format specification.
// Implements the format characters used in templates. Unknown characters are
// passed through unchanged; a preceding '\' escapes the next character literally.
func djangoDate(t time.Time, format string) string {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		c := format[i]
		if c == '\\' && i+1 < len(format) {
			b.WriteByte(format[i+1])
			i++
			continue
		}
		b.WriteString(djangoDateChar(t, c))
	}
	return b.String()
}

func djangoDateChar(t time.Time, c byte) string {
	switch c {
	case 'd': // day, zero-padded 2 digits
		return pad2(t.Day())
	case 'j': // day, no leading zero
		return strconv.Itoa(t.Day())
	case 'D': // short weekday name
		return djangoWeekdaysD[int(t.Weekday())]
	case 'l': // full weekday name
		return djangoWeekdaysL[int(t.Weekday())]
	case 'N': // month, AP style
		return djangoMonthsAP[int(t.Month())]
	case 'M': // short month name
		return djangoMonthsM[int(t.Month())]
	case 'F': // full month name
		return djangoMonthsF[int(t.Month())]
	case 'm': // month, zero-padded 2 digits
		return pad2(int(t.Month()))
	case 'n': // month, no leading zero
		return strconv.Itoa(int(t.Month()))
	case 'Y': // 4-digit year
		return strconv.Itoa(t.Year())
	case 'y': // 2-digit year
		return pad2(t.Year() % 100)
	case 'H': // 24-hour, zero-padded
		return pad2(t.Hour())
	case 'G': // 24-hour, no leading zero
		return strconv.Itoa(t.Hour())
	case 'h': // 12-hour, zero-padded
		return pad2(hour12(t))
	case 'g': // 12-hour, no leading zero
		return strconv.Itoa(hour12(t))
	case 'i': // minutes, zero-padded
		return pad2(t.Minute())
	case 's': // seconds, zero-padded
		return pad2(t.Second())
	case 'A': // AM/PM uppercase
		if t.Hour() < 12 {
			return "AM"
		}
		return "PM"
	case 'a': // a.m./p.m. with periods
		if t.Hour() < 12 {
			return "a.m."
		}
		return "p.m."
	case 'P': // 12-hour time with special cases for midnight and noon
		return djangoTimeP(t)
	default:
		return string(c)
	}
}

// djangoTimeP formats a time using the 'P' specifier: 12-hour clock with a.m./p.m.,
// whole hours omit the ':00' suffix, and midnight/noon are written as words.
func djangoTimeP(t time.Time) string {
	if t.Minute() == 0 && t.Hour() == 0 {
		return "midnight"
	}
	if t.Minute() == 0 && t.Hour() == 12 {
		return "noon"
	}
	ampm := "a.m."
	if t.Hour() >= 12 {
		ampm = "p.m."
	}
	if t.Minute() == 0 {
		return strconv.Itoa(hour12(t)) + " " + ampm
	}
	return strconv.Itoa(hour12(t)) + ":" + pad2(t.Minute()) + " " + ampm
}

func hour12(t time.Time) int {
	h := t.Hour() % 12
	if h == 0 {
		h = 12
	}
	return h
}

func pad2(n int) string {
	if n < 10 {
		return "0" + strconv.Itoa(n)
	}
	return strconv.Itoa(n)
}

// djangoDateTimeDefault renders a time value using the default datetime format
// "N j, Y, P" (e.g. "Jan. 5, 2024, 3:30 p.m."). Non-time values are passed
// through numberString. Times are formatted in UTC.
func djangoDateTimeDefault(v any) string {
	if v == nil {
		return ""
	}
	t, ok := toTime(v)
	if !ok {
		return numberString(v)
	}
	return djangoDate(t.UTC(), "N j, Y, P")
}

func toTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	default:
		return time.Time{}, false
	}
}
