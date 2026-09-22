package queue

import "strconv"

func itoa(n int) string { return strconv.Itoa(n) }

func formatUint(n uint64) string { return strconv.FormatUint(n, 10) }
