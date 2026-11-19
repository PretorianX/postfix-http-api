// Package seqmark counts unique zero-padded sequence markers in log text.
package seqmark

import (
	"regexp"
	"strconv"
)

// CountUnique returns how many distinct values in [1, total] appear as
// `<prefix><zero-padded width digits>` in text, plus a sample of missing values.
func CountUnique(text, prefix string, width, total int) (count int, missing []int) {
	if total < 0 || width < 1 {
		return 0, nil
	}
	re := regexp.MustCompile(regexp.QuoteMeta(prefix) + `(\d{` + strconv.Itoa(width) + `})`)
	seen := make([]bool, total+1)
	for _, m := range re.FindAllStringSubmatch(text, -1) {
		n, err := strconv.Atoi(m[1])
		if err != nil || n < 1 || n > total || seen[n] {
			continue
		}
		seen[n] = true
		count++
	}
	if count == total {
		return count, nil
	}
	for i := 1; i <= total && len(missing) < 20; i++ {
		if !seen[i] {
			missing = append(missing, i)
		}
	}
	return count, missing
}
