package installer

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	subjectPatternRe = regexp.MustCompile(`(?i)subject`)
	// Overriding Postfix header_checks(5) actions that may shadow a later Subject INFO rule.
	overrideActionRe = regexp.MustCompile(`(?i)\b(IGNORE|DISCARD|REJECT|HOLD|REDIRECT|REPLACE|FILTER|PREPEND|INFO|WARN|BCC|DUNNO|OK|PASS)\b`)
)

// splitHeaderChecks splits a Postfix header_checks parameter value (comma and/or whitespace separated)
// into individual table specifications.
func splitHeaderChecks(val string) []string {
	fields := strings.FieldsFunc(val, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// ScanHeaderChecksConflicts inspects every table listed BEFORE ignoreFile in headerChecksValue
// and returns human-readable warnings for any non-comment rule matching Subject with an
// overriding header_checks action.
func ScanHeaderChecksConflicts(headerChecksValue, ignoreFile string) []string {
	var warnings []string
	cleanIgnore := ""
	if ignoreFile != "" {
		cleanIgnore = filepath.Clean(ignoreFile)
	}

	for _, spec := range splitHeaderChecks(headerChecksValue) {
		idx := strings.IndexByte(spec, ':')
		if idx <= 0 || idx == len(spec)-1 {
			continue
		}
		tablePath := spec[idx+1:]
		if cleanIgnore != "" && filepath.Clean(tablePath) == cleanIgnore {
			// Only inspect tables listed BEFORE postfix-api-header-checks.
			break
		}

		info, err := os.Stat(tablePath)
		if err != nil || info.IsDir() {
			continue
		}

		fileWarnings := scanHeaderChecksFile(tablePath)
		warnings = append(warnings, fileWarnings...)
	}

	return warnings
}

func scanHeaderChecksFile(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var warnings []string
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		trimmed := strings.TrimSpace(scanner.Text())
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if subjectPatternRe.MatchString(trimmed) && overrideActionRe.MatchString(trimmed) {
			warnings = append(warnings, fmt.Sprintf(
				"WARNING: existing header_checks rule in %s:%d matches Subject (%s) and may shadow postfix-api Subject capture",
				path, lineNum, trimmed,
			))
		}
	}
	return warnings
}
