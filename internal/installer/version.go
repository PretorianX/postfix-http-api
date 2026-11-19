package installer

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

const (
	minPostfixMajor = 3
	minPostfixMinor = 5
)

// ParsePostfixVersion parses a Postfix mail_version string (e.g., "3.11.7", "3.7.11-1~deb12u1",
// "3.5.0-20200315") into numeric (major, minor) components.
func ParsePostfixVersion(version string) (major int, minor int, err error) {
	v := strings.TrimSpace(version)
	if v == "" {
		return 0, 0, fmt.Errorf("empty mail_version")
	}

	// Strip optional patch/distro suffixes (anything starting at first non-digit, non-dot character).
	cut := len(v)
	for i := 0; i < len(v); i++ {
		c := v[i]
		if (c < '0' || c > '9') && c != '.' {
			cut = i
			break
		}
	}
	numeric := strings.TrimRight(v[:cut], ".")
	parts := strings.Split(numeric, ".")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("invalid Postfix version %q", version)
	}

	major, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Postfix major version in %q: %w", version, err)
	}
	minor, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Postfix minor version in %q: %w", version, err)
	}
	return major, minor, nil
}

// CheckPostfixVersion validates that mail_version is >= 3.5.
func CheckPostfixVersion(version string) error {
	major, minor, err := ParsePostfixVersion(version)
	if err != nil {
		return err
	}
	if major < minPostfixMajor || (major == minPostfixMajor && minor < minPostfixMinor) {
		return fmt.Errorf("Postfix version %q is unsupported; minimum required version is %d.%d", version, minPostfixMajor, minPostfixMinor)
	}
	return nil
}

// ValidateListenAddr validates a master.cf inet listen address (e.g. "0.0.0.0:8025",
// "127.0.0.1:8025", "[::1]:8025") ensuring a valid host:port split and numeric port in 1..65535.
func ValidateListenAddr(listen string) error {
	listen = strings.TrimSpace(listen)
	if listen == "" {
		return fmt.Errorf("listen address must not be empty")
	}
	host, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", listen, err)
	}
	if strings.ContainsAny(host, " \t\r\n") {
		return fmt.Errorf("invalid listen host in %q", listen)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid listen port %q in %q: must be between 1 and 65535", portStr, listen)
	}
	return nil
}
