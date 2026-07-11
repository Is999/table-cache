package tablecache

import "strings"

// stripLeadingLuaComments 剥离 go:embed Lua 资产开头的连续行注释，正文注释保持不变。
func stripLeadingLuaComments(source string) string {
	source = strings.TrimPrefix(source, "\ufeff")
	lines := strings.SplitAfter(source, "\n")
	body := 0
	stripped := false
	for body < len(lines) {
		line := strings.TrimSpace(lines[body])
		if line == "" {
			body++
			continue
		}
		if strings.HasPrefix(line, "--") {
			stripped = true
			body++
			continue
		}
		break
	}
	if !stripped {
		return source
	}
	return strings.TrimLeft(strings.Join(lines[body:], ""), "\r\n")
}
