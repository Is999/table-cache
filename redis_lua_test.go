package tablecache

import (
	"strings"
	"testing"
)

// TestStripLeadingLuaComments 验证只剥离文件头注释，不修改脚本正文注释。
func TestStripLeadingLuaComments(t *testing.T) {
	source := "\ufeff-- 资产说明\n-- KEYS说明\n\nlocal value = '-- keep'\n-- body\nreturn value\n"
	want := "local value = '-- keep'\n-- body\nreturn value\n"
	if got := stripLeadingLuaComments(source); got != want {
		t.Fatalf("stripLeadingLuaComments() = %q, want %q", got, want)
	}
	for name, script := range map[string]string{
		"guarded_mutation":        guardedMutationLua,
		"read_collection_limited": readCollectionLimitedLua,
		"replace_fields_empty":    replaceFieldsWithEmptyLua,
	} {
		if strings.HasPrefix(strings.TrimSpace(script), "--") {
			t.Fatalf("%s executable script still has leading comment", name)
		}
	}
}

// TestCollectionReplacementUnlinksOldValue 验证大集合替换使用异步释放旧值，并在释放后原子改名。
func TestCollectionReplacementUnlinksOldValue(t *testing.T) {
	tests := []struct {
		name   string
		script string
		unlink string
		rename string
	}{
		{
			name:   "replace_collection",
			script: replaceCollectionScript,
			unlink: `redis.call("unlink", target)`,
			rename: `redis.call("rename", temporary, target)`,
		},
		{
			name:   "guarded_mutation",
			script: guardedMutationLua,
			unlink: `redis.call("unlink", target)`,
			rename: `redis.call("rename", destination, target)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			unlinkAt := strings.LastIndex(test.script, test.unlink)
			renameAt := strings.LastIndex(test.script, test.rename)
			if unlinkAt < 0 || renameAt < 0 || unlinkAt > renameAt {
				t.Fatalf("collection replacement must unlink target before rename")
			}
		})
	}
}
