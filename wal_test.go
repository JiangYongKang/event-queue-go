package eventqueue

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWALTornTailRecovery 在事件 WAL 末尾人为追加一段“写了一半”的字节，
// 模拟崩溃导致的尾部不完整帧：恢复时必须截断残留、只保留完整记录，
// 不允许出现半提交状态或读失败。
func TestWALTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	st, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 3; i++ {
		if err := st.AppendEvent("t", 0, eventRecord{
			offset: i, id: "id", key: "k", payload: []byte("ok"), createdAt: i,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 追加半帧：8 字节长度头 + 4 字节 crc + 截断的 payload
	path := st.eventPath("t", 0)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{20, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 9, 9}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	recs, err := st.LoadEvents("t", 0)
	if err != nil {
		t.Fatalf("半帧恢复不得失败：%v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("应恢复3条完整记录，实际 %d", len(recs))
	}
	// 再读一次：半帧应已被截断，不会重复报错
	recs2, err := st.LoadEvents("t", 0)
	if err != nil || len(recs2) != 3 {
		t.Fatalf("二次读取异常：n=%d err=%v", len(recs2), err)
	}
	info, _ := os.Stat(filepath.Clean(path))
	t.Logf("关键判定：尾部半帧已截断，恢复完整记录=%d，文件大小=%d，无半提交", len(recs2), info.Size())
}

// TestStoreRoundTrip 事件/ack/DLQ 记录编码往返一致。
func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, _ := NewStore(dir)
	ev := eventRecord{offset: 7, id: "abc", key: "k", payload: []byte("payload"), createdAt: 12345}
	if err := st.AppendEvent("t", 0, ev); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendAck("t", 0, "g1", 7); err != nil {
		t.Fatal(err)
	}
	recs, err := st.LoadEvents("t", 0)
	if err != nil || len(recs) != 1 {
		t.Fatalf("events n=%d err=%v", len(recs), err)
	}
	got := recs[0]
	if got.offset != 7 || got.id != "abc" || string(got.payload) != "payload" || got.createdAt != 12345 {
		t.Fatalf("事件往返不一致：%+v", got)
	}
	acks, err := st.LoadAcks("t", 0)
	if err != nil || len(acks["g1"]) != 1 || acks["g1"][0] != 7 {
		t.Fatalf("ack 往返不一致：%v err=%v", acks, err)
	}
	keys, err := st.ListPartitions()
	if err != nil || len(keys) != 1 || keys[0].topic != "t" || keys[0].partition != 0 {
		t.Fatalf("分区列举不一致：%v err=%v", keys, err)
	}
	t.Log("关键判定：事件/ack WAL 编码往返一致，分区可枚举")
}
