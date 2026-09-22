package eventqueue

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Store 负责本地落盘：每个 (topic,partition) 一个事件 WAL 与一个 ack WAL，
// 另有一个全局死信文件。所有写入均 fsync，保证崩溃/重启后可完整恢复。
//
// 帧格式（小端序）：
//
//	[u64 payloadLen][u32 crc32][payload bytes]
//
// crc32 仅覆盖 payload。事件/ack 负载分别用 net-string 风格的
// 长度前缀字段串联（见 encode/decode），字段本身不含内部换行，避免歧义。
type Store struct {
	dir string
}

const (
	eventsDir = "events"
	acksDir   = "acks"
	dlqFile   = "dlq.log"
)

// eventRecord 是事件 WAL 的一条记录。
type eventRecord struct {
	offset    int64
	id        string
	key       string
	payload   []byte
	createdAt int64 // unix nano
}

// ackRecord 是 ack WAL 的一条记录：某 offset 已被某消费者组确认（含死信终止位）。
type ackRecord struct {
	group  string
	offset int64
}

func NewStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, newError(ErrInvalidConfig, "NewStore", "empty data dir", nil)
	}
	if err := os.MkdirAll(filepath.Join(dir, eventsDir), 0o755); err != nil {
		return nil, newError(ErrInvalidConfig, "NewStore", "mkdir events", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, acksDir), 0o755); err != nil {
		return nil, newError(ErrInvalidConfig, "NewStore", "mkdir acks", err)
	}
	return &Store{dir: dir}, nil
}

func partName(topic string, partition int) string {
	return fmt.Sprintf("%s-%010d", topic, partition)
}

// parsePartName 从文件名解析 (topic, partition)，无法解析返回 false。
func parsePartName(name string) (string, int, bool) {
	i := strings.LastIndex(name, "-")
	if i <= 0 {
		return "", 0, false
	}
	n, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return "", 0, false
	}
	return name[:i], n, true
}

func (s *Store) eventPath(topic string, partition int) string {
	return filepath.Join(s.dir, eventsDir, partName(topic, partition)+".log")
}

func (s *Store) ackPath(topic string, partition int) string {
	return filepath.Join(s.dir, acksDir, partName(topic, partition)+".log")
}

func (s *Store) dlqPath() string {
	return filepath.Join(s.dir, dlqFile)
}

// appendFrame 在文件尾追加一帧并 fsync；文件按 O_APPEND 打开，保证追加原子可见。
func appendFrame(path string, payload []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	hdr := make([]byte, 12)
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(len(payload)))
	binary.LittleEndian.PutUint32(hdr[8:12], crc32.ChecksumIEEE(payload))
	if _, err := f.Write(hdr); err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		return err
	}
	return f.Sync()
}

// readFrames 顺序读取全部帧；遇到尾部不完整帧（崩溃写一半）时截断其残留字节，
// 保证日志只包含完整记录，避免恢复时出现“半提交”。
func readFrames(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out [][]byte
	pos := 0
	for pos < len(data) {
		if len(data)-pos < 12 {
			break
		}
		n := int(binary.LittleEndian.Uint64(data[pos : pos+8]))
		if n <= 0 || pos+12+n > len(data) {
			break
		}
		payload := data[pos+12 : pos+12+n]
		want := binary.LittleEndian.Uint32(data[pos+8 : pos+12])
		if crc32.ChecksumIEEE(payload) != want {
			break
		}
		out = append(out, append([]byte(nil), payload...))
		pos += 12 + n
	}
	if pos < len(data) {
		// 截断尾部不完整/损坏帧
		if err := os.WriteFile(path, data[:pos], 0o644); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// 字段编码器：[u64 len][bytes]，字符串/字节统一处理。
func putField(b *[]byte, v []byte) {
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(v)))
	*b = append(*b, hdr[:]...)
	*b = append(*b, v...)
}

func getFields(data []byte, n int) ([][]byte, error) {
	out := make([][]byte, 0, n)
	pos := 0
	for i := 0; i < n; i++ {
		if pos+8 > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		l := int(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8
		if pos+l > len(data) {
			return nil, io.ErrUnexpectedEOF
		}
		out = append(out, data[pos:pos+l])
		pos += l
	}
	if pos != len(data) {
		return nil, fmt.Errorf("trailing bytes in record")
	}
	return out, nil
}

func encodeEvent(rec eventRecord) []byte {
	var b []byte
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(rec.offset))
	putField(&b, hdr[:])
	binary.LittleEndian.PutUint64(hdr[:], uint64(rec.createdAt))
	putField(&b, hdr[:])
	putField(&b, []byte(rec.id))
	putField(&b, []byte(rec.key))
	putField(&b, rec.payload)
	return b
}

func decodeEvent(data []byte) (eventRecord, error) {
	fields, err := getFields(data, 5)
	if err != nil {
		return eventRecord{}, err
	}
	return eventRecord{
		offset:    int64(binary.LittleEndian.Uint64(fields[0])),
		createdAt: int64(binary.LittleEndian.Uint64(fields[1])),
		id:        string(fields[2]),
		key:       string(fields[3]),
		payload:   append([]byte(nil), fields[4]...),
	}, nil
}

func encodeAck(rec ackRecord) []byte {
	var b []byte
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(rec.offset))
	putField(&b, hdr[:])
	putField(&b, []byte(rec.group))
	return b
}

func decodeAck(data []byte) (ackRecord, error) {
	fields, err := getFields(data, 2)
	if err != nil {
		return ackRecord{}, err
	}
	return ackRecord{
		offset: int64(binary.LittleEndian.Uint64(fields[0])),
		group:  string(fields[1]),
	}, nil
}

func (s *Store) AppendEvent(topic string, partition int, rec eventRecord) error {
	if err := appendFrame(s.eventPath(topic, partition), encodeEvent(rec)); err != nil {
		return newError(ErrInternal, "AppendEvent", "write event wal", err)
	}
	return nil
}

func (s *Store) AppendAck(topic string, partition int, group string, offset int64) error {
	if err := appendFrame(s.ackPath(topic, partition), encodeAck(ackRecord{group: group, offset: offset})); err != nil {
		return newError(ErrInternal, "AppendAck", "write ack wal", err)
	}
	return nil
}

func (s *Store) AppendDLQ(rec DLQRecord) error {
	var b []byte
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(rec.Offset))
	putField(&b, hdr[:])
	binary.LittleEndian.PutUint64(hdr[:], uint64(rec.Attempts))
	putField(&b, hdr[:])
	binary.LittleEndian.PutUint64(hdr[:], uint64(rec.DeadAt.UnixNano()))
	putField(&b, hdr[:])
	putField(&b, []byte(rec.EventID))
	putField(&b, []byte(rec.Topic))
	putField(&b, []byte(rec.Reason))
	var pn [4]byte
	binary.LittleEndian.PutUint32(pn[:], uint32(rec.Partition))
	putField(&b, pn[:])
	if err := appendFrame(s.dlqPath(), b); err != nil {
		return newError(ErrInternal, "AppendDLQ", "write dlq", err)
	}
	return nil
}

func (s *Store) LoadEvents(topic string, partition int) ([]eventRecord, error) {
	frames, err := readFrames(s.eventPath(topic, partition))
	if err != nil {
		return nil, newError(ErrInternal, "LoadEvents", "read event wal", err)
	}
	recs := make([]eventRecord, 0, len(frames))
	for _, f := range frames {
		rec, err := decodeEvent(f)
		if err != nil {
			return nil, newError(ErrInternal, "LoadEvents", "corrupt event record", err)
		}
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].offset < recs[j].offset })
	return recs, nil
}

func (s *Store) LoadAcks(topic string, partition int) (map[string][]int64, error) {
	frames, err := readFrames(s.ackPath(topic, partition))
	if err != nil {
		return nil, newError(ErrInternal, "LoadAcks", "read ack wal", err)
	}
	out := map[string][]int64{}
	for _, f := range frames {
		rec, err := decodeAck(f)
		if err != nil {
			return nil, newError(ErrInternal, "LoadAcks", "corrupt ack record", err)
		}
		out[rec.group] = append(out[rec.group], rec.offset)
	}
	return out, nil
}

func (s *Store) LoadDLQ() ([]DLQRecord, error) {
	frames, err := readFrames(s.dlqPath())
	if err != nil {
		return nil, newError(ErrInternal, "LoadDLQ", "read dlq", err)
	}
	out := make([]DLQRecord, 0, len(frames))
	for _, f := range frames {
		fields, err := getFields(f, 7)
		if err != nil {
			return nil, newError(ErrInternal, "LoadDLQ", "corrupt dlq record", err)
		}
		out = append(out, DLQRecord{
			Offset:    int64(binary.LittleEndian.Uint64(fields[0])),
			Attempts:  int(binary.LittleEndian.Uint64(fields[1])),
			DeadAt:    time.Unix(0, int64(binary.LittleEndian.Uint64(fields[2]))),
			EventID:   string(fields[3]),
			Topic:     string(fields[4]),
			Reason:    string(fields[5]),
			Partition: int(binary.LittleEndian.Uint32(fields[6])),
		})
	}
	return out, nil
}

// ListPartitions 返回磁盘上存在的全部 (topic, partition)，用于恢复。
func (s *Store) ListPartitions() ([]pKey, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, eventsDir))
	if err != nil {
		return nil, newError(ErrInternal, "ListPartitions", "read events dir", err)
	}
	var keys []pKey
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".log" {
			continue
		}
		topic, part, ok := parsePartName(strings.TrimSuffix(e.Name(), ".log"))
		if !ok {
			continue
		}
		keys = append(keys, pKey{topic: topic, partition: part})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic != keys[j].topic {
			return keys[i].topic < keys[j].topic
		}
		return keys[i].partition < keys[j].partition
	})
	return keys, nil
}
