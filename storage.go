package queue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type offsetKey struct {
	Topic     string
	Partition int
	Group     string
}

// fileStore 以本地文件模拟消息落盘与恢复：
//   - <dir>/<topic>/<part>/events.log：事件追加日志（JSON 行），发布时同步落盘；
//   - <dir>/<topic>/<part>/offsets.json：各消费组已确认位点，临时文件 + rename 原子替换；
//   - <dir>/dlq.log：死信追加日志（JSON 行）。
//
// 所有写操作先持久化后返回，保证重启恢复时不存在半提交状态。
type fileStore struct {
	dir string
	mu  sync.Mutex // 序列化同目录下的并发追加/替换
}

func newFileStore(dir string) (*fileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: empty data dir", ErrInvalidConfig)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("%w: mkdir data dir: %v", ErrInvalidConfig, err)
	}
	return &fileStore{dir: dir}, nil
}

func (s *fileStore) partDir(topic string, partition int) string {
	return filepath.Join(s.dir, topic, strconv.Itoa(partition))
}

func (s *fileStore) eventsPath(topic string, partition int) string {
	return filepath.Join(s.partDir(topic, partition), "events.log")
}

func (s *fileStore) offsetsPath(topic string, partition int) string {
	return filepath.Join(s.partDir(topic, partition), "offsets.json")
}

func (s *fileStore) deadLettersPath() string {
	return filepath.Join(s.dir, "dlq.log")
}

// appendEvent 将事件追加到分区日志并 fsync；失败返回 ErrInternal 包装（不直接透出）。
func (s *fileStore) appendEvent(e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.partDir(e.Topic, e.Partition)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%w: create partition dir: %v", ErrInvalidConfig, err)
	}
	return s.appendJSON(s.eventsPath(e.Topic, e.Partition), e)
}

// appendJSON 以 JSON 行形式追加并同步落盘。
func (s *fileStore) appendJSON(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("%w: open %s: %v", ErrPersistence, filepath.Base(path), err)
	}
	defer f.Close()
	line, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("%w: encode record: %v", ErrInvalidConfig, err)
	}
	line = append(line, '\n')
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("%w: write record: %v", ErrPersistence, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("%w: sync record: %v", ErrPersistence, err)
	}
	return nil
}

// readEvent 按位点从分区日志读回单个事件。
// 事件日志只增不改，offset 即行号；用于内存窗口已逐出、但消费组仍需从头消费
// 历史事件时的按需回读（消费端晚到场景）。与并发追加无冲突：只读取已落盘的旧行。
func (s *fileStore) readEvent(topic string, partition int, offset uint64) (Event, error) {
	f, err := os.Open(s.eventsPath(topic, partition))
	if err != nil {
		return Event{}, fmt.Errorf("%w: open events for read-back: %v", ErrPersistence, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	var idx uint64
	for sc.Scan() {
		if idx == offset {
			var e Event
			if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
				return Event{}, fmt.Errorf("%w: corrupt event record: %v", ErrPersistence, err)
			}
			return e, nil
		}
		idx++
	}
	if err := sc.Err(); err != nil {
		return Event{}, fmt.Errorf("%w: scan events: %v", ErrPersistence, err)
	}
	return Event{}, fmt.Errorf("%w: event offset %d not found in log", ErrPersistence, offset)
}

// loadEvents 读取分区的全部已落盘事件。
func (s *fileStore) loadEvents(topic string, partition int) ([]Event, error) {
	f, err := os.Open(s.eventsPath(topic, partition))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: read events: %v", ErrPersistence, err)
	}
	defer f.Close()
	var out []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var e Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("%w: corrupt event record: %v", ErrPersistence, err)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%w: scan events: %v", ErrPersistence, err)
	}
	return out, nil
}

// commitOffset 原子替换分区的位点文件（group -> committed offset）。
func (s *fileStore) commitOffset(topic string, partition int, group string, offset uint64, all map[string]uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.partDir(topic, partition)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("%w: create partition dir: %v", ErrInvalidConfig, err)
	}
	if all == nil {
		all = map[string]uint64{}
	}
	all[group] = offset
	data, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: encode offsets: %v", ErrInvalidConfig, err)
	}
	final := s.offsetsPath(topic, partition)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("%w: write offsets: %v", ErrPersistence, err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("%w: replace offsets: %v", ErrPersistence, err)
	}
	return nil
}

// loadOffsets 扫描全部主题分区并恢复消费组位点。
func (s *fileStore) loadOffsets() (map[offsetKey]uint64, error) {
	out := map[offsetKey]uint64{}
	topics, err := s.listTopics()
	if err != nil {
		return nil, err
	}
	for topic, parts := range topics {
		for _, p := range parts {
			data, err := os.ReadFile(s.offsetsPath(topic, p))
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("%w: read offsets: %v", ErrPersistence, err)
			}
			var m map[string]uint64
			if len(strings.TrimSpace(string(data))) == 0 {
				continue
			}
			if err := json.Unmarshal(data, &m); err != nil {
				return nil, fmt.Errorf("%w: corrupt offsets: %v", ErrPersistence, err)
			}
			for g, off := range m {
				out[offsetKey{Topic: topic, Partition: p, Group: g}] = off
			}
		}
	}
	return out, nil
}

func (s *fileStore) appendDeadLetter(dl DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendJSON(s.deadLettersPath(), dl)
}

func (s *fileStore) loadDeadLetters() ([]DeadLetter, error) {
	f, err := os.Open(s.deadLettersPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: read dlq: %v", ErrPersistence, err)
	}
	defer f.Close()
	var out []DeadLetter
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var dl DeadLetter
		if err := json.Unmarshal(sc.Bytes(), &dl); err != nil {
			return nil, fmt.Errorf("%w: corrupt dlq record: %v", ErrPersistence, err)
		}
		out = append(out, dl)
	}
	return out, sc.Err()
}

// listTopics 扫描数据目录，返回 topic -> 分区号列表。
func (s *fileStore) listTopics() (map[string][]int, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string][]int{}, nil
		}
		return nil, fmt.Errorf("%w: scan data dir: %v", ErrPersistence, err)
	}
	out := map[string][]int{}
	for _, te := range entries {
		if !te.IsDir() || te.Name() == "" {
			continue
		}
		topic := te.Name()
		parts, err := os.ReadDir(filepath.Join(s.dir, topic))
		if err != nil {
			return nil, fmt.Errorf("%w: scan topic dir: %v", ErrPersistence, err)
		}
		for _, pe := range parts {
			if !pe.IsDir() {
				continue
			}
			n, err := strconv.Atoi(pe.Name())
			if err != nil {
				continue // 非分区目录忽略
			}
			out[topic] = append(out[topic], n)
		}
	}
	return out, nil
}

func (s *fileStore) close() error { return nil }
