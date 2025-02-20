package kinesis2sse

import (
	"cmp"
	"errors"
	"fmt"
	"sync"
	"time"

	"modernc.org/b/v2"
)

// Timestamp2Offset is a map from offsets to timestamps. It's not thread-safe by default. Callers should use the embedded mutex.
type Timestamp2Offset struct {
	*sync.Mutex

	// capacity is the capacity of Timestamp2Offset.
	capacity int

	// lastOffset is the last added offset (used for error checking).
	lastOffset int

	// offset2Timestamp stores the mapping from offsets to timestamps.
	offset2Timestamp map[int]time.Time

	// timestamp2Offsets stores the mapping from timestamps to unordered offsets.
	timestamp2Offsets *b.Tree[timestamp2OffsetsKey, struct{}]
}

type timestamp2OffsetsKey struct {
	timestamp time.Time
	offset    int
}

func timestamp2OffsetsKeyCmp(a, b timestamp2OffsetsKey) int {
	c := a.timestamp.Compare(b.timestamp)
	if c == 0 {
		c = cmp.Compare(a.offset, b.offset)
	}
	return c
}

// NewTimestamp2Offset returns a new Timestamp2Offset with the specified capacity.
func NewTimestamp2Offset(capacity int) (*Timestamp2Offset, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity must be greater than 1")
	}

	return &Timestamp2Offset{
		Mutex:             &sync.Mutex{},
		capacity:          capacity,
		lastOffset:        -1,
		offset2Timestamp:  make(map[int]time.Time),
		timestamp2Offsets: b.TreeNew[timestamp2OffsetsKey, struct{}](timestamp2OffsetsKeyCmp),
	}, nil
}

// NearestOffset returns the smallest offset since the specified timestamp. If there is no smallest timestamp since
// the specified timestamp, it returns the next earliest offset, if any.
func (m *Timestamp2Offset) NearestOffset(timestamp time.Time) (int, bool) {
	// Go forward…
	e, _ := m.timestamp2Offsets.Seek(timestamp2OffsetsKey{
		timestamp: timestamp,
		offset:    0,
	})
	if k, _, err := e.Next(); err == nil {
		return k.offset, true
	}

	// Go backward…
	e, _ = m.timestamp2Offsets.Seek(timestamp2OffsetsKey{
		timestamp: timestamp,
		offset:    0,
	})
	if k, _, err := e.Prev(); err == nil {
		return k.offset, true
	}

	return -1, false
}

// Add adds an offset and its timestamp. Offsets must be added in order.
func (m *Timestamp2Offset) Add(offset int, timestamp time.Time) error {
	if offset < 0 {
		return errors.New("offsets must be non-negative")
	}

	n := len(m.offset2Timestamp)
	if n == 0 {
		// Set the initial offset.
		m.lastOffset = offset
	} else if m.lastOffset != offset-1 {
		return fmt.Errorf("cannot add offset %d when last offset was %d", offset, m.lastOffset)
	}

	if n == m.capacity {
		// We are at capacity. Remove the oldest entry oldestOffset.
		oldestOffset := offset - m.capacity
		oldestOffsetTimestamp, ok := m.offset2Timestamp[oldestOffset]
		if !ok {
			return fmt.Errorf("old offset to remove %d not found", oldestOffset)
		}

		if ok = m.timestamp2Offsets.Delete(timestamp2OffsetsKey{
			timestamp: oldestOffsetTimestamp,
			offset:    oldestOffset,
		}); !ok {
			return fmt.Errorf("timestamp %s for old offset to remove %d not found", oldestOffsetTimestamp.Format(time.RFC3339), oldestOffset)
		}

		delete(m.offset2Timestamp, oldestOffset)
	}

	// Add the newest entry offset.
	m.offset2Timestamp[offset] = timestamp
	m.timestamp2Offsets.Set(
		timestamp2OffsetsKey{
			timestamp: timestamp,
			offset:    offset,
		},
		struct{}{},
	)

	m.lastOffset = offset
	return nil
}
