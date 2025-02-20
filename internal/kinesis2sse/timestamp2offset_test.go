package kinesis2sse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimestamp2Offset(t *testing.T) {
	r := require.New(t)

	t2o, err := NewTimestamp2Offset(2)
	r.NoError(err)

	// t2o is empty. Therefore, NearestOffset returns nothing.
	// []
	off, ok := t2o.NearestOffset(time.UnixMilli(0))
	r.Equal(-1, off)
	r.False(ok)

	// t2O has a single offset. Asking for timestamps before and after the singular offset returns it.
	// [0 → 100]
	err = t2o.Add(0, time.UnixMilli(100))
	r.NoError(err)

	off, ok = t2o.NearestOffset(time.UnixMilli(0))
	r.Equal(0, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(100))
	r.Equal(0, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(1_000))
	r.Equal(0, off)
	r.True(ok)

	// t2O has two offsets. Asking for timestamps before the first offset returns it. Asking for timestamps before and after the last offset returns it.
	// [0 → 100, 1 → 500]
	err = t2o.Add(1, time.UnixMilli(500))
	r.NoError(err)

	off, ok = t2o.NearestOffset(time.UnixMilli(0))
	r.Equal(0, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(100))
	r.Equal(0, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(250))
	r.Equal(1, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(500))
	r.Equal(1, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(1_000))
	r.Equal(1, off)
	r.True(ok)

	// t20 has two offsets, but the earliest has been shifted out.
	// [1 → 500, 2 → 250]
	err = t2o.Add(2, time.UnixMilli(250))
	r.NoError(err)

	off, ok = t2o.NearestOffset(time.UnixMilli(0))
	r.Equal(2, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(100))
	r.Equal(2, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(250))
	r.Equal(2, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(300))
	r.Equal(1, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(500))
	r.Equal(1, off)
	r.True(ok)

	off, ok = t2o.NearestOffset(time.UnixMilli(1_000))
	r.Equal(1, off)
	r.True(ok)
}
