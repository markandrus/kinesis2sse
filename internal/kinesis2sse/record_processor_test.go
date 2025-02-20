package kinesis2sse

import (
	"context"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/embano1/memlog"
	"github.com/stretchr/testify/require"
	kc "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
)

func TestRecordProcessor(t *testing.T) {
	unparseableEvent := `bogus`
	eventWithoutTime := `{"detail":{}}`
	eventWithoutDetail := `{"time":"1970-01-01T00:00:00.000Z"}`
	goodEvent1 := `{"time":"1970-01-01T00:00:00.000Z","detail":{"good":true,"event":1}}`
	goodEvent2 := `{"time":"1970-01-01T00:00:00.001Z","detail":{"good":true,"event":2}}`
	goodEvent3 := `{"time":"1970-01-01T00:00:00.001Z","detail":{"good":true,"event":3}}`

	r := require.New(t)

	ml, err := memlog.New(context.Background(), memlog.WithMaxSegmentSize(100))
	r.NoError(err)

	t2o, err := NewTimestamp2Offset(100)
	r.NoError(err)

	rp := dumpRecordProcessor{
		ml:     ml,
		t2o:    t2o,
		logger: slog.New(slog.DiscardHandler),
	}

	rp.ProcessRecords(&kc.ProcessRecordsInput{
		Records: []types.Record{
			{
				Data: []byte(unparseableEvent),
			},
			{
				Data: []byte(eventWithoutTime),
			},
			{
				Data: []byte(eventWithoutDetail),
			},
			{
				Data: []byte(goodEvent1),
			},
			{
				Data: []byte(goodEvent2),
			},
		},
	})

	// Skips over invalid events…

	rec, err := ml.Read(context.Background(), 0)
	r.NoError(err)
	r.Equal(`{"event":1,"good":true}`, string(rec.Data))

	rec, err = ml.Read(context.Background(), 1)
	r.NoError(err)
	r.Equal(`{"event":2,"good":true}`, string(rec.Data))

	_, err = ml.Read(context.Background(), 2)
	r.Error(err)

	// Can process more…

	rp.ProcessRecords(&kc.ProcessRecordsInput{
		Records: []types.Record{
			{
				Data: []byte(goodEvent3),
			},
		},
	})

	rec, err = ml.Read(context.Background(), 2)
	r.NoError(err)
	r.Equal(`{"event":3,"good":true}`, string(rec.Data))

	_, err = ml.Read(context.Background(), 3)
	r.Error(err)
}
