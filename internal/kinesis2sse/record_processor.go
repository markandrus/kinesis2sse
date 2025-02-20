package kinesis2sse

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/embano1/memlog"
	kc "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
)

// NOTE(mroberts): I took this from
//
//   https://github.com/vmware/vmware-go-kcl-v2/blob/main/test/worker_test.go
//

func recordProcessorFactory(ml *memlog.Log, t2o *Timestamp2Offset, logger *slog.Logger) kc.IRecordProcessorFactory {
	return &dumpRecordProcessorFactory{
		ml:     ml,
		t2o:    t2o,
		logger: logger,
	}
}

type dumpRecordProcessorFactory struct {
	ml     *memlog.Log
	t2o    *Timestamp2Offset
	logger *slog.Logger // required
}

func (d *dumpRecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &dumpRecordProcessor{
		ml:     d.ml,
		t2o:    d.t2o,
		logger: d.logger,
	}
}

type dumpRecordProcessor struct {
	ml     *memlog.Log
	t2o    *Timestamp2Offset
	logger *slog.Logger // required
}

func (dd *dumpRecordProcessor) Initialize(input *kc.InitializationInput) {
	dd.logger.Debug(fmt.Sprintf("Processing ShardId: %v at checkpoint: %v", input.ShardId, aws.ToString(input.ExtendedSequenceNumber.SequenceNumber)))
}

func (dd *dumpRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	dd.t2o.Lock()
	for _, v := range input.Records {
		var awsEvent map[string]any
		var err error
		if err = json.Unmarshal(v.Data, &awsEvent); err != nil {
			dd.logger.Warn("Skipping an event due to un-parseable JSON", "err", err)
			continue
		}

		timestampString, ok := awsEvent["time"].(string)
		if !ok {
			dd.logger.Warn(`Skipping an event due to missing "time" key`)
			continue
		}
		var timestamp time.Time
		if timestamp, err = time.Parse(time.RFC3339, timestampString); err != nil {
			dd.logger.Warn(`Skipping an event due to un-parseable "time" key`, "err", err)
			continue
		}

		cloudEvent, ok := awsEvent["detail"]
		if !ok {
			dd.logger.Warn(`Skipping an event due to missing "detail" key`)
			continue
		}

		bytes, err := json.Marshal(cloudEvent)
		if err != nil {
			dd.logger.Error(`Skipping an event because we were unable to marshal it to JSON`, "err", err)
			continue
		}

		off, err := dd.ml.Write(context.Background(), bytes)
		if err != nil {
			dd.logger.Error(`Skipping an event because we were unable to write it to the memlog`, "err", err)
			continue
		}

		if err = dd.t2o.Add(int(off), timestamp); err != nil {
			// NOTE(mroberts): If we get an error here, it's really a programming error.
			dd.logger.Error("Incorrect usage of Timestamp2Offset. Programming error or memory corruption? Exiting!", "err", err)
			panic(err)
		}
	}
	dd.t2o.Unlock()

	// checkpoint it after processing this batch.
	// Especially, for processing de-aggregated KPL records, checkpointing has to happen at the end of batch
	// because de-aggregated records share the same sequence number.
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	// Calculate the time taken from polling records and delivering to record processor for a batch.
	if input.CacheEntryTime != nil {
		diff := input.CacheExitTime.Sub(*input.CacheEntryTime)
		dd.logger.Debug(fmt.Sprintf("Checkpoint progress at: %v, MillisBehindLatest = %v, KCLProcessTime = %v", lastRecordSequenceNumber, input.MillisBehindLatest, diff))
	}
	if input.Checkpointer != nil {
		_ = input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
	}
}

func (dd *dumpRecordProcessor) Shutdown(input *kc.ShutdownInput) {
	dd.logger.Info(fmt.Sprintf("Shutdown Reason: %v", aws.ToString(kc.ShutdownReasonMessage(input.ShutdownReason))))

	// When the value of {@link ShutdownInput#getShutdownReason()} is
	// {@link com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason#TERMINATE} it is required that you
	// checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
	if input.ShutdownReason == kc.TERMINATE {
		_ = input.Checkpointer.Checkpoint(nil)
	}
}
