package kinesis2sse

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	chk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/checkpoint"
	par "github.com/vmware/vmware-go-kcl-v2/clientlibrary/partition"
)

// inMemoryCheckpointer is an in-memory implementation of the DynamoDB-based Checkpointer that ships with the KCL. We
// can use an in-memory implementation, because we don't need durability across restarts, and we don't need multiple
// workers. In order to implement this, I started with a copy of the DynamoDB-based Checkpointer and just deleted
// everything we didn't need.
type inMemoryCheckpointer struct {
	workerID string
	m        map[string]marshalledCheckpoint
	logger   *slog.Logger // required
	lock     *sync.Mutex
}

type marshalledCheckpoint struct {
	sequenceNumber string
	leaseTimeout   time.Time
	parentShardId  string
}

func NewInMemoryCheckpointer(workerID string, logger *slog.Logger) chk.Checkpointer {
	return &inMemoryCheckpointer{
		workerID: workerID,
		m:        make(map[string]marshalledCheckpoint),
		logger:   logger,
		lock:     &sync.Mutex{},
	}
}

func (checkpointer *inMemoryCheckpointer) Init() error {
	checkpointer.logger.Debug("Init")
	return nil
}

func (checkpointer *inMemoryCheckpointer) GetLease(shard *par.ShardStatus, newAssignTo string) error {
	checkpointer.logger.Debug(fmt.Sprintf("GetLease: shardID=%q; newAssignTo=%q", shard.ID, newAssignTo))

	// NOTE(mroberts): Lease duration can be nearly infinite, since this is just in-memory.
	newLeaseTimeout := time.Now().AddDate(1, 0, 0).UTC()

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

func (checkpointer *inMemoryCheckpointer) CheckpointSequence(shard *par.ShardStatus) error {
	checkpointer.logger.Debug(fmt.Sprintf("CheckpointSequence: shardID=%q", shard.ID))

	leaseTimeout := shard.GetLeaseTimeout().UTC()

	item := marshalledCheckpoint{
		sequenceNumber: shard.GetCheckpoint(),
		leaseTimeout:   leaseTimeout,
		parentShardId:  shard.ParentShardId,
	}

	return checkpointer.saveItem(shard.ID, item)
}

func (checkpointer *inMemoryCheckpointer) FetchCheckpoint(shard *par.ShardStatus) error {
	checkpointer.logger.Debug(fmt.Sprintf("FetchCheckpoint: shardID=%q", shard.ID))

	item, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	shard.SetCheckpoint(item.sequenceNumber)

	shard.SetLeaseOwner(checkpointer.workerID)

	shard.LeaseTimeout = item.leaseTimeout

	return nil
}

func (checkpointer *inMemoryCheckpointer) RemoveLeaseInfo(shardID string) error {
	checkpointer.logger.Debug(fmt.Sprintf("RemoveLeaseInfo: shardID=%q", shardID))

	checkpointer.removeItem(shardID)
	return nil
}

func (checkpointer *inMemoryCheckpointer) RemoveLeaseOwner(shardID string) error {
	checkpointer.logger.Debug(fmt.Sprintf("RemoveLeaseOwner: shardID=%q", shardID))

	checkpointer.removeItem(shardID)
	return nil
}

func (checkpointer *inMemoryCheckpointer) GetLeaseOwner(shardID string) (string, error) {
	checkpointer.logger.Debug(fmt.Sprintf("GetLeaseOwner: shardID=%q", shardID))

	_, err := checkpointer.getItem(shardID)
	if err != nil {
		return "", err
	}

	return checkpointer.workerID, nil
}

func (checkpointer *inMemoryCheckpointer) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	checkpointer.logger.Debug("ListActiveWorkers")

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == chk.ShardEnd {
			continue
		}

		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			checkpointer.logger.Debug(fmt.Sprintf("Shard Not Assigned Error. ShardID: %s", shard.ID))
			return nil, chk.ErrShardNotAssigned
		}

		if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}

	return workers, nil
}

func (checkpointer *inMemoryCheckpointer) ClaimShard(shard *par.ShardStatus, _ string) error {
	checkpointer.logger.Debug(fmt.Sprintf("ClaimShard: shardID=%q", shard.ID))

	err := checkpointer.FetchCheckpoint(shard)
	if err != nil && !errors.Is(err, chk.ErrSequenceIDNotFound) {
		return err
	}

	item := marshalledCheckpoint{
		leaseTimeout:   shard.GetLeaseTimeout(),
		sequenceNumber: shard.Checkpoint,
		parentShardId:  shard.ParentShardId,
	}

	return checkpointer.saveItem(shard.ID, item)
}

func (checkpointer *inMemoryCheckpointer) saveItem(shardID string, item marshalledCheckpoint) error {
	checkpointer.lock.Lock()
	defer checkpointer.lock.Unlock()
	checkpointer.m[shardID] = item
	return nil
}

func (checkpointer *inMemoryCheckpointer) getItem(shardID string) (marshalledCheckpoint, error) {
	checkpointer.lock.Lock()
	defer checkpointer.lock.Unlock()
	item, ok := checkpointer.m[shardID]
	if !ok {
		return marshalledCheckpoint{}, chk.ErrSequenceIDNotFound
	}
	return item, nil
}

func (checkpointer *inMemoryCheckpointer) removeItem(shardID string) {
	checkpointer.lock.Lock()
	defer checkpointer.lock.Unlock()
	delete(checkpointer.m, shardID)
}
