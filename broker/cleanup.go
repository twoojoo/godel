package broker

import (
	"godel/options"
	"log/slog"
	"time"
)

// scheduleRetentionCheck asyncronously starts retention checks
// based on log.retention.check.interval.ms options. The first
// check is started immediately.
func (b *Broker) scheduleRetentionCheck() {
	slog.Info("scheduling retention checks", "interval", b.options.LogRetentionCheckIntervalMilli)

	go func() {
		for {
			b.runRetentionCheck()
			schedule := time.Millisecond * time.Duration(b.options.LogRetentionCheckIntervalMilli)
			time.Sleep(schedule)
		}
	}()
}

func (b *Broker) runRetentionCheck() {
	slog.Info("started retention check")
	now := uint64(time.Now().Unix())

	for i := range b.topics {
		retentionMilli := b.topics[i].options.RetentionMilli
		retentionBytes := b.topics[i].options.RetentionBytes
		cleanupPolicy := b.topics[i].options.CleanupPolicy

		for j := range b.topics[i].partitions {
			if retentionMilli > -1 {
				slog.Info("running retention.ms check", "segments", len(b.topics[i].partitions[j].segments))

				for k := range b.topics[i].partitions[j].segments {
					expired, err := b.topics[i].partitions[j].segments[k].runMaxRetentionMilliCheck(now, retentionMilli)
					if err != nil {
						slog.Error("failed retention.ms check",
							"topic", b.topics[i].name,
							"partition", b.topics[i].partitions[j].num,
							"segment", b.topics[i].partitions[j].segments[k].baseOffset,
							"error", err,
						)
					}

					slog.Info("retention.bytes check result",
						"topic", b.topics[i].name,
						"partition", b.topics[i].partitions[j].num,
						"segment", b.topics[i].partitions[j].segments[k].baseOffset,
						"expired", expired,
					)

					if expired && cleanupPolicy == options.CleanupPolicyDelete {
						err = b.topics[i].partitions[j].deleteSegment(k)
						if err != nil {
							slog.Error("failed retention.ms check segment deletion",
								"topic", b.topics[i].name,
								"partition", b.topics[i].partitions[j].num,
								"segment", b.topics[i].partitions[j].segments[k].baseOffset,
								"error", err,
							)
						}

						slog.Info("retention.ms check segment deletion done",
							"topic", b.topics[i].name,
							"partition", b.topics[i].partitions[j].num,
							"segment", b.topics[i].partitions[j].segments[k].baseOffset,
						)
					}

					// if expired && cleanupPolicy == CleanupPolicyCompact {
					// }
				}
			}

			if retentionBytes > -1 {
				for k := range b.topics[i].partitions[j].segments {
					if b.topics[i].partitions[j].getSize() < uint32(retentionBytes) {
						break
					}

					if cleanupPolicy == options.CleanupPolicyDelete {
						err := b.topics[i].partitions[j].deleteSegment(k)
						if err != nil {
							slog.Error("failed retention.bytes check segment deletion",
								"topic", b.topics[i].name,
								"partition", b.topics[i].partitions[j].num,
								"segment", b.topics[i].partitions[j].segments[k].baseOffset,
								"error", err,
							)
						}

						slog.Info("retention.bytes check segment deletion done",
							"topic", b.topics[i].name,
							"partition", b.topics[i].partitions[j].num,
							"segment", b.topics[i].partitions[j].segments[k].baseOffset,
						)
					}

					// if cleanupPolicy == CleanupPolicyCompact {
					// }
				}
			}
		}
	}

	slog.Info("retention check done for all topics")
}
