(ns com.flocktory.component.kafka-log-tracer
  (:require [com.flocktory.protocol.tracer :as tracer]
            [clojure.tools.logging :as log]))

(defrecord log-tracer []
  tracer/ITracerName
  (tracer-name [this] "log-tracer")

  tracer/IOnConsumerStart
  (on-consumer-start
    [this group-id]
    (log/infof "[%s] consumer started" group-id))

  tracer/IOnConsumerStop
  (on-consumer-stop
    [this group-id]
    (log/infof "[%s] consumer stopped" group-id))

  tracer/IOnConsumerFail
  (on-consumer-fail
    [this group-id error]
    (log/errorf error "[%s] exception in poll loop" group-id))

  tracer/IOnConsumerFailLoop
  (on-consumer-fail-loop
    [this group-id failed-ago]
    (log/errorf "[%s] consumer failed %s ms ago" group-id failed-ago))

  tracer/IOnConsumerFailFast
  (on-consumer-fail-fast
    [this group-id opts]
    (log/errorf "[%s] consumer failed. Hard shutdown..." group-id))

  tracer/IOnConsumerIncError
  (on-consumer-inc-error
    [this group-id opts error]
    (log/errorf "[%s] consumer failed. Error: %s" group-id (.getMessage error)))

  tracer/IBeforePoll
  (before-poll
    [this group-id]
    (log/debugf "[%s] poll started" group-id))

  tracer/IAfterPoll
  (after-poll
    [this group-id records-count]
    (log/debugf "[%s] poll completed, %s records were fetched" group-id records-count))

  tracer/IBeforeConsumeRecord
  (before-consume-record
    [this group-id record]
    (log/tracef "[%s] before consume record %s" group-id
                (select-keys record [:topic :partition :offset])))

  tracer/IAfterConsumeRecord
  (after-consume-record
    [this group-id record]
    (log/tracef "[%s] after consume record %s" group-id
                (select-keys record [:topic :partition :offset])))

  tracer/IOnConsumeRecordError
  (on-consume-record-error
    [this group-id record exception]
    (log/errorf exception "[%s] error processing record %s" group-id
                (select-keys record [:topic :partition :offset])))

  tracer/IBeforeConsume
  (before-consume
    [this group-id records-count]
    (log/tracef "[%s] before consume %s records" group-id records-count))

  tracer/IAfterConsume
  (after-consume
    [this group-id records-count]
    (log/tracef "[%s] after consume %s records" group-id records-count))

  tracer/IOnConsumeError
  (on-consume-error
    [this group-id records-count exception]
    (log/errorf exception "[%s] error consuming %s records" group-id records-count))

  tracer/IBeforeConsumePartition
  (before-consume-partition
    [this group-id topic-partition records-count]
    (log/tracef "[%s] before consume %s records from topic-partition %s"
                group-id records-count topic-partition))

  tracer/IAfterConsumePartition
  (after-consume-partition
    [this group-id topic-partition records-count]
    (log/tracef "[%s] after consume %s records from topic-partition %s"
                group-id records-count topic-partition))

  tracer/IOnConsumePartitionError
  (on-consume-partition-error
    [this group-id topic-partition records-count exception]
    (log/errorf exception "[%s] error processing %s records from topic-partition %s"
                group-id records-count topic-partition))

  tracer/IBeforeCommit
  (before-commit
    [this group-id offsets]
    (log/debugf "[%s] before commit offsets %s" group-id (pr-str offsets)))

  tracer/IAfterCommit
  (after-commit
    [this group-id offsets]
    (log/debugf "[%s] after commit offsets %s" group-id (pr-str offsets)))

  tracer/IOnPartitionsAssigned
  (on-partitions-assigned
    [this group-id topic-partitions]
    (log/infof "[%s] assigned topic-partitions %s" group-id (pr-str topic-partitions)))

  tracer/IOnPartitionsRevoked
  (on-partitions-revoked
    [this group-id topic-partitions]
    (log/infof "[%s] revoked topic-partitions %s" group-id (pr-str topic-partitions)))

  tracer/IAfterPartitionsPaused
  (after-partitions-paused
    [this group-id topic-partitions]
    (log/infof "[%s] paused topic-partitions %s" group-id (pr-str topic-partitions)))

  tracer/IAfterPartitionsResumed
  (after-partitions-resumed
    [this group-id topic-partitions]
    (log/infof "[%s] resumed topic-partitions %s" group-id (pr-str topic-partitions))))
