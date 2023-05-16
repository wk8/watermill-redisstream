package redisstream

import (
	"context"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	errorbuffer "github.com/wk8/go-error-buffer"
)

const (
	DefaultBlockTime = 100 * time.Millisecond

	// DefaultClaimInterval is how often to check for dead workers to claim pending messages from
	DefaultClaimInterval = 5 * time.Second

	DefaultClaimBatchSize = int64(100)

	// DefaultMaxIdleTime is the default max idle time for pending message.
	// After timeout, the message will be claimed and its idle consumer will be removed from consumer group
	DefaultMaxIdleTime = time.Minute

	DefaultRedisErrorsMaxCount = uint(3)
	DefaultRedisErrorsWindow   = time.Minute

	redisBusyGroup = "BUSYGROUP Consumer Group name already exists"
)

type SubscriberConfig struct {
	Client redis.UniversalClient

	Unmarshaller Unmarshaller

	// Redis stream consumer id, paired with ConsumerGroup
	Consumer string

	// Cannot be empty
	ConsumerGroup string

	// How long after Nack message should be redelivered
	NackResendSleep time.Duration

	// Claim idle pending message maximum interval
	ClaimInterval time.Duration

	// How many pending messages are claimed at most each claim interval
	ClaimBatchSize int64

	// How long should we treat a consumer as offline
	MaxIdleTime time.Duration

	// RedisErrorsMaxCount is how many redis errors in a RedisErrorsWindow duration is too many
	// When there are that many errors in the given duration, the subscriber will give up and close
	// its output channel.
	RedisErrorsMaxCount uint
	RedisErrorsWindow   time.Duration

	// If this is set, it will be called to decide whether messages that
	// have been idle for longer than MaxIdleTime should actually be re-claimed,
	// and the consumer that had previously claimed it should be kicked out.
	// If this is not set, then all messages that have been idle for longer
	// than MaxIdleTime will be re-claimed.
	// This can be useful e.g. for tasks where the processing time can be very variable -
	// so we can't just use a short MaxIdleTime; but where at the same time dead
	// workers should be spotted quickly - so we can't just use a long MaxIdleTime either.
	// In such cases, if we have another way of checking for workers' health, then we can
	// leverage that in this callback.
	ShouldClaimPendingMessage func(redis.XPendingExt) bool
}

func (sc *SubscriberConfig) setDefaults() {
	if sc.Unmarshaller == nil {
		sc.Unmarshaller = DefaultMarshallerUnmarshaller{}
	}
	if sc.Consumer == "" {
		sc.Consumer = watermill.NewShortUUID()
	}
	if sc.ClaimInterval <= 0 {
		sc.ClaimInterval = DefaultClaimInterval
	}
	if sc.ClaimBatchSize <= 0 {
		sc.ClaimBatchSize = DefaultClaimBatchSize
	}
	if sc.MaxIdleTime <= 0 {
		sc.MaxIdleTime = DefaultMaxIdleTime
	}
	if sc.RedisErrorsMaxCount == 0 {
		sc.RedisErrorsMaxCount = DefaultRedisErrorsMaxCount
	}
	if sc.RedisErrorsWindow <= 0 {
		sc.RedisErrorsWindow = DefaultRedisErrorsWindow
	}
	if sc.ShouldClaimPendingMessage == nil {
		sc.ShouldClaimPendingMessage = func(_ redis.XPendingExt) bool {
			return true
		}
	}
}

func (sc *SubscriberConfig) Validate() error {
	if sc.Client == nil {
		return errors.New("redis client is empty")
	}
	if sc.ConsumerGroup == "" {
		return errors.New("consumer group is empty")
	}
	if sc.ClaimBatchSize < 2 {
		return errors.New("claim batch size must be at least 2")
	}

	return nil
}

type Subscriber struct {
	config     *SubscriberConfig
	client     redis.UniversalClient
	baseLogger watermill.LoggerAdapter

	baseCtx          context.Context
	baseCtxCancel    context.CancelFunc
	topicSubscribers sync.WaitGroup
}

var _ message.Subscriber = &Subscriber{}

// NewSubscriber creates a new redis stream Subscriber
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = &watermill.NopLogger{}
	}

	baseCtx, baseCtxCancel := context.WithCancel(context.Background())

	return &Subscriber{
		config:        &config,
		client:        config.Client,
		baseLogger:    logger,
		baseCtx:       baseCtx,
		baseCtxCancel: baseCtxCancel,
	}, nil
}

type topicSubscriber struct {
	config      *SubscriberConfig
	topic       string
	client      redis.UniversalClient
	logger      watermill.LoggerAdapter
	redisErrors *errorbuffer.ErrorBuffer
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if err := s.baseCtx.Err(); err != nil {
		return nil, errors.New("subscriber closed")
	}

	// create consumer group
	if _, err := s.client.XGroupCreateMkStream(ctx, topic, s.config.ConsumerGroup, "0").Result(); err != nil && err.Error() != redisBusyGroup {
		return nil, errors.Wrap(err, "unable to create redis consumer group")
	}

	logFields := watermill.LogFields{
		"provider":       "redis",
		"topic":          topic,
		"consumer_group": s.config.ConsumerGroup,
		"consumer_uuid":  s.config.Consumer,
	}
	logger := s.baseLogger.With(logFields)
	logger.Debug("Subscribing to redis stream topic", nil)

	subscriber := &topicSubscriber{
		config:      s.config,
		topic:       topic,
		client:      s.client,
		logger:      logger,
		redisErrors: errorbuffer.NewErrorBuffer(s.config.RedisErrorsMaxCount, s.config.RedisErrorsWindow),
	}

	output := make(chan *message.Message)

	runCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-runCtx.Done():
		case <-s.baseCtx.Done():
			cancel()
		}
	}()

	s.topicSubscribers.Add(1)

	go func() {
		defer s.topicSubscribers.Done()
		defer cancel()
		defer close(output)

		if err := subscriber.run(runCtx, output); !isContextDoneErr(err) {
			logger.Error("subscriber exited with error", err, nil)
		}
	}()

	return output, nil
}

func (s *Subscriber) Close() error {
	s.baseCtxCancel()
	s.topicSubscribers.Wait()
	s.baseLogger.Debug("Redis stream subscriber closed", nil)
	return nil
}

// blocking call; never returns nil
func (s *topicSubscriber) run(ctx context.Context, output chan *message.Message) error {
	for {
		// first process any pending messages
		redisMessage, claimedFrom, err := s.claimPendingMessage(ctx)
		if err != nil {
			return err
		}

		if redisMessage == nil {
			// read directly from the message queue
			redisMessage, err = s.read(ctx)
			if err != nil {
				return err
			}
		}

		if redisMessage == nil {
			continue
		}

		if err := s.processMessage(ctx, redisMessage, output); err != nil {
			return err
		}

		if err := s.deleteIdleConsumer(ctx, claimedFrom); err != nil {
			return err
		}
	}
}

// claimPendingMessage tries to claim a message from the ones that are pending, if any
// If it returns a non-nil message, it also returns which consumer it was claimed from.
func (s *topicSubscriber) claimPendingMessage(ctx context.Context) (*redis.XMessage, string, error) {
	for startID := "0"; ; {
		pendingMessages, err := s.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: s.topic,
			Group:  s.config.ConsumerGroup,
			Idle:   s.config.MaxIdleTime,
			Start:  startID,
			End:    "+",
			Count:  s.config.ClaimBatchSize,
		}).Result()
		if err != nil {
			if isContextDoneErr(err) {
				return nil, "", err
			} else {
				s.logger.Error("xpendingext failed", err, nil)
				return nil, "", s.redisErrors.Add(err)
			}
		}

		for _, pendingMessage := range pendingMessages {
			if !s.config.ShouldClaimPendingMessage(pendingMessage) {
				continue
			}

			// try to claim this message
			claimed, err := s.client.XClaim(ctx, &redis.XClaimArgs{
				Stream:   s.topic,
				Group:    s.config.ConsumerGroup,
				Consumer: s.config.Consumer,
				// this is important: it ensures that 2 concurrent subscribers
				// won't claim the same pending message at the same time
				MinIdle:  s.config.MaxIdleTime,
				Messages: []string{pendingMessage.ID},
			}).Result()
			if err != nil {
				if isContextDoneErr(err) {
					return nil, "", err
				} else {
					s.logger.Error("xclaim failed", err, watermill.LogFields{"pending": pendingMessage})
					return nil, "", s.redisErrors.Add(err)
				}
			}

			if len(claimed) != 0 {
				if len(claimed) != 1 {
					// shouldn't happen, we only tried to claim one message
					err := errors.Errorf("claimed %d messages", len(claimed))
					s.logger.Error("claimed more than 1 messages", err, watermill.LogFields{"claimed": claimed})
				}

				return &claimed[0], pendingMessage.Consumer, nil
			}
		}

		if int64(len(pendingMessages)) < s.config.ClaimBatchSize {
			return nil, "", nil
		}

		startID = pendingMessages[len(pendingMessages)-1].ID
	}
}

// read reads directly from the queue
func (s *topicSubscriber) read(ctx context.Context) (*redis.XMessage, error) {
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.config.ConsumerGroup,
		Consumer: s.config.Consumer,
		Streams:  []string{s.topic, ">"},
		Count:    1,
		Block:    s.config.ClaimInterval,
	}).Result()
	if err != nil && err != redis.Nil {
		if isContextDoneErr(err) {
			return nil, err
		} else {
			s.logger.Error("read failed", err, nil)
			return nil, s.redisErrors.Add(err)
		}
	}

	if len(streams) == 0 {
		return nil, nil
	}
	if len(streams) != 1 {
		// shouldn't happen, we only queried one stream
		err := errors.Errorf("read from %d streams", len(streams))
		names := make([]string, len(streams))
		for i, stream := range streams {
			names[i] = stream.Stream
		}
		s.logger.Error("read from more than 1 stream", err, watermill.LogFields{"stream_names": names})
	}

	if len(streams[0].Messages) == 0 {
		return nil, nil
	}
	if len(streams[0].Messages) != 1 {
		// same, we only asked for 1 message
		err := errors.Errorf("read %d messages", len(streams[0].Messages))
		s.logger.Error("read more than 1 message", err, nil)
	}

	return &streams[0].Messages[0], nil
}

func (s *topicSubscriber) processMessage(ctx context.Context, redisMessage *redis.XMessage, output chan *message.Message) error {
	logger := s.logger.With(watermill.LogFields{"message_id": redisMessage.ID})
	logger.Trace("Processing redis message", nil)

	msg, err := s.config.Unmarshaller.Unmarshal(redisMessage.Values)
	if err != nil {
		return errors.Wrapf(err, "message unmarshal failed")
	}

	msgCtx, cancel := context.WithCancel(ctx)
	msg.SetContext(msgCtx)
	defer cancel()

	for {
		select {
		case output <- msg:
			logger.Trace("message sent to consumer", nil)
		case <-ctx.Done():
			return ctx.Err()
		}

		select {
		case <-msg.Acked():
			var redisError error

			return retry.Retry(
				func(attempt uint) error {
					xAckErr := s.client.XAck(ctx, s.topic, s.config.ConsumerGroup, redisMessage.ID).Err()

					if xAckErr == nil {
						logger.Trace("message successfully acked", nil)
					} else if !isContextDoneErr(xAckErr) {
						redisError = s.redisErrors.Add(xAckErr)
						logger.Error("message ack failed", xAckErr, watermill.LogFields{"attempt": attempt, "retrying": redisError == nil})
						if redisError == nil {
							time.Sleep(100 * time.Millisecond)
						} else {
							xAckErr = redisError
						}
					}

					return xAckErr
				},
				func(_ uint) bool {
					return ctx.Err() == nil && redisError == nil
				},
			)

		case <-msg.Nacked():
			logger.Trace("message nacked", nil)

			if s.config.NackResendSleep > 0 {
				select {
				case <-time.After(s.config.NackResendSleep):
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			// reset acks, etc.
			msg = msg.Copy()

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *topicSubscriber) deleteIdleConsumer(ctx context.Context, consumerName string) error {
	if consumerName == "" {
		return nil
	}

	err := s.client.XGroupDelConsumer(ctx, s.topic, s.config.ConsumerGroup, consumerName).Err()

	if err != nil && !isContextDoneErr(err) {
		s.logger.Error("xgroupdelconsumer failed", err, watermill.LogFields{"consumerName": consumerName})
		err = s.redisErrors.Add(err)
	}

	return err
}

func isContextDoneErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
