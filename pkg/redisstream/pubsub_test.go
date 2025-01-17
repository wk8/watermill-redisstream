package redisstream

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// should be long enough to be robust even for CI boxes
const testInterval = 250 * time.Millisecond

func redisClient() (redis.UniversalClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:6379",
		DB:          0,
		ReadTimeout: -1,
		PoolTimeout: 10 * time.Minute,
	})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, errors.Wrap(err, "redis simple connect fail")
	}
	return client, nil
}

func redisClientOrFail(t *testing.T) redis.UniversalClient {
	client, err := redisClient()
	require.NoError(t, err)
	return client
}

func newPubSub(t *testing.T, subConfig *SubscriberConfig) (message.Publisher, message.Subscriber) {
	logger := watermill.NewStdLogger(true, false)

	publisher, err := NewPublisher(
		PublisherConfig{
			Client: redisClientOrFail(t),
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	subscriber, err := NewSubscriber(*subConfig, logger)
	require.NoError(t, err)

	return publisher, subscriber
}

func createPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPubSubWithConsumerGroup(t, watermill.NewShortUUID())
}

func createPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(t, &SubscriberConfig{
		Client:        redisClientOrFail(t),
		Consumer:      watermill.NewShortUUID(),
		ConsumerGroup: consumerGroup,
		ClaimInterval: 10 * time.Millisecond,
		MaxIdleTime:   5 * time.Second,
	})
}

func TestPublishSubscribe(t *testing.T) {
	features := tests.Features{
		ConsumerGroups:                      true,
		ExactlyOnceDelivery:                 false,
		GuaranteedOrder:                     false,
		GuaranteedOrderWithSingleSubscriber: true,
		Persistent:                          true,
		RequireSingleInstance:               false,
		NewSubscriberReceivesOldMessages:    true,
	}

	tests.TestPubSub(t, features, createPubSub, createPubSubWithConsumerGroup)
}

func TestSubscriber(t *testing.T) {
	topic := watermill.NewShortUUID()

	subscriber, err := NewSubscriber(
		SubscriberConfig{
			Client:        redisClientOrFail(t),
			Consumer:      watermill.NewShortUUID(),
			ConsumerGroup: watermill.NewShortUUID(),
		},
		watermill.NewStdLogger(true, false),
	)
	require.NoError(t, err)
	messages, err := subscriber.Subscribe(context.Background(), topic)
	require.NoError(t, err)

	publisher, err := NewPublisher(
		PublisherConfig{
			Client: redisClientOrFail(t),
		},
		watermill.NewStdLogger(false, false),
	)
	require.NoError(t, err)

	var sentMsgs message.Messages
	for i := 0; i < 50; i++ {
		msg := message.NewMessage(watermill.NewShortUUID(), nil)
		require.NoError(t, publisher.Publish(topic, msg))
		sentMsgs = append(sentMsgs, msg)
	}

	var receivedMsgs message.Messages
	for i := 0; i < 50; i++ {
		msg := <-messages
		if msg == nil {
			t.Fatal("msg nil")
		}
		receivedMsgs = append(receivedMsgs, msg)
		msg.Ack()
	}
	tests.AssertAllMessagesReceived(t, sentMsgs, receivedMsgs)

	require.NoError(t, publisher.Close())
	require.NoError(t, subscriber.Close())
}

func TestClaimIdle(t *testing.T) {
	topic := watermill.NewShortUUID()
	consumerGroup := watermill.NewShortUUID()
	testLogger := watermill.NewStdLogger(true, false)

	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: testInterval,
	}, testLogger)
	require.NoError(t, err)

	type messageWithMeta struct {
		msgID        int
		subscriberID int
	}

	receivedCh := make(chan *messageWithMeta)

	// let's start a few subscribers; each will wait between 3 and 5 intervals every time
	// it receives a message
	nSubscribers := 20
	seen := make(map[string]map[string]bool)
	var seenLock sync.Mutex
	for subscriberID := 0; subscriberID < nSubscribers; subscriberID++ {
		// need to assign to a variable local to the loop because of how golang
		// handles loop variables in function literals
		subID := subscriberID

		subscriber, err := NewSubscriber(
			SubscriberConfig{
				Client:        redisClientOrFail(t),
				Consumer:      strconv.Itoa(subID),
				ConsumerGroup: consumerGroup,
				ClaimInterval: testInterval,
				MaxIdleTime:   2 * testInterval,
				// we're only going to claim messages for consumers with odd IDs
				ShouldClaimPendingMessage: func(ext redis.XPendingExt) bool {
					idleConsumerID, err := strconv.Atoi(ext.Consumer)
					require.NoError(t, err)

					if idleConsumerID%2 == 0 {
						return false
					}

					seenLock.Lock()
					defer seenLock.Unlock()

					if seen[ext.ID] == nil {
						seen[ext.ID] = make(map[string]bool)
					}
					if seen[ext.ID][ext.Consumer] {
						return false
					}
					seen[ext.ID][ext.Consumer] = true
					return true
				},
			},
			testLogger,
		)
		require.NoError(t, err)

		router.AddNoPublisherHandler(
			strconv.Itoa(subID),
			topic,
			subscriber,
			func(msg *message.Message) error {
				msgID, err := strconv.Atoi(string(msg.Payload))
				require.NoError(t, err)

				receivedCh <- &messageWithMeta{
					msgID:        msgID,
					subscriberID: subID,
				}
				sleepInterval := (3 + 2*rand.Float64()) * float64(testInterval)
				time.Sleep(time.Duration(sleepInterval))

				return nil
			},
		)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, router.Run(runCtx))
	}()

	// now let's push a few messages
	publisher, err := NewPublisher(
		PublisherConfig{
			Client: redisClientOrFail(t),
		},
		testLogger,
	)
	require.NoError(t, err)

	nMessages := 100
	for msgID := 0; msgID < nMessages; msgID++ {
		msg := message.NewMessage(watermill.NewShortUUID(), []byte(strconv.Itoa(msgID)))
		require.NoError(t, publisher.Publish(topic, msg))
	}

	// now let's wait to receive them
	receivedByID := make(map[int][]*messageWithMeta)
	for len(receivedByID) != nMessages {
		select {
		case msg := <-receivedCh:
			receivedByID[msg.msgID] = append(receivedByID[msg.msgID], msg)
		case <-time.After(8 * testInterval):
			t.Fatalf("timed out waiting for new messages, only received %d unique messages", len(receivedByID))
		}
	}

	// shut down the router and the subscribers
	cancel()
	wg.Wait()

	// now let's look at what we've received:
	// * at least some messages should have been retried
	// * for retried messages, there should be at most one consumer with an even ID
	nMsgsWithRetries := 0
	for _, withSameID := range receivedByID {
		require.Greater(t, len(withSameID), 0)
		if len(withSameID) == 1 {
			// this message was not retried at all
			continue
		}

		nMsgsWithRetries++

		nEvenConsumers := 0
		for _, msg := range withSameID {
			if msg.subscriberID%2 == 0 {
				nEvenConsumers++
			}
		}
		assert.LessOrEqual(t, nEvenConsumers, 1)
	}

	assert.GreaterOrEqual(t, nMsgsWithRetries, 3)
}

// this test checks that even workers that are idle for a while will
// try to claim messages that have been idle for too long, which is not covered by TestClaimIdle
func TestMessagesGetClaimedEvenByIdleWorkers(t *testing.T) {
	topic := watermill.NewShortUUID()
	consumerGroup := watermill.NewShortUUID()
	testLogger := watermill.NewStdLogger(true, false)

	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: testInterval,
	}, testLogger)
	require.NoError(t, err)

	receivedCh := make(chan int)
	payload := message.Payload("coucou toi")

	// let's create a few subscribers, that just wait for a while each time they receive anything
	nSubscribers := 8
	for subscriberID := 0; subscriberID < nSubscribers; subscriberID++ {
		subID := subscriberID

		subscriber, err := NewSubscriber(
			SubscriberConfig{
				Client:        redisClientOrFail(t),
				Consumer:      strconv.Itoa(subID),
				ConsumerGroup: consumerGroup,
				ClaimInterval: testInterval,
				MaxIdleTime:   testInterval,
			},
			testLogger,
		)
		require.NoError(t, err)

		router.AddNoPublisherHandler(
			strconv.Itoa(subID),
			topic,
			subscriber,
			func(msg *message.Message) error {
				assert.Equal(t, msg.Payload, payload)

				receivedCh <- subID
				time.Sleep(time.Duration(nSubscribers+2) * testInterval)

				return nil
			},
		)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, router.Run(runCtx))
	}()

	// now let's push only one message
	publisher, err := NewPublisher(
		PublisherConfig{
			Client: redisClientOrFail(t),
		},
		testLogger,
	)
	require.NoError(t, err)
	msg := message.NewMessage(watermill.NewShortUUID(), payload)
	require.NoError(t, publisher.Publish(topic, msg))

	// it should get retried by all subscribers
	seenSubscribers := make([]bool, nSubscribers)
	for receivedCount := 0; receivedCount != nSubscribers; receivedCount++ {
		select {
		case subscriberID := <-receivedCh:
			assert.False(t, seenSubscribers[subscriberID], "subscriber %d seen more than once", subscriberID)
			seenSubscribers[subscriberID] = true

		case <-time.After(time.Duration(nSubscribers) * 2 * testInterval):
			t.Fatalf("timed out waiting for new messages, only received %d messages", receivedCount)
		}
	}

	// shut everything down
	cancel()
	wg.Wait()
}
