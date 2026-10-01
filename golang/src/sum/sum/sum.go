package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue        middleware.Middleware
	outputExchange    middleware.Middleware
	controlExchange   middleware.Middleware
	mutex             sync.Mutex
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	aggregationAmount int
	aggregationPrefix string
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.inputQueue.Close()
	sum.outputExchange.Close()
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlExchange, _ := middleware.CreateExchangeMiddleware("sum_control", []string{"flush"}, connSettings)

	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		controlExchange:   controlExchange,
		fruitItemMap:      make(map[string]map[string]fruititem.FruitItem),
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
	}, nil
}

func (sum *Sum) Run() {
	go sum.handleSignals()
	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})

	sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		clientID, _, _, _ := inner.DeserializeMessage(&msg)
		sum.handleEndOfRecordMessage(clientID)
		ack()
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.controlExchange.Send(msg); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientID string) error {
	slog.Info("Received End Of Records message")

	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if clientMap, exists := sum.fruitItemMap[clientID]; exists {
		for key := range clientMap {
			fruitRecord := []fruititem.FruitItem{clientMap[key]}
			message, err := inner.SerializeMessage(clientID, fruitRecord)
			if err != nil {
				slog.Debug("While serializing message", "err", err)
				return err
			}

			// SHARDING: Calo a qué Aggregator le toca esta fruta
			aggregatorIdx := int(hashString(key)) % sum.aggregationAmount
			routeKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, aggregatorIdx)

			if ex, ok := sum.outputExchange.(*middleware.ExchangeMiddleware); ok {
				if err := ex.SendWithKey(*message, routeKey); err != nil {
					slog.Debug("While sending message with key", "err", err)
					return err
				}
			} else {
				if err := sum.outputExchange.Send(*message); err != nil {
					slog.Debug("While sending message", "err", err)
					return err
				}
			}
		}
		delete(sum.fruitItemMap, clientID)
	}

	// El EOF se envía a todos los Aggregators
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientID, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	return nil
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if _, exists := sum.fruitItemMap[clientID]; !exists {
		sum.fruitItemMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	clientMap := sum.fruitItemMap[clientID]

	for _, fruitRecord := range fruitRecords {
		_, ok := clientMap[fruitRecord.Fruit]
		if ok {
			clientMap[fruitRecord.Fruit] = clientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
