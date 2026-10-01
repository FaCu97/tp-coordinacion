package join

import (
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	globalFruitsMap   map[string]map[string]fruititem.FruitItem
	messagesCountMap  map[string]int
	aggregationAmount int
	topSize           int
	mutex             sync.Mutex
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		globalFruitsMap:   make(map[string]map[string]fruititem.FruitItem),
		messagesCountMap:  make(map[string]int),
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
	}, nil
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.inputQueue.Close()
	join.outputQueue.Close()
}

func (join *Join) Run() {
	go join.handleSignals()
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("Failed to deserialize message", "err", err)
		return
	}

	join.mutex.Lock()
	defer join.mutex.Unlock()

	if _, exists := join.globalFruitsMap[clientID]; !exists {
		join.globalFruitsMap[clientID] = make(map[string]fruititem.FruitItem)
	}

	clientMap := join.globalFruitsMap[clientID]
	for _, fruitRecord := range fruitRecords {
		if _, ok := clientMap[fruitRecord.Fruit]; ok {
			clientMap[fruitRecord.Fruit] = clientMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}

	join.messagesCountMap[clientID]++

	// Si ya recibimos los Tops de Todos los Aggregators para el cliente, armamos el top final
	if join.messagesCountMap[clientID] == join.aggregationAmount {
		join.sendGlobalTop(clientID)
	}
}

func (join *Join) sendGlobalTop(clientID string) {
	clientMap := join.globalFruitsMap[clientID]

	var finalFruits []fruititem.FruitItem
	for _, fruit := range clientMap {
		finalFruits = append(finalFruits, fruit)
	}

	sort.Slice(finalFruits, func(i, j int) bool {
		if finalFruits[i].Amount == finalFruits[j].Amount {
			return finalFruits[i].Fruit < finalFruits[j].Fruit
		}
		return finalFruits[i].Amount > finalFruits[j].Amount
	})

	if len(finalFruits) > join.topSize {
		finalFruits = finalFruits[:join.topSize]
	}

	message, err := inner.SerializeMessage(clientID, finalFruits)
	if err != nil {
		slog.Error("While serializing global top", "err", err)
	} else {
		if err := join.outputQueue.Send(*message); err != nil {
			slog.Error("While sending global top", "err", err)
		} else {
			slog.Info("Global Top sent to Gateway!", "client", clientID)
		}
	}

	delete(join.globalFruitsMap, clientID)
	delete(join.messagesCountMap, clientID)
}
