package messagehandler

import (
	"crypto/rand"
	"encoding/hex"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	client_id string
}

func generateID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{client_id: generateID()}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.client_id, data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := []fruititem.FruitItem{}
	return inner.SerializeMessage(messageHandler.client_id, data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if clientID != messageHandler.client_id {
		return nil, nil
	}

	if isEof {
		return nil, nil
	}

	return fruitRecords, nil
}
