package broker

import (
	"bufio"
	"errors"
	"godel/internal/protocol"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

type writeResult struct {
	Error         error
	CorrelationID int32
}

func (b *Broker) runServer(port int) error {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	defer listener.Close()

	slog.Info("server ready to accept connections", "port", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("failed to accept connection", "error", err)
			continue
		}

		go b.handleConnection(conn)
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			slog.Error("failed to close connection", "error", err)
		}
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	responsesCh := make(chan *protocol.BaseResponse, 100)
	resultsCh := make(chan *writeResult) // used to stop consumer on write error

	for {
		req, err := protocol.DeserializeRequest(reader)
		if err == io.EOF {
			slog.Debug("client disconnected, closing connection")
			return

		}
		if err != nil {
			slog.Error("failed to deserialize message, closing connection", "error", err)
			return
		}

		// responses goroutine
		go func() {
			for resp := range responsesCh {
				if err := writeFull(writer, resp.Serialize()); err != nil {
					slog.Error("failed to send response", "cmd", resp.Cmd, "error", err)
					resultsCh <- &writeResult{
						Error:         err,
						CorrelationID: resp.CorrelationID,
					}
					return
				}
				resultsCh <- &writeResult{
					Error:         nil,
					CorrelationID: resp.CorrelationID,
				}
			}
		}()

		// requests goroutine
		go func(req *protocol.BaseRequest) {
			responder := func(resp *protocol.BaseResponse) {
				responsesCh <- resp
			}

			slog.Debug("received request", "cmd", req.Cmd)
			respPayload, err := b.processRequest(req, responder, resultsCh)
			if err != nil {
				slog.Error("error while processing request", "error", err)
				return
			}
			if respPayload == nil {
				return
			}

			responsesCh <- &protocol.BaseResponse{
				Cmd:           req.Cmd,
				CorrelationID: req.CorrelationID,
				Payload:       respPayload,
			}
		}(req)
	}
}

func writeFull(w *bufio.Writer, data []byte) error {
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Flush()
}

func (b *Broker) processRequest(req *protocol.BaseRequest, responder func(resp *protocol.BaseResponse), resultsCh chan *writeResult) ([]byte, error) {
	switch req.ApiVersion {
	case 0:
		return b.processApiV0Request(req, responder, resultsCh)
	default:
		return nil, errors.New("unsupported api version")
	}
}

func (b *Broker) processApiV0Request(r *protocol.BaseRequest, responder func(resp *protocol.BaseResponse), resultsCh chan *writeResult) ([]byte, error) {
	switch r.Cmd {
	case protocol.CmdCreateTopics:
		req, err := protocol.Deserialize[protocol.ReqCreateTopics](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		return b.processCreateTopicsReq(req)
	case protocol.CmdProduce:
		req, err := protocol.Deserialize[protocol.ReqProduce](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		return b.processProduceReq(req)
	case protocol.CmdConsume:
		req, err := protocol.Deserialize[protocol.ReqConsume](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processConsumeReq(r.CorrelationID, req, responder, resultsCh)
		if resp == nil {
			return nil, nil
		}

		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdDeleteConsumer:
		req, err := protocol.Deserialize[protocol.ReqDeleteConsumer](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processDeleteConsumerReq(req)

		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdListConsumerGroups:
		req, err := protocol.Deserialize[protocol.ReqListConsumerGroups](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processListConsumerGroupsReq(req)
		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdCommitOffset:
		req, err := protocol.Deserialize[protocol.ReqCommitOffset](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processCommitOffsetRequest(req)
		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdHeartbeat:
		req, err := protocol.Deserialize[protocol.ReqHeartbeat](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processHeartbeatRequest(req)
		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdListTopics:
		req, err := protocol.Deserialize[protocol.ReqListTopics](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processListTopicsReq(req)
		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdCreateConsumer:
		req, err := protocol.Deserialize[protocol.ReqCreateConsumer](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processReqCreateConsumer(req)
		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdDeleteTopic:
		req, err := protocol.Deserialize[protocol.ReqDeleteTopic](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processDeleteTopicReq(req)
		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	case protocol.CmdGetConsumerGroup:
		req, err := protocol.Deserialize[protocol.ReqGetConsumerGroup](r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processGetConsumerGroupReq(req)
		if resp == nil {
			return nil, nil
		}
		buf, err := protocol.Serialize(resp)
		if err != nil {
			return nil, err
		}

		return buf, nil
	default:
		return nil, errors.New("unknonw command " + strconv.Itoa(int(r.Cmd)))
	}
}

func (b *Broker) processCreateTopicsReq(req *protocol.ReqCreateTopics) ([]byte, error) {
	var resp protocol.RespCreateTopics
	resp.Topics = make([]protocol.RespCreateTopicTopic, 0, len(req.Topics))

	for i := range req.Topics {
		_, err := b.CreateTopic(req.Topics[i].Name, &req.Topics[i].Configs)
		if err != nil {
			resp.Topics = append(resp.Topics, protocol.RespCreateTopicTopic{
				Name:         req.Topics[i].Name,
				ErrorCode:    1,
				ErrorMessage: err.Error(),
			})
			continue
		}

		resp.Topics = append(resp.Topics, protocol.RespCreateTopicTopic{
			Name: req.Topics[i].Name,
		})
	}

	respBuf, err := protocol.Serialize(resp)
	if err != nil {
		return nil, err
	}

	return respBuf, nil
}

func (b *Broker) processProduceReq(req *protocol.ReqProduce) ([]byte, error) {
	var resp protocol.RespProduce
	resp.Messages = make([]protocol.RespProduceMessage, 0, len(req.Messages))

	for i := range req.Messages {
		timestamp := uint64(time.Now().Unix())
		message := NewMessage(timestamp, req.Messages[i].Key, req.Messages[i].Value)

		offset, partition, err := b.Produce(req.Topic, message)
		if err != nil {
			resp.Messages = append(resp.Messages, protocol.RespProduceMessage{
				Key:          string(req.Messages[i].Key),
				ErrorCode:    1,
				ErrorMessage: err.Error(),
			})
			continue
		}

		resp.Messages = append(resp.Messages, protocol.RespProduceMessage{
			Key:       string(req.Messages[i].Key),
			Partition: &partition,
			Offset:    &offset,
		})
	}

	respBuf, err := protocol.Serialize(resp)
	if err != nil {
		return nil, err
	}

	return respBuf, nil
}

func (b *Broker) processConsumeReq(cID int32, req *protocol.ReqConsume, responder func(resp *protocol.BaseResponse), resultsCh chan *writeResult) *protocol.RespConsume {
	topic, err := b.GetTopic(req.Topic)
	if err != nil {
		return &protocol.RespConsume{
			ErrorCode:    1,
			ErrorMessage: err.Error(),
		}
	}

	consumer, err := topic.getConsumer(req.Group, req.ID)
	if err != nil {
		return &protocol.RespConsume{
			ErrorCode:    1,
			ErrorMessage: err.Error(),
		}
	}

	b.RUnlock()

	onMessage := func(message *Message) error {
		r := protocol.RespConsume{
			Messages: []protocol.RespConsumeMessage{
				{
					Key:       string(message.key),
					Group:     req.Group,
					Partition: &message.partition,
					Offset:    &message.offset,
					Payload:   message.payload,
				},
			},
		}

		respBuf, err := protocol.Serialize(r)
		if err != nil {
			return err
		}

		resp := protocol.BaseResponse{
			Cmd:           protocol.CmdConsume,
			CorrelationID: cID,
			Payload:       respBuf,
		}

		responder(&resp)

		writeResult := <-resultsCh
		if writeResult.Error != nil {
			slog.Debug("write error, stopping consumer", "corrID", cID, "offet", message.offset, "err", err)
			return writeResult.Error
		}

		return nil
	}

	err = consumer.start(cID, onMessage, responder)
	if err != nil {
		return &protocol.RespConsume{
			ErrorCode:    1,
			ErrorMessage: err.Error(),
		}
	}

	return nil
}

func (b *Broker) processDeleteConsumerReq(req *protocol.ReqDeleteConsumer) *protocol.RespDeleteConsumer {
	resp := &protocol.RespDeleteConsumer{
		ID:    req.ID,
		Group: req.Group,
		Topic: req.Topic,
	}

	topic, err := b.GetTopic(req.Topic)
	defer b.RUnlock()
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	err = topic.removeConsumer(req.Group, req.ID)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	return resp
}

func (b *Broker) processListConsumerGroupsReq(req *protocol.ReqListConsumerGroups) *protocol.RespListConsumerGroups {
	resp := &protocol.RespListConsumerGroups{
		Groups: []protocol.ConsumerGroup{},
	}

	topic, err := b.GetTopic(req.Topic)
	defer b.RUnlock()
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	groups := topic.listConsumerGroups()

	for k := range groups {
		consumers := []protocol.Consumer{}
		offsets := []protocol.ConsumerGroupOffset{}

		for partition := range groups[k].offsets {
			offsets = append(offsets, protocol.ConsumerGroupOffset{
				Partition: partition,
				Offset:    groups[k].offsets[partition],
			})
		}

		for i := range groups[k].consumers {
			consumerPartitions := make([]uint32, len(groups[k].consumers[i].partitions))
			for i := range groups[k].consumers[i].partitions {
				consumerPartitions[i] = groups[k].consumers[i].partitions[i].num
			}

			consumers = append(consumers, protocol.Consumer{
				ID:         groups[k].consumers[i].id,
				Partitions: consumerPartitions,
			})
		}

		resp.Groups = append(resp.Groups, protocol.ConsumerGroup{
			Name:      groups[k].name,
			Consumers: consumers,
			Offsets:   offsets,
		})
	}

	return resp
}

func (b *Broker) processCommitOffsetRequest(req *protocol.ReqCommitOffset) *protocol.RespCommitOffset {
	resp := &protocol.RespCommitOffset{
		Partition: &req.Partition,
		Offset:    &req.Offset,
	}

	topic, err := b.GetTopic(req.Topic)
	defer b.RUnlock()
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	err = topic.commitOffset(req.Group, req.Partition, req.Offset)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	return resp
}

func (b *Broker) processHeartbeatRequest(req *protocol.ReqHeartbeat) *protocol.RespHeartbeat {
	resp := &protocol.RespHeartbeat{
		ConsumerID: req.ConsumerID,
	}

	topic, err := b.GetTopic(req.Topic)
	defer b.RUnlock()
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	err = topic.heartbeat(req.Group, req.ConsumerID)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	return resp
}

func (b *Broker) processListTopicsReq(req *protocol.ReqListTopics) *protocol.RespListTopics {
	resp := &protocol.RespListTopics{
		Topics: []protocol.Topic{},
	}

	topic := b.listTopics()
	defer b.RUnlock()

	for k := range topic {
		if req.NameFilter != "" && !strings.Contains(topic[k].name, req.NameFilter) {
			continue
		}

		partitions := []uint32{}
		groups := []string{}

		for i := range topic[k].partitions {
			partitions = append(partitions, topic[k].partitions[i].num)
		}

		for i := range topic[k].consumerGroups {
			groups = append(groups, topic[k].consumerGroups[i].name)
		}

		resp.Topics = append(resp.Topics, protocol.Topic{
			Name:       topic[k].name,
			Partitions: partitions,
			Groups:     groups,
			Options:    *topic[k].options,
		})
	}

	return resp
}

func (b *Broker) processReqCreateConsumer(req *protocol.ReqCreateConsumer) *protocol.RespCreateConsumer {
	resp := &protocol.RespCreateConsumer{
		ID:    req.ID,
		Topic: req.Topic,
		Group: req.Group,
	}

	topic, err := b.GetTopic(req.Topic)
	defer b.RUnlock()
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	consumer, err := topic.createConsumer(req.Group, req.ID, &req.Options)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	resp.ID = consumer.id

	return resp
}

func (b *Broker) processDeleteTopicReq(req *protocol.ReqDeleteTopic) *protocol.RespDeleteTopic {
	resp := &protocol.RespDeleteTopic{
		Topic: req.Topic,
	}

	err := b.deleteTopic(req.Topic)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	return resp
}

func (b *Broker) processGetConsumerGroupReq(req *protocol.ReqGetConsumerGroup) *protocol.RespGetConsumerGroup {
	resp := &protocol.RespGetConsumerGroup{}

	topic, err := b.GetTopic(req.Topic)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	cg, err := topic.getConsumerGroup(req.Name)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	consumers := make([]protocol.Consumer, len(cg.consumers))
	for i := range cg.consumers {
		partitions := make([]uint32, len(cg.consumers[i].partitions))
		for j := range cg.consumers[i].partitions {
			partitions[j] = cg.consumers[i].partitions[j].num
		}

		consumers[i] = protocol.Consumer{
			ID:         cg.consumers[i].id,
			Partitions: partitions,
		}
	}

	offsets := make([]protocol.ConsumerGroupOffset, 0, len(cg.offsets))
	for partition, offset := range cg.offsets {
		offsets = append(offsets, protocol.ConsumerGroupOffset{
			Partition: partition,
			Offset:    offset,
		})
	}

	resp.Group = protocol.ConsumerGroup{
		Name:      cg.name,
		Consumers: consumers,
		Offsets:   offsets,
	}

	return resp
}
