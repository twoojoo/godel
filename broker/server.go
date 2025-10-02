package broker

import (
	"bufio"
	"errors"
	"godel/internal/protocol"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"
)

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

	for {
		req, err := protocol.DeserializeRequest(reader)
		if err == io.EOF {
			slog.Info("client disconnected, closing connection")
			return

		}
		if err != nil {
			slog.Error("failed to deserialize message, closing connection", "error", err)
			return
		}

		responder := func(resp *protocol.BaseResponse) error {
			err = writeFull(writer, resp.Serialize())
			if err != nil {
				return err
			}

			return nil
		}

		respPayload, err := b.processRequest(req, responder)
		if err != nil {
			slog.Error("error while processing request", "error", err)
			continue
		}
		if respPayload == nil {
			continue
		}

		resp := protocol.BaseResponse{
			CorrelationID: req.CorrelationID,
			Payload:       respPayload,
		}

		err = writeFull(writer, resp.Serialize())
		if err != nil {
			slog.Error("failed to send response to client")
		}
	}
}

func writeFull(w *bufio.Writer, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := w.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}

	return w.Flush()
}

func (b *Broker) processRequest(req *protocol.BaseRequest, responder func(resp *protocol.BaseResponse) error) ([]byte, error) {
	switch req.ApiVersion {
	case 0:
		return b.processApiV0Request(req, responder)
	default:
		return nil, errors.New("unsupported api version")
	}
}

func (b *Broker) processApiV0Request(r *protocol.BaseRequest, responder func(resp *protocol.BaseResponse) error) ([]byte, error) {
	switch r.Cmd {
	case protocol.CmdCreateTopics:
		req, err := protocol.DeserializeRequestCreateTopic(r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		return b.processCreateTopicsReq(req)
	case protocol.CmdProduce:
		req, err := protocol.DeserializeRequestProduce(r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		return b.processProduceReq(req)
	case protocol.CmdConsume:
		req, err := protocol.DeserializeRequestConsume(r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		return b.processConsumeReq(r.CorrelationID, req, responder)
	case protocol.CmdLeaveGroup:
		req, err := protocol.DeserializeRequestDeleteConsumer(r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		return b.processDeleteConsumerReq(req)
	case protocol.CmdListGroups:
		req, err := protocol.DeserializeReqListConsumerGroups(r.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		resp := b.processListConsumerGroupsReq(req)
		buf, err := resp.Serialize()
		if err != nil {
			return nil, err
		}

		return buf, nil
	default:
		return nil, errors.New("unknonw command " + strconv.Itoa(int(r.Cmd)))
	}
}

func (b *Broker) processCreateTopicsReq(req *protocol.ReqCreateTopic) ([]byte, error) {
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

	respBuf, err := resp.Serialize()
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

	respBuf, err := resp.Serialize()
	if err != nil {
		return nil, err
	}

	return respBuf, nil
}

func (b *Broker) processConsumeReq(cID int32, req *protocol.ReqConsume, responder func(resp *protocol.BaseResponse) error) ([]byte, error) {
	topic, err := b.GetTopic(req.Topic)
	if err != nil {
		return nil, err
	}

	consumer := topic.createConsumer(req.Group, req.FromBeginning)

	err = consumer.start(func(message *Message) error {
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

		respBuf, err := r.Serialize()
		if err != nil {
			return err
		}

		resp := protocol.BaseResponse{
			CorrelationID: cID,
			Payload:       respBuf,
		}

		return responder(&resp)
	})

	return nil, err
}

func (b *Broker) processDeleteConsumerReq(req *protocol.ReqDeleteConsumer) ([]byte, error) {
	resp := protocol.RespDeleteConsumer{
		ID:    req.ID,
		Group: req.Group,
		Topic: req.Topic,
	}

	topic, err := b.GetTopic(req.Topic)
	if err != nil {
		return nil, err
	}

	err = topic.removeConsumer(req.Group, req.ID)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
	}

	respBuf, err := resp.Serialize()
	if err != nil {
		return nil, err
	}

	return respBuf, nil
}

func (b *Broker) processListConsumerGroupsReq(req *protocol.ReqListConsumerGroups) *protocol.RespListConsumerGroups {
	resp := &protocol.RespListConsumerGroups{
		Groups: []protocol.ConsumerGroup{},
	}

	topic, err := b.GetTopic(req.Topic)
	if err != nil {
		resp.ErrorCode = 1
		resp.ErrorMessage = err.Error()
		return resp
	}

	groups := topic.listConsumerGroups()

	for k := range groups {
		consumers := []protocol.Consumer{}
		for i := range groups[k].consumers {
			offsets := []protocol.ConsumerOffset{}

			for partition := range groups[k].consumers[i].offsets {
				offsets = append(offsets, protocol.ConsumerOffset{
					Partition: partition,
					Offset:    groups[k].consumers[i].offsets[partition],
				})
			}

			consumers = append(consumers, protocol.Consumer{
				ID:      groups[k].consumers[i].id,
				Offsets: offsets,
			})
		}

		resp.Groups = append(resp.Groups, protocol.ConsumerGroup{
			Name:      groups[k].name,
			Consumers: consumers,
		})
	}

	return resp
}
