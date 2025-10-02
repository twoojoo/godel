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
			// should respond with some error here
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
	default:
		return nil, errors.New("unknonw command")
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

	err = topic.Consume(0, func(message *Message) error {
		r := protocol.RespConsume{
			Messages: []protocol.RespConsumeMessage{
				{
					Key:       string(message.key),
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
	if err != nil {
		return nil, err
	}

	return nil, nil
}
