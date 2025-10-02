package broker

import (
	"bufio"
	"errors"
	"godel/internal/protocol"
	"io"
	"log/slog"
	"net"
	"strconv"
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

		respPayload, err := b.processRequest(req)
		if err != nil {
			// should respond with some error here
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

func (b *Broker) processRequest(req *protocol.BaseRequest) ([]byte, error) {
	switch req.ApiVersion {
	case 0:
		return b.processApiV0Request(req)
	default:
		return nil, errors.New("unsupported api version")
	}
}

func (b *Broker) processApiV0Request(req *protocol.BaseRequest) ([]byte, error) {
	switch req.Cmd {
	case protocol.CreateTopics:
		req, err := protocol.DeserializeRequestCreateTopic(req.Payload)
		if err != nil {
			return nil, errors.New("failed to deserialize request")
		}

		return b.processCreateTopicsReq(req)
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
			Name:      req.Topics[i].Name,
			ErrorCode: 0,
		})
	}

	respBuf, err := resp.Serialize()
	if err != nil {
		return nil, err
	}

	return respBuf, nil
}
