package broker

import (
	"godel/internal/protocol"
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

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func() {
		err := conn.Close()
		if err != nil {
			slog.Error("failed to close connection", "error", err)
		}
	}()

	for {
		req, err := protocol.DeserializeRequest(conn)
		if err != nil {
			slog.Error("failed to deserialize message, closing connection", "error", err)
			return
		}

		respPayload, err := protocol.ProcessRequest(req)
		if err != nil {
			// should respond with some error here
			continue
		}

		resp := protocol.BaseResponse{
			CorrelationID: req.CorrelationID,
			Payload:       respPayload,
		}

		err = writeFull(conn, resp.Serialize())
		if err != nil {
			slog.Error("failed to send response to client")
		}
	}
}

func writeFull(conn net.Conn, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := conn.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}
	return nil
}
