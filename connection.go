package vertigo

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

var (
	SslNotSupported                  = errors.New("SSL not available on this server")
	AuthenticationMethodNotSupported = errors.New("Authentication method not supported")
)

type ConnectionInfo struct {
	Address   string
	User      string
	Database  string
	Password  string
	SslConfig *tls.Config
}

type Connection struct {
	l sync.Mutex

	socket            net.Conn
	parameters        map[string]string
	backendPid        uint32
	backendKey        uint32
	transactionStatus byte
}

func Connect(info *ConnectionInfo) (connection Connection, connectionError error) {
	connection = Connection{}
	defer func() {
		if r := recover(); r != nil {
			connection.resetConnection()
			connectionError = r.(error)
		}
	}()

	connection.l.Lock()
	defer connection.l.Unlock()

	if socket, dialError := net.Dial("tcp", info.Address); dialError != nil {
		panic(dialError)
	} else {
		connection.socket = socket
	}


	if info.SslConfig != nil {
		connection.sendMessage(SSLRequestMessage{})

		sslResponse := make([]byte, 1)
		io.ReadFull(connection.socket, sslResponse)
		if sslResponse[0] == byte('S') {
			tlsSocket := tls.Client(connection.socket, info.SslConfig)
			connection.socket = tlsSocket
			if tlsError := tlsSocket.Handshake(); tlsError != nil {
				panic(tlsError)
			}
		} else {
			panic(SslNotSupported)
		}
	}

	connection.parameters = make(map[string]string)
	connection.initConnection(info)
	return connection, nil
}

func (c *Connection) handleStatelessMessage(msg IncomingMessage) {
	switch msg := msg.(type) {
	case ParameterStatusMessage:
		c.parameters[msg.Name] = msg.Value

	case BackendKeyDataMessage:
		c.backendPid = msg.Pid
		c.backendKey = msg.Key

	default:
		panic(fmt.Sprintf("Unexpected message: %#+v", msg))
	}
}

func (c *Connection) initConnection(info *ConnectionInfo) {
	c.sendMessage(StartupMessage{User: info.User, Database: info.Database})

	for msg := c.receiveMessage(); !c.isReadyForQuery(msg); msg = c.receiveMessage() {
		switch msg := msg.(type) {
		case AuthenticationRequestMessage:
			switch msg.AuthCode {
			case AuthenticationOK:
				continue
			case AuthenticationCleartextPassword:
				c.sendMessage(PasswordMessage{Password: info.Password, AuthenticationMethod: msg.AuthCode})
			default:
				panic(AuthenticationMethodNotSupported)
			}

		case ErrorResponseMessage:
			panic(msg)

		default:
			c.handleStatelessMessage(msg)
		}
	}
	return
}

func (c *Connection) isReadyForQuery(msg IncomingMessage) bool {
	typeMsg, ok := msg.(ReadyForQueryMessage)
	if ok {
		c.transactionStatus = typeMsg.TransactionStatus
	}
	return ok
}

func (c *Connection) Query(sql string) (resultset *Resultset, queryError error) {
	c.l.Lock()
	defer c.l.Unlock()

	defer func() {
		if r := recover(); r != nil {
			c.Close()
			queryError = r.(error)
		}
	}()

	c.sendMessage(QueryMessage{SQL: sql})
	for msg := c.receiveMessage(); !c.isReadyForQuery(msg); msg = c.receiveMessage() {
		switch msg := msg.(type) {
		case EmptyQueryMessage, ErrorResponseMessage:
			queryError = msg.(error)

		case RowDescriptionMessage:
			resultset = &Resultset{Fields: msg.Fields}

		case DataRowMessage:
			resultset.Rows = append(resultset.Rows, Row{Values: msg.Values})

		case CommandCompleteMessage:
			resultset.Result = msg.Result

		default:
			c.handleStatelessMessage(msg)
		}
	}
	return
}

func (c *Connection) Close() (err error) {
	defer c.resetConnection()
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()

	c.sendMessage(TerminateMessage{})
	return nil
}

func (connection *Connection) resetConnection() {
	if connection.socket != nil {
		connection.socket.Close()
	}
	connection.parameters = make(map[string]string)
	connection.backendPid = 0
	connection.backendKey = 0
}

func (c *Connection) sendMessage(msg OutgoingMessage) {
	err := SendMessage(c.socket, msg)
	if err != nil {
		panic(err)
	}
	if TrafficLogger != nil {
		TrafficLogger.Printf("=> %#+v\n", msg)
	}
}

func (c *Connection) receiveMessage() IncomingMessage {
	msg, err := ReadMessage(c.socket)
	if err != nil {
		panic(err)
	}
	if TrafficLogger != nil {
		TrafficLogger.Printf("<= %#+v", msg)
	}
	return msg
}
