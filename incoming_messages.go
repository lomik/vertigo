package vertigo

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type ErrorResponse interface {
	Error() string
	Code() string
}

type IncomingMessage interface{}

type ErrorResponseMessage struct {
	Fields map[byte]string
}

func parseErrorResponseMessage(body []byte) (IncomingMessage, error) {
	msg := ErrorResponseMessage{}
	msg.Fields = make(map[byte]string)
	offset := 0
	for {
		var fieldType byte
		if err := decodeUint8(body[offset:], &fieldType); err != nil {
			return msg, err
		}

		offset += 1

		if fieldType == 0 {
			break
		}

		if str, err := decodeCString(body[offset:]); err != nil {
			return msg, err
		} else {
			msg.Fields[fieldType] = str
			offset += len(str) + 1
		}
	}
	return msg, nil
}

func (msg ErrorResponseMessage) Error() string {
	return fmt.Sprintf("Vertica %s %s: %s", msg.Fields['S'], msg.Fields['C'], msg.Fields['M'])
}

func (msg ErrorResponseMessage) Code() string {
	return msg.Fields['C']
}

func (msg ErrorResponseMessage) Severity() string {
	return msg.Fields['S']
}

type EmptyQueryMessage struct{}

func parseEmptyQueryMessage(body []byte) (IncomingMessage, error) {
	return EmptyQueryMessage{}, nil
}

func (msg EmptyQueryMessage) Error() string {
	return "The provided SQL string was empty"
}

func (msg EmptyQueryMessage) Code() string {
	return ""
}

func (msg EmptyQueryMessage) Severity() string {
	return "ERROR"
}

type AuthenticationRequestMessage struct {
	AuthCode uint32
	Salt     []byte
}

func parseAuthenticationRequestMessage(body []byte) (IncomingMessage, error) {
	msg := AuthenticationRequestMessage{}
	err := decodeUint32(body, &msg.AuthCode)
	return msg, err
}

type ReadyForQueryMessage struct {
	TransactionStatus byte
}

func parseReadyForQueryMessage(body []byte) (IncomingMessage, error) {
	msg := ReadyForQueryMessage{}
	err := decodeUint8(body, &msg.TransactionStatus)
	return msg, err
}

type ParameterStatusMessage struct {
	Name  string
	Value string
}

func parseParameterStatusMessage(body []byte) (IncomingMessage, error) {
	msg := ParameterStatusMessage{}
	var offset int
	if str, err := decodeCString(body[offset:]); err != nil {
		return msg, err
	} else {
		msg.Name = str
		offset += len(str) + 1
	}
	if str, err := decodeCString(body[offset:]); err != nil {
		return msg, err
	} else {
		msg.Value = str
		offset += len(str) + 1
	}
	return msg, nil
}

type BackendKeyDataMessage struct {
	Pid uint32
	Key uint32
}

func parseBackendKeyDataMessage(body []byte) (IncomingMessage, error) {
	msg := BackendKeyDataMessage{}

	if err := decodeUint32(body, &msg.Pid); err != nil {
		return msg, err
	}
	if err := decodeUint32(body[4:], &msg.Key); err != nil {
		return msg, err
	}
	return msg, nil
}

type CommandCompleteMessage struct {
	Result string
}

func parseCommandCompleteMessage(body []byte) (IncomingMessage, error) {
	msg := CommandCompleteMessage{}
	if str, err := decodeCString(body); err != nil {
		return msg, err
	} else {
		msg.Result = str
	}

	return msg, nil
}

type RowDescriptionMessage struct {
	Fields []Field
}

func parseRowDescriptionMessage(body []byte) (IncomingMessage, error) {
	msg := RowDescriptionMessage{}
	var numFields uint16
	if err := decodeUint16(body, &numFields); err != nil {
		return msg, err
	}

	offset := 2

	msg.Fields = make([]Field, numFields)
	for i := range msg.Fields {
		field := &msg.Fields[i]

		if name, err := decodeCString(body[offset:]); err != nil {
			return msg, err
		} else {
			field.Name = name
			offset += len(name) + 1
		}

		if err := decodeUint32(body[offset:], &field.TableOID); err != nil {
			return msg, err
		}
		offset += 4

		if err := decodeUint16(body[offset:], &field.AttributeNumber); err != nil {
			return msg, err
		}
		offset += 2

		if err := decodeUint32(body[offset:], &field.DataTypeOID); err != nil {
			return msg, err
		}
		offset += 4

		if err := decodeUint16(body[offset:], &field.DataTypeSize); err != nil {
			return msg, err
		}
		offset += 2

		if err := decodeUint32(body[offset:], &field.TypeModifier); err != nil {
			return msg, err
		}
		offset += 4

		if err := decodeUint16(body[offset:], &field.FormatCode); err != nil {
			return msg, err
		}
		offset += 2
	}
	return msg, nil
}

type DataRowMessage struct {
	Values [][]byte
}

func parseDataRowMessage(body []byte) (IncomingMessage, error) {
	msg := DataRowMessage{}
	var numValues uint16
	if err := decodeUint16(body, &numValues); err != nil {
		return msg, err
	}

	offset := 2
	bodyLen := len(body)

	msg.Values = make([][]byte, numValues)
	for i := range msg.Values {
		var size uint32
		if err := decodeUint32(body[offset:], &size); err != nil {
			return msg, err
		}
		offset += 4

		if size == 0xffffffff {
			msg.Values[i] = nil
		} else {
			if offset+int(size) > bodyLen {
				return msg, errors.New("parseDataRowMessage: truncated message")
			}
			msg.Values[i] = body[offset : offset+int(size)]
			offset += int(size)
		}
	}

	return msg, nil
}

type messageFactoryMethod func(raw []byte) (IncomingMessage, error)

var messageFactoryMethods = map[byte]messageFactoryMethod{
	'R': parseAuthenticationRequestMessage,
	'Z': parseReadyForQueryMessage,
	'E': parseErrorResponseMessage,
	'I': parseEmptyQueryMessage,
	'S': parseParameterStatusMessage,
	'K': parseBackendKeyDataMessage,
	'T': parseRowDescriptionMessage,
	'C': parseCommandCompleteMessage,
	'D': parseDataRowMessage,
}

func receiveMessage(r io.Reader) (message IncomingMessage, err error) {
	var (
		messageType byte
		messageSize uint32
	)

	header := make([]byte, 5)
	if _, err = io.ReadAtLeast(r, header, 5); err != nil {
		return
	}

	messageType = header[0]
	messageSize = unpackUint32(header[1:5])

	var messageContent []byte
	if messageSize >= 4 {
		messageContent = make([]byte, messageSize-4)
		if messageSize > 4 {
			if _, err = io.ReadFull(r, messageContent); err != nil {
				return
			}
		}
	} else {
		err = errors.New("A message should be at least 4 bytes long")
		return
	}

	factoryMethod := messageFactoryMethods[messageType]
	if factoryMethod == nil {
		panic(fmt.Sprintf("Unknown message type: %c", messageType))
	}
	return factoryMethod(messageContent)
}

func decodeNumeric(reader *bufio.Reader, data interface{}) error {
	return binary.Read(reader, binary.BigEndian, data)
}

func decodeString(reader *bufio.Reader) (str string, err error) {
	if str, err = reader.ReadString(0); err != nil {
		return str, err
	}
	return str[0 : len(str)-1], nil
}

func decodeCString(p []byte) (str string, err error) {
	size := len(p)
	for i := 0; i < size; i++ {
		if p[i] == 0x0 {
			return string(p[:i]), nil
		}
	}
	return "", errors.New("decodeCString: delimeter not found")
}

func unpackUint32(p []byte) uint32 {
	result := uint32(0)
	for i := uint(0); i < 4; i++ {
		result |= uint32(p[i]) << (8 * (3 - i))
	}
	return result
}

func unpackUint16(p []byte) uint16 {
	result := uint16(0)
	for i := uint(0); i < 2; i++ {
		result |= uint16(p[i]) << (8 * (1 - i))
	}
	return result
}

func decodeUint32(p []byte, v *uint32) error {
	if len(p) < 4 {
		return errors.New("decodeUint32: A buffer should be at least 4 bytes long")
	}
	*v = unpackUint32(p)
	return nil
}

func decodeUint16(p []byte, v *uint16) error {
	if len(p) < 2 {
		return errors.New("decodeUint16: A buffer should be at least 2 bytes long")
	}
	*v = unpackUint16(p)
	return nil
}

func decodeUint8(p []byte, v *uint8) error {
	if len(p) < 1 {
		return errors.New("decodeUint8: A buffer should be at least 1 bytes long")
	}
	*v = p[0]
	return nil
}
