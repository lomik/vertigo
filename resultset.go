package vertigo

type Resultset struct {
	Fields []Field
	Rows   []Row
	Result string
}

type Row struct {
	Values [][]byte
}

type Field struct {
	Name            string
	TableOID        uint32
	AttributeNumber uint16
	DataTypeOID     uint32
	DataTypeSize    uint16
	TypeModifier    uint32
	FormatCode      uint16
}
