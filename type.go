package sharding

const (
	TypeTextField = iota
	TypeBigIntegerField
	TypePositiveBigIntegerField
	TypeFloatField
)
// field info collection
type modelInfo struct {
	name 	string
	fullName	string
	fields	map[string]*fieldInfo
	n2c map[string]string
	c2n map[string]string
	columns string
	uk 	string
	pk 	string
}

type fieldInfo struct {
	name string
	colume	string
	pk 	bool
	uk bool
	fieldIndex int
	fieldType int
}