package internal

type Node struct {
	ID    uint32
	Type  byte
	InUse byte
	_     [2]byte  // Padding
	Value [64]byte // Fixed-size payload (e.g., name or encoded props)
}

type Edge struct {
	ID     uint32
	InUse  byte
	_      [2]byte // Padding
	FromID uint32
	ToID   uint32
}
