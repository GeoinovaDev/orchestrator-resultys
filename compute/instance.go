package compute

// READY
const (
	READY   = 0
	RUNNING = 1
	WAITING = 2
	BLOCKED = 3
)

// Instance struct
type Instance struct {
	IP     string
	Status int
}
