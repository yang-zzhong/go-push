package push

type Action int

const (
	ActionCreate Action = 0
	ActionUpdate Action = 1
	ActionDelete Action = 2
)

type Event[Data any, Id any] struct {
	Action Action `json:"action"`
	IDs    []Id   `json:"ids,omitempty"`
	Data   []Data `json:"data,omitempty"`
}
