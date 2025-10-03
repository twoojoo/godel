package broker

type topicState struct {
	ConsumerGroups []topicStateGroup `json:"groups"`
}

type topicStateGroup struct {
	Name    string
	Offsets map[uint32]uint64 `json:"offsets"`
}
