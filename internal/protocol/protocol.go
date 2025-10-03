package protocol

const (
	CmdProduce      int16 = 0
	CmdConsume      int16 = 1
	CmdListTopics   int16 = 2
	CmdOffsetCommit int16 = 8
	CmdHeartbeat    int16 = 12
	CmdLeaveGroup   int16 = 13
	CmdListGroups   int16 = 16
	CmdCreateTopics int16 = 19
)
