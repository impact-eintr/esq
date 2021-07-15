package esqd

type TopicStats struct {
}

func NewTopicStats(t *Topic, channels []ChannelStats) TopicStats {
	return TopicStats{}
}

type ChannelStats struct {
}

//func NewChannelStats(c *Channel, clients []ClientStats, clientCount int) ChannelStats {
//
//}

type PubCount struct {
}

type ClientStats struct {
}

type Topics []*Topic

func (t Topics) Len() int {
	return len(t)
}

func (t Topics) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type TopicByName struct {
	Topics
}

func (t TopicByName) Less(i, j int) bool {
	return t.Topics[i].name < t.Topics[j].name
}

type Channels []*Channel

func (c Channels) Len() int {
	return len(c)
}

func (c Channels) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

type ChannelByName struct {
	Channels
}

func (c ChannelByName) Less(i, j int) bool {
	return c.Channels[i].name < c.Channels[j].name
}

func (e *ESQD) GetStats(topic string, channel string, includeClients bool) []TopicStats {

}
