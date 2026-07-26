package server

import "time"

func (s *Server) handlePublish(c *Connection, args []string) RESPValue {
	if len(args) != 2 {
		return RESPValue{
			Type: Error,
			Str:  "ERR wrong number of arguments for 'publish' command",
		}
	}
	channel := args[0]
	message := args[1]

	// Hitung subscriber dulu (sync)
	s.subscribeMu.RLock()
	subscribers := s.subscribers[channel]
	count := len(subscribers)
	s.subscribeMu.RUnlock()

	if count > 0 {
		go func() {
			for _, sub := range subscribers {
				if sub == c {
					continue
				}
				sub.SendMessage(channel, message)
			}
		}()
	}

	return RESPValue{
		Type: Integer,
		Int:  int64(count),
	}
}

// SUBSCRIBE channel_1 channel_2 ...
func (s *Server) handleSubscribe(c *Connection, args []string) RESPValue {
	if len(args) < 1 {
		return RESPValue{
			Type: Error,
			Str:  "ERR wrong number of arguments for 'subscribe' command",
		}
	}

	for _, channel := range args {
		s.addSubscriber(channel, c)
		resp := RESPValue{
			Type: Array,
			Array: []RESPValue{
				{Type: BulkString, Str: "subscribe"},
				{Type: BulkString, Str: channel},
				{Type: Integer, Int: int64(len(s.subscribers[channel]))},
			},
		}

		encoded := EncodeRESP(resp)
		c.conn.Write([]byte(encoded))
	}

	c.conn.SetReadDeadline(time.Time{})
	c.conn.SetWriteDeadline(time.Time{})
	c.SetSubscriberMode(true)

	return RESPValue{}
}

// UNSUBSCRIBE channel_1 channel_2 ...
func (s *Server) handleUnsubscribe(c *Connection, args []string) RESPValue {
	if len(args) < 1 {
		channels := c.GetChannels()
		if len(channels) == 0 {
			// Tidak ada channel yang di-subscribe
			return RESPValue{
				Type: SimpleString,
				Str:  "OK",
			}
		}
		results := make([]RESPValue, 0, len(channels)*3)
		for _, channel := range channels {
			s.removeSubscriber(channel, c)
			results = append(results, RESPValue{
				Type: BulkString,
				Str:  "unsubscribe",
			})
			results = append(results, RESPValue{
				Type: BulkString,
				Str:  channel,
			})
			results = append(results, RESPValue{
				Type: Integer,
				Int:  int64(len(s.subscribers[channel])),
			})
		}

		// Reset timeout setelah unsubscribe
		c.SetSubscriberMode(false)
		c.conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		c.conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))

		return RESPValue{
			Type:  Array,
			Array: results,
		}
	}

	results := make([]RESPValue, 0, len(args)*3)
	for _, channel := range args {
		s.removeSubscriber(channel, c)
		results = append(results, RESPValue{
			Type: BulkString,
			Str:  "unsubscribe",
		})
		results = append(results, RESPValue{
			Type: BulkString,
			Str:  channel,
		})
		results = append(results, RESPValue{
			Type: Integer,
			Int:  int64(len(s.subscribers[channel])),
		})
	}

	if len(c.channels) == 0 {
		c.SetSubscriberMode(false)
		c.conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
		c.conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
	}

	return RESPValue{
		Type:  Array,
		Array: results,
	}
}

// removes a connection from subscribers untuk sejumlah channel
func (s *Server) removeSubscriberByChannels(channels []string, conn *Connection) {
	for _, channel := range channels {
		s.removeSubscriber(channel, conn)
	}
}

// removes a connection from subscribers for a channel
func (s *Server) removeSubscriber(channel string, conn *Connection) {
	s.subscribeMu.Lock()
	defer s.subscribeMu.Unlock()

	s.removeSubscriberUnsafe(channel, conn)
}

// removes a connection from subscribers for a channel tanpa lock
func (s *Server) removeSubscriberUnsafe(channel string, conn *Connection) {
	if subscribers, exists := s.subscribers[channel]; exists {
		newSubscribers := make([]*Connection, 0, len(subscribers))
		for _, w := range subscribers {
			if w != conn {
				newSubscribers = append(newSubscribers, w)
			}
		}
		if len(newSubscribers) == 0 {
			delete(s.subscribers, channel)
		} else {
			s.subscribers[channel] = newSubscribers
		}
		conn.Unsubscribe(channel)
	}
}

// adds a connection to subscribers for a channel
func (s *Server) addSubscriber(channel string, conn *Connection) {
	s.subscribeMu.Lock()
	defer s.subscribeMu.Unlock()

	// Remove existing subscriber for this connection
	// Menggunakan unsafe karena sudah dilock di sini untuk mencegah deadlock
	s.removeSubscriberUnsafe(channel, conn)
	conn.Subscribe(channel)

	s.subscribers[channel] = append(s.subscribers[channel], conn)
}

// removes all subscribers for a connection
func (s *Server) cleanupSubscribers(conn *Connection) {
	conn.mu.Lock()
	channels := make([]string, 0, len(conn.channels))
	for channel := range conn.channels {
		channels = append(channels, channel)
	}
	conn.mu.Unlock()

	for _, channel := range channels {
		s.removeSubscriber(channel, conn)
	}
}
