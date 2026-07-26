package server

import "time"

func (s *Server) handleAuth(c *Connection, args []string) RESPValue {
	if s.config.RequirePass == "" {
		return RESPValue{Type: Error, Str: "ERR Client sent AUTH, but no password is set"}
	}
	if len(args) != 1 {
		return RESPValue{Type: Error, Str: "ERR wrong number of arguments for 'auth' command"}
	}
	if args[0] == s.config.RequirePass {
		c.SetAuthenticated(true)
		return RESPValue{Type: SimpleString, Str: "OK"}
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		c.Close()
	}()
	return RESPValue{Type: Error, Str: "ERR invalid password"}
}
