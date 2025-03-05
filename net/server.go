package net

import (
	"go-raft/types"
	"strings"
	"sync"
)

type Server struct {
	mu       sync.Mutex
	services map[string]*Service
}

func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()

	dot := strings.LastIndex(req.SvcMeth, ".")
	serviceName := req.SvcMeth[:dot]
	methodName := req.SvcMeth[dot+1:]
	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		choices := []string{}
		for k := range rs.services {
			choices = append(choices, k)
		}
		return replyMsg{false, types.ErrUnkonwnService, nil}
	}
}
