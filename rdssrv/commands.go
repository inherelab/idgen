package rdssrv

import (
	"strconv"

	"github.com/inherelab/genid/mysqlid"
)

func (s *Server) handleGet(r *Request) Reply {
	var id int64
	var err error

	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	serviceKey := string(r.Arguments[0])
	if len(serviceKey) == 0 {
		return ErrNoKey
	}

	// var ok bool
	// var gen *mysqlid.Generator
	// s.Lock()
	// gen, ok = s.generatorMap[serviceKey]
	// if ok == false {
	// 	s.Unlock()
	// 	return &BulkReply{
	// 		value: nil,
	// 	}
	// }
	// s.Unlock()
	//
	// id, err = gen.Next()
	// if err != nil {
	// 	return &ErrorReply{
	// 		message: err.Error(),
	// 	}
	// }

	id, err = s.NextId(serviceKey)
	if err != nil {
		// service not exists
		if err == mysqlid.ErrServiceNotExists {
			return &BulkReply{
				value: nil,
			}
		}

		return &ErrorReply{
			message: err.Error(),
		}
	}

	idStr := strconv.FormatInt(id, 10)
	return &BulkReply{
		value: []byte(idStr),
	}
}

// redis command(set abc 12)
func (s *Server) handleSet(r *Request) Reply {

	var err error

	serviceName, errReply := r.GetString(0)
	if errReply != nil {
		return errReply
	}

	serviceName, err = mysqlid.GoodServiceKey(serviceName)
	if err != nil {
		return &ErrorReply{err.Error()}
	}

	idValue, errReply := r.GetInt(1)
	if errReply != nil {
		return errReply
	}

	// force reset id: set service_user 10 true
	force, _ := r.GetBool(2)

	// if r.HasArgument(0) == false {
	// 	return ErrNotEnoughArgs
	// }
	// serviceName := string(r.Arguments[0])
	// if len(serviceName) == 0 {
	// 	return ErrNoKey
	// }

	// var ok bool
	// var gen *mysqlid.Generator
	// s.Lock()
	// gen, ok = s.generatorMap[serviceName]
	// if ok == false {
	// 	gen, err = mysqlid.NewGenerator(s.db, serviceName)
	// 	if err != nil {
	// 		s.Unlock()
	// 		return &ErrorReply{
	// 			message: err.Error(),
	// 		}
	// 	}
	// 	s.generatorMap[serviceName] = gen
	// }
	//
	// s.Unlock()
	// err = s.SetKey(serviceName)
	// if err != nil {
	// 	return &ErrorReply{
	// 		message: err.Error(),
	// 	}
	// }

	// err = gen.Reset(idValue, false)
	_, err = s.SetServiceId(serviceName, idValue, force)
	if err != nil {
		return &ErrorReply{
			message: err.Error(),
		}
	}

	return &StatusReply{
		code: "OK",
	}
}

func (s *Server) handleExists(r *Request) Reply {
	var id int64
	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	serviceName := string(r.Arguments[0])
	if len(serviceName) == 0 {
		return ErrNoKey
	}

	// var ok bool
	// s.Lock()
	// _, ok = s.generatorMap[serviceName]
	// s.Unlock()
	// if ok {
	// 	id = 1
	// }

	if s.ServiceExists(serviceName) {
		id = 1
	}

	return &IntReply{
		number: id,
	}
}

func (s *Server) handleDel(r *Request) Reply {
	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	serviceName := string(r.Arguments[0])
	if len(serviceName) == 0 {
		return ErrNoKey
	}

	// s.Lock()
	// gen, ok = s.generatorMap[serviceName]
	// if ok {
	// 	delete(s.generatorMap, serviceName)
	// }
	// s.Unlock()
	//
	// if ok {
	// 	err := gen.DelKeyTable(serviceName)
	// 	if err != nil {
	// 		return &ErrorReply{
	// 			message: err.Error(),
	// 		}
	// 	}
	// 	err = s.DelKey(serviceName)
	// 	if err != nil {
	// 		return &ErrorReply{
	// 			message: err.Error(),
	// 		}
	// 	}
	// 	id = 1
	// }

	err := s.DelService(serviceName)
	if err != nil {
		return &ErrorReply{
			message: err.Error(),
		}
	}

	return &IntReply{
		number: 1,
	}
}

func (s *Server) handleSelect(r *Request) Reply {
	if r.HasArgument(0) == false {
		return ErrNotEnoughArgs
	}

	num := string(r.Arguments[0])
	if len(num) == 0 {
		return ErrNotEnoughArgs
	}

	return &StatusReply{
		code: "OK",
	}
}
