package net

import (
	"bytes"
	"encoding/gob"
	"go-raft/types"
	"log"
	"reflect"
)

type Service struct {
	//服务名称
	name string
	//服务实例
	rcvr reflect.Value
	//服务类型信息
	typ reflect.Type
	//服务所有方法
	methods map[string]reflect.Method
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mname := method.Name
		svc.methods[mname] = method
	}

	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		// 获取参数类型
		argsType := method.Type.In(1)
		args := reflect.New(argsType)
		ab := bytes.NewBuffer(req.Args)
		ad := gob.NewDecoder(ab)
		if err := ad.Decode(args.Interface()); err != nil {
			log.Fatalf("Service.dispatch(): 解码参数失败: %v\n", err)
		}

		// 获取回复类型
		replyType := method.Type.In(2).Elem()
		replyv := reflect.New(replyType)

		// 调用方法
		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		// 序列化回复
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		if err := re.Encode(replyv.Interface()); err != nil {
			log.Fatalf("Service.dispatch(): 序列化回复失败: %v\n", err)
		}

		return replyMsg{true, "", rb.Bytes()}
	} else {
		return replyMsg{false, types.ErrUnkonwnMethod, nil}
	}
}
