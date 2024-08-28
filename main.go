package main

import (
	"fmt"
	"log"
	"time"
)

type QueueMsg struct {
	wakeup func()
	call   func()
}

type RunCtx struct {
	Running bool
}

type Actor struct {
	queue    chan *QueueMsg
	quit     chan struct{}
	quitFlag bool
	runCtx   *RunCtx
	suspend  chan error
}

func (a *Actor) Enqueue(f func()) {
	a.queue <- &QueueMsg{call: f}
}

func (a *Actor) Quit() {
	a.quitFlag = true
	close(a.quit)
}

func (a *Actor) Await(call func()) {
	a.runCtx.Running = false
	ctx := a.runCtx
	a.suspend <- nil
	call()
	// TODO: use sync.Pool??
	c := make(chan struct{})
	a.queue <- &QueueMsg{
		wakeup: func() {
			c <- struct{}{}
		},
	}
	<-c
	ctx.Running = true
	a.runCtx = ctx
}

func (a *Actor) Run() {
	for !a.quitFlag {
		select {
		case msg := <-a.queue:
			if msg.wakeup != nil {
				msg.wakeup()
			} else {
				go a.handleMsg(msg.call)
			}
			<-a.suspend
			a.runCtx = nil
		case <-a.quit:
			break
		}
	}

	for {
		select {
		case msg := <-a.queue:
			if msg.wakeup != nil {
				msg.wakeup()
			} else {
				go a.handleMsg(msg.call)
			}
			<-a.suspend
			a.runCtx = nil
		default:
			return
		}
	}
}

func (a *Actor) handleMsg(call func()) {
	a.runCtx = &RunCtx{
		Running: true,
	}
	ctx := a.runCtx
	defer func() {
		if e := recover(); e != nil {
			log.Printf("go running err, %v\n", e)
			if ctx.Running {
				a.suspend <- fmt.Errorf("%v", e)
			}
		} else {
			a.suspend <- nil
		}
	}()
	call()
}

func main() {
	a := &Actor{
		queue:   make(chan *QueueMsg, 10),
		quit:    make(chan struct{}),
		suspend: make(chan error, 1),
	}

	f1 := func() {
		log.Println("begin run f1")
		a.Await(func() {
			log.Println("before f1 rpc")
			time.Sleep(time.Second * 2)
			log.Println("after f1 rpc")
		})
		log.Println("end run f1")
	}
	f2 := func() {
		log.Println("begin run f2")
		log.Println("end run f2")
	}
	f3 := func() {
		log.Println("begin run f3")
		a.Await(func() {
			log.Println("before f3 rpc")
			time.Sleep(time.Second * 4)
			log.Println("after f3 rpc")
		})
		log.Println("end run f3 && call Quit")
		a.Quit()
	}
	f4 := func() {
		log.Println("begin run f4")
		log.Println("end run f4")
	}

	var ptr *int
	f5 := func() {
		log.Println("begin run f5")
		*ptr = 100
		log.Println("end run f5")
	}

	f6 := func() {
		log.Println("begin run f6")
		a.Await(func() {
			log.Println("before f6 rpc")
			time.Sleep(time.Second * 3)
			log.Println("after f6 rpc")
			*ptr = 200
		})
		log.Println("end run f6")
	}

	a.Enqueue(f1)
	a.Enqueue(f2)
	a.Enqueue(f3)
	a.Enqueue(f4)
	a.Enqueue(f5)
	a.Enqueue(f6)

	log.Println("----enter service dispatch")
	a.Run()
	log.Println("----quit service dispatch----")
}
