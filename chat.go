package chat;

import (
	"fmt"
	"bufio"
	"net/http"
	"time"
	"errors"
	"code.google.com/p/go.net/websocket"
)

type ConnType int

const (
	Unknown = iota
	Websocket ConnType = iota
	LongpollingPull = iota
	LongpollingPush = iota
)

type Message interface {
	GetRoom() string
	SetRoom(room string)
}

type Uid string

type Decoder interface {
	Decode() (Message, error)
	Encode(msg Message) ([]byte, error)
}

type EngineConfig interface {
	OnConnection(conn Conn) (*Uid, *Uid)
	OnSubscriberDisconnect(subscriber Subscriber)
	OnMessage(subscriber Subscriber, data Message)
	NewDecoder(reader *bufio.Reader) Decoder
	GetConnType(r *http.Request) ConnType
	GetLpDropTime(subscriber Subscriber) time.Duration
	GetLpWaitTime(subscriber Subscriber) time.Duration
}

type Send interface {
	Send(data Message)
}

type Conn interface {
	Send
	Request() *http.Request
	Cid() Uid
}

type Object interface {
	Send
	Uid() Uid
	Engine() Engine
}

type Subscriber interface {
	Object
	Rooms() *[]Room

	addRoom(room Room)
	remRoom(room Room)
}

type Room interface {
	Object
	Join(client Subscriber)
	Remove(client Subscriber)
	Subscribers() *map[Uid]Subscriber

	remove(client Subscriber)
}

type Engine interface {
	Send
	http.Handler
	GetRoom(name Uid) Room
	AddRoom(name Uid) Room
	Config() EngineConfig

	remove(*clientImpl)
}


type roomImpl struct {
	engine Engine
	uid Uid
	subscribers map[Uid]Subscriber
}

func (self *roomImpl) Uid() Uid {
	return self.uid
}

func (self *roomImpl) Engine() Engine {
	return self.engine
}

func (self *roomImpl) Subscribers() *map[Uid]Subscriber {
	return &self.subscribers
}

func (self *roomImpl) Send(data Message) {
	for _, subscriber := range self.subscribers {
		subscriber.Send(data)
	}
}

func (self *roomImpl) remove(subscriber Subscriber) {
	delete(self.subscribers, subscriber.Uid())
}

func (self *roomImpl) Remove(subscriber Subscriber) {
	subscriber.remRoom(self)
	self.remove(subscriber)
}

func (self *roomImpl) Join(subscriber Subscriber) {
	uid := subscriber.Uid()
	_, ok := self.subscribers[uid]
	if !ok {
		self.subscribers[uid] = subscriber
		subscriber.addRoom(self)
	}
}

func newRoom(engine Engine, uid Uid) Room {
	room := &roomImpl{
		engine: engine,
		uid: uid,
		subscribers: make(map[Uid]Subscriber, 0),
	}
	return room
}


type clientImpl struct {
	engine Engine
	uid Uid
	conns []Conn
	rooms []Room
}

func (self *clientImpl) Uid() Uid {
	return self.uid
}

func (self *clientImpl) Rooms() *[]Room {
	return &self.rooms
}

func (self *clientImpl) connections() *[]Conn {
	return &self.conns
}

func (self *clientImpl) Engine() Engine {
	return self.engine
}

func (self *clientImpl) Send(data Message) {
	for _, conn := range self.conns {
		conn.Send(data)
	}
}

func (self *clientImpl) addRoom(room Room) {
	self.rooms = append(self.rooms, room)
}

func (self *clientImpl) remRoom(room Room) {
	for i, v := range self.rooms {
		if v == room {
			self.rooms = append(self.rooms[:i], self.rooms[i+1:]...)
			break
		}
	}
}

func (self *clientImpl) addConnection(conn Conn) {
	self.conns = append(self.conns, conn)
}

func (self *clientImpl) remConnection(conn Conn) {
	for i, v := range self.conns {
		if v == conn {
			self.conns = append(self.conns[:i], self.conns[i+1:]...)
			break
		}
	}
	if len(self.conns) == 0 {
		self.engine.remove(self)
		self.engine.Config().OnSubscriberDisconnect(self)
		self.rooms = nil
		self.conns = nil
		self.engine = nil
		fmt.Println("Client disconnect", self.uid)
	}
}

func newClient(engine Engine, uid Uid, conn Conn) *clientImpl {
	fmt.Println("New client", uid)
	return &clientImpl{
		engine: engine,
		uid: uid,
		conns: []Conn{conn},
		rooms: []Room{},
	}
}

type engineImpl struct {
	config EngineConfig
	clients map[Uid]*clientImpl
	rooms map[Uid]Room
	handlerWs http.Handler
	handlerLpPull http.Handler
	handlerLpPush http.Handler
}

func (self *engineImpl) remove(client *clientImpl) {
	delete(self.clients, client.Uid())
	for _, room := range self.rooms {
		room.remove(client)
	}
}

func (self *engineImpl) Send(data Message) {
	for _, client := range self.clients {
		client.Send(data)
	}
}

func (self *engineImpl) GetRoom(name Uid) Room {
	room, ok := self.rooms[name]
	if !ok {
		return nil
	}
	return room
}

func (self *engineImpl) AddRoom(name Uid) Room {
	room := newRoom(self, name)
	self.rooms[name] = room
	return room
}

func (self *engineImpl) getClient(uid Uid, conn Conn) *clientImpl {
	client, wasAlready := self.clients[uid]
	if !wasAlready {
		client = newClient(self, uid, conn)
		self.clients[uid] = client
	} else {
		client.addConnection(conn)
	}
	return client
}

func (self *engineImpl) Config() EngineConfig {
	return self.config
}

func (self *engineImpl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch self.config.GetConnType(r) {
	case Websocket:
		self.handlerWs.ServeHTTP(w, r)
	case LongpollingPull:
		self.handlerLpPull.ServeHTTP(w, r)
	case LongpollingPush:
		self.handlerLpPush.ServeHTTP(w, r)
	}
}

func (self *engineImpl) onWsConn(ws *websocket.Conn) error {
	conn := &connWs{ws, nil, make(chan Message), bufio.NewWriter(ws), self.config.NewDecoder(bufio.NewReader(ws))}

	uid, _ := self.config.OnConnection(conn)
	if uid == nil {
		return errors.New("Need to provide uid from OnConnection!")
	}

	client := self.getClient(*uid, conn)
	conn.client = client
	fmt.Println("Conn new unrecoverable", client.Uid())
	conn.run()
	exitConnWs(conn, client)
	return nil
}

func (self *engineImpl) onLpPushConn(w http.ResponseWriter, r *http.Request) error {
	uid, _ := self.config.OnConnection(&connLp{r, "<nil>", nil, nil, nil, nil})
	if uid == nil {
		return errors.New("Need to provide uid from OnConnection!")
	}

	client, ok := self.clients[*uid]
	if !ok {
		return errors.New("There is no client with uid " + string(*uid))
	}

	msg, err := self.config.NewDecoder(bufio.NewReader(r.Body)).Decode()
	if err != nil {
		return err
	}

	fmt.Println("Conn read unrecoverable: ", client.Uid(), msg)
	go self.Config().OnMessage(client, msg)
	return nil
}

func (self *engineImpl) onLpPullConn(w http.ResponseWriter, r *http.Request) error {
	uid, cid := self.config.OnConnection(&connLp{r, "<nil>", nil, nil, nil, nil})
	if uid == nil {
		return errors.New("Need to provide uid from OnConnection!")
	}

	if cid == nil { // соединение, которое нельзя восстанавливать, просто запустим
		conn := newConnLp(*cid, r, w)
		client := self.getClient(*uid, conn)
		conn.client = client
		fmt.Println("Conn new unrecoverable", client.Uid())
		conn.run()
		exitConnLp(conn, client)
		return nil
	}

	// соединение, которое можно восстанавливать:
	// сначала попробуем восстановить, если нельзя, создаем новое

	client, wasAlready := self.clients[*uid]
	var conn *connLp = nil
	if wasAlready {
		for _, cn := range client.conns {
			if cn.Cid() == *cid {
				conn = cn.(*connLp) // TODO: fix this shit!
				break
			}
		}
		if conn == nil {
			conn = newConnLp(*cid, r, w)
			client.addConnection(conn)
			fmt.Println("Conn new", client.Uid(), conn.Cid())
			conn.client = client
		} else {
			fmt.Println("Conn reinit", client.Uid(), conn.Cid())
			conn.reinit(r, w)
			conn.restarted <- struct{}{}
		}
	} else {
		conn = newConnLp(*cid, r, w)
		client = newClient(self, *uid, conn)
		self.clients[*uid] = client
		fmt.Println("Conn new", client.Uid(), conn.Cid())
		conn.client = client
	}

	conn.run()

	go func() {
		select {
		case <-time.After(self.config.GetLpDropTime(client)):
			exitConnLp(conn, client)
		case <-conn.restarted:
			fmt.Println("Conn restarted", client.Uid(), conn.Cid())
		}
	}()
	fmt.Println("Conn paused", client.Uid(), conn.Cid())
	return nil
}


func NewEngine(config EngineConfig) Engine {
	engine := &engineImpl{
		config: config,
		clients: map[Uid]*clientImpl{},
		rooms: map[Uid]Room{},
	}

	engine.handlerWs = websocket.Handler(func(ws *websocket.Conn) {
		if err := engine.onWsConn(ws); err != nil {
			fmt.Println(err)
		}
	});

	engine.handlerLpPull = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := engine.onLpPullConn(w, r); err != nil {
			http.NotFound(w, r)
			fmt.Println(err)
		}
	})

	engine.handlerLpPush = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := engine.onLpPushConn(w, r); err != nil {
			http.NotFound(w, r)
			fmt.Println(err)
		}
	})

	return engine
}



type connWs struct {
	*websocket.Conn
	client Subscriber
	outgoing chan Message
	writer *bufio.Writer
	decoder Decoder
}

func (self *connWs) Cid() Uid {
	return "<nil>"
}

func (self *connWs) run() {
	go func() {
		for msg := range self.outgoing {
			res, err := self.decoder.Encode(msg)
			if err != nil {
				panic(err)
			}
			fmt.Println("Conn write", self.client.Uid(), self.Cid(), msg)
			self.writer.Write(res)
			self.writer.Flush()
		}
	}()

	for {
		msg, err := self.decoder.Decode()
		if err != nil {
			fmt.Println(err)
			break
		}

		if msg == nil {
			break
		}

		fmt.Println("Conn read", self.client.Uid(), self.Cid(), msg)
		go self.client.Engine().Config().OnMessage(self.client, msg)
	}
}

func SendData(ch chan Message, data Message) {
	go func() {
		// ch might be detached from the client already, when we try to write to the channel, so we need to recover from this failure.
		// TODO: FIX THIS SHIT!!
		defer func() {
			if err := recover(); err != nil {
				fmt.Println("Conn already detached", err)
			}
		}()
		ch <- data
	}()
}

func (self *connWs) Send(data Message) {
	SendData(self.outgoing, data)
}


type connLp struct {
	request *http.Request
	cid Uid
	client *clientImpl
	outgoing chan Message
	writer *bufio.Writer
	restarted chan struct{}
}

func (self *connLp) Request() *http.Request {
	return self.request
}

func (self *connLp) Cid() Uid {
	return self.cid
}

func (self *connLp) run() {
	select {
	case <-time.After(self.client.Engine().Config().GetLpWaitTime(self.client)):
		fmt.Println("Waited too long", self.client.Uid(), self.Cid())
	case msg := <-self.outgoing:
		res, err := self.client.Engine().Config().NewDecoder(nil).Encode(msg)
		if err != nil {
			panic(err)
		}
		fmt.Println("Conn write: ", self.client.Uid(), self.Cid(), msg)
		self.writer.Write(res)
		self.writer.Flush()
	}
}

func (self *connLp) Send(data Message) {
	SendData(self.outgoing, data)
}

func (self *connLp) reinit(r *http.Request, w http.ResponseWriter) {
	self.writer = bufio.NewWriter(w)
	self.request = r
}

func newConnLp(cid Uid, r *http.Request, w http.ResponseWriter) *connLp {
	return &connLp{r, cid,  nil, make(chan Message), bufio.NewWriter(w), make(chan struct{})}
}


func exitConnWs(conn *connWs, client *clientImpl) {
	fmt.Println("Conn exit", client.Uid(), conn.Cid())
	client.remConnection(conn)
	close(conn.outgoing)
	conn.client = nil
}

func drain(conn *connLp) {
	for {
		select {
		case msg := <-conn.outgoing:
			fmt.Println("Conn drain", conn.client.Uid(), conn.Cid(), msg)
		default:
			return
		}
	}
}

func exitConnLp(conn *connLp, client *clientImpl) {
	fmt.Println("Conn exit", client.Uid(), conn.Cid())
	client.remConnection(conn)
	drain(conn)
	close(conn.outgoing)
	conn.client = nil
}
