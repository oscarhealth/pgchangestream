// pgchangestream allows you to read a stream of changes from a postgres
// database. In order to use the library, you first need to create a
// StreamReader, and then call `Listen` to start streaming data onto the
// provided channel.
package pgchangestream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

// Message represents a single replication message.
// Once the message has been handled, Commit should be called.
type Message interface {
	Data() []byte
	Commit()
}

// A StreamReader reads off of a replication slot and writes messages to a
// channel.
type StreamReader struct {
	conn     *pgx.ReplicationConn // the connection is not threadsafe
	slotName string
	outChan  chan<- Message

	// in order to keep the connection alive, we need to send periodic status
	// messages as a client-side heartbeat.
	// heartbeatInterval is the time between heartbeats.
	heartbeatInterval time.Duration
	// nextHeartbeatDeadline is the time of the next heartbeat.
	nextHeartbeatDeadline time.Time

	lastCommittedMessage      *message
	lastCommittedMessageMutex sync.Mutex
}

// NewStreamReader creates a new StreaReader.
func NewStreamReader(conn *pgx.ReplicationConn, slotName string, outChan chan<- Message) *StreamReader {
	return &StreamReader{
		conn:              conn,
		slotName:          slotName,
		outChan:           outChan,
		heartbeatInterval: time.Second * 10,
	}
}

// Listen consumes the replication stream and emits events to outChan.
// pluginArgs is the list of extra arguments to pass to the logical decoding plugin.
//
// The replicaion protocol is documented at
// https://www.postgresql.org/docs/10/static/protocol-replication.html
//
// The replication protocol sends two types of messages: WAL messages, and heartbeats.
// It accepts one kind of message: Status messages.
//
// We need to send status updates periodically to keep the connection alive and
// to update postgres the latest WAL offset we can commit.
// In order to compute the most recent WAL offset, we use the offset of the
// last message (heartbeat or WAL) before an uncommitted WAL message.
func (s *StreamReader) Listen(ctx context.Context, pluginArgs ...string) error {
	if err := s.conn.StartReplication(s.slotName, 0, -1, pluginArgs...); err != nil {
		return fmt.Errorf("error starting replication: %v", err)
	}
	s.nextHeartbeatDeadline = time.Now().Add(s.heartbeatInterval)
	s.lastCommittedMessage = newMessage(s, nil, 0)
	currentMessage := s.lastCommittedMessage
	for {
		select {
		case <-ctx.Done():
			// send one last time to ensure things are committed.
			if err := s.sendStatus(); err != nil {
				return fmt.Errorf("error sending final status: %v", err)
			}
			return ctx.Err()
		default:
		}
		// we need to set a deadline because the connection is not
		// threadsafe and we still need to periodically send status
		// messages.
		newCtx, cancelFunc := context.WithDeadline(ctx, s.nextHeartbeatDeadline)
		msg, err := s.conn.WaitForReplicationMessage(newCtx)
		ctxErr := newCtx.Err()
		cancelFunc()
		if err != nil {
			if err == ctxErr {
				if err := s.sendHeartbeatIfDue(); err != nil {
					return fmt.Errorf("error sending status: %v", err)
				}
				continue
			}
			return fmt.Errorf("error waiting for replication message: %v", err)

		}

		switch {
		// sometimes the message is nil. This seems to happen especially if
		// there is a message output by the decoder plugin. For example, when
		// we see an unchanged toast
		// https://github.com/eulerto/wal2json/blob/9e962bad61ef2bfa53747470bac4d465e71df880/wal2json.c#L576
		// This should be safe to ignore
		case msg == nil:
			continue
		// we got a new message; send it on the channel.
		case msg.WalMessage != nil:
			log.WithFields(log.Fields{
				"wal_start": msg.WalMessage.WalStart,
				"lag":       msg.WalMessage.ByteLag(),
				"len":       len(msg.WalMessage.WalData),
			}).Debug("got WalMessage")
			if msg.WalMessage.ByteLag() != 0 {
				// We haven't seen this before, and I'm not sure what it
				// means, so to be safe let's error and investigate if it
				// happens.
				return errors.New("unexpected case: nonzero byte lag")
			}
			currentMessage = newMessage(s, msg.WalMessage.WalData, msg.WalMessage.WalStart)
			if err := s.produce(ctx, currentMessage); err != nil {
				return fmt.Errorf("error producing message: %v", err)
			}
		// we received a heartbeat from the server.
		// these can indicate that we should immediately reply
		// also, we update the end offset of the current message.
		case msg.ServerHeartbeat != nil:
			log.WithFields(log.Fields{
				"wal_end":         msg.ServerHeartbeat.ServerWalEnd,
				"reply_requested": msg.ServerHeartbeat.ReplyRequested,
			}).Debug("got server heartbeat")
			currentMessage.updateOffset(msg.ServerHeartbeat.ServerWalEnd)
			if msg.ServerHeartbeat.ReplyRequested != 0 {
				// In the protocol documentation above, it says
				// 1 means that the client should reply to this message as soon
				// as possible, to avoid a timeout disconnect. 0 otherwise.
				if err := s.sendStatus(); err != nil {
					return fmt.Errorf("error sending requested status: %v", err)
				}
			}
		}
	}
}

// markMessageReadyToCommit marks the message as ready to commit, allowing us
// to send back statuses which mark it as read. Note that this does not
// immediately commit the message, but rather queues it up to be committed on
// the next heartbeat.
func (s *StreamReader) markMessageReadyToCommit(msg *message) {
	s.lastCommittedMessageMutex.Lock()
	defer s.lastCommittedMessageMutex.Unlock()
	msgOffset := msg.getOffset()
	lastCommittedOffset := s.lastCommittedMessage.getOffset()
	if msgOffset < lastCommittedOffset {
		log.WithFields(log.Fields{
			"message_offset":   msgOffset,
			"committed_offset": lastCommittedOffset,
		}).Debug("not committing because a higher offset was already committed")
		return
	}
	s.lastCommittedMessage = msg
}

// produce sends the message along the channel.  If the channel is blocked, it
// still keeps sending status messages.
func (s *StreamReader) produce(ctx context.Context, msg *message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.outChan <- msg:
			return nil
		case <-time.After(time.Until(s.nextHeartbeatDeadline)):
			log.Info("sending heartbeat while blocked on channel")
			if err := s.sendHeartbeatIfDue(); err != nil {
				return fmt.Errorf("error sending heartbeat: %v", err)
			}
		}
	}
}

// sendStatus sends a status message for the current highest committed message
// and resets the clock for the next heartbeat.
func (s *StreamReader) sendStatus() error {
	s.lastCommittedMessageMutex.Lock()
	walOffset := s.lastCommittedMessage.getOffset()
	s.lastCommittedMessageMutex.Unlock()

	log.WithField("wal_pos", walOffset).Debug("sending heartbeat")
	status, err := pgx.NewStandbyStatus(walOffset)
	if err != nil {
		return fmt.Errorf("error creating standby status: %v", err)
	}
	if err := s.conn.SendStandbyStatus(status); err != nil {
		return err
	}
	s.nextHeartbeatDeadline = time.Now().Add(s.heartbeatInterval)
	return nil
}

func (s *StreamReader) sendHeartbeatIfDue() error {
	if !s.nextHeartbeatDeadline.After(time.Now()) {
		return s.sendStatus()
	}
	return nil
}

type message struct {
	s    *StreamReader // only needed to be able to commit the message back
	data []byte

	// offset is the latest WAL offset that it is safe to commit after this
	// message is done.
	offset      uint64
	offsetMutex sync.Mutex
}

func newMessage(s *StreamReader, data []byte, offset uint64) *message {
	return &message{
		s:      s,
		data:   data,
		offset: offset,
	}
}

func (m *message) getOffset() uint64 {
	m.offsetMutex.Lock()
	defer m.offsetMutex.Unlock()
	return m.offset
}

func (m *message) updateOffset(offset uint64) {
	m.offsetMutex.Lock()
	defer m.offsetMutex.Unlock()
	m.offset = offset
}

// Data gives whatever the logical decoding output plugin creates. If using
// wal2json, this will be a json represntation of changes.
func (m *message) Data() []byte {
	return m.data
}

// Commit marks this message as ready to commit.
// This also marks any messages that came before it as ready to commit.
// Note that this does not commit the message back to postgres immediately, but
// rather marks it to be committed on the next status update.
func (m *message) Commit() {
	m.s.markMessageReadyToCommit(m)
}
