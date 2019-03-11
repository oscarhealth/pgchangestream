package pgchangestream

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
)

var ep *EphemeralPostgres

func getReplicationConn(t *testing.T) *pgx.ReplicationConn {
	conn, err := pgx.ReplicationConnect(
		pgx.ConnConfig{Database: ep.Name(), Host: ep.Hostname()},
	)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func dbExec(t *testing.T, db *sql.DB, sql string) {
	if _, err := db.Exec(sql); err != nil {
		t.Fatal(err)
	}
}
func TestReadAndRestart(t *testing.T) {
	db, err := ep.Open()
	if err != nil {
		t.Fatal(err)
	}
	dbExec(t, db, `CREATE TABLE test (i SERIAL PRIMARY KEY, s TEXT);`)
	dbExec(t, db, `SELECT * FROM pg_create_logical_replication_slot('stream_slot', 'test_decoding');`)
	// Looks like postgres <=9.5 can't drop a database with an inactive slot...
	defer dbExec(t, db, `SELECT * FROM pg_drop_replication_slot('stream_slot');`)
	outChan := make(chan Message, 1)

	stop := startReader(t, "stream_slot", outChan)
	defer stop()
	expectNoMessage(t, outChan)
	dbExec(t, db, `INSERT INTO test (s) VALUES ('a');`)
	expectMessage(t, outChan, true, "BEGIN")
	expectMessage(t, outChan, true, `table public.test: INSERT: i[integer]:1 s[text]:'a'`)
	expectMessage(t, outChan, true, "COMMIT")
	expectNoMessage(t, outChan)
	stop()

	stop = startReader(t, "stream_slot", outChan)
	defer stop()
	expectNoMessage(t, outChan)
	dbExec(t, db, `INSERT INTO test (s) VALUES ('b');`)
	expectMessage(t, outChan, true, "BEGIN")
	expectMessage(t, outChan, true, `table public.test: INSERT: i[integer]:2 s[text]:'b'`)
	expectMessage(t, outChan, true, "COMMIT")
	expectNoMessage(t, outChan)
	stop()
}

func TestReadAndRestartPartialCommit(t *testing.T) {
	db, err := ep.Open()
	if err != nil {
		t.Fatal(err)
	}
	dbExec(t, db, `CREATE TABLE test (i SERIAL PRIMARY KEY, s TEXT);`)
	dbExec(t, db, `SELECT * FROM pg_create_logical_replication_slot('stream_slot', 'test_decoding');`)
	// Looks like postgres <=9.5 can't drop a database with an inactive slot...
	defer dbExec(t, db, `SELECT * FROM pg_drop_replication_slot('stream_slot');`)
	outChan := make(chan Message, 1)

	stop := startReader(t, "stream_slot", outChan)
	defer stop()
	expectNoMessage(t, outChan)
	dbExec(t, db, `INSERT INTO test (s) VALUES ('a');`)
	dbExec(t, db, `INSERT INTO test (s) VALUES ('b');`)
	expectMessage(t, outChan, true, "BEGIN")
	expectMessage(t, outChan, true, `table public.test: INSERT: i[integer]:1 s[text]:'a'`)
	expectMessage(t, outChan, true, "COMMIT")
	expectMessage(t, outChan, false, "BEGIN")
	expectMessage(t, outChan, false, `table public.test: INSERT: i[integer]:2 s[text]:'b'`)
	expectMessage(t, outChan, false, "COMMIT")
	expectNoMessage(t, outChan)
	stop()

	stop = startReader(t, "stream_slot", outChan)
	defer stop()
	expectMessage(t, outChan, false, "BEGIN")
	expectMessage(t, outChan, false, `table public.test: INSERT: i[integer]:2 s[text]:'b'`)
	expectMessage(t, outChan, false, "COMMIT")
	expectNoMessage(t, outChan)
	stop()

	stop = startReader(t, "stream_slot", outChan)
	defer stop()
	expectMessage(t, outChan, true, "BEGIN")
	expectMessage(t, outChan, true, `table public.test: INSERT: i[integer]:2 s[text]:'b'`)
	expectMessage(t, outChan, true, "COMMIT")
	expectNoMessage(t, outChan)
	stop()

	stop = startReader(t, "stream_slot", outChan)
	defer stop()
	expectNoMessage(t, outChan)
	stop()
}

// TestMain sets up a postgres cluster in `ep` for use in the tests.
func TestMain(m *testing.M) {
	os.Exit(TestingMainWithPostgres(m, &ep))
}

func expectMessage(t *testing.T, outChan <-chan Message, commit bool, prefix string) {
	t.Helper()
	select {
	case <-time.After(time.Second):
		t.Fatal("did not see expected message")
	case msg := <-outChan:
		if msg == nil {
			t.Fatal("channel sent nil message")
		}
		data := string(msg.Data())
		if !strings.HasPrefix(data, prefix) {
			t.Errorf("unexpected message %q, expected prefix %q", data, prefix)
		}
		if commit {
			msg.Commit()
		}
	}
}

func expectNoMessage(t *testing.T, outChan <-chan Message) {
	t.Helper()
	select {
	case msg := <-outChan:
		t.Errorf("expected no message, got %q", string(msg.Data()))
	default:
		return
	}
}

func startReader(t *testing.T, slotName string, outChan chan<- Message) (stop func()) {
	conn := getReplicationConn(t)
	reader := NewStreamReader(conn, slotName, outChan)
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := reader.Listen(ctx)
		select {
		case <-ctx.Done():
			if err != ctx.Err() {
				t.Errorf("expected context error %q, got %q", ctx.Err(), err)
			}
		default:
			t.Errorf("errored before context finished: %v", err)
		}
	}()
	stopped := false
	return func() {
		if stopped {
			return
		}
		cancel()
		wg.Wait()
		conn.Close()
		stopped = true
	}
}
