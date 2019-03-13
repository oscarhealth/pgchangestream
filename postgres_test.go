//
package pgchangestream

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/lib/pq" // Postgres driver

	"database/sql"
	"io"
)

const (
	// psqlConf is additional configuration that we will apply to our pg instance
	psqlConf = `
listen_addresses = ''         # Don't try to connect to any Internet ports
unix_socket_directories='%s'  # Put the unix socket in the database dir
shared_buffers = 12MB         # Make shared buffers smaller for faster startup
fsync = off                   # Don't bother persisting to disk too much
full_page_writes = off        # More relaxing consistency
synchronous_commit = off      # consistency? who needs it!
# settings for testing replication connections
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
`
	pgHbaConf = `
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
`
	dbname = "test"
)

// TestingMainWithPostgres runs the given test suite, after filling in the given
// pointer with an EphemeralPostgres instance.  Returns an error code to be
// passed to os.Exit.
//
// This is ported from an internal Oscar library.
func TestingMainWithPostgres(m *testing.M, epPtr **EphemeralPostgres) int {
	ep, err := NewEphemeralPostgres()
	*epPtr = ep
	if err != nil {
		panic(err)
	}
	defer ep.ShutDown()
	return m.Run()
}

// EphemeralPostgres represents an instance of Postgres only alive for the duration of
// tests. This is a library meant to
type EphemeralPostgres struct {
	host        string // hostname of database
	name        string // Database name
	dir         string
	currentConn io.Closer
	binDir      string
}

// NewEphemeralPostgres returns a new instance.  Each one must be ShutDown
// after being started.
func NewEphemeralPostgres() (*EphemeralPostgres, error) {
	binDir, err := findBinDir()
	if err != nil {
		return nil, err
	}
	pgCtl := filepath.Join(binDir, "pg_ctl")
	dir, err := ioutil.TempDir("", "ephemeral_psql")
	if err != nil {
		return nil, err
	}
	// First, initialize the database
	err = command(pgCtl, "-D", dir, "-o", "-A trust", "initdb").Run()
	if err != nil {
		return nil, err
	}
	// Then, add custom configuration to it
	err = writeConf(dir)
	if err != nil {
		return nil, err
	}
	// Finally, start the process
	err = command(pgCtl, "-D", dir, "-w", "start").Run()
	if err != nil {
		return nil, err
	}
	return &EphemeralPostgres{
		host:   dir,
		name:   dbname,
		dir:    dir,
		binDir: binDir,
	}, nil
}

// Openreturns a new sql.DB.  This will be connected to a completely empty
// database.  The caller should not close this connection.  Subsequent calls
// to Open invalidate previous connections, so it is recommended to call this
// once per test, and run tests sequentially.
func (e *EphemeralPostgres) Open() (*sql.DB, error) {
	err := e.initDB()
	if err != nil {
		return nil, err
	}

	// Open a connection to the database
	connString := e.GetConnectionString()
	db, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}
	// Perform a ping here so that errors with this module show up here, rather than
	// when the tester tries to use it.
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	e.currentConn = db
	return db, nil
}

// GetConnectionString returns the connection string for the test DB
func (e *EphemeralPostgres) GetConnectionString() string {
	return fmt.Sprintf("host=%s dbname=%s sslmode=disable", e.host, e.name)
}

// ShutDown release all resources and removes any trace of the database.  It
// must be called for every instance that is created.
func (e *EphemeralPostgres) ShutDown() error {
	if e.currentConn != nil {
		// Close the most recent connection and drop the database
		err := e.currentConn.Close()
		if err != nil {
			return err
		}
		err = command(e.binPath("dropdb"),
			"--no-password", "--if-exists", "-h", e.host, e.name).Run()
		if err != nil {
			return err
		}
	}
	// Stop the database process
	err := command(e.binPath("pg_ctl"), "-D", e.dir, "-mi", "stop").Run()
	if err != nil {
		return err
	}
	// Remove the temp directory
	return os.RemoveAll(e.dir)
}

// Hostname returns the hostname to use when connecting to the database.
// This will be a filepath to the unix domain socket.
func (e *EphemeralPostgres) Hostname() string {
	return e.host
}

// Name returns the name of the database.
func (e *EphemeralPostgres) Name() string {
	return e.name
}

// initDB closes any open connection, drops any existing database, and
// creates a new one.
func (e *EphemeralPostgres) initDB() error {
	// Close the most recent connection and drop the database
	if e.currentConn != nil {
		err := e.currentConn.Close()
		if err != nil {
			return err
		}
		err = command(e.binPath("dropdb"),
			"--no-password", "--if-exists", "-h", e.host, e.name).Run()
		if err != nil {
			return err
		}
	}
	// Initialize a brand new database
	err := command(e.binPath("createdb"), "--no-password", "-h", e.host, e.name).Run()
	if err != nil {
		return err
	}

	return nil
}

func (e *EphemeralPostgres) binPath(executablename string) string {
	return filepath.Join(e.binDir, executablename)
}

func command(name string, arg ...string) *exec.Cmd {
	cmd := exec.Command(name, arg...)
	cmd.Stderr = os.Stderr
	return cmd

}

func writeConf(dir string) error {
	confFiles := map[string]string{
		"postgresql.conf": fmt.Sprintf(psqlConf, dir),
		"pg_hba.conf":     pgHbaConf,
	}
	for fname, text := range confFiles {
		fpath := filepath.Join(dir, fname)
		file, err := os.OpenFile(fpath, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer file.Close()
		if _, err := file.WriteString(text); err != nil {
			return err
		}
	}
	return nil
}

// Finds the directory containing postgres binaries (pg_ctl, createdb, etc.)
func findBinDir() (string, error) {
	// Add possible locations to the PATH, since on some machines pg_ctl is not in
	// the default PATH
	cmd := command("pg_config", "--bindir")
	cmd.Env = os.Environ()
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.Trim(string(out), " \n"), nil
}
