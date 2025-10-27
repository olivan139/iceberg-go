package io

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2"
	krb "github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	krbkeytab "github.com/jcmturner/gokrb5/v8/keytab"

	"github.com/apache/iceberg-go/metrics"
)

// Constants for HDFS configuration options
const (
	HDFSNameNode            = "hdfs.namenode"
	HDFSUser                = "hdfs.user"
	HDFSUseDatanodeHostname = "hdfs.use-datanode-hostname"
	HDFSKerberosPrincipal   = "hdfs.kerberos.principal"
	HDFSKerberosKrb5Conf    = "hdfs.kerberos.krb5-conf"
	HDFSKerberosKeytab      = "hdfs.kerberos.keytab"
)

// HdfsFS is an implementation of IO backed by an HDFS cluster.
type HdfsFS struct{ client *hdfs.Client }

func (h *HdfsFS) preprocess(name string) string {
	if strings.HasPrefix(name, "hdfs://") {
		if u, err := url.Parse(name); err == nil {
			if u.Path != "" {
				return u.Path
			}
		}
		name = strings.TrimPrefix(name, "hdfs://")
		if idx := strings.IndexByte(name, '/'); idx >= 0 {
			name = name[idx:]
		}
	}
	return name
}

// Open opens the named file for reading from HDFS.
func (h *HdfsFS) Open(name string) (File, error) {
	name = h.preprocess(name)
	start := time.Now()
	f, err := h.client.Open(name)
	if err != nil {
		metrics.RecordHDFSAccess(context.Background(), inferHDFSFormat(name), inferHDFSContent(name), time.Since(start))
		return nil, err
	}
	hf := &hdfsFile{
		FileReader: f,
		name:       name,
		format:     inferHDFSFormat(name),
		content:    inferHDFSContent(name),
	}
	hf.ioNanos.Store(time.Since(start).Nanoseconds())
	return hf, nil
}

// ReadFile reads the named file and returns its contents.
func (h *HdfsFS) ReadFile(name string) ([]byte, error) {
	name = h.preprocess(name)
	start := time.Now()
	data, err := h.client.ReadFile(name)
	duration := time.Since(start)
	metrics.RecordHDFSAccess(context.Background(), inferHDFSFormat(name), inferHDFSContent(name), duration)
	if err == nil {
		metrics.AddHDFSVolume(context.Background(), inferHDFSFormat(name), inferHDFSContent(name), int64(len(data)))
	}
	return data, err
}

// Remove removes the named file or (empty) directory from HDFS.
func (h *HdfsFS) Remove(name string) error {
	name = h.preprocess(name)
	return h.client.Remove(name)
}

// hdfsFile wraps a FileReader to implement fs.File, io.ReadSeekCloser, and io.ReaderAt.
type hdfsFile struct {
	*hdfs.FileReader

	name    string
	format  string
	content string

	bytesRead atomic.Int64
	ioNanos   atomic.Int64

	closeOnce sync.Once
}

func (f *hdfsFile) Stat() (fs.FileInfo, error) { return f.FileReader.Stat(), nil }

func (f *hdfsFile) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := f.FileReader.Read(p)
	f.ioNanos.Add(time.Since(start).Nanoseconds())
	if n > 0 {
		f.bytesRead.Add(int64(n))
	}
	return n, err
}

func (f *hdfsFile) ReadAt(p []byte, off int64) (int, error) {
	start := time.Now()
	n, err := f.FileReader.ReadAt(p, off)
	f.ioNanos.Add(time.Since(start).Nanoseconds())
	if n > 0 {
		f.bytesRead.Add(int64(n))
	}
	return n, err
}

func (f *hdfsFile) Close() error {
	var closeErr error
	f.closeOnce.Do(func() {
		start := time.Now()
		closeErr = f.FileReader.Close()
		f.ioNanos.Add(time.Since(start).Nanoseconds())
		duration := time.Duration(f.ioNanos.Load())
		metrics.RecordHDFSAccess(context.Background(), f.format, f.content, duration)
		if read := f.bytesRead.Load(); read > 0 {
			metrics.AddHDFSVolume(context.Background(), f.format, f.content, read)
		}
	})
	return closeErr
}

func inferHDFSFormat(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".parquet":
		return "parquet"
	case ".avro":
		return "avro"
	case ".orc":
		return "orc"
	default:
		return ""
	}
}

func inferHDFSContent(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "metadata"):
		return "metadata"
	case strings.Contains(lower, "manifest"):
		return "manifest"
	case strings.Contains(lower, "delete"):
		return "deletes"
	case strings.Contains(lower, "data"):
		return "data"
	default:
		return ""
	}
}

// createHDFSFS constructs an HDFS-backed IO from a parsed URL and configuration properties.
func createHDFSFS(parsed *url.URL, props map[string]string) (IO, error) {
	addresses := []string{}
	if nn := props[HDFSNameNode]; nn != "" {
		addresses = append(addresses, nn)
	} else if parsed != nil && parsed.Host != "" {
		addresses = append(addresses, parsed.Host)
	}
	if len(addresses) == 0 {
		return nil, errors.New("hdfs namenode not specified")
	}
	opts := hdfs.ClientOptions{Addresses: addresses}
	if user := props[HDFSUser]; user != "" {
		opts.User = user
	}
	if v, ok := props[HDFSUseDatanodeHostname]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			opts.UseDatanodeHostname = b
		}
	}

	if principal := props[HDFSKerberosPrincipal]; principal != "" {
		confPath := props[HDFSKerberosKrb5Conf]
		keytabPath := props[HDFSKerberosKeytab]
		if confPath == "" || keytabPath == "" {
			return nil, errors.New("kerberos configuration requires krb5-conf and keytab")
		}
		kt, err := krbkeytab.Load(keytabPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load kerberos keytab: %w", err)
		}
		cfg, err := krbconfig.Load(confPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load krb5 config: %w", err)
		}
		parts := strings.Split(principal, "@")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid kerberos principal: %s", principal)
		}
		client := krb.NewWithKeytab(parts[0], parts[1], kt, cfg)
		if err := client.Login(); err != nil {
			return nil, fmt.Errorf("kerberos login failed: %w", err)
		}
		opts.KerberosClient = client
		opts.KerberosServicePrincipleName = "nn/_HOST"
	}

	client, err := hdfs.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to HDFS: %w", err)
	}
	return &HdfsFS{client: client}, nil
}
