package io

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/internal/telemetry/metrics"
	"go.opentelemetry.io/otel/attribute"

	hdfs "github.com/colinmarc/hdfs/v2"
	krb "github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	krbkeytab "github.com/jcmturner/gokrb5/v8/keytab"
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
	metrics.RecordHDFSRequest("open", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	return &hdfsFile{FileReader: f, path: name}, nil
}

// ReadFile reads the named file and returns its contents.
func (h *HdfsFS) ReadFile(name string) ([]byte, error) {
	name = h.preprocess(name)
	start := time.Now()
	data, err := h.client.ReadFile(name)
	metrics.RecordHDFSRequest("read_file", time.Since(start), err)
	if err != nil {
		return nil, err
	}
	recordHDFSBytes(name, "read_file", len(data))
	return data, nil
}

// Remove removes the named file or (empty) directory from HDFS.
func (h *HdfsFS) Remove(name string) error {
	name = h.preprocess(name)
	start := time.Now()
	err := h.client.Remove(name)
	metrics.RecordHDFSRequest("remove", time.Since(start), err)
	return err
}

// hdfsFile wraps a FileReader to implement fs.File, io.ReadSeekCloser, and io.ReaderAt.
type hdfsFile struct {
	*hdfs.FileReader
	path string
}

func (f *hdfsFile) Stat() (fs.FileInfo, error) { return f.FileReader.Stat(), nil }

func (f *hdfsFile) Read(p []byte) (int, error) {
	n, err := f.FileReader.Read(p)
	recordHDFSBytes(f.path, "read", n)
	return n, err
}

func (f *hdfsFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.FileReader.ReadAt(p, off)
	recordHDFSBytes(f.path, "read_at", n)
	return n, err
}

func recordHDFSBytes(path, operation string, n int) {
	if n <= 0 {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("operation", operation),
	}

	if kind := classifyHDFSPath(path); kind != "" {
		attrs = append(attrs, attribute.String("file_kind", kind))
	}

	metrics.AddHDFSBytes(int64(n), attrs...)
}

func classifyHDFSPath(path string) string {
	lower := strings.ToLower(path)
	switch {
	case strings.Contains(lower, "/metadata/"):
		return "metadata"
	case strings.HasSuffix(lower, ".avro"):
		return "avro"
	case strings.HasSuffix(lower, ".parquet"):
		return "parquet"
	case strings.HasSuffix(lower, ".orc"):
		return "orc"
	}
	return ""
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
