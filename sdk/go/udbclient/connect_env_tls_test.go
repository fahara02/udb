package udbclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeSelfSignedPair writes a throwaway self-signed certificate and its key as
// PEM files and returns their paths.
func writeSelfSignedPair(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "udb-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "client.pem")
	keyPath = filepath.Join(dir, "client.key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func setBaseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("SVC_TARGET", "127.0.0.1:50051")
	t.Setenv("SVC_TENANT_ID", "00000000-0000-0000-0000-000000000001")
	t.Setenv("SVC_PURPOSE", "notes")
	t.Setenv("SVC_API_KEY", "svc-key")
	for _, name := range []string{"TLS", "TLS_SERVER_NAME", "TLS_CA_FILE", "TLS_CERT_FILE", "TLS_KEY_FILE"} {
		t.Setenv("SVC_"+name, "")
	}
}

// The CA file and the client pair load into the TLS config, and either one
// turns TLS on without UDB_TLS.
func TestConfigFromEnvLoadsTLSFiles(t *testing.T) {
	setBaseEnv(t)
	certPath, keyPath := writeSelfSignedPair(t)
	t.Setenv("SVC_TLS_CA_FILE", certPath)
	t.Setenv("SVC_TLS_CERT_FILE", certPath)
	t.Setenv("SVC_TLS_KEY_FILE", keyPath)
	t.Setenv("SVC_TLS_SERVER_NAME", "broker.internal")
	cfg, err := ConfigFromEnv("SVC_")
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if cfg.TLS == nil {
		t.Fatal("the TLS file variables must turn TLS on")
	}
	if cfg.TLS.RootCAs == nil || len(cfg.TLS.Certificates) != 1 || cfg.TLS.ServerName != "broker.internal" {
		t.Fatalf("TLS config = %+v", cfg.TLS)
	}

	setBaseEnv(t)
	t.Setenv("SVC_TLS_CA_FILE", certPath)
	cfg, err = ConfigFromEnv("SVC_")
	if err != nil {
		t.Fatalf("CA only: %v", err)
	}
	if cfg.TLS == nil || cfg.TLS.RootCAs == nil || len(cfg.TLS.Certificates) != 0 {
		t.Fatalf("CA only: TLS config = %+v", cfg.TLS)
	}

	setBaseEnv(t)
	cfg, err = ConfigFromEnv("SVC_")
	if err != nil || cfg.TLS != nil {
		t.Fatalf("no TLS variables: cfg.TLS = %+v, err = %v", cfg.TLS, err)
	}
}

// A missing file, a file without PEM, and half a client pair are refused, each
// naming the variable.
func TestConfigFromEnvRefusesBadTLSFiles(t *testing.T) {
	certPath, keyPath := writeSelfSignedPair(t)
	missing := filepath.Join(t.TempDir(), "absent.pem")
	junk := filepath.Join(t.TempDir(), "junk.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		env  map[string]string
		want []string
	}{
		{"missing CA", map[string]string{"TLS_CA_FILE": missing}, []string{"SVC_TLS_CA_FILE", missing}},
		{"CA without PEM", map[string]string{"TLS_CA_FILE": junk}, []string{"SVC_TLS_CA_FILE", junk}},
		{"cert without key", map[string]string{"TLS_CERT_FILE": certPath}, []string{"SVC_TLS_CERT_FILE", "SVC_TLS_KEY_FILE"}},
		{"key without cert", map[string]string{"TLS_KEY_FILE": keyPath}, []string{"SVC_TLS_CERT_FILE", "SVC_TLS_KEY_FILE"}},
		{"missing key", map[string]string{"TLS_CERT_FILE": certPath, "TLS_KEY_FILE": missing}, []string{"SVC_TLS_KEY_FILE", missing}},
		{"mismatched pair", map[string]string{"TLS_CERT_FILE": certPath, "TLS_KEY_FILE": junk}, []string{"SVC_TLS_CERT_FILE", "SVC_TLS_KEY_FILE"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setBaseEnv(t)
			for name, value := range tc.env {
				t.Setenv("SVC_"+name, value)
			}
			_, err := ConfigFromEnv("SVC_")
			if err == nil {
				t.Fatal("must fail")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %s", err, want)
				}
			}
		})
	}
}
