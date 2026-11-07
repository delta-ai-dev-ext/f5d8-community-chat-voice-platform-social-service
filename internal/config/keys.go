package config

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// VerificationKey is one accepted access-token verification key. Identity
// publishes a current key and the next key so rotation needs no downtime.
type VerificationKey struct {
	// KeyID is a short label ("current", "next", or the JSON map key) used in
	// logs. It is never logged together with key material.
	KeyID string
	// PublicKey is an RSA, ECDSA, or Ed25519 public key.
	PublicKey any
}

// ErrNoVerificationKeys is returned when no usable key could be loaded.
var ErrNoVerificationKeys = errors.New("no token verification keys could be loaded")

// LoadVerificationKeys resolves the current + next verification keys from
// either an inline JSON/PEM document or a path to a file/directory.
func LoadVerificationKeys(inline, path string) ([]VerificationKey, error) {
	if strings.TrimSpace(inline) != "" {
		return parseKeyDocument([]byte(inline))
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: neither TOKEN_VERIFICATION_KEYS nor TOKEN_VERIFICATION_KEYS_PATH is set", ErrNoVerificationKeys)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("token verification keys: %w", err)
	}
	if !info.IsDir() {
		content, err := os.ReadFile(path) //nolint:gosec // operator-supplied config path
		if err != nil {
			return nil, fmt.Errorf("token verification keys: %w", err)
		}
		return parseKeyDocument(content)
	}
	keys := make([]VerificationKey, 0, 2)
	for _, name := range []string{"current", "next"} {
		content, err := os.ReadFile(filepath.Join(path, name+".pem")) //nolint:gosec // operator-supplied config path
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("token verification keys: %s: %w", name, err)
		}
		key, err := parseSingleKey(content)
		if err != nil {
			return nil, fmt.Errorf("token verification keys: %s: %w", name, err)
		}
		keys = append(keys, VerificationKey{KeyID: name, PublicKey: key})
	}
	if len(keys) == 0 {
		return nil, ErrNoVerificationKeys
	}
	return keys, nil
}

type keyDocument struct {
	CurrentKey string `json:"current_key"`
	NextKey    string `json:"next_key"`
	// Also accept the "current"/"next" spellings.
	Current string `json:"current"`
	Next    string `json:"next"`
}

func parseKeyDocument(content []byte) ([]VerificationKey, error) {
	trimmed := strings.TrimSpace(string(content))
	if trimmed == "" {
		return nil, ErrNoVerificationKeys
	}
	if strings.HasPrefix(trimmed, "{") {
		return parseJSONKeyDocument([]byte(trimmed))
	}
	key, err := parseSingleKey(content)
	if err != nil {
		return nil, err
	}
	return []VerificationKey{{KeyID: "current", PublicKey: key}}, nil
}

func parseJSONKeyDocument(content []byte) ([]VerificationKey, error) {
	var doc keyDocument
	if err := json.Unmarshal(content, &doc); err != nil {
		return nil, fmt.Errorf("token verification keys: %w", err)
	}
	pairs := []struct {
		label string
		pem   string
	}{
		{"current", firstNonEmpty(doc.CurrentKey, doc.Current)},
		{"next", firstNonEmpty(doc.NextKey, doc.Next)},
	}
	keys := make([]VerificationKey, 0, len(pairs))
	for _, pair := range pairs {
		if strings.TrimSpace(pair.pem) == "" {
			continue
		}
		key, err := parseSingleKey([]byte(pair.pem))
		if err != nil {
			return nil, fmt.Errorf("token verification keys: %s: %w", pair.label, err)
		}
		keys = append(keys, VerificationKey{KeyID: pair.label, PublicKey: key})
	}
	if len(keys) == 0 {
		return nil, ErrNoVerificationKeys
	}
	return keys, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func parseSingleKey(content []byte) (any, error) {
	block, _ := pem.Decode(content)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	if cert, err := x509.ParseCertificate(block.Bytes); err == nil {
		return cert.PublicKey, nil
	}
	if publicKey, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		switch publicKey.(type) {
		case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey:
			return publicKey, nil
		default:
			return nil, fmt.Errorf("unsupported public key type %T", publicKey)
		}
	}
	if publicKey, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return publicKey, nil
	}
	return nil, errors.New("unsupported key encoding")
}
