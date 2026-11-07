package events

import (
	"github.com/IBM/sarama"
	xdgscram "github.com/xdg-go/scram"
)

type scramHash int

const (
	scramSHA256 scramHash = iota
	scramSHA512
)

// newSCRAMClientGenerator adapts xdg-go/scram to sarama's SCRAMClient contract.
func newSCRAMClientGenerator(hash scramHash) func() sarama.SCRAMClient {
	return func() sarama.SCRAMClient {
		return &scramClient{hash: hash}
	}
}

type scramClient struct {
	hash scramHash
	conv *xdgscram.ClientConversation
}

func (c *scramClient) hashGenerator() xdgscram.HashGeneratorFcn {
	if c.hash == scramSHA512 {
		return xdgscram.SHA512
	}
	return xdgscram.SHA256
}

// Begin starts a SCRAM conversation for the given credentials.
func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.hashGenerator().NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.conv = client.NewConversation()
	return nil
}

// Step advances the SCRAM exchange.
func (c *scramClient) Step(challenge string) (string, error) {
	if c.conv == nil {
		return "", ErrBrokerUnavailable
	}
	return c.conv.Step(challenge)
}

// Done reports whether the exchange completed.
func (c *scramClient) Done() bool {
	return c.conv != nil && c.conv.Done()
}
