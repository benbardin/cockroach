// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
)

func subscribeInternal(
	ctx context.Context, feed pgx.Rows, eventsChan chan streamingccl.Event, closeChan chan struct{},
) error {
	// Get the next event from the cursor.
	var bufferedEvent *streampb.StreamEvent
	getNextEvent := func() (streamingccl.Event, error) {
		if e := parseEvent(bufferedEvent); e != nil {
			return e, nil
		}

		if !feed.Next() {
			if err := feed.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		var data []byte
		if err := feed.Scan(&data); err != nil {
			return nil, err
		}
		var streamEvent streampb.StreamEvent
		if err := protoutil.Unmarshal(data, &streamEvent); err != nil {
			return nil, err
		}
		bufferedEvent = &streamEvent
		return parseEvent(bufferedEvent), nil
	}

	for {
		event, err := getNextEvent()
		if err != nil {
			return err
		}
		select {
		case eventsChan <- event:
		case <-closeChan:
			// Exit quietly to not cause other subscriptions in the same
			// ctxgroup.Group to exit.
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// parseEvent parses next event from the batch of events inside streampb.StreamEvent.
func parseEvent(streamEvent *streampb.StreamEvent) streamingccl.Event {
	if streamEvent == nil {
		return nil
	}

	if streamEvent.Checkpoint != nil {
		event := streamingccl.MakeCheckpointEvent(streamEvent.Checkpoint.ResolvedSpans)
		streamEvent.Checkpoint = nil
		return event
	}

	var event streamingccl.Event
	if streamEvent.Batch != nil {
		switch {
		case len(streamEvent.Batch.Ssts) > 0:
			event = streamingccl.MakeSSTableEvent(streamEvent.Batch.Ssts[0])
			streamEvent.Batch.Ssts = streamEvent.Batch.Ssts[1:]
		case len(streamEvent.Batch.KeyValues) > 0:
			event = streamingccl.MakeKVEvent(streamEvent.Batch.KeyValues[0])
			streamEvent.Batch.KeyValues = streamEvent.Batch.KeyValues[1:]
		case len(streamEvent.Batch.DelRanges) > 0:
			event = streamingccl.MakeDeleteRangeEvent(streamEvent.Batch.DelRanges[0])
			streamEvent.Batch.DelRanges = streamEvent.Batch.DelRanges[1:]
		case len(streamEvent.Batch.SpanConfigs) > 0:
			event = streamingccl.MakeSpanConfigEvent(streamEvent.Batch.SpanConfigs[0])
			streamEvent.Batch.SpanConfigs = streamEvent.Batch.SpanConfigs[1:]
		}

		if len(streamEvent.Batch.KeyValues) == 0 &&
			len(streamEvent.Batch.Ssts) == 0 &&
			len(streamEvent.Batch.DelRanges) == 0 &&
			len(streamEvent.Batch.SpanConfigs) == 0 {
			streamEvent.Batch = nil
		}
	}
	return event
}

type tlsCerts struct {
	certs        []tls.Certificate
	rootCertPool *x509.CertPool
}

const (
	// sslInlineURLParam is a non-standard connection URL
	// parameter. When true, we assume that sslcert, sslkey, and
	// sslrootcert contain URL-encoded data rather than paths.
	sslInlineURLParam = "sslinline"

	sslModeURLParam     = "sslmode"
	sslCertURLParam     = "sslcert"
	sslKeyURLParam      = "sslkey"
	sslRootCertURLParam = "sslrootcert"
)

var RedactableURLParameters = []string{
	sslCertURLParam,
	sslKeyURLParam,
	sslRootCertURLParam,
}

func setupPGXConfig(remote *url.URL) (*pgx.ConnConfig, error) {
	noInlineCertURI, tlsInfo, err := uriWithInlineTLSCertsRemoved(remote)
	if err != nil {
		return nil, err
	}
	config, err := pgx.ParseConfig(noInlineCertURI.String())
	if err != nil {
		return nil, err
	}
	tlsInfo.addTLSCertsToConfig(config.TLSConfig)

	// The default pgx dialer uses a KeepAlive of 5 minutes. Set a lower KeepAlive
	// threshold, so if two nodes disconnect, we eagerly replan the job with
	// potentially new node pairings.
	dialer := &net.Dialer{KeepAlive: time.Second * 15}
	config.DialFunc = dialer.DialContext
	return config, nil
}

// uriWithInlineTLSCertsRemoved handles the non-standard sslinline
// option. The returned URL can be passed to pgx. The returned
// tlsCerts struct can be used to apply the certificate data to the
// tls.Config produced by pgx.
func uriWithInlineTLSCertsRemoved(remote *url.URL) (*url.URL, *tlsCerts, error) {
	if remote.Query().Get(sslInlineURLParam) != "true" {
		return remote, nil, nil
	}

	retURL := *remote
	v := retURL.Query()
	cert := v.Get(sslCertURLParam)
	key := v.Get(sslKeyURLParam)
	rootcert := v.Get(sslRootCertURLParam)

	if (cert != "" && key == "") || (cert == "" && key != "") {
		return nil, nil, errors.New(`both "sslcert" and "sslkey" are required`)
	}

	tlsInfo := &tlsCerts{}
	if rootcert != "" {
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM([]byte(rootcert)) {
			return nil, nil, errors.New("unable to add CA to cert pool")
		}
		tlsInfo.rootCertPool = caCertPool
	}
	if cert != "" && key != "" {
		// TODO(ssd): pgx supports sslpassword here. But, it
		// only supports PKCS#1 and relies on functions that
		// are deprecated in the stdlib. For now, I've skipped
		// it.
		block, _ := pem.Decode([]byte(key))
		pemKey := pem.EncodeToMemory(block)
		keyPair, err := tls.X509KeyPair([]byte(cert), pemKey)
		if err != nil {
			return nil, nil, errors.Wrap(err, "unable to construct x509 key pair")
		}
		tlsInfo.certs = []tls.Certificate{keyPair}
	}

	// lib/pq, pgx, and the C libpq implement this backwards
	// compatibility quirk. Since we are removing sslrootcert, we
	// have to re-implement it here.
	//
	// TODO(ssd): This may be a sign that we should implement the
	// entire configTLS function from pgx and remove all tls
	// options.
	if v.Get(sslModeURLParam) == "require" && rootcert != "" {
		v.Set(sslModeURLParam, "verify-ca")
	}

	v.Del(sslCertURLParam)
	v.Del(sslKeyURLParam)
	v.Del(sslRootCertURLParam)
	v.Del(sslInlineURLParam)
	retURL.RawQuery = v.Encode()
	return &retURL, tlsInfo, nil
}

func (c *tlsCerts) addTLSCertsToConfig(tlsConfig *tls.Config) {
	if c == nil {
		return
	}

	if c.rootCertPool != nil {
		tlsConfig.RootCAs = c.rootCertPool
		tlsConfig.ClientCAs = c.rootCertPool
	}

	if len(c.certs) > 0 {
		tlsConfig.Certificates = c.certs
	}
}
