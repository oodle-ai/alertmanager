// Copyright 2019 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	commoncfg "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/notify/test"
	"github.com/prometheus/alertmanager/types"
)

func TestWebhookRetry(t *testing.T) {
	u, err := url.Parse("http://example.com")
	if err != nil {
		require.NoError(t, err)
	}
	notifier, err := New(
		&config.WebhookConfig{
			URL:        &config.SecretURL{URL: u},
			HTTPConfig: &commoncfg.HTTPClientConfig{},
		},
		test.CreateTmpl(t),
		log.NewNopLogger(),
	)
	if err != nil {
		require.NoError(t, err)
	}

	t.Run("test retry status code", func(t *testing.T) {
		for statusCode, expected := range test.RetryTests(test.DefaultRetryCodes()) {
			actual, _ := notifier.retrier.Check(statusCode, nil)
			require.Equal(t, expected, actual, fmt.Sprintf("error on status %d", statusCode))
		}
	})

	t.Run("test retry error details", func(t *testing.T) {
		for _, tc := range []struct {
			status int
			body   io.Reader

			exp string
		}{
			{
				status: http.StatusBadRequest,
				body: bytes.NewBuffer([]byte(
					`{"status":"invalid event"}`,
				)),

				exp: fmt.Sprintf(`unexpected status code %d: %s: {"status":"invalid event"}`, http.StatusBadRequest, u.String()),
			},
			{
				status: http.StatusBadRequest,

				exp: fmt.Sprintf(`unexpected status code %d: %s`, http.StatusBadRequest, u.String()),
			},
		} {
			t.Run("", func(t *testing.T) {
				_, err = notifier.retrier.Check(tc.status, tc.body)
				require.Equal(t, tc.exp, err.Error())
			})
		}
	})
}

func TestWebhookTruncateAlerts(t *testing.T) {
	alerts := make([]*types.Alert, 10)

	truncatedAlerts, numTruncated := truncateAlerts(0, alerts)
	require.Len(t, truncatedAlerts, 10)
	require.EqualValues(t, 0, numTruncated)

	truncatedAlerts, numTruncated = truncateAlerts(4, alerts)
	require.Len(t, truncatedAlerts, 4)
	require.EqualValues(t, 6, numTruncated)

	truncatedAlerts, numTruncated = truncateAlerts(100, alerts)
	require.Len(t, truncatedAlerts, 10)
	require.EqualValues(t, 0, numTruncated)
}

func TestWebhookRedactedURL(t *testing.T) {
	ctx, u, fn := test.GetContextWithCancelingURL()
	defer fn()

	secret := "secret"
	notifier, err := New(
		&config.WebhookConfig{
			URL:        &config.SecretURL{URL: u},
			HTTPConfig: &commoncfg.HTTPClientConfig{},
		},
		test.CreateTmpl(t),
		log.NewNopLogger(),
	)
	require.NoError(t, err)

	test.AssertNotifyLeaksNoSecret(ctx, t, notifier, secret)
}

func TestWebhookReadingURLFromFile(t *testing.T) {
	ctx, u, fn := test.GetContextWithCancelingURL()
	defer fn()

	f, err := os.CreateTemp("", "webhook_url")
	require.NoError(t, err, "creating temp file failed")
	_, err = f.WriteString(u.String() + "\n")
	require.NoError(t, err, "writing to temp file failed")

	notifier, err := New(
		&config.WebhookConfig{
			URLFile:    f.Name(),
			HTTPConfig: &commoncfg.HTTPClientConfig{},
		},
		test.CreateTmpl(t),
		log.NewNopLogger(),
	)
	require.NoError(t, err)

	test.AssertNotifyLeaksNoSecret(ctx, t, notifier, u.String())
}

type roundTripFunc func(req *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func testAlerts() []*types.Alert {
	return []*types.Alert{
		{
			Alert: model.Alert{
				Labels:       model.LabelSet{"alertname": "TestAlert"},
				Annotations:  model.LabelSet{"summary": "Test summary"},
				StartsAt:     time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				EndsAt:       time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC),
				GeneratorURL: "http://generator.url",
			},
		},
	}
}

// capturePayload posts the given config's notification and returns the raw
// request body that would have been sent to the webhook endpoint.
func capturePayload(t *testing.T, conf *config.WebhookConfig, alerts []*types.Alert) []byte {
	t.Helper()

	var capturedPayload []byte
	mockTransport := roundTripFunc(func(req *http.Request) *http.Response {
		var err error
		capturedPayload, err = io.ReadAll(req.Body)
		require.NoError(t, err)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       http.NoBody,
		}
	})

	ctx := notify.WithGroupKey(context.Background(), "{}:{alertname=\"test1\"}")
	ctx = notify.WithReceiverName(ctx, "test_receiver")

	n, err := New(conf, test.CreateTmpl(t), log.NewNopLogger())
	require.NoError(t, err)
	n.client.Transport = mockTransport

	_, err = n.Notify(ctx, alerts...)
	require.NoError(t, err)
	require.NotEmpty(t, capturedPayload)

	return capturedPayload
}

// TestWebhookDefaultPayload tests that the default payload sent by the webhook
// notifier matches the behaviour before introducing templating.
func TestWebhookDefaultPayload(t *testing.T) {
	u, err := url.Parse("http://localhost")
	require.NoError(t, err)

	conf := &config.WebhookConfig{
		URL:        &config.SecretURL{URL: u},
		HTTPConfig: &commoncfg.HTTPClientConfig{},
	}

	alerts := testAlerts()
	tmpl := test.CreateTmpl(t)
	ctx := notify.WithGroupKey(context.Background(), "{}:{alertname=\"test1\"}")
	ctx = notify.WithReceiverName(ctx, "test_receiver")
	data := notify.GetTemplateData(ctx, tmpl, alerts, log.NewNopLogger())

	msg := &Message{
		Version:  "4",
		Data:     data,
		GroupKey: "{}:{alertname=\"test1\"}",
	}

	var buf bytes.Buffer
	require.NoError(t, json.NewEncoder(&buf).Encode(msg))

	require.JSONEq(t, buf.String(), string(capturePayload(t, conf, alerts)))
}

func TestWebhookCustomPayload(t *testing.T) {
	u, err := url.Parse("http://localhost")
	require.NoError(t, err)

	conf := &config.WebhookConfig{
		URL:        &config.SecretURL{URL: u},
		HTTPConfig: &commoncfg.HTTPClientConfig{},
		Payload: map[string]interface{}{
			"custom":       `some custom content`,
			"commonLabels": "{{ .CommonLabels | toJson }}",
			"status":       "{{ .Status }}",
			"nested": map[string]interface{}{
				"alerts": []interface{}{
					map[string]interface{}{
						"name": "{{ .CommonLabels.alertname }}",
					},
				},
			},
		},
	}

	expected := map[string]interface{}{
		"custom":       `some custom content`,
		"commonLabels": map[string]string{"alertname": "TestAlert"},
		"status":       "resolved",
		"nested": map[string]interface{}{
			"alerts": []interface{}{
				map[string]interface{}{
					"name": "TestAlert",
				},
			},
		},
	}

	var buf bytes.Buffer
	require.NoError(t, json.NewEncoder(&buf).Encode(expected))

	require.JSONEq(t, buf.String(), string(capturePayload(t, conf, testAlerts())))
}
