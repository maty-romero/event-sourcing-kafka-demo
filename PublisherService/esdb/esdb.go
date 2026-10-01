package esdb

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

const (
	eventsMediaType = "application/vnd.eventstore.events+json"
	eventMediaType  = "application/vnd.eventstore.event+json"
)

// ResolvedEvent es un evento original, ya resuelto por la API HTTP de
// EventStoreDB al leer un enlace de la proyeccion de categoria $ce-<cat>.
type ResolvedEvent struct {
	EventStreamID string          `json:"eventStreamId"`
	EventNumber   int64           `json:"eventNumber"`
	EventType     string          `json:"eventType"`
	EventID       string          `json:"eventId"`
	Data          json.RawMessage `json:"data"`
	Metadata      json.RawMessage `json:"metadata"`
}

type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{baseURL: baseURL, http: &http.Client{}}
}

// ReadCategoryEvent lee el enésimo evento (0-based) del stream de categoria
// $ce-<category>. La API resuelve el enlace y devuelve el evento original.
// found=false si todavia no hay un evento en esa posicion (404).
func (c *Client) ReadCategoryEvent(ctx context.Context, category string, n int64) (ResolvedEvent, bool, error) {
	stream := url.PathEscape("$ce-" + category)
	u := fmt.Sprintf("%s/streams/%s/%d", c.baseURL, stream, n)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return ResolvedEvent{}, false, err
	}
	req.Header.Set("Accept", eventMediaType)

	resp, err := c.http.Do(req)
	if err != nil {
		return ResolvedEvent{}, false, fmt.Errorf("leer %s/%d: %w", stream, n, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ResolvedEvent{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return ResolvedEvent{}, false, fmt.Errorf("leer %s/%d: HTTP %d %s", stream, n, resp.StatusCode, body)
	}

	var ev ResolvedEvent
	if err := json.NewDecoder(resp.Body).Decode(&ev); err != nil {
		return ResolvedEvent{}, false, fmt.Errorf("decodificar %s/%d: %w", stream, n, err)
	}
	return ev, true, nil
}

// ReadPosition devuelve la ultima posicion guardada en el stream de checkpoint
// junto con la revision de ese evento (-1/-1 si el stream no existe).
func (c *Client) ReadPosition(ctx context.Context, stream string) (position, revision int64, err error) {
	u := fmt.Sprintf("%s/streams/%s/head/1?embed=content", c.baseURL, url.PathEscape(stream))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return -1, -1, err
	}
	req.Header.Set("Accept", eventsMediaType)

	resp, err := c.http.Do(req)
	if err != nil {
		return -1, -1, fmt.Errorf("leer checkpoint %s: %w", stream, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return -1, -1, nil
	}
	if resp.StatusCode != http.StatusOK {
		return -1, -1, fmt.Errorf("leer checkpoint %s: HTTP %d", stream, resp.StatusCode)
	}

	var envelope struct {
		Entries []struct {
			Content struct {
				EventNumber int64 `json:"eventNumber"`
				Data        struct {
					Position int64 `json:"position"`
				} `json:"data"`
			} `json:"content"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return -1, -1, fmt.Errorf("decodificar checkpoint %s: %w", stream, err)
	}
	if len(envelope.Entries) == 0 {
		return -1, -1, nil
	}
	last := envelope.Entries[0].Content
	return last.Data.Position, last.EventNumber, nil
}

// AppendPosition graba la nueva posicion con expected revision (OCC).
// ok=false significa WrongExpectedVersion (otro instance publico antes).
func (c *Client) AppendPosition(ctx context.Context, stream string, expectedRevision, position int64) (bool, error) {
	body, err := json.Marshal([]map[string]any{
		{
			"eventId":   newUUID(),
			"eventType": "PositionSaved",
			"data":      map[string]int64{"position": position},
		},
	})
	if err != nil {
		return false, err
	}

	u := fmt.Sprintf("%s/streams/%s", c.baseURL, url.PathEscape(stream))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", eventsMediaType)
	req.Header.Set("ES-ExpectedVersion", strconv.FormatInt(expectedRevision, 10))

	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("append checkpoint %s: %w", stream, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		return true, nil
	case http.StatusBadRequest, http.StatusGone:
		return false, nil
	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return false, fmt.Errorf("append checkpoint %s: HTTP %d %s", stream, resp.StatusCode, msg)
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
