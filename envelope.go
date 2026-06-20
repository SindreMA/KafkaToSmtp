package main

import (
	"bytes"
	"encoding/json"
	"errors"
)

// Envelope is the JSON message format apps publish to the Kafka topic.
//
//	{
//	  "from": "noreply@sindrema.com",      // optional; falls back to DEFAULT_FROM
//	  "to": ["alice@example.com"],          // string or array of strings
//	  "cc": [],                             // optional
//	  "bcc": [],                            // optional (not added to headers)
//	  "replyTo": "support@sindrema.com",    // optional
//	  "subject": "Hello",
//	  "text": "plain text body",            // text and/or html (at least one)
//	  "html": "<p>html body</p>",
//	  "headers": {"X-Campaign": "welcome"}  // optional extra headers
//	}
type Envelope struct {
	From    string            `json:"from"`
	To      StringOrSlice     `json:"to"`
	Cc      StringOrSlice     `json:"cc"`
	Bcc     StringOrSlice     `json:"bcc"`
	ReplyTo string            `json:"replyTo"`
	Subject string            `json:"subject"`
	Text    string            `json:"text"`
	HTML    string            `json:"html"`
	Headers map[string]string `json:"headers"`
}

// Validate checks that the envelope has the minimum needed to send.
// hasFallbackFrom is true when a From can be supplied even if the message omits
// it (via DEFAULT_FROM or a provider's From override).
func (e *Envelope) Validate(hasFallbackFrom bool) error {
	if e.From == "" && !hasFallbackFrom {
		return errors.New(`missing "from" and no DEFAULT_FROM / provider From configured`)
	}
	if len(e.To)+len(e.Cc)+len(e.Bcc) == 0 {
		return errors.New("no recipients (to/cc/bcc all empty)")
	}
	if e.Text == "" && e.HTML == "" {
		return errors.New(`empty body (need "text" and/or "html")`)
	}
	return nil
}

// StringOrSlice accepts either a JSON string or an array of strings, so senders
// can write "to": "a@b.com" or "to": ["a@b.com", "c@d.com"].
type StringOrSlice []string

func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		*s = nil
		return nil
	}
	if data[0] == '[' {
		var arr []string
		if err := json.Unmarshal(data, &arr); err != nil {
			return err
		}
		*s = arr
		return nil
	}
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	if str == "" {
		*s = nil
	} else {
		*s = []string{str}
	}
	return nil
}
