package payloads

type Payload interface {
	// Clone returns a new Payload that is a deep-copy of the original.
	Clone() Payload

	Out() (string, string, []byte)

	// MarkAsProcessed is invoked by the pipeline when the Payload either
	// reaches the pipeline sink or it gets discarded by one of the
	// pipeline stages.
	MarkAsProcessed()
}
