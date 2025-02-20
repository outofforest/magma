package p2c

// ClientRequest represents a client's request to append item to the log.
type ClientRequest struct {
	Data []byte
}

// ClientResponse represents a server's response indicating the result of processing a client's request.
type ClientResponse struct {
	Success bool
}
