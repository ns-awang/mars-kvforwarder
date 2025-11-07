package aggregator

import "strconv"

// Header represents a Kafka-style header key/value pair.
type Header struct {
	Key   string
	Value []byte
}

const (
	HeaderSHA256     = "sha256"
	HeaderPrevTxID   = "prev-txid"
	HeaderCurrTxID   = "curr-txid"
	HeaderBatchIndex = "batch-index"
	HeaderBatchTotal = "batch-total"
)

// BuildHeaders constructs the required headers for a data message based on the batch
// and provided transaction ids. The sha256 value is taken from Batch.PayloadSHA256.
func BuildHeaders(batch Batch, prevTxID, currTxID uint64) []Header {
	return []Header{
		{Key: HeaderSHA256, Value: []byte(batch.PayloadSHA256)},
		{Key: HeaderPrevTxID, Value: []byte(strconv.FormatUint(prevTxID, 10))},
		{Key: HeaderCurrTxID, Value: []byte(strconv.FormatUint(currTxID, 10))},
		{Key: HeaderBatchIndex, Value: []byte(strconv.Itoa(batch.BatchIndex))},
		{Key: HeaderBatchTotal, Value: []byte(strconv.Itoa(batch.BatchTotal))},
	}
}
