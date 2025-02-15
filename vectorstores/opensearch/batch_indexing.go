package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

func (s *Store) batchDocumentIndexing(
	ctx context.Context,
	indexName string,
	documents []struct {
		ID       string
		Text     string
		Vector   []float32
		Metadata map[string]any
	},
) (*opensearchapi.Response, error) {
	// Prepare bulk request body
	var bulkBody bytes.Buffer

	for _, doc := range documents {
		// Prepare index action
		indexMeta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
				"_id":    doc.ID,
			},
		}

		// Prepare document
		document := document{
			FieldsContent:       doc.Text,
			FieldsContentVector: doc.Vector,
			FieldsMetadata:      doc.Metadata,
		}

		// Encode index action
		if err := json.NewEncoder(&bulkBody).Encode(indexMeta); err != nil {
			return nil, fmt.Errorf("error encoding index metadata: %w", err)
		}
		bulkBody.WriteRune('\n')

		// Encode document
		if err := json.NewEncoder(&bulkBody).Encode(document); err != nil {
			return nil, fmt.Errorf("error encoding document: %w", err)
		}
		bulkBody.WriteRune('\n')
	}

	// Perform bulk indexing
	bulkRequest := opensearchapi.BulkRequest{
		Body: bytes.NewReader(bulkBody.Bytes()),
	}

	response, err := bulkRequest.Do(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("error performing bulk indexing: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
		}
	}(response.Body)

	return response, nil
}
