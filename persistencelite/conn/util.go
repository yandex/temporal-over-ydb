package conn

import (
	"encoding/base64"
	"fmt"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/persistence"
)

func Base64ToBlob(base64Data, encoding string) (*common.DataBlob, error) {
	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 data: %w", err)
	}
	return persistence.NewDataBlob(data, encoding), nil
}
