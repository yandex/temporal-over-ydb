package conn

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/primitives"
)

type EncodingTypeRaw = int32

func (client *Client) EncodingType() types.Type {
	if client.useOldTypes {
		return types.TypeUTF8
	}
	return types.TypeInt16
}

func (client *Client) NamespaceIDType() types.Type {
	if client.useOldTypes {
		return types.TypeUTF8
	}
	return types.TypeBytes
}

func (client *Client) RunIDType() types.Type {
	if client.useOldTypes {
		return types.TypeUTF8
	}
	return types.TypeBytes
}

func (client *Client) HistoryIDType() types.Type {
	if client.useOldTypes {
		return types.TypeUTF8
	}
	return types.TypeBytes
}

func (client *Client) HostIDType() types.Type {
	if client.useOldTypes {
		return types.TypeUTF8
	}
	return types.TypeBytes
}

func (client *Client) NamspaceIDDecl() string {
	if client.useOldTypes {
		return "DECLARE $namespace_id AS Utf8;\n"
	}
	return "DECLARE $namespace_id AS Bytes;\n"
}

func (client *Client) RunIDDecl() string {
	if client.useOldTypes {
		return "DECLARE $run_id AS Utf8;\n"
	}
	return "DECLARE $run_id AS Bytes;\n"
}

func (client *Client) CurrentRunIDDecl() string {
	if client.useOldTypes {
		return "DECLARE $current_run_id AS Utf8;\n"
	}
	return "DECLARE $current_run_id AS Bytes;\n"
}

func (client *Client) HostIDDecl() string {
	if client.useOldTypes {
		return "DECLARE $host_id AS Utf8;\n"
	}
	return "DECLARE $host_id AS Bytes;\n"
}

func (client *Client) NamespaceIDValue(v string) types.Value {
	if client.useOldTypes {
		return types.UTF8Value(primitives.MustValidateUUID(v))
	}
	return types.BytesValue(primitives.MustParseUUID(v))
}

func (client *Client) NamespaceIDValueFromUUID(uuid primitives.UUID) types.Value {
	if client.useOldTypes {
		return types.UTF8Value(uuid.String())
	}
	return types.BytesValue(uuid)
}

func (client *Client) EmptyNamespaceIDValue() types.Value {
	if client.useOldTypes {
		return types.UTF8Value("")
	}
	return types.BytesValue(primitives.UUID{})
}

func (client *Client) RunIDValue(v string) types.Value {
	if client.useOldTypes {
		return types.UTF8Value(primitives.MustValidateUUID(v))
	}
	return types.BytesValue(primitives.MustParseUUID(v))
}

func (client *Client) RunIDValueFromUUID(uuid primitives.UUID) types.Value {
	if client.useOldTypes {
		return types.UTF8Value(uuid.String())
	}
	return types.BytesValue(uuid)
}

func (client *Client) EmptyRunIDValue() types.Value {
	if client.useOldTypes {
		return types.UTF8Value("")
	}
	return types.BytesValue(primitives.UUID{})
}

func (client *Client) HistoryIDValue(v string) types.Value {
	if client.useOldTypes {
		return types.UTF8Value(primitives.MustValidateUUID(v))
	}
	return types.BytesValue(primitives.MustParseUUID(v))
}

func (client *Client) HostIDValueFromUUID(uuid []byte) types.Value {
	if client.useOldTypes {
		return types.UTF8Value(primitives.UUIDString(uuid))
	}
	return types.BytesValue(uuid)
}

func (client *Client) EncodingTypeValue(v enums.EncodingType) types.Value {
	if client.useOldTypes {
		return types.UTF8Value(v.String())
	}
	return types.Int16Value(int16(v))
}

func (client *Client) NewEncodingTypeValue(v enums.EncodingType) types.Value {
	return types.Int16Value(int16(v))
}

func (client *Client) UseIntForEncoding() bool {
	return !client.useOldTypes
}

func (client *Client) UseBytesForNamespaceIDs() bool {
	return !client.useOldTypes
}

func (client *Client) UseBytesForRunIDs() bool {
	return !client.useOldTypes
}

func (client *Client) UseBytesForHistoryIDs() bool {
	return !client.useOldTypes
}

func (client *Client) UseBytesForHostIDs() bool {
	return !client.useOldTypes
}
