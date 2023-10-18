package persistence

import (
	"context"
	"fmt"
	"math"

	"github.com/yandex/temporal-over-ydb/xydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

const (
	// NOTE: transaction ID is *= -1 in DB
	MinTxnID int64 = math.MaxInt64
	MaxTxnID int64 = math.MinInt64 + 1 // int overflow
)

type (
	HistoryStore struct {
		Client *xydb.Client
		Logger log.Logger
		p.HistoryBranchUtilImpl
	}
)

func NewHistoryStore(
	client *xydb.Client,
	logger log.Logger,
) *HistoryStore {
	return &HistoryStore{
		Client: client,
		Logger: logger,
	}
}

// AppendHistoryNodes upsert a batch of events as a single node to a history branch
func (h *HistoryStore) AppendHistoryNodes(
	ctx context.Context,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	nodeRows, treeRows := h.prepareAppendHistoryNodesRows(request)
	return upsertHistory(ctx, h.Client, nodeRows, treeRows)
}

func (h *HistoryStore) prepareAppendHistoryNodesRows(
	request *p.InternalAppendHistoryNodesRequest,
) (nodeRow types.Value, treeRow types.Value) {
	treeID := types.UTF8Value(request.BranchInfo.TreeId)
	branchID := types.UTF8Value(request.BranchInfo.BranchId)
	nodeRow = types.StructValue(
		types.StructFieldValue("tree_id", treeID),
		types.StructFieldValue("branch_id", branchID),
		types.StructFieldValue("node_id", types.Int64Value(request.Node.NodeID)),
		types.StructFieldValue("prev_txn_id", types.Int64Value(-request.Node.PrevTransactionID)),
		types.StructFieldValue("txn_id", types.Int64Value(-request.Node.TransactionID)),
		types.StructFieldValue("data", types.BytesValue(request.Node.Events.Data)),
		types.StructFieldValue("data_encoding", types.UTF8Value(request.Node.Events.EncodingType.String())),
	)
	if request.IsNewBranch {
		treeRow = types.StructValue(
			types.StructFieldValue("tree_id", treeID),
			types.StructFieldValue("branch_id", branchID),
			types.StructFieldValue("branch", types.BytesValue(request.TreeInfo.Data)),
			types.StructFieldValue("branch_encoding", types.UTF8Value(request.TreeInfo.EncodingType.String())),
		)
	}
	return nodeRow, treeRow
}

// DeleteHistoryNodes delete a history node
func (h *HistoryStore) DeleteHistoryNodes(
	ctx context.Context,
	request *p.InternalDeleteHistoryNodesRequest,
) (err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("DeleteHistoryNodes", err)
	}()
	branchInfo := request.BranchInfo
	treeID := branchInfo.TreeId
	branchID := branchInfo.BranchId
	nodeID := request.NodeID
	txnID := request.TransactionID

	if nodeID < p.GetBeginNodeID(branchInfo) {
		return xydb.WrapErrorAsRootCause(&p.InvalidPersistenceRequestError{
			Msg: "cannot delete from ancestors' nodes",
		})
	}

	template := h.Client.AddQueryPrefix(`
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;
DECLARE $node_id AS int64;
DECLARE $txn_id AS int64;

DELETE FROM history_node
WHERE tree_id = $tree_id
AND branch_id = $branch_id
AND node_id = $node_id
AND txn_id = $txn_id;
`)
	return h.Client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$tree_id", types.UTF8Value(treeID)),
		table.ValueParam("$branch_id", types.UTF8Value(branchID)),
		table.ValueParam("$node_id", types.Int64Value(nodeID)),
		table.ValueParam("$txn_id", types.Int64Value(-txnID)),
	))
}

// ReadHistoryBranch returns history node data for a branch
func (h *HistoryStore) ReadHistoryBranch(
	ctx context.Context,
	request *p.InternalReadHistoryBranchRequest,
) (resp *p.InternalReadHistoryBranchResponse, err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("ReadHistoryBranch", err)
	}()

	branch, err := h.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	treeID, err := primitives.ValidateUUID(branch.TreeId)
	if err != nil {
		return nil, fmt.Errorf("TreeId UUID cast failed: %w", err)
	}

	branchID, err := primitives.ValidateUUID(request.BranchID)
	if err != nil {
		return nil, fmt.Errorf("BranchId UUID cast failed: %w", err)
	}

	var template string
	if request.MetadataOnly {
		template = `
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;
DECLARE $node_id_gte AS int64;
DECLARE $node_id_lt AS int64;
DECLARE $txn_id_gt AS int64;
DECLARE $page_size AS int32;

SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id
FROM history_node
WHERE tree_id = $tree_id
AND branch_id = $branch_id
-- make sure it's TableRangesScan for YDB:
AND node_id >= $node_id_gte
AND node_id < $node_id_lt
-- refine the condition later:
AND ((node_id = $node_id_gte AND txn_id > $txn_id_gt) OR node_id > $node_id_gte)
ORDER BY tree_id, branch_id, node_id, txn_id
LIMIT $page_size;
`

	} else if request.ReverseOrder {
		template = `
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;
DECLARE $node_id_gte AS int64;
DECLARE $node_id_lt AS int64;
DECLARE $txn_id_lt AS int64;
DECLARE $page_size AS int32;

SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding
FROM history_node
WHERE tree_id = $tree_id
AND branch_id = $branch_id
-- make sure it's TableRangesScan for YDB:
AND node_id >= $node_id_gte
AND node_id < $node_id_lt
-- refine the condition later:
AND ((node_id = $node_id_gte AND txn_id < $txn_id_lt) OR node_id < $node_id_lt)
ORDER BY tree_id, branch_id DESC, node_id DESC, txn_id DESC
LIMIT $page_size;
`
	} else {
		template = `
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;
DECLARE $node_id_gte AS int64;
DECLARE $node_id_lt AS int64;
DECLARE $txn_id_gt AS int64;
DECLARE $page_size AS int32;

SELECT tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding
FROM history_node
WHERE tree_id = $tree_id
AND branch_id = $branch_id
AND node_id >= $node_id_gte
AND node_id < $node_id_lt
AND ((node_id = $node_id_gte AND txn_id > $txn_id_gt) OR node_id > $node_id_gte)
ORDER BY tree_id, branch_id, node_id, txn_id
LIMIT $page_size;
`
	}

	var token historyNodePageToken
	if len(request.NextPageToken) == 0 {
		if request.ReverseOrder {
			token = historyNodePageToken{LastNodeID: request.MaxNodeID, LastTxnID: MaxTxnID}
		} else {
			token = historyNodePageToken{LastNodeID: request.MinNodeID, LastTxnID: MinTxnID}
		}
	} else {
		if err = token.deserialize(request.NextPageToken); err != nil {
			return nil, err
		}
	}

	minNodeID, maxNodeID := request.MinNodeID, request.MaxNodeID
	minTxnID, maxTxnID := MinTxnID, MaxTxnID
	if request.ReverseOrder {
		maxNodeID = token.LastNodeID
		maxTxnID = token.LastTxnID
	} else {
		minNodeID = token.LastNodeID
		minTxnID = token.LastTxnID
	}

	params := table.NewQueryParameters(
		table.ValueParam("$tree_id", types.UTF8Value(treeID)),
		table.ValueParam("$branch_id", types.UTF8Value(branchID)),
		table.ValueParam("$node_id_gte", types.Int64Value(minNodeID)),
		table.ValueParam("$node_id_lt", types.Int64Value(maxNodeID)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
	)
	if request.ReverseOrder {
		params.Add(table.ValueParam("$txn_id_lt", types.Int64Value(-maxTxnID)))
	} else {
		params.Add(table.ValueParam("$txn_id_gt", types.Int64Value(-minTxnID)))
	}

	res, err := h.Client.Do(ctx, h.Client.AddQueryPrefix(template), xydb.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	nodes := make([]p.InternalHistoryNode, 0, request.PageSize)
	for res.NextRow() {
		node, err := scanHistoryNode(res, request.MetadataOnly)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	resp = &p.InternalReadHistoryBranchResponse{
		Nodes: nodes,
	}
	if len(nodes) >= request.PageSize {
		lastRow := nodes[len(nodes)-1]
		nextPageToken := historyNodePageToken{
			LastNodeID: lastRow.NodeID,
			LastTxnID:  lastRow.TransactionID,
		}
		if resp.NextPageToken, err = nextPageToken.serialize(); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// ForkHistoryBranch forks a new branch from an existing branch
func (h *HistoryStore) ForkHistoryBranch(
	ctx context.Context,
	request *p.InternalForkHistoryBranchRequest,
) (err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("ForkHistoryBranch", err)
	}()

	treeIDBytes, err := primitives.ValidateUUID(request.ForkBranchInfo.TreeId)
	if err != nil {
		return fmt.Errorf("TreeId UUID cast failed: %w", err)
	}

	newBranchIDBytes, err := primitives.ValidateUUID(request.NewBranchID)
	if err != nil {
		return fmt.Errorf("NewBranchID UUID cast failed: %w", err)
	}

	template := h.Client.AddQueryPrefix(`
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;
DECLARE $branch AS string;
DECLARE $branch_encoding AS utf8;

UPSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding)
VALUES ($tree_id, $branch_id, $branch, $branch_encoding)
`)
	treeInfoBlob := request.TreeInfo
	return h.Client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$tree_id", types.UTF8Value(treeIDBytes)),
		table.ValueParam("$branch_id", types.UTF8Value(newBranchIDBytes)),
		table.ValueParam("$branch", types.BytesValue(treeInfoBlob.Data)),
		table.ValueParam("$branch_encoding", types.UTF8Value(treeInfoBlob.EncodingType.String())),
	))
}

// DeleteHistoryBranch removes a branch
func (h *HistoryStore) DeleteHistoryBranch(
	ctx context.Context,
	request *p.InternalDeleteHistoryBranchRequest,
) error {
	template := h.Client.AddQueryPrefix(`
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;
DECLARE $input AS List<Struct<
tree_id: utf8,
branch_id: utf8,
node_id_gte: int64
>>;

DELETE FROM history_tree
WHERE tree_id = $tree_id
AND branch_id = $branch_id;

$to_delete = (
SELECT
history_node.tree_id AS tree_id,
history_node.branch_id AS branch_id,
history_node.node_id AS node_id,
history_node.txn_id AS txn_id
FROM AS_TABLE($input) AS input
JOIN history_node
USING (tree_id, branch_id)
WHERE history_node.node_id >= input.node_id_gte
);

DELETE FROM history_node ON SELECT * FROM $to_delete;
`)
	input := make([]types.Value, 0, len(request.BranchRanges))
	for _, br := range request.BranchRanges {
		input = append(input, types.StructValue(
			types.StructFieldValue("tree_id", types.UTF8Value(request.BranchInfo.TreeId)),
			types.StructFieldValue("branch_id", types.UTF8Value(br.BranchId)),
			types.StructFieldValue("node_id_gte", types.Int64Value(br.BeginNodeId)),
		))
	}
	err := h.Client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$tree_id", types.UTF8Value(request.BranchInfo.TreeId)),
		table.ValueParam("$branch_id", types.UTF8Value(request.BranchInfo.BranchId)),
		table.ValueParam("$input", types.ListValue(input...)),
	))
	return xydb.ConvertToTemporalError("DeleteHistoryBranch", err)
}

func (h *HistoryStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *p.GetAllHistoryTreeBranchesRequest,
) (resp *p.InternalGetAllHistoryTreeBranchesResponse, err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("GetAllHistoryTreeBranches", err)
	}()
	template := h.Client.AddQueryPrefix(`
DECLARE $tree_id_gt AS utf8;
DECLARE $branch_id_gt AS utf8;
DECLARE $page_size AS int32;

SELECT tree_id, branch_id, branch, branch_encoding
FROM history_tree
WHERE (tree_id, branch_id) > ($tree_id_gt, $branch_id_gt)
ORDER BY tree_id, branch_id
LIMIT $page_size;
`)

	var token historyTreeBranchPageToken
	if err = token.deserialize(request.NextPageToken); err != nil {
		return nil, err
	}

	res, err := h.Client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$tree_id_gt", types.UTF8Value(token.TreeID)),
		table.ValueParam("$branch_id_gt", types.UTF8Value(token.BranchID)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
	), table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	branches := make([]p.InternalHistoryBranchDetail, 0, request.PageSize)
	for res.NextRow() {
		branch, err := scanInternalHistoryBranchDetail(res)
		if err != nil {
			return nil, err
		}
		branches = append(branches, branch)
	}
	resp = &p.InternalGetAllHistoryTreeBranchesResponse{
		Branches: branches,
	}
	if len(branches) >= request.PageSize {
		// if we filled the page with rows, then set the next page token
		lastBranch := branches[len(branches)-1]
		nextPageToken := historyTreeBranchPageToken{
			TreeID:   lastBranch.TreeID,
			BranchID: lastBranch.BranchID,
		}

		if resp.NextPageToken, err = nextPageToken.serialize(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// GetHistoryTree returns all branch information of a tree
func (h *HistoryStore) GetHistoryTree(
	ctx context.Context,
	request *p.GetHistoryTreeRequest,
) (resp *p.InternalGetHistoryTreeResponse, err error) {
	defer func() {
		err = xydb.ConvertToTemporalError("GetHistoryTree", err)
	}()

	treeID, err := primitives.ValidateUUID(request.TreeID)
	if err != nil {
		return nil, fmt.Errorf("treeId UUID cast failed: %v", err)
	}

	template := h.Client.AddQueryPrefix(`
DECLARE $tree_id AS utf8;

SELECT branch_id, branch, branch_encoding
FROM history_tree
WHERE tree_id = $tree_id;
`)

	res, err := h.Client.Do(ctx, template, xydb.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$tree_id", types.UTF8Value(treeID)),
	), table.WithIdempotent())
	if err != nil {
		return nil, err
	}
	defer func() {
		err2 := res.Close()
		if err == nil {
			err = err2
		}
	}()
	if err = res.NextResultSetErr(ctx); err != nil {
		return nil, err
	}

	pageSize := 100
	treeInfos := make([]*commonpb.DataBlob, 0, pageSize)

	for res.NextRow() {
		treeInfo, err := scanTreeInfo(res)
		if err != nil {
			return nil, err
		}
		treeInfos = append(treeInfos, treeInfo)
	}

	return &p.InternalGetHistoryTreeResponse{
		TreeInfos: treeInfos,
	}, nil
}

func scanHistoryNode(res result.Result, metadataOnly bool) (p.InternalHistoryNode, error) {
	var data []byte
	var encoding string
	var nodeID int64
	var prevTxnID int64
	var txnID int64
	values := []named.Value{
		named.OptionalWithDefault("node_id", &nodeID),
		named.OptionalWithDefault("prev_txn_id", &prevTxnID),
		named.OptionalWithDefault("txn_id", &txnID),
	}
	if !metadataOnly {
		values = append(
			values,
			named.OptionalWithDefault("data", &data),
			named.OptionalWithDefault("data_encoding", &encoding),
		)
	}
	if err := res.ScanNamed(values...); err != nil {
		return p.InternalHistoryNode{}, fmt.Errorf("failed to scan history node: %w", err)
	}
	return p.InternalHistoryNode{
		NodeID:            nodeID,
		PrevTransactionID: -prevTxnID,
		TransactionID:     -txnID,
		Events:            p.NewDataBlob(data, encoding),
	}, nil
}

func scanInternalHistoryBranchDetail(res result.Result) (p.InternalHistoryBranchDetail, error) {
	var treeID string
	var branchID string
	var data []byte
	var encoding string
	if err := res.ScanNamed(
		named.OptionalWithDefault("tree_id", &treeID),
		named.OptionalWithDefault("branch_id", &branchID),
		named.OptionalWithDefault("branch", &data),
		named.OptionalWithDefault("branch_encoding", &encoding),
	); err != nil {
		return p.InternalHistoryBranchDetail{}, fmt.Errorf("failed to scan history branch: %w", err)
	}

	return p.InternalHistoryBranchDetail{
		TreeID:   treeID,
		BranchID: branchID,
		Data:     data,
		Encoding: encoding,
	}, nil
}

func scanTreeInfo(res result.Result) (*commonpb.DataBlob, error) {
	var data []byte
	var encoding string
	if err := res.ScanNamed(
		named.OptionalWithDefault("branch", &data),
		named.OptionalWithDefault("branch_encoding", &encoding),
	); err != nil {
		return nil, fmt.Errorf("failed to scan tree info: %w", err)
	}
	return p.NewDataBlob(data, encoding), nil
}
