package ydb

import (
	"context"
	"fmt"
	"math"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"golang.org/x/sync/errgroup"

	"github.com/yandex/temporal-over-ydb/persistence/pkg/base/tokens"
	"github.com/yandex/temporal-over-ydb/persistence/pkg/ydb/conn"
)

const (
	// NOTE: transaction ID is *= -1 in DB
	MinTxnID int64 = math.MaxInt64
	MaxTxnID int64 = math.MinInt64 + 1 // int overflow
)

type (
	HistoryStore struct {
		Client *conn.Client
		Logger log.Logger
		p.HistoryBranchUtilImpl

		upsertHistoryNodeQ           string
		upsertHistoryNodeAndTreeQ    string
		upsertSingleHistoryNodeQuery string
		upsertSingleHistoryTreeQuery string
	}
)

func NewHistoryStore(
	client *conn.Client,
	logger log.Logger,
) *HistoryStore {
	treeRowDecl := fmt.Sprintf("DECLARE $%s AS %s;", "tree_row_to_add",
		types.Struct(
			types.StructField("tree_id", client.HistoryIDType()),
			types.StructField("branch_id", client.HistoryIDType()),
			types.StructField("branch", types.TypeBytes),
			types.StructField("branch_encoding", client.EncodingType()),
		),
	)

	nodeRowDecl := fmt.Sprintf("DECLARE $%s AS %s;", "node_row_to_add",
		types.Struct(
			types.StructField("tree_id", client.HistoryIDType()),
			types.StructField("branch_id", client.HistoryIDType()),
			types.StructField("node_id", types.TypeInt64),
			types.StructField("txn_id", types.TypeInt64),
			types.StructField("prev_txn_id", types.TypeInt64),
			types.StructField("data", types.TypeBytes),
			types.StructField("data_encoding", client.EncodingType()),
		),
	)

	upsertHistoryNodeQ := nodeRowDecl + `
UPSERT INTO history_node (tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding)
VALUES ($node_row_to_add.tree_id, $node_row_to_add.branch_id, $node_row_to_add.node_id, $node_row_to_add.txn_id, $node_row_to_add.prev_txn_id, $node_row_to_add.data, $node_row_to_add.data_encoding);
`

	upsertHistoryNodeAndTreeQ := treeRowDecl + "\n" + nodeRowDecl + `
UPSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding)
VALUES ($tree_row_to_add.tree_id, $tree_row_to_add.branch_id, $tree_row_to_add.branch, $tree_row_to_add.branch_encoding);

UPSERT INTO history_node (tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding)
VALUES ($node_row_to_add.tree_id, $node_row_to_add.branch_id, $node_row_to_add.node_id, $node_row_to_add.txn_id, $node_row_to_add.prev_txn_id, $node_row_to_add.data, $node_row_to_add.data_encoding);
`

	upsertSingleHistoryNodeQuery := nodeRowDecl + `
UPSERT INTO history_node (tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding)
VALUES ($node_row_to_add.tree_id, $node_row_to_add.branch_id, $node_row_to_add.node_id, $node_row_to_add.txn_id, $node_row_to_add.prev_txn_id, $node_row_to_add.data, $node_row_to_add.data_encoding);
`

	upsertSingleHistoryTreeQuery := treeRowDecl + `
UPSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding)
VALUES ($tree_row_to_add.tree_id, $tree_row_to_add.branch_id, $tree_row_to_add.branch, $tree_row_to_add.branch_encoding);
`

	return &HistoryStore{
		Client:                       client,
		Logger:                       logger,
		upsertHistoryNodeQ:           upsertHistoryNodeQ,
		upsertHistoryNodeAndTreeQ:    upsertHistoryNodeAndTreeQ,
		upsertSingleHistoryNodeQuery: upsertSingleHistoryNodeQuery,
		upsertSingleHistoryTreeQuery: upsertSingleHistoryTreeQuery,
	}
}

func (h *HistoryStore) upsertHistory(ctx context.Context, client *conn.Client, nodeRow types.Value, treeRow types.Value) error {
	var err error
	if treeRow != nil {
		err = client.Write(ctx, client.AddQueryPrefix(h.upsertHistoryNodeAndTreeQ), table.NewQueryParameters(
			table.ValueParam("$tree_row_to_add", treeRow),
			table.ValueParam("$node_row_to_add", nodeRow),
		))
	} else {
		err = client.Write(ctx, client.AddQueryPrefix(h.upsertHistoryNodeQ), table.NewQueryParameters(
			table.ValueParam("$node_row_to_add", nodeRow),
		))
	}
	return conn.ConvertToTemporalError("AppendHistoryNodes", err)
}

// AppendHistoryNodes upsert a batch of events as a single node to a history branch
func (h *HistoryStore) AppendHistoryNodes(
	ctx context.Context,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	nodeRows, treeRows := h.prepareHistoryRows(request)
	err := h.upsertHistory(ctx, h.Client, nodeRows, treeRows)
	if err != nil {
		return conn.ConvertToTemporalError("AppendHistoryNodes", err)
	}
	return nil
}

func (h *HistoryStore) AppendHistoryNodesForCreateWorkflowExecutionRequest(
	ctx context.Context,
	request *p.InternalCreateWorkflowExecutionRequest,
) (nodeCount int, treeCount int, err error) {
	n := len(request.NewWorkflowNewEvents)
	if n == 0 {
		return 0, 0, nil
	}
	nodes := make([]types.Value, 0, n)
	trees := make([]types.Value, 0, n)
	for _, req := range request.NewWorkflowNewEvents {
		n, t := h.prepareHistoryRows(req)
		nodes = append(nodes, n)
		if t != nil {
			trees = append(trees, t)
		}
	}
	err = h.upsertHistoryConcurrently(ctx, nodes, trees)
	if err != nil {
		return 0, 0, err
	}
	return len(nodes), len(trees), nil
}

func (h *HistoryStore) AppendHistoryNodesForUpdateWorkflowExecutionRequest(
	ctx context.Context,
	request *p.InternalUpdateWorkflowExecutionRequest,
) (nodeCount int, treeCount int, err error) {
	n := len(request.NewWorkflowNewEvents) + len(request.UpdateWorkflowNewEvents)
	if n == 0 {
		return 0, 0, nil
	}
	nodes := make([]types.Value, 0, n)
	trees := make([]types.Value, 0, n)
	for _, req := range request.NewWorkflowNewEvents {
		n, t := h.prepareHistoryRows(req)
		nodes = append(nodes, n)
		if t != nil {
			trees = append(trees, t)
		}
	}
	for _, req := range request.UpdateWorkflowNewEvents {
		n, t := h.prepareHistoryRows(req)
		nodes = append(nodes, n)
		if t != nil {
			trees = append(trees, t)
		}
	}
	err = h.upsertHistoryConcurrently(ctx, nodes, trees)
	if err != nil {
		return 0, 0, err
	}
	return len(nodes), len(trees), nil
}

func (h *HistoryStore) AppendHistoryNodesForConflictResolveWorkflowExecutionRequest(
	ctx context.Context,
	request *p.InternalConflictResolveWorkflowExecutionRequest,
) (nodeCount int, treeCount int, err error) {
	n := len(request.CurrentWorkflowEventsNewEvents) + len(request.ResetWorkflowEventsNewEvents) + len(request.NewWorkflowEventsNewEvents)
	if n == 0 {
		return 0, 0, nil
	}
	nodes := make([]types.Value, 0, n)
	trees := make([]types.Value, 0, n)
	for _, req := range request.CurrentWorkflowEventsNewEvents {
		n, t := h.prepareHistoryRows(req)
		nodes = append(nodes, n)
		if t != nil {
			trees = append(trees, t)
		}
	}
	for _, req := range request.ResetWorkflowEventsNewEvents {
		n, t := h.prepareHistoryRows(req)
		nodes = append(nodes, n)
		if t != nil {
			trees = append(trees, t)
		}
	}
	for _, req := range request.NewWorkflowEventsNewEvents {
		n, t := h.prepareHistoryRows(req)
		nodes = append(nodes, n)
		if t != nil {
			trees = append(trees, t)
		}
	}
	err = h.upsertHistoryConcurrently(ctx, nodes, trees)
	if err != nil {
		return 0, 0, err
	}
	return len(nodes), len(trees), nil
}

func (h *HistoryStore) upsertHistoryConcurrently(ctx context.Context, nodes []types.Value, trees []types.Value) error {
	treeQuery := h.Client.AddQueryPrefix(h.upsertSingleHistoryTreeQuery)
	nodeQuery := h.Client.AddQueryPrefix(h.upsertSingleHistoryNodeQuery)
	eg, ctx := errgroup.WithContext(ctx)
	for _, treeRow := range trees {
		eg.Go(func() error {
			return h.Client.Write(ctx, treeQuery, table.NewQueryParameters(
				table.ValueParam("$tree_row_to_add", treeRow),
			))
		})
	}
	for _, nodeRow := range nodes {
		eg.Go(func() error {
			return h.Client.Write(ctx, nodeQuery, table.NewQueryParameters(
				table.ValueParam("$node_row_to_add", nodeRow),
			))
		})
	}
	return eg.Wait()
}

func (h *HistoryStore) prepareHistoryRows(
	request *p.InternalAppendHistoryNodesRequest,
) (nodeRow types.Value, treeRow types.Value) {
	nodeRow = types.StructValue(
		types.StructFieldValue("tree_id", h.Client.HistoryIDValue(request.BranchInfo.TreeId)),
		types.StructFieldValue("branch_id", h.Client.HistoryIDValue(request.BranchInfo.BranchId)),
		types.StructFieldValue("node_id", types.Int64Value(request.Node.NodeID)),
		types.StructFieldValue("prev_txn_id", types.Int64Value(-request.Node.PrevTransactionID)),
		types.StructFieldValue("txn_id", types.Int64Value(-request.Node.TransactionID)),
		types.StructFieldValue("data", types.BytesValue(request.Node.Events.Data)),
		types.StructFieldValue("data_encoding", h.Client.EncodingTypeValue(request.Node.Events.EncodingType)),
	)
	if request.IsNewBranch {
		treeRow = types.StructValue(
			types.StructFieldValue("tree_id", h.Client.HistoryIDValue(request.BranchInfo.TreeId)),
			types.StructFieldValue("branch_id", h.Client.HistoryIDValue(request.BranchInfo.BranchId)),
			types.StructFieldValue("branch", types.BytesValue(request.TreeInfo.Data)),
			types.StructFieldValue("branch_encoding", h.Client.EncodingTypeValue(request.TreeInfo.EncodingType)),
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
		err = conn.ConvertToTemporalError("DeleteHistoryNodes", err)
	}()
	branchInfo := request.BranchInfo
	nodeID := request.NodeID
	txnID := request.TransactionID

	if nodeID < p.GetBeginNodeID(branchInfo) {
		return conn.WrapErrorAsRootCause(&p.InvalidPersistenceRequestError{
			Msg: "cannot delete from ancestors' nodes",
		})
	}

	var decl string
	if h.Client.UseBytesForHistoryIDs() {
		decl = `
DECLARE $tree_id AS string;
DECLARE $branch_id AS string;`
	} else {
		decl = `
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;`
	}

	template := h.Client.AddQueryPrefix(decl + `
DECLARE $node_id AS int64;
DECLARE $txn_id AS int64;

DELETE FROM history_node
WHERE tree_id = $tree_id
AND branch_id = $branch_id
AND node_id = $node_id
AND txn_id = $txn_id;
`)
	return h.Client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$tree_id", h.Client.HistoryIDValue(branchInfo.TreeId)),
		table.ValueParam("$branch_id", h.Client.HistoryIDValue(branchInfo.BranchId)),
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
		err = conn.ConvertToTemporalError("ReadHistoryBranch", err)
	}()

	branch, err := h.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	var decl string
	if h.Client.UseBytesForHistoryIDs() {
		decl = `
DECLARE $tree_id AS string;
DECLARE $branch_id AS string;`
	} else {
		decl = `
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;`
	}

	var template string
	if request.MetadataOnly {
		template = decl + `
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
		template = decl + `
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
		template = decl + `
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

	var token tokens.HistoryNodePageToken
	if len(request.NextPageToken) == 0 {
		if request.ReverseOrder {
			token = tokens.HistoryNodePageToken{LastNodeID: request.MaxNodeID, LastTxnID: MaxTxnID}
		} else {
			token = tokens.HistoryNodePageToken{LastNodeID: request.MinNodeID, LastTxnID: MinTxnID}
		}
	} else {
		if err = token.Deserialize(request.NextPageToken); err != nil {
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
		table.ValueParam("$tree_id", h.Client.HistoryIDValue(branch.TreeId)),
		table.ValueParam("$branch_id", h.Client.HistoryIDValue(request.BranchID)),
		table.ValueParam("$node_id_gte", types.Int64Value(minNodeID)),
		table.ValueParam("$node_id_lt", types.Int64Value(maxNodeID)),
		table.ValueParam("$page_size", types.Int32Value(int32(request.PageSize))),
	)
	if request.ReverseOrder {
		params.Add(table.ValueParam("$txn_id_lt", types.Int64Value(-maxTxnID)))
	} else {
		params.Add(table.ValueParam("$txn_id_gt", types.Int64Value(-minTxnID)))
	}

	res, err := h.Client.Do(ctx, h.Client.AddQueryPrefix(template), conn.OnlineReadOnlyTxControl(), params, table.WithIdempotent())
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
		node, err := h.scanHistoryNode(res, request.MetadataOnly)
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
		nextPageToken := tokens.HistoryNodePageToken{
			LastNodeID: lastRow.NodeID,
			LastTxnID:  lastRow.TransactionID,
		}
		if resp.NextPageToken, err = nextPageToken.Serialize(); err != nil {
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
		err = conn.ConvertToTemporalError("ForkHistoryBranch", err)
	}()

	_, err = primitives.ValidateUUID(request.ForkBranchInfo.TreeId)
	if err != nil {
		return fmt.Errorf("TreeId UUID cast failed: %w", err)
	}

	_, err = primitives.ValidateUUID(request.NewBranchID)
	if err != nil {
		return fmt.Errorf("NewBranchID UUID cast failed: %w", err)
	}

	var decl string
	if h.Client.UseBytesForHistoryIDs() {
		decl = `
DECLARE $tree_id AS string;
DECLARE $branch_id AS string;`
	} else {
		decl = `
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;`
	}

	template := h.Client.AddQueryPrefix(decl + `
DECLARE $branch AS string;
DECLARE $branch_encoding AS ` + h.Client.EncodingType().String() + `;

UPSERT INTO history_tree (tree_id, branch_id, branch, branch_encoding)
VALUES ($tree_id, $branch_id, $branch, $branch_encoding)
`)
	treeInfoBlob := request.TreeInfo
	return h.Client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$tree_id", h.Client.HistoryIDValue(request.ForkBranchInfo.TreeId)),
		table.ValueParam("$branch_id", h.Client.HistoryIDValue(request.NewBranchID)),
		table.ValueParam("$branch", types.BytesValue(treeInfoBlob.Data)),
		table.ValueParam("$branch_encoding", h.Client.EncodingTypeValue(treeInfoBlob.EncodingType)),
	))
}

// DeleteHistoryBranch removes a branch
func (h *HistoryStore) DeleteHistoryBranch(
	ctx context.Context,
	request *p.InternalDeleteHistoryBranchRequest,
) error {
	var decl string
	if h.Client.UseBytesForHistoryIDs() {
		decl = `
DECLARE $tree_id AS string;
DECLARE $branch_id AS string;
DECLARE $input AS List<Struct<
tree_id: string,
branch_id: string,
node_id_gte: int64
>>;`
	} else {
		decl = `
DECLARE $tree_id AS utf8;
DECLARE $branch_id AS utf8;
DECLARE $input AS List<Struct<
tree_id: utf8,
branch_id: utf8,
node_id_gte: int64
>>;`
	}

	template := h.Client.AddQueryPrefix(decl + `
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
			types.StructFieldValue("tree_id", h.Client.HistoryIDValue(request.BranchInfo.TreeId)),
			types.StructFieldValue("branch_id", h.Client.HistoryIDValue(br.BranchId)),
			types.StructFieldValue("node_id_gte", types.Int64Value(br.BeginNodeId)),
		))
	}
	err := h.Client.Write(ctx, template, table.NewQueryParameters(
		table.ValueParam("$tree_id", h.Client.HistoryIDValue(request.BranchInfo.TreeId)),
		table.ValueParam("$branch_id", h.Client.HistoryIDValue(request.BranchInfo.BranchId)),
		table.ValueParam("$input", types.ListValue(input...)),
	))
	return conn.ConvertToTemporalError("DeleteHistoryBranch", err)
}

func (h *HistoryStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *p.GetAllHistoryTreeBranchesRequest,
) (resp *p.InternalGetAllHistoryTreeBranchesResponse, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("GetAllHistoryTreeBranches", err)
	}()

	var token tokens.HistoryTreeBranchPageToken
	if err = token.Deserialize(request.NextPageToken); err != nil {
		return nil, err
	}

	var decl string
	if h.Client.UseBytesForHistoryIDs() {
		decl = `
DECLARE $tree_id_gt AS string;
DECLARE $branch_id_gt AS string;`
	} else {
		decl = `
DECLARE $tree_id_gt AS utf8;
DECLARE $branch_id_gt AS utf8;`
	}

	template := h.Client.AddQueryPrefix(decl + `
DECLARE $page_size AS int32;

SELECT tree_id, branch_id, branch, branch_encoding
FROM history_tree
WHERE (tree_id, branch_id) > ($tree_id_gt, $branch_id_gt)
ORDER BY tree_id, branch_id
LIMIT $page_size;
`)

	res, err := h.Client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$tree_id_gt", h.Client.HistoryIDValue(token.TreeID)),
		table.ValueParam("$branch_id_gt", h.Client.HistoryIDValue(token.BranchID)),
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
		branch, err := h.scanInternalHistoryBranchDetail(res)
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
		nextPageToken := tokens.HistoryTreeBranchPageToken{
			TreeID:   lastBranch.TreeID,
			BranchID: lastBranch.BranchID,
		}

		if resp.NextPageToken, err = nextPageToken.Serialize(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// GetHistoryTree returns all branch information of a tree
func (h *HistoryStore) GetHistoryTreeContainingBranch(
	ctx context.Context,
	request *p.InternalGetHistoryTreeContainingBranchRequest,
) (resp *p.InternalGetHistoryTreeContainingBranchResponse, err error) {
	defer func() {
		err = conn.ConvertToTemporalError("GetHistoryTreeContainingBranch", err)
	}()

	branch, err := h.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	treeID, err := primitives.ValidateUUID(branch.TreeId)
	if err != nil {
		return nil, fmt.Errorf("treeId UUID cast failed: %v", err)
	}

	var decl string
	if h.Client.UseBytesForHistoryIDs() {
		decl = `DECLARE $tree_id AS string;`
	} else {
		decl = `DECLARE $tree_id AS utf8;`
	}

	template := h.Client.AddQueryPrefix(decl + `
SELECT branch_id, branch, branch_encoding
FROM history_tree
WHERE tree_id = $tree_id;
`)

	res, err := h.Client.Do(ctx, template, conn.OnlineReadOnlyTxControl(), table.NewQueryParameters(
		table.ValueParam("$tree_id", h.Client.HistoryIDValue(treeID)),
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
		treeInfo, err := h.scanTreeInfo(res)
		if err != nil {
			return nil, err
		}
		treeInfos = append(treeInfos, treeInfo)
	}

	return &p.InternalGetHistoryTreeContainingBranchResponse{
		TreeInfos: treeInfos,
	}, nil
}

func (h *HistoryStore) scanHistoryNode(res result.Result, metadataOnly bool) (p.InternalHistoryNode, error) {
	var data []byte
	var encoding string
	var encodingType conn.EncodingTypeRaw
	var nodeID int64
	var prevTxnID int64
	var txnID int64
	values := []named.Value{
		named.OptionalWithDefault("node_id", &nodeID),
		named.OptionalWithDefault("prev_txn_id", &prevTxnID),
		named.OptionalWithDefault("txn_id", &txnID),
	}
	if !metadataOnly {
		var encodingScanner named.Value
		if h.Client.UseIntForEncoding() {
			encodingScanner = named.OptionalWithDefault("data_encoding", &encodingType)
		} else {
			encodingScanner = named.OptionalWithDefault("data_encoding", &encoding)
		}
		values = append(
			values,
			named.OptionalWithDefault("data", &data),
			encodingScanner,
		)

	}
	if err := res.ScanNamed(values...); err != nil {
		return p.InternalHistoryNode{}, fmt.Errorf("failed to scan history node: %w", err)
	}
	if h.Client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}
	return p.InternalHistoryNode{
		NodeID:            nodeID,
		PrevTransactionID: -prevTxnID,
		TransactionID:     -txnID,
		Events:            p.NewDataBlob(data, encoding),
	}, nil
}

func (h *HistoryStore) scanInternalHistoryBranchDetail(res result.Result) (p.InternalHistoryBranchDetail, error) {
	if h.Client.UseBytesForHistoryIDs() {
		var treeID primitives.UUID
		var branchID primitives.UUID
		var data []byte

		var encoding string
		var encodingType conn.EncodingTypeRaw
		var encodingScanner named.Value
		if h.Client.UseIntForEncoding() {
			encodingScanner = named.OptionalWithDefault("branch_encoding", &encodingType)
		} else {
			encodingScanner = named.OptionalWithDefault("branch_encoding", &encoding)
		}

		if err := res.ScanNamed(
			named.OptionalWithDefault("tree_id", &treeID),
			named.OptionalWithDefault("branch_id", &branchID),
			named.OptionalWithDefault("branch", &data),
			encodingScanner,
		); err != nil {
			return p.InternalHistoryBranchDetail{}, fmt.Errorf("failed to scan history branch: %w", err)
		}

		if h.Client.UseIntForEncoding() {
			encoding = enumspb.EncodingType(encodingType).String()
		}

		return p.InternalHistoryBranchDetail{
			TreeID:   treeID.String(),
			BranchID: branchID.String(),
			Data:     data,
			Encoding: encoding,
		}, nil
	} else {
		var treeID string
		var branchID string
		var data []byte

		var encoding string
		var encodingType conn.EncodingTypeRaw
		var encodingScanner named.Value
		if h.Client.UseIntForEncoding() {
			encodingScanner = named.OptionalWithDefault("branch_encoding", &encodingType)
		} else {
			encodingScanner = named.OptionalWithDefault("branch_encoding", &encoding)
		}

		if err := res.ScanNamed(
			named.OptionalWithDefault("tree_id", &treeID),
			named.OptionalWithDefault("branch_id", &branchID),
			named.OptionalWithDefault("branch", &data),
			encodingScanner,
		); err != nil {
			return p.InternalHistoryBranchDetail{}, fmt.Errorf("failed to scan history branch: %w", err)
		}

		if h.Client.UseIntForEncoding() {
			encoding = enumspb.EncodingType(encodingType).String()
		}

		return p.InternalHistoryBranchDetail{
			TreeID:   treeID,
			BranchID: branchID,
			Data:     data,
			Encoding: encoding,
		}, nil
	}
}

func (h *HistoryStore) scanTreeInfo(res result.Result) (*commonpb.DataBlob, error) {
	var data []byte

	var encoding string
	var encodingType conn.EncodingTypeRaw
	var encodingScanner named.Value
	if h.Client.UseIntForEncoding() {
		encodingScanner = named.OptionalWithDefault("branch_encoding", &encodingType)
	} else {
		encodingScanner = named.OptionalWithDefault("branch_encoding", &encoding)
	}
	if err := res.ScanNamed(
		named.OptionalWithDefault("branch", &data),
		encodingScanner,
	); err != nil {
		return nil, fmt.Errorf("failed to scan tree info: %w", err)
	}

	if h.Client.UseIntForEncoding() {
		encoding = enumspb.EncodingType(encodingType).String()
	}

	return p.NewDataBlob(data, encoding), nil
}
