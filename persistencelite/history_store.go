package persistencelite

import (
	"context"
	"math"

	"github.com/rqlite/gorqlite"
	"github.com/yandex/temporal-over-ydb/persistencelite/conn"
	errorslite "github.com/yandex/temporal-over-ydb/persistencelite/errors"
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
		rqliteFactory conn.RqliteFactory
		shardCount    int32
		Logger        log.Logger
		p.HistoryBranchUtilImpl
	}
)

func NewHistoryStore(
	rqliteFactory conn.RqliteFactory,
	shardCount int32,
	logger log.Logger,
) *HistoryStore {
	return &HistoryStore{
		rqliteFactory: rqliteFactory,
		shardCount:    shardCount,
		Logger:        logger,
	}
}

// AppendHistoryNodes upsert a batch of events as a single node to a history branch
func (h *HistoryStore) AppendHistoryNodes(
	ctx context.Context,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	rql := h.rqliteFactory.GetClient(request.ShardID)
	_, err := rql.WriteParameterizedContext(ctx, h.prepareAppendHistoryStmts(request))
	if err != nil {
		return errorslite.NewInternalF("failed to append history nodes: %v", err)
	}
	return nil
}

type nodeRow struct {
	ShardID      int32
	TreeID       string
	BranchID     string
	NodeID       int64
	PrevTxnID    int64
	TxnID        int64
	Data         []byte
	DataEncoding string
}

func (r *nodeRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT INTO history_node (shard_id, tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding)
			SELECT * FROM (VALUES 
				(?, ?, ?, ?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0
		`,
		Arguments: []interface{}{r.ShardID, r.TreeID, r.BranchID, r.NodeID, r.TxnID, r.PrevTxnID, r.Data, r.DataEncoding, conditionID},
	}
}

func (r *nodeRow) ToUnconditionalWriteQuery() gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT INTO history_node (shard_id, tree_id, branch_id, node_id, txn_id, prev_txn_id, data, data_encoding)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`,
		Arguments: []interface{}{r.ShardID, r.TreeID, r.BranchID, r.NodeID, r.TxnID, r.PrevTxnID, r.Data, r.DataEncoding},
	}
}

type treeRow struct {
	ShardID        int32
	TreeID         string
	BranchID       string
	Branch         []byte
	BranchEncoding string
}

func (r *treeRow) ToWriteQuery(conditionID string) gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT INTO history_tree (shard_id, tree_id, branch_id, data, data_encoding)
			SELECT * FROM (VALUES 
				(?, ?, ?, ?, ?)
			) WHERE (SELECT COUNT(*) FROM conditions WHERE uuid = ? AND valid = false) = 0;
		`,
		Arguments: []interface{}{r.ShardID, r.TreeID, r.BranchID, r.Branch, r.BranchEncoding, conditionID},
	}
}

func (r *treeRow) ToUnconditionalWriteQuery() gorqlite.ParameterizedStatement {
	return gorqlite.ParameterizedStatement{
		Query: `
			INSERT INTO history_tree (shard_id, tree_id, branch_id, data, data_encoding)
			VALUES (?, ?, ?, ?, ?)
		`,
		Arguments: []interface{}{r.ShardID, r.TreeID, r.BranchID, r.Branch, r.BranchEncoding},
	}
}

func getTreeRow(
	request *p.InternalAppendHistoryNodesRequest,
) *treeRow {
	shardID := request.ShardID
	treeID := request.BranchInfo.TreeId
	branchID := request.BranchInfo.BranchId
	if request.IsNewBranch {
		return &treeRow{
			ShardID:        shardID,
			TreeID:         treeID,
			BranchID:       branchID,
			Branch:         request.TreeInfo.Data,
			BranchEncoding: request.TreeInfo.EncodingType.String(),
		}
	} else {
		return nil
	}
}

func getNodeRow(
	request *p.InternalAppendHistoryNodesRequest,
) *nodeRow {
	shardID := request.ShardID
	treeID := request.BranchInfo.TreeId
	branchID := request.BranchInfo.BranchId
	return &nodeRow{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		NodeID:       request.Node.NodeID,
		PrevTxnID:    -request.Node.PrevTransactionID,
		TxnID:        -request.Node.TransactionID,
		Data:         request.Node.Events.Data,
		DataEncoding: request.Node.Events.EncodingType.String(),
	}
}

func (h *HistoryStore) prepareAppendHistoryStmts(
	request *p.InternalAppendHistoryNodesRequest,
) []gorqlite.ParameterizedStatement {
	shardID := request.ShardID
	treeID := request.BranchInfo.TreeId
	branchID := request.BranchInfo.BranchId
	nr := &nodeRow{
		ShardID:      shardID,
		TreeID:       treeID,
		BranchID:     branchID,
		NodeID:       request.Node.NodeID,
		PrevTxnID:    -request.Node.PrevTransactionID,
		TxnID:        -request.Node.TransactionID,
		Data:         request.Node.Events.Data,
		DataEncoding: request.Node.Events.EncodingType.String(),
	}
	stmts := []gorqlite.ParameterizedStatement{
		nr.ToUnconditionalWriteQuery(),
	}
	if request.IsNewBranch {
		tr := &treeRow{
			ShardID:        shardID,
			TreeID:         treeID,
			BranchID:       branchID,
			Branch:         request.TreeInfo.Data,
			BranchEncoding: request.TreeInfo.EncodingType.String(),
		}
		stmts = append(stmts, tr.ToUnconditionalWriteQuery())
	}
	return stmts
}

// DeleteHistoryNodes delete a history node
func (h *HistoryStore) DeleteHistoryNodes(
	ctx context.Context,
	request *p.InternalDeleteHistoryNodesRequest,
) error {
	shardID := request.ShardID
	branchInfo := request.BranchInfo
	treeID := branchInfo.TreeId
	branchID := branchInfo.BranchId
	nodeID := request.NodeID
	txnID := request.TransactionID

	if nodeID < p.GetBeginNodeID(branchInfo) {
		return &p.InvalidPersistenceRequestError{Msg: "cannot delete from ancestors' nodes"}
	}

	rql := h.rqliteFactory.GetClient(shardID)
	_, err := rql.WriteParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     `DELETE FROM history_node WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ?`,
			Arguments: []interface{}{shardID, treeID, branchID, nodeID, -txnID},
		},
	})
	if err != nil {
		return errorslite.NewInternalF("failed to delete history node: %v", err)
	}
	return nil
}

// ReadHistoryBranch returns history node data for a branch
func (h *HistoryStore) ReadHistoryBranch(
	ctx context.Context,
	request *p.InternalReadHistoryBranchRequest,
) (resp *p.InternalReadHistoryBranchResponse, err error) {
	branch, err := h.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, errorslite.NewInvalidArgumentF("failed to parse BranchToken: %v", err)
	}

	treeID, err := primitives.ValidateUUID(branch.TreeId)
	if err != nil {
		return nil, errorslite.NewInvalidArgumentF("invalid TreeId: %v", err)
	}

	branchID, err := primitives.ValidateUUID(request.BranchID)
	if err != nil {
		return nil, errorslite.NewInvalidArgumentF("invalid BranchID: %v", err)
	}

	shardID := request.ShardID
	var token historyNodePageToken
	if len(request.NextPageToken) == 0 {
		if request.ReverseOrder {
			token = historyNodePageToken{LastNodeID: request.MaxNodeID, LastTxnID: MaxTxnID}
		} else {
			token = historyNodePageToken{LastNodeID: request.MinNodeID, LastTxnID: MinTxnID}
		}
	} else {
		if err = token.deserialize(request.NextPageToken); err != nil {
			return nil, errorslite.NewInvalidArgumentF("failed to deserialize NextPageToken: %v", err)
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

	var template string
	var args []interface{}
	if request.MetadataOnly {
		template = `
SELECT node_id, prev_txn_id, txn_id
FROM history_node
WHERE shard_id = ? 
AND tree_id = ?
AND branch_id = ?
AND ((node_id = ? AND txn_id > ?) OR node_id > ?) AND node_id < ?
ORDER BY tree_id, branch_id, node_id, txn_id
LIMIT ?
`
		args = []interface{}{shardID, treeID, branchID, minNodeID, -minTxnID, minNodeID, maxNodeID, request.PageSize}
	} else if request.ReverseOrder {
		template = `
SELECT node_id, prev_txn_id, txn_id, data, data_encoding
FROM history_node
WHERE shard_id = ? 
AND tree_id = ?
AND branch_id = ?
AND node_id >= ? AND ((node_id = ? AND txn_id < ?) OR node_id < ?)
ORDER BY tree_id, branch_id DESC, node_id DESC, txn_id DESC
LIMIT ?
`
		args = []interface{}{shardID, treeID, branchID, minNodeID, maxNodeID, -maxTxnID, maxNodeID, request.PageSize}
	} else {
		template = `
SELECT node_id, prev_txn_id, txn_id, data, data_encoding
FROM history_node
WHERE shard_id = ? 
AND tree_id = ?
AND branch_id = ?
AND ((node_id = ? AND txn_id > ?) OR node_id > ?) AND node_id < ?
ORDER BY tree_id, branch_id, node_id, txn_id
LIMIT ?
`
		args = []interface{}{shardID, treeID, branchID, minNodeID, -minTxnID, minNodeID, maxNodeID, request.PageSize}
	}

	rql := h.rqliteFactory.GetClient(shardID)
	rs, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query:     template,
			Arguments: args,
		},
	})
	if err != nil {
		return nil, err
	}
	res := rs[0]

	nodes := make([]p.InternalHistoryNode, 0, request.PageSize)
	for res.Next() {
		node, err := scanHistoryNode(res, request.MetadataOnly)
		if err != nil {
			return nil, errorslite.NewInternalF("failed to scan history node: %v", err)
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
			return nil, errorslite.NewInternalF("failed to serialize NextPageToken: %v", err)
		}
	}

	return resp, nil
}

// ForkHistoryBranch forks a new branch from an existing branch
func (h *HistoryStore) ForkHistoryBranch(
	ctx context.Context,
	request *p.InternalForkHistoryBranchRequest,
) (err error) {
	treeIDBytes, err := primitives.ValidateUUID(request.ForkBranchInfo.TreeId)
	if err != nil {
		return errorslite.NewInvalidArgumentF("invalid ForkBranchInfo.TreeID: %v", err)
	}

	newBranchIDBytes, err := primitives.ValidateUUID(request.NewBranchID)
	if err != nil {
		return errorslite.NewInvalidArgumentF("invalid NewBranchID: %v", err)
	}

	shardID := request.ShardID
	treeInfoBlob := request.TreeInfo
	tr := &treeRow{
		ShardID:        shardID,
		TreeID:         treeIDBytes,
		BranchID:       newBranchIDBytes,
		Branch:         treeInfoBlob.Data,
		BranchEncoding: treeInfoBlob.EncodingType.String(),
	}
	rql := h.rqliteFactory.GetClient(shardID)
	_, err = rql.WriteParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		tr.ToUnconditionalWriteQuery(),
	})
	if err != nil {
		return errorslite.NewInternalF("failed to fork history branch: %v", err)
	}
	return nil
}

// DeleteHistoryBranch removes a branch
func (h *HistoryStore) DeleteHistoryBranch(
	ctx context.Context,
	request *p.InternalDeleteHistoryBranchRequest,
) error {
	branchIDBytes, err := primitives.ValidateUUID(request.BranchInfo.BranchId)
	if err != nil {
		return errorslite.NewInvalidArgumentF("invalid BranchInfo.BranchId: %v", err)
	}

	treeIDBytes, err := primitives.ValidateUUID(request.BranchInfo.TreeId)
	if err != nil {
		return errorslite.NewInvalidArgumentF("invalid BranchInfo.TreeId: %v", err)
	}

	shardID := request.ShardID
	stmts := make([]gorqlite.ParameterizedStatement, 0, len(request.BranchRanges)+1)
	stmts = append(stmts, gorqlite.ParameterizedStatement{
		Query:     `DELETE FROM history_tree WHERE shard_id = ? AND tree_id = ? AND branch_id = ?`,
		Arguments: []interface{}{shardID, treeIDBytes, branchIDBytes},
	})
	for _, br := range request.BranchRanges {
		branchIDBytes, err := primitives.ValidateUUID(br.BranchId)
		if err != nil {
			return errorslite.NewInvalidArgumentF("invalid BranchId: %v", err)
		}
		stmts = append(stmts, gorqlite.ParameterizedStatement{
			Query:     `DELETE FROM history_node WHERE shard_id = ? AND tree_id = ? AND branch_id = ? AND node_id >= ?`,
			Arguments: []interface{}{shardID, treeIDBytes, branchIDBytes, br.BeginNodeId},
		})
	}

	rql := h.rqliteFactory.GetClient(shardID)
	_, err = rql.WriteParameterizedContext(ctx, stmts)
	if err != nil {
		return errorslite.NewInternalF("failed to delete history branch: %v", err)
	}
	return nil
}

func (h *HistoryStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *p.GetAllHistoryTreeBranchesRequest,
) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {

	pageSize := request.PageSize
	if pageSize <= 0 {
		return nil, errorslite.NewInvalidArgumentF("PageSize must be greater than 0, but was %d", pageSize)
	}

	var token historyTreeBranchPageToken
	if err := token.deserialize(request.NextPageToken); err != nil {
		return nil, errorslite.NewInvalidArgumentF("failed to deserialize NextPageToken: %v", err)
	}

	shardID := token.ShardID
	if shardID > h.shardCount {
		return &p.InternalGetAllHistoryTreeBranchesResponse{}, nil
	}

	rql := h.rqliteFactory.GetClient(shardID)
	rs, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query: `
				SELECT shard_id, tree_id, branch_id, data, data_encoding
		        FROM history_tree
		        WHERE (shard_id, tree_id, branch_id) > (?, ?, ?)
		        ORDER BY shard_id, tree_id, branch_id
		        LIMIT ?
		`,
			Arguments: []interface{}{shardID, token.TreeID, token.BranchID, request.PageSize},
		},
	})
	if err != nil {
		return nil, errorslite.NewInternalF("failed to select: %v", err)
	}

	res := rs[0]
	branches := make([]p.InternalHistoryBranchDetail, 0, pageSize)
	var lastBranchShardID = shardID
	for res.Next() {
		branchShardID, branch, err := scanInternalHistoryBranchDetail(res)
		if err != nil {
			return nil, errorslite.NewInternalF("failed to scan history branch: %v", err)
		}
		lastBranchShardID = branchShardID
		branches = append(branches, branch)
	}

	resp := &p.InternalGetAllHistoryTreeBranchesResponse{
		Branches: branches,
	}
	if len(branches) >= request.PageSize {
		// if we filled the page with rows, then set the next page token
		lastBranch := branches[len(branches)-1]
		nextPageToken := historyTreeBranchPageToken{
			ShardID:  lastBranchShardID,
			TreeID:   lastBranch.TreeID,
			BranchID: lastBranch.BranchID,
		}
		if resp.NextPageToken, err = nextPageToken.serialize(); err != nil {
			return nil, errorslite.NewInternalF("failed to serialize NextPageToken: %v", err)
		}
	} else if shardID < h.shardCount {
		nextPageToken := historyTreeBranchPageToken{
			ShardID: lastBranchShardID + 1,
		}
		if resp.NextPageToken, err = nextPageToken.serialize(); err != nil {
			return nil, errorslite.NewInternalF("failed to serialize NextPageToken: %v", err)
		}
	}
	return resp, nil
}

// GetHistoryTree returns all branch information of a tree
func (h *HistoryStore) GetHistoryTree(
	ctx context.Context,
	request *p.GetHistoryTreeRequest,
) (resp *p.InternalGetHistoryTreeResponse, err error) {
	treeID, err := primitives.ValidateUUID(request.TreeID)
	if err != nil {
		return nil, errorslite.NewInvalidArgumentF("invalid TreeID: %v", err)
	}

	shardID := request.ShardID
	rql := h.rqliteFactory.GetClient(shardID)

	rs, err := rql.QueryParameterizedContext(ctx, []gorqlite.ParameterizedStatement{
		{
			Query: `
SELECT data, data_encoding
FROM history_tree
WHERE shard_id = ? AND tree_id = ?
`,
			Arguments: []interface{}{shardID, treeID},
		},
	})
	if err != nil {
		return nil, errorslite.NewInternalF("failed to select: %v", err)
	}

	res := rs[0]
	treeInfos := make([]*commonpb.DataBlob, 0, 100)
	for res.Next() {
		treeInfo, err := scanTreeInfo(res)
		if err != nil {
			return nil, errorslite.NewInternalF("failed to scan tree: %v", err)
		}
		treeInfos = append(treeInfos, treeInfo)
	}

	return &p.InternalGetHistoryTreeResponse{
		TreeInfos: treeInfos,
	}, nil
}

func scanHistoryNode(res gorqlite.QueryResult, metadataOnly bool) (p.InternalHistoryNode, error) {
	var dataB64 string
	var encoding string
	var nodeID int64
	var prevTxnID int64
	var txnID int64
	if metadataOnly {
		if err := res.Scan(&nodeID, &prevTxnID, &txnID); err != nil {
			return p.InternalHistoryNode{}, err
		}
	} else {
		if err := res.Scan(&nodeID, &prevTxnID, &txnID, &dataB64, &encoding); err != nil {
			return p.InternalHistoryNode{}, err
		}
	}
	blob, err := conn.Base64ToBlob(dataB64, encoding)
	if err != nil {
		return p.InternalHistoryNode{}, err
	}
	return p.InternalHistoryNode{
		NodeID:            nodeID,
		PrevTransactionID: -prevTxnID,
		TransactionID:     -txnID,
		Events:            blob,
	}, nil
}

func scanInternalHistoryBranchDetail(res gorqlite.QueryResult) (int32, p.InternalHistoryBranchDetail, error) {
	var shardID int
	var treeID string
	var branchID string
	var dataB64 string
	var encoding string
	if err := res.Scan(&shardID, &treeID, &branchID, &dataB64, &encoding); err != nil {
		return 0, p.InternalHistoryBranchDetail{}, err
	}
	blob, err := conn.Base64ToBlob(dataB64, encoding)
	if err != nil {
		return 0, p.InternalHistoryBranchDetail{}, err
	}
	return int32(shardID), p.InternalHistoryBranchDetail{
		TreeID:   treeID,
		BranchID: branchID,
		Data:     blob.Data,
		Encoding: blob.EncodingType.String(),
	}, nil
}

func scanTreeInfo(res gorqlite.QueryResult) (*commonpb.DataBlob, error) {
	var dataB64 string
	var encoding string
	if err := res.Scan(&dataB64, &encoding); err != nil {
		return nil, err
	}
	return conn.Base64ToBlob(dataB64, encoding)
}
