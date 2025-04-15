package cache

import (
	"slices"
	"sort"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type Tasks []p.InternalHistoryTask

// sort sorts the tasks by key
func (ts Tasks) sort() SortedTasks {
	slices.SortFunc(ts, func(a, b p.InternalHistoryTask) int {
		return a.Key.CompareTo(b.Key)
	})
	return SortedTasks(ts)
}

// getRange returns the range of keys in the tasks.
// Must not be called on an empty list.
func (ts Tasks) getRange() keyRange {
	minKey := tasks.MaximumKey
	maxKey := tasks.MinimumKey
	for _, t := range ts {
		minKey = tasks.MinKey(minKey, t.Key)
		maxKey = tasks.MaxKey(maxKey, t.Key)
	}
	return newKeyRange(minKey, maxKey)
}

func (ts Tasks) String() string {
	return formatTasks(ts)
}

type SortedTasks []p.InternalHistoryTask

// splitBy splits the tasks into two parts: the first part contains tasks with key <= key, the second part contains the rest
func (ts SortedTasks) splitBy(key tasks.Key) (SortedTasks, SortedTasks) {
	i := sort.Search(len(ts), func(i int) bool {
		return ts[i].Key.CompareTo(key) > 0
	})
	return ts[:i], ts[i:]
}

// query returns a batch of tasks that are in the range r
func (ts SortedTasks) query(r keyRange, batchSize int) SortedTasks {
	s := sort.Search(len(ts), func(i int) bool {
		return ts[i].Key.CompareTo(r.inclusiveFrom) >= 0
	})
	e := sort.Search(len(ts), func(i int) bool {
		return ts[i].Key.CompareTo(r.inclusiveTo) > 0
	})
	if e-s > batchSize {
		e = s + batchSize
	}
	return ts[s:e]
}

// merge merges two sorted task lists into one
func (ts SortedTasks) merge(ts2 SortedTasks) SortedTasks {
	var res SortedTasks
	if len(ts2) == 0 {
		res = ts
	} else if len(ts) == 0 {
		res = ts2
	} else {
		n := len(ts) + len(ts2)
		res = make(SortedTasks, 0, n)
		i := 0
		j := 0
		for i < len(ts) && j < len(ts2) {
			task1 := ts[i]
			task2 := ts2[j]
			if task2.Key.CompareTo(task1.Key) <= 0 {
				res = append(res, task2)
				j++
			} else {
				res = append(res, task1)
				i++
			}
		}
		res = append(res, ts[i:]...)
		res = append(res, ts2[j:]...)
	}
	res = slices.CompactFunc(res, func(a p.InternalHistoryTask, b p.InternalHistoryTask) bool {
		return a.Key.CompareTo(b.Key) == 0
	})
	return res
}

func (ts SortedTasks) String() string {
	return formatTasks(ts)
}

func formatTasks(ts []p.InternalHistoryTask) string {
	res := ""
	for _, t := range ts {
		res += formatKey(t.Key) + ", "
	}
	return res
}
