package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	svc "appmeta/api/service/v1"
	"appmeta/pkg/data"
)

// MetadataPersistence provide the API to read/write application metadata on a persistence layer
// The real backend storage system can be MySql, ZooKeeper, ElasticSearch (which is best for this) etc.
type MetadataPersistence interface {
	// UploadMetadata add or update application metadata record using website key
	UploadMetadata(
		ctx context.Context,
		website string,
		content string,
		updateTime time.Time,
		meta *data.AppMetadata,
	) error

	// SearchMetadata search application metadata records based on query string
	SearchMetadata(ctx context.Context, query string) ([]*svc.MetadataRecord, error)
}

type record struct {
	// the Internal version number which is increased on each update
	version uint64
	// last update time
	updateTime time.Time
	// the website URL used as record key
	website string
	// the original uploaded content
	content string
}

type indexRecord struct {
	// the version of the metadata when this index record is updated
	// if not equal to the latest version means this index is outdated
	ver uint64
	// the pointer to the latest metadata record
	ptr *record
}

// Here we use a simple in memory storage implementation
// which support search on multiple fields matching or contains matching
type mp struct {
	logger *zap.Logger
	scope  tally.Scope

	// for concurrent update handling
	mu sync.RWMutex
	// the simple hashmap index: {field_name: {field_value: {website: index record}}}
	idx map[string]map[string]map[string]*indexRecord
	// the data store
	store map[string]*record
}

// Creates a persistence as configured
func NewPersistence(
	logger *zap.Logger,
	scope tally.Scope,
) *mp {
	return &mp{
		logger: logger,
		scope:  scope,
		store:  map[string]*record{},
		idx:    map[string]map[string]map[string]*indexRecord{},
	}
}

func (p *mp) UploadMetadata(
	ctx context.Context,
	website string,
	content string,
	updateTime time.Time,
	meta *data.AppMetadata,
) error {
	if meta == nil {
		return errors.New("metadata is nil")
	}

	// update data store
	// TODO: we may want to keep the old versions in a history DB so user can check what changed recently
	p.mu.Lock()
	defer p.mu.Unlock()
	rd, ok := p.store[website]
	if !ok {
		rd = &record{website: website}
		p.store[website] = rd
	}
	// increase version each update
	rd.version++
	rd.updateTime = updateTime
	rd.content = content

	// index update function
	addIndex := func(field string, value string) {
		// get the field index
		fi, ok := p.idx[field]
		if !ok {
			fi = map[string]map[string]*indexRecord{}
			p.idx[field] = fi
		}
		// get the value index
		vi, ok := fi[value]
		if !ok {
			vi = map[string]*indexRecord{}
			fi[value] = vi
		}
		// TODO: in real production we should have way to cleanup the oudated index record
		vi[website] = &indexRecord{ver: rd.version, ptr: rd}
	}
	meta.UpdateIndex(addIndex)
	return nil
}

// here we only support a very simple multiple fields matching by AND clause:
//     field1="value1" AND filed2="value2" ...
// the value part can support full matching or contains matching.
// another query type is just a raw string without "=", which will do a full scan on all content
func (p *mp) SearchMetadata(ctx context.Context, query string) ([]*svc.MetadataRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// case 1: it may need a full scan
	// in real production it can be a inverted index search by breaking the query into words
	var ret []*svc.MetadataRecord
	if strings.IndexByte(query, '=') < 0 {
		for _, rd := range p.store {
			if strings.Contains(rd.content, query) {
				ret = append(ret, &svc.MetadataRecord{
					Website:    rd.website,
					Metadata:   rd.content,
					UpdateTime: rd.updateTime.Format(time.RFC3339),
				})
			}
		}
		return ret, nil
	}

	// case 2: multiple fields matching
	// a very rough query parsing logic assuming there is no syntax error or invalid string
	// TODO: use a real syntax parser to handle query
	var keys map[string]*indexRecord
	conds := strings.Split(query, " AND ")
	for _, cond := range conds {
		parts := strings.Split(cond, "=")
		if len(parts) != 2 {
			return nil, errors.New("invalid query string")
		}
		// get field name
		field := strings.TrimSpace(parts[0])
		// get the value part, which maybe quoted
		value := strings.TrimSpace(parts[1])
		start := strings.IndexByte(value, '"')
		if start == 0 {
			value = value[1:strings.LastIndexByte(value, '"')]
		}
		// search the index
		// get the field index
		fi, ok := p.idx[field]
		if !ok {
			return ret, nil
		}
		// check if there is a full match
		// TODO: may also support prefix match and wildcard match
		vi, ok := fi[value]
		if !ok {
			return ret, nil
		}
		// put the first matching list into result
		if keys == nil {
			keys = map[string]*indexRecord{}
			for k, ir := range vi {
				// need skip the outdated record
				if ir.ver == ir.ptr.version {
					keys[k] = &indexRecord{
						ver: 1, // here we use ver to temporarily track the matching counter
						ptr: ir.ptr,
					}
				}
			}
		} else {
			// filter the result with this matching list
			for k, ir := range vi {
				// need skip the outdated record
				if ir.ver != ir.ptr.version {
					continue
				}
				rd, ok := keys[k]
				// update matching counter
				if ok {
					rd.ver++
				}
			}
		}
	}
	// only use the record matching all conditions in result
	for _, rd := range keys {
		if rd.ver < uint64(len(conds)) {
			continue
		}
		ret = append(ret, &svc.MetadataRecord{
			Website:    rd.ptr.website,
			Metadata:   rd.ptr.content,
			UpdateTime: rd.ptr.updateTime.Format(time.RFC3339),
		})
	}
	return ret, nil
}
