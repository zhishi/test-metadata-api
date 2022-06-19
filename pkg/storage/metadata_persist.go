package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	gozk "github.com/samuel/go-zookeeper/zk"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"pinterest.com/tower/api/v1/core"
	"pinterest.com/tower/api/v1/pinconf"
	"pinterest.com/tower/dependency/zk"
	"pinterest.com/tower/pkg/common/constants"
	tc "pinterest.com/tower/pkg/config"
	"pinterest.com/tower/pkg/utils/lang"
)

const (
	artifactZNode    = "artifacts"
	scopeDeployZNode = "scope_deploy"
	commitZNode      = "commits"
	reportZNode      = "reports"
	eventZNode       = "events"

	defaultAZ       = ""
	maxEmbeddedSize = 100
	maxRollbackNum  = 5
)

var (
	// ErrArtifactNotExist indicates the artifact does not exist.
	ErrArtifactNotExist = errors.New("artifact does not exist")
	// ErrConfigContentExist indicates the target config content already exist in s3.
	ErrConfigContentExist = errors.New("config content already exist in s3")
)

// MetadataPersistence provide the API to read/write application metadata on a persistence layer
type MetadataPersistence interface {
	// IsComplete check if inital loading is finished
	IsComplete() bool

	// ListArtifacts lists artifact names with latest version info
	// An error is returned if failed to list.
	ListArtifacts(ctx context.Context) ([]*pinconf.ArtifactInfo, error)

	// GetArtifactStatus retrieves latest status of a given artifact name
	// An error is returned if failed to retrieve the status.
	GetArtifactStatus(ctx context.Context, name string) (*pinconf.ArtifactStatus, error)

	// GetArtifactVersion retrieves content of a given artifact version.
	// An error is returned if failed to retrieve the content.
	GetArtifactVersion(ctx context.Context, name string, version string) (*pinconf.Artifact, error)

	// UploadArtifact uplaod a new version of a given artifact name
	// An error is returned if failed to upload the artifact.
	UploadArtifact(ctx context.Context, atf *pinconf.Artifact) error

	// UploadConfig will return the presigned location URL for target config content
	// An error is returned if failed or the content already exist
	UploadConfig(ctx context.Context, name string, digest string) (string, error)

	// SealCommit will create commit snapshot for a PR version,
	// which means all artifacts of that version are ready to deploy
	SealCommit(ctx context.Context, commit *pinconf.CommitSnapshot) error

	// ListCommits list all commit versions
	// An error is returned if failed to list.
	ListCommits(ctx context.Context) ([]string, error)

	// GetCommitSnapshot retrieves a commit snapshot of a given version
	// An error is returned if failed to retrieve the content
	GetCommitSnapshot(ctx context.Context, version string) (*pinconf.CommitSnapshot, error)

	// DeployArtifactScope will deploy an artifact version to scopes
	// An error is returned if failed or the content already exist
	DeployArtifactScope(ctx context.Context, req *pinconf.DeployArtifactScopeRequest) error

	// GetArtifactScopeTree retrieves full scoped deployment tree of an artifact
	// An error is returned if failed
	GetArtifactScopeTree(ctx context.Context, name string) (*pinconf.GetArtifactScopeTreeResponse, error)

	// RestoreArtifactScopeTree restore the full scoped deployment tree of an artifact
	// This API should only be used for perfect rollback in the phase level auto-rollback
	// An error is returned if failed
	RestoreArtifactScopeTree(ctx context.Context, req *pinconf.RestoreArtifactScopeTreeRequest) error

	// GetArtifactScopeHistory retrieves scoped deployment history for an artifact
	// An error is returned if failed or the content already exist
	GetArtifactScopeHistory(ctx context.Context, name string, count int) ([]*pinconf.ArtifactScopeDeployment, error)

	// UpdateArtifactScopeReport upload the aggregated scope report tree for an artifact
	UpdateArtifactScopeReport(ctx context.Context, instance string, name string, report *pinconf.ScopeReportNode) error

	// GetArtifactScopeReports retrieves all the scoped reports of an artifact, no earlier than noEarlier time
	GetArtifactScopeReports(ctx context.Context, noEarlier time.Time, name string) (map[string]*pinconf.ScopeReportNode, error)

	// AddArtifactEvent add a pipeline event record for an artifact
	AddArtifactEvent(ctx context.Context, name string, event *pinconf.ArtifactEvent) error

	// GetArtifactEvents get artifact events history
	GetArtifactEvents(ctx context.Context, name string) ([]*pinconf.ArtifactEvent, error)

	// CleanupPinconfConfigs remove old configs not in any artifact
	CleanupPinconfConfigs(ctx context.Context, isDryRun bool, resp *pinconf.CleanupPinconfConfigsResponse) error

	// Below two interfaces are for scope tree based producer

	// GetScopeResourceType return the resource type this scope tree producer will generate
	GetScopeResourceType() string

	// WatchScopeTreeUpdates watch artifacts scoped deployment status and generate scope tree updates
	WatchScopeTreeUpdates(ctx context.Context) (<-chan *core.TreeUpdate, error)
}

type pczp struct {
	cfg    *tc.PinconfConfig
	logger *zap.Logger
	scope  tally.Scope
	zkMgr  zk.Manager

	// predefined znodes
	artifactPath    string
	scopeDeployPath string
	commitPath      string
	reportPath      string
	eventPath       string
	completed       int32

	s3Client *s3.S3
}

// NewPinconfPersistence creates a persistence as configured
func NewPinconfPersistence(
	cfg *tc.PinconfConfig,
	logger *zap.Logger,
	scope tally.Scope,
	zkMgr zk.Manager,
	s3Client *s3.S3,
) *pczp {
	if cfg.Zookeeper == nil {
		panic("missing zookeeper config")
	}

	// in case there is mistake
	if cfg.Zookeeper.VersionsToKeep < 10 {
		cfg.Zookeeper.VersionsToKeep = 10
	}

	return &pczp{
		cfg:             cfg,
		logger:          logger,
		scope:           scope,
		zkMgr:           zkMgr,
		s3Client:        s3Client,
		artifactPath:    cfg.Zookeeper.RootPath + "/" + artifactZNode,
		scopeDeployPath: cfg.Zookeeper.RootPath + "/" + scopeDeployZNode,
		commitPath:      cfg.Zookeeper.RootPath + "/" + commitZNode,
		reportPath:      cfg.Zookeeper.RootPath + "/" + reportZNode,
		eventPath:       cfg.Zookeeper.RootPath + "/" + eventZNode,
	}
}

func (p *pczp) Name() string {
	return "pinconf"
}

func (p *pczp) IsComplete() bool {
	return atomic.LoadInt32(&p.completed) == 1
}

func (p *pczp) ListArtifacts(ctx context.Context) ([]*pinconf.ArtifactInfo, error) {
	// we use two level path to store artifact
	paths, err := p.zkMgr.ChildrenPaths(ctx, p.artifactPath, 2)
	if err != nil {
		return nil, fmt.Errorf("failed to list pinconf artifacts: %v", err)
	}
	sort.Strings(paths)
	ret := make([]*pinconf.ArtifactInfo, 0, len(paths))
	for _, path := range paths {
		name := p.artifactNameFromPath(path)
		// get the latest status
		stat, err := p.GetArtifactStatus(ctx, name)
		if err != nil {
			return nil, err
		}
		ret = append(ret, &pinconf.ArtifactInfo{
			Name:        name,
			LastVersion: stat.GetLastVersion(),
			UpdateTime:  stat.GetUpdateTime(),
		})
	}
	return ret, nil
}

func (p *pczp) GetArtifactStatus(ctx context.Context, name string) (*pinconf.ArtifactStatus, error) {
	if name == "" {
		return nil, errors.New("artifact name is required")
	}

	path := p.artifactStatPath(name)

	// get status content
	val, _, ut, err := p.zkMgr.GetWithUpdateTime(ctx, path)
	switch err {
	case zk.ErrNoNode:
		return nil, ErrArtifactNotExist
	case nil:
	default:
		return nil, fmt.Errorf("failed to get status of artifact %s: %v", name, err)
	}

	st := &pinconf.ArtifactStatus{}
	if err := proto.Unmarshal(val, st); err != nil {
		return nil, fmt.Errorf("failed to unmarshal status of artifact %s: %v", name, err)
	}
	// handle old data without the update time field
	if st.UpdateTime == nil {
		st.UpdateTime = timestamppb.New(ut)
	}
	return st, nil
}

func (p *pczp) GetArtifactVersion(ctx context.Context, name string, version string) (*pinconf.Artifact, error) {
	if name == "" {
		return nil, errors.New("artifact name is required")
	}

	path := p.artifactVersionPath(name, version)
	val, _, err := p.zkMgr.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get artifact version %s: %v", path, err)
	}
	// may need unzip
	uza, err := unzip(val)
	if err != nil {
		return nil, fmt.Errorf("failed to unzip artifact version %s: %v", path, err)
	}

	atf := &pinconf.Artifact{}
	if err := proto.Unmarshal(uza, atf); err != nil {
		return nil, fmt.Errorf("failed to unmarshal artifact %s: %v", path, err)
	}
	// re-fill the digest for S3 path and embedded content
	for key, meta := range atf.Configs {
		u, err := url.Parse(meta.Location)
		if err != nil {
			return nil, fmt.Errorf("malformed location url for target config %s: %v", key, err)
		}
		schema := strings.ToLower(u.Scheme)
		switch schema {
		case constants.EmbeddedSchemaName:
			meta.Digest = lang.GetSha256String(meta.GetContent())
		case constants.S3SchemaName:
			if meta.Location[len(meta.Location)-1] == '/' {
				meta.Location += meta.Digest
			}
		}
	}
	return atf, nil
}

func (p *pczp) UploadArtifact(ctx context.Context, atf *pinconf.Artifact) error {
	if atf == nil {
		return errors.New("artifact is nil")
	}
	if err := validateArtifactName(atf.Name); err != nil {
		return err
	}
	if atf.Version == "" {
		return errors.New("artifact version is missing")
	}

	// validate configs
	err := p.validateTargetConfigs(atf.Configs)
	if err != nil {
		return err
	}

	apath := p.artifactStatPath(atf.Name)
	stat := &pinconf.ArtifactStatus{}
	tx := p.zkMgr.NewTransaction()
	// get status content
	val, aver, err := p.zkMgr.Get(ctx, apath)
	switch err {
	case zk.ErrNoNode:
		// create the domain node firstly
		domainPath := apath[:strings.LastIndex(apath, "/")]
		_, err = p.zkMgr.CreateIfNotExists(ctx, domainPath, []byte{}, false)
		if err != nil {
			return fmt.Errorf("failed to create artifact domain path %s: %v", domainPath, err)
		}
		// create the final node in transaction to avoid invalid node in case there is error
		tx.Create(apath, []byte{}, gozk.WorldACL(gozk.PermAll), 0)
	case nil:
		if err = proto.Unmarshal(val, stat); err != nil {
			return fmt.Errorf("failed to unmarshal status of artifact %s: %v", atf.Name, err)
		}
	default:
		return fmt.Errorf("failed to get status of artifact %s: %v", atf.Name, err)
	}

	ma, err := proto.Marshal(atf)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact %s:%s, %v", atf.Name, atf.Version, err)
	}
	za, err := zip(ma)
	if err != nil {
		return fmt.Errorf("failed to gzip artifact %s:%s, %v", atf.Name, atf.Version, err)
	}

	// only record large artifact size to avoid thousands of tags value
	if len(za) > 50*1024 {
		p.scope.Tagged(
			map[string]string{
				"artifact": atf.Name,
			}).Gauge("artifact_size").Update(float64(len(za)))
	}

	// if the version already exist just overwrite it unless the commit is sealed
	vpath := p.artifactVersionPath(atf.Name, atf.Version)
	ver, _, err := p.zkMgr.Stat(ctx, vpath)
	switch err {
	case zk.ErrNoNode:
		tx.Create(vpath, za, gozk.WorldACL(gozk.PermAll), 0)
	case nil:
		// check if the version already sealed
		cpath := p.commitSnapshotPath(atf.Version)
		_, _, err := p.zkMgr.Stat(ctx, cpath)
		if err == nil {
			return fmt.Errorf("artifact version already exists and the commit is sealed: %s", atf.Version)
		}
		if err != zk.ErrNoNode {
			return fmt.Errorf("failed to check commit version %s: %v", cpath, err)
		}
		tx.Set(vpath, za, ver)
	default:
		return fmt.Errorf("failed to check artifact version %s: %v", vpath, err)
	}

	// it maybe the retry of old landing job
	isNew := true
	for _, v := range stat.Versions {
		if v == atf.Version {
			isNew = false
			break
		}
	}
	if isNew {
		stat.LastVersion = atf.Version
		stat.Versions = append(stat.Versions, atf.Version)
		// evict versions as set by p.cfg.Zookeeper.VersionsToKeep
		if len(stat.Versions) > p.cfg.Zookeeper.VersionsToKeep {
			newVers, err := p.evictArtifactVersions(ctx, tx, atf.Name, stat.Versions)
			if err != nil {
				p.logger.Error("Failed to evict old artifact version.",
					zap.String("artifact-name", atf.Name),
					zap.Error(err))
				return err
			}
			stat.Versions = newVers
		}
	}
	stat.UpdateTime = atf.UpdateTime
	m, err := proto.Marshal(stat)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact status: %v", err)
	}
	tx.Set(apath, m, aver)
	if err := p.zkMgr.CommitTransaction(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit transaction for artifact uploading %s: %v", atf.Name, err)
	}
	return nil
}

func (p *pczp) SealCommit(ctx context.Context, commit *pinconf.CommitSnapshot) error {
	if commit == nil || commit.GetVersion() == "" {
		return errors.New("pinconf commit version string is required")
	}

	ch := &pinconf.CommitHistory{}
	tx := p.zkMgr.NewTransaction()
	val, cver, err := p.zkMgr.Get(ctx, p.commitPath)
	switch err {
	case zk.ErrNoNode:
		tx.Create(p.commitPath, []byte{}, gozk.WorldACL(gozk.PermAll), 0)
	case nil:
		if err := proto.Unmarshal(val, ch); err != nil {
			return fmt.Errorf("failed to unmarsal pinconf commit history: %v", err)
		}
	default:
		return fmt.Errorf("failed to get pinconf commit history %s: %v", p.commitPath, err)
	}

	// validate artifacts existence
	for name, ver := range commit.Artifacts {
		apath := p.artifactVersionPath(name, ver)
		_, _, err := p.zkMgr.Stat(ctx, apath)
		if err != nil {
			return fmt.Errorf("failed to check artifact version %s: %v", apath, err)
		}
	}

	ss, err := proto.Marshal(commit)
	if err != nil {
		return fmt.Errorf("failed to marshal pinconf commit snapshot %s: %v", commit.Version, err)
	}
	m, err := zip(ss)
	if err != nil {
		return fmt.Errorf("failed to gzip pinconf commit snapshot %s: %v", commit.Version, err)
	}

	path := p.commitSnapshotPath(commit.Version)
	sver, _, err := p.zkMgr.Stat(ctx, path)
	switch err {
	case nil:
		// commit snapshot already exists
		tx.Set(path, m, sver)
	case zk.ErrNoNode:
		tx.Create(path, m, gozk.WorldACL(gozk.PermAll), 0)
	default:
		return err
	}
	// evict old commits as set by p.cfg.Zookeeper.VersionsToKeep
	hist := append(ch.Commits, commit.Version)
	if len(hist) > p.cfg.Zookeeper.VersionsToKeep {
		for i := 0; i < len(hist)-p.cfg.Zookeeper.VersionsToKeep; i++ {
			path = p.commitSnapshotPath(hist[i])
			v, _, err := p.zkMgr.Stat(ctx, path)
			if err != nil {
				// it maybe already deleted, just skip it this time.
				continue
			}
			tx.Delete(path, v)
		}
		hist = hist[len(hist)-p.cfg.Zookeeper.VersionsToKeep:]
	}
	ch.Commits = hist
	m, err = proto.Marshal(ch)
	if err != nil {
		return fmt.Errorf("failed to marshal commits history: %v", err)
	}
	tx.Set(p.commitPath, m, cver)

	if err := p.zkMgr.CommitTransaction(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit transaction for commit snapshot %s: %v", commit.Version, err)
	}
	return nil
}

func (p *pczp) ListCommits(ctx context.Context) ([]string, error) {
	ch := &pinconf.CommitHistory{}
	val, _, err := p.zkMgr.Get(ctx, p.commitPath)
	switch err {
	case zk.ErrNoNode:
		return nil, nil
	case nil:
		if err := proto.Unmarshal(val, ch); err != nil {
			return nil, fmt.Errorf("failed to unmarsal pinconf commit history: %v", err)
		}
		return ch.Commits, nil
	default:
		return nil, fmt.Errorf("failed to get pinconf commit history %s: %v", p.commitPath, err)
	}
}

func (p *pczp) GetCommitSnapshot(ctx context.Context, version string) (*pinconf.CommitSnapshot, error) {
	if version == "" {
		return nil, errors.New("commit version is required")
	}

	path := p.commitSnapshotPath(version)
	val, _, err := p.zkMgr.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit snapshot %s: %v", version, err)
	}
	// may need unzip
	m, err := unzip(val)
	if err != nil {
		return nil, fmt.Errorf("failed to unzip commit snapshot %s: %v", version, err)
	}
	ret := &pinconf.CommitSnapshot{}
	if err = proto.Unmarshal(m, ret); err != nil {
		return nil, fmt.Errorf("failed to unmarshal commit snapshot %s: %v", version, err)
	}
	return ret, nil
}

func (p *pczp) UploadConfig(ctx context.Context, name string, digest string) (string, error) {
	if name == "" || digest == "" {
		return "", errors.New("config name and digest are required")
	}

	path := p.targetConfigPath(name, digest)
	// check existence
	if p.s3Client == nil {
		return "", errors.New("S3 client is not setup")
	}
	_, err := p.s3Client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(p.cfg.S3.Bucket),
		Key:    aws.String(path),
	})
	if err == nil {
		return "", ErrConfigContentExist
	}
	// generate presigned url
	req, _ := p.s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(p.cfg.S3.Bucket),
		Key:    aws.String(path),
	})
	str, err := req.Presign(p.cfg.S3.UploadTimeout)
	if err != nil {
		return "", err
	}

	return str, nil
}

func (p *pczp) DeployArtifactScope(ctx context.Context, req *pinconf.DeployArtifactScopeRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}
	if err := validateArtifactName(req.Name); err != nil {
		return err
	}
	if req.Version == "" {
		return errors.New("artifact version is required")
	}

	// for deployment latency metric
	start := time.Now()
	dpath := p.artifactScopeDeployPath(req.Name)
	tx := p.zkMgr.NewTransaction()
	ds := &pinconf.ArtifactScopeDeploymentStatus{}
	val, dver, err := p.zkMgr.Get(ctx, dpath)
	switch err {
	case zk.ErrNoNode:
		// create the domain node firstly
		domainPath := dpath[:strings.LastIndex(dpath, "/")]
		_, err = p.zkMgr.CreateIfNotExists(ctx, domainPath, []byte{}, false)
		if err != nil {
			return fmt.Errorf("failed to create artifact domain path %s: %v", domainPath, err)
		}
		// create the final node in transaction to avoid invalid node in case there is error
		tx.Create(dpath, []byte{}, gozk.WorldACL(gozk.PermAll), 0)
	case nil:
		if err := proto.Unmarshal(val, ds); err != nil {
			return fmt.Errorf("failed to unmarshal scoped deployment status of artifact %s: %v", req.Name, err)
		}
	default:
		return err
	}

	// TODO: check if the version is sealed
	if req.InternalVersion != dver && req.Force == false {
		return ErrUpdateConflict
	}
	// make sure the version exist
	_, err = p.GetArtifactVersion(ctx, req.Name, req.Version)
	if err != nil {
		return fmt.Errorf("failed to get artifact version %s:%s, %v", req.Name, req.Version, err)
	}

	if ds.Root == nil {
		ds.Root = &pinconf.ScopeNode{}
	}

	// deploy each scope
	for _, scope := range req.Scopes {
		var root interface{}
		if err := json.Unmarshal([]byte(scope), &root); err != nil {
			return fmt.Errorf("failed to unmarshal scope as json %s: %v", scope, err)
		}
		// merge the scope to the tree with new version
		err = mergeScope(root, req.Zone, req.Version, ds.Root)
		if err != nil {
			return fmt.Errorf("failed to merge scope into deployment tree %s: %v", scope, err)
		}
	}
	// normalize and cut unnecessary subtrees
	reduceTree(ds.Root, nil)

	newNum := ds.SerialNumber + 1
	ad := &pinconf.ArtifactScopeDeployment{
		User:          req.User,
		UpdateTime:    timestamppb.Now(),
		Comment:       req.Comment,
		ActiveVersion: req.Version,
		Scopes:        req.Scopes,
		Zone:          req.Zone,
	}
	md, err := proto.Marshal(ad)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact scoped deployment: %v", err)
	}
	tx.Create(p.artifactScopeHistoryPath(req.Name, newNum), md, gozk.WorldACL(gozk.PermAll), 0)
	ds.SerialNumber = newNum
	// evict old deployment history as set by p.cfg.VersionsToKeep
	err = p.evictDeploymentHistory(ctx, tx, req.Name)
	if err != nil {
		return err
	}
	m, err := proto.Marshal(ds)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact scoped deployment status %s: %v", req.Name, err)
	}
	tx.Set(dpath, m, dver)
	// only record long latency on big trees
	lat := time.Now().Sub(start).Seconds()
	if lat > 1 {
		p.scope.Gauge("scope_deploy_time").Update(lat)
	}

	if err := p.zkMgr.CommitTransaction(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit transaction for artifact scoped deployment %s:%s, %v", req.Name, req.Version, err)
	}
	return nil
}

func (p *pczp) GetArtifactScopeTree(ctx context.Context, name string) (*pinconf.GetArtifactScopeTreeResponse, error) {
	if name == "" {
		return nil, errors.New("artifact name is required")
	}

	dpath := p.artifactScopeDeployPath(name)
	ds := &pinconf.ArtifactScopeDeploymentStatus{}
	val, dver, err := p.zkMgr.Get(ctx, dpath)
	if err == zk.ErrNoNode {
		return nil, ErrArtifactNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get scoped deployment status of artifact %s: %v", name, err)
	}
	if err = proto.Unmarshal(val, ds); err != nil {
		return nil, fmt.Errorf("failed to unmarshal scoped deployment status of artifact %s: %v", name, err)
	}

	return &pinconf.GetArtifactScopeTreeResponse{
		Name:            name,
		Root:            ds.Root,
		InternalVersion: dver,
		SerialNumber:    ds.SerialNumber,
	}, nil
}

func (p *pczp) RestoreArtifactScopeTree(ctx context.Context, req *pinconf.RestoreArtifactScopeTreeRequest) error {
	if req == nil {
		return errors.New("request is nil")
	}

	if err := validateArtifactName(req.Name); err != nil {
		return err
	}

	dpath := p.artifactScopeDeployPath(req.Name)
	tx := p.zkMgr.NewTransaction()
	ds := &pinconf.ArtifactScopeDeploymentStatus{}
	val, dver, err := p.zkMgr.Get(ctx, dpath)
	if err != nil {
		return fmt.Errorf("failed to get scoped deployment status of artifact %s: %v", req.Name, err)
	}
	if err := proto.Unmarshal(val, ds); err != nil {
		return fmt.Errorf("failed to unmarshal scoped deployment status of artifact %s: %v", req.Name, err)
	}
	// make sure it's a valid in-phase rollback, usually there should be only one deployment be rollback
	// if it's too old could be a wrong job or worker logic trigger
	if ds.SerialNumber <= req.SerialNumber || ds.SerialNumber-req.SerialNumber > maxRollbackNum {
		return fmt.Errorf("invalid or too old number: %d, current serial number is %d",
			req.SerialNumber, ds.SerialNumber)
	}
	// replace the whole tree
	ds.Root = req.Root
	// update and truncate history
	newNum := ds.SerialNumber + 1
	ad := &pinconf.ArtifactScopeDeployment{
		User:          req.User,
		UpdateTime:    timestamppb.Now(),
		Comment:       req.Comment,
		ActiveVersion: fmt.Sprintf("restore: %d", req.SerialNumber),
	}
	md, err := proto.Marshal(ad)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact scoped deployment: %v", err)
	}
	tx.Create(p.artifactScopeHistoryPath(req.Name, newNum), md, gozk.WorldACL(gozk.PermAll), 0)
	ds.SerialNumber = newNum
	// evict old deployment history as set by p.cfg.VersionsToKeep
	err = p.evictDeploymentHistory(ctx, tx, req.Name)
	if err != nil {
		return err
	}
	m, err := proto.Marshal(ds)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact scoped deployment status %s: %v", req.Name, err)
	}
	tx.Set(dpath, m, dver)
	if err := p.zkMgr.CommitTransaction(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit transaction for restore artifact scope tree %s: %v", req.Name, err)
	}
	return nil
}

func (p *pczp) GetArtifactScopeHistory(
	ctx context.Context,
	name string,
	count int,
) ([]*pinconf.ArtifactScopeDeployment, error) {
	if name == "" {
		return nil, errors.New("artifact name is required")
	}

	dpath := p.artifactScopeDeployPath(name)
	children, err := p.zkMgr.Children(ctx, dpath)
	if err != nil {
		return nil, fmt.Errorf("failed to list scoped deployment history of artifact %s: %v", name, err)
	}
	// we use fixed length number string, so it can be sorted directly
	sort.Strings(children)
	if len(children) > count {
		children = children[len(children)-count:]
	}
	ret := make([]*pinconf.ArtifactScopeDeployment, 0, len(children))
	for _, num := range children {
		path := dpath + "/" + num
		dh := &pinconf.ArtifactScopeDeployment{}
		v, _, err := p.zkMgr.Get(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed to get scoped deployment history %s: %v", path, err)
		}
		if err := proto.Unmarshal(v, dh); err != nil {
			return nil, fmt.Errorf("failed to unmarshal scoped deployment history %s: %v", path, err)
		}
		ret = append(ret, dh)
	}
	return ret, nil
}

func (p *pczp) AddArtifactEvent(ctx context.Context, name string, event *pinconf.ArtifactEvent) error {
	if err := validateArtifactName(name); err != nil {
		return err
	}
	if event == nil {
		return errors.New("artifact event is nil")
	}

	epath := p.artifactEventPath(name)
	eh := &pinconf.ArtifactEventsHistory{}
	tx := p.zkMgr.NewTransaction()
	val, hver, err := p.zkMgr.Get(ctx, epath)
	switch err {
	case zk.ErrNoNode:
		// create the domain node firstly
		domainPath := epath[:strings.LastIndex(epath, "/")]
		_, err = p.zkMgr.CreateIfNotExists(ctx, domainPath, []byte{}, false)
		if err != nil {
			return fmt.Errorf("failed to create artifact domain path %s: %v", domainPath, err)
		}
		// create the final node in transaction to avoid invalid node in case there is error
		tx.Create(epath, []byte{}, gozk.WorldACL(gozk.PermAll), 0)
	case nil:
		if err := proto.Unmarshal(val, eh); err != nil {
			return fmt.Errorf("failed to unmarshal events history of artifact %s: %v", name, err)
		}
	default:
		return err
	}

	// evict old events as set by p.cfg.Zookeeper.VersionsToKeep
	hist := append(eh.Events, event)
	if len(hist) > p.cfg.Zookeeper.VersionsToKeep {
		hist = hist[len(hist)-p.cfg.Zookeeper.VersionsToKeep:]
	}
	eh.Events = hist
	m, err := proto.Marshal(eh)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact events history: %v", err)
	}
	tx.Set(epath, m, hver)

	if err := p.zkMgr.CommitTransaction(ctx, tx); err != nil {
		return fmt.Errorf("failed to commit transaction for artifact events %s: %v", name, err)
	}
	return nil
}

func (p *pczp) GetArtifactEvents(ctx context.Context, name string) ([]*pinconf.ArtifactEvent, error) {
	if name == "" {
		return nil, errors.New("artifact name is required")
	}

	epath := p.artifactEventPath(name)
	eh := &pinconf.ArtifactEventsHistory{}
	val, _, err := p.zkMgr.Get(ctx, epath)
	if err != nil {
		return nil, fmt.Errorf("failed to get artifact events history %s: %v", epath, err)
	}
	if err = proto.Unmarshal(val, eh); err != nil {
		return nil, fmt.Errorf("failed to unmarshal artifact events history %s: %v", epath, err)
	}
	return eh.Events, nil
}

func (p *pczp) UpdateArtifactScopeReport(
	ctx context.Context,
	instance string,
	name string,
	report *pinconf.ScopeReportNode,
) error {
	if report == nil {
		return errors.New("report root cannot be nil")
	}
	if instance == "" {
		return errors.New("tower instance name is required")
	}
	if err := validateArtifactName(name); err != nil {
		return err
	}

	rpath := p.artifactScopeReportInstancePath(name, instance)
	// use ephemeral znode
	ver, err := p.zkMgr.CreateIfNotExists(ctx, rpath, []byte{}, true)
	if err != nil {
		return fmt.Errorf("failed to ensure report path %s: %v", rpath, err)
	}

	m, err := proto.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal artifact scope report %s: %v", rpath, err)
	}
	_, err = p.zkMgr.Set(ctx, rpath, m, ver)
	if err != nil {
		return fmt.Errorf("failed to update artifact scope report %s: %v", rpath, err)
	}
	return nil
}

func (p *pczp) GetArtifactScopeReports(
	ctx context.Context,
	noEarlier time.Time,
	name string,
) (map[string]*pinconf.ScopeReportNode, error) {
	if name == "" {
		return nil, errors.New("artifact name is required")
	}

	path := p.artifactScopeReportPath(name)
	towers, err := p.zkMgr.Children(ctx, path)
	switch err {
	case zk.ErrNoNode:
		// this artifact may not exist
		return nil, nil
	case nil:
	default:
		return nil, fmt.Errorf("failed to get instance list of artifact scope report from zk path %s: %v", path, err)
	}
	// get each report
	path += "/"
	ret := map[string]*pinconf.ScopeReportNode{}
	for _, ins := range towers {
		rpath := path + ins
		val, _, upt, err := p.zkMgr.GetWithUpdateTime(ctx, rpath)
		switch err {
		case zk.ErrNoNode:
			// the tower instance may restart or dead just now, skip it
			continue
		case nil:
		default:
			return nil, fmt.Errorf("failed to get artifact scope report instance %s: %v", rpath, err)
		}
		// skip old data
		if upt.Before(noEarlier) {
			continue
		}
		report := &pinconf.ScopeReportNode{}
		if err = proto.Unmarshal(val, report); err != nil {
			return nil, fmt.Errorf("failed to unmarshal artifact scope report %s: %v", rpath, err)
		}
		ret[ins] = report
	}
	return ret, nil
}

func (p *pczp) CleanupPinconfConfigs(
	ctx context.Context,
	isDryRun bool,
	resp *pinconf.CleanupPinconfConfigsResponse,
) error {
	if p.cfg.S3.EnableCleanup == false {
		return errors.New("configs cleanup api is not enabled")
	}
	if resp == nil {
		return errors.New("response object is required")
	}
	p.logger.Info("Prepare GC configs from S3.")
	bucketPrefix := "s3://" + p.cfg.S3.Bucket + "/"
	// get all the config versions from each artifact snapshot
	cfgRefs := map[string]uint32{}
	// we use two level path to store artifact status, and version snapshot as children
	paths, err := p.zkMgr.ChildrenPaths(ctx, p.artifactPath, 3)
	if err != nil {
		return fmt.Errorf("failed to list all pinconf artifact versions: %v", err)
	}
	for _, path := range paths {
		// get the artifact version snapshot
		val, _, err := p.zkMgr.Get(ctx, path)
		if err != nil {
			return fmt.Errorf("failed to get artifact version %s: %v", path, err)
		}
		uza, err := unzip(val)
		if err != nil {
			return fmt.Errorf("failed to unzip artifact version %s: %v", path, err)
		}
		atf := &pinconf.Artifact{}
		if err := proto.Unmarshal(uza, atf); err != nil {
			return fmt.Errorf("failed to unmarshal artifact version %s: %v", path, err)
		}
		updateRefs(cfgRefs, atf, len(bucketPrefix))
	}

	// get all the config versions in S3 folder
	if p.s3Client == nil {
		return errors.New("S3 client is not setup")
	}
	s3files := make([]*s3.Object, 0, len(cfgRefs))
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(p.cfg.S3.Bucket),
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(p.cfg.S3.Root + "/"),
	}
	err = p.s3Client.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			s3files = append(s3files, page.Contents...)
			return !lastPage
		})
	if err != nil {
		return err
	}

	// delete old config version not in any artifact
	removes := make([]string, 0)
	keepDate := time.Now().Add(-(time.Duration(p.cfg.S3.MinKeepDays) * 24 * time.Hour))
	for _, file := range s3files {
		key := *file.Key
		if (*file.LastModified).Before(keepDate) && cfgRefs[key] == 0 {
			removes = append(removes, key)
		}
	}

	p.logger.Info("Start cleanup configs from S3.",
		zap.Bool("dry-run", isDryRun),
		zap.Int("known-count", len(cfgRefs)),
		zap.Int("s3-count", len(s3files)),
		zap.Int("remove-count", len(removes)))
	sort.Strings(removes)
	// dry run don't do real deletion
	if isDryRun {
		resp.RemovedConfigs = append(resp.RemovedConfigs, removes...)
		return nil
	}
	// for safety we move the deleted configs to pinconf/XXX/configs_delete/ folder
	gcRoot := p.cfg.S3.Root + "_delete"
	for _, path := range removes {
		input := &s3.CopyObjectInput{
			Bucket:     aws.String(p.cfg.S3.Bucket),
			CopySource: aws.String(p.cfg.S3.Bucket + "/" + path),
			Key:        aws.String(gcRoot + path[len(p.cfg.S3.Root):]),
		}
		_, err := p.s3Client.CopyObject(input)
		// just skip this one
		if err != nil {
			p.logger.Error("Failed to copy s3 object, skip it.",
				zap.String("path", path),
				zap.Error(err))
			continue
		}
		// delete the s3 object
		_, err = p.s3Client.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(p.cfg.S3.Bucket),
			Key:    aws.String(path),
		})
		if err != nil {
			p.logger.Error("Failed to delete s3 object.",
				zap.String("path", path),
				zap.Error(err))
			continue
		}
		resp.RemovedConfigs = append(resp.RemovedConfigs, path)
		p.logger.Info("Successfully removed config version.", zap.String("path", path))
	}
	p.scope.Gauge("config_remove").Update(float64(len(resp.RemovedConfigs)))
	p.logger.Info("Finished configs cleanup.", zap.Int("total", len(resp.RemovedConfigs)))
	return nil
}

func (p *pczp) validateTargetConfigs(configs map[string]*pinconf.ConfigMetadata) error {
	for key, meta := range configs {
		if key == "" {
			return errors.New("target config key is empty")
		}
		if meta.Location == "" {
			return fmt.Errorf("target config location is empty: %s", key)
		}
		if meta.Digest == "" {
			return fmt.Errorf("target config digest is empty: %s", key)
		}
		u, err := url.Parse(meta.Location)
		if err != nil {
			return fmt.Errorf("malformed location url for target config %s: %v", key, err)
		}
		schema := strings.ToLower(u.Scheme)
		switch schema {
		case constants.EmbeddedSchemaName:
			if len(meta.GetContent()) != int(meta.Length) {
				return fmt.Errorf("content length does not match: %s", key)
			}
			if meta.Length > maxEmbeddedSize {
				return fmt.Errorf("exceed max allowed embedded size: %s", key)
			}
			if meta.Digest != lang.GetSha256String(meta.GetContent()) {
				return fmt.Errorf("digest does not match the embedded content: %s", key)
			}
			// we can re-generate the digest from content
			meta.Digest = ""
		case constants.S3SchemaName:
			// embedded content is only allowed for embedded:// url
			if len(meta.GetContent()) > 0 {
				return fmt.Errorf("content field is not empty: %s", key)
			}
			if p.s3Client == nil {
				return errors.New("S3 client is not setup")
			}
			path := p.targetConfigPath(key, meta.Digest)
			out, err := p.s3Client.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(p.cfg.S3.Bucket),
				Key:    aws.String(path),
			})
			if err != nil {
				return fmt.Errorf("failed to find target config %s: %v", key, err)
			}
			if *out.ContentLength != int64(meta.Length) {
				return fmt.Errorf("content length does not match: %s", key)
			}
			// always re-generate the location here to make sure beacon won't download from unsafe s3 url
			// also remove the digest part to save ZK memory usage since we already store the digest
			meta.Location = p.targetConfigLocation(key)
		default:
			return fmt.Errorf("unsupported location schema: %s", key)
		}
	}
	return nil
}

func mergeScope(scope interface{}, zone string, ver string, root *pinconf.ScopeNode) error {
	m, ok := scope.(map[string]interface{})
	if !ok {
		return fmt.Errorf("scope should only contain struct type")
	}
	if root.Children == nil {
		root.Children = map[string]*pinconf.ScopeNode{}
	}
	for k, v := range m {
		node, ok := root.Children[k]
		// create a new branch
		if !ok {
			node = &pinconf.ScopeNode{}
			root.Children[k] = node
		}
		// nil means leaf node which is the deployment target scope
		if v == nil {
			// default deployment will clear all AZ versions and children versions because of fallback
			if zone == defaultAZ {
				node.AzVersions = map[string]string{defaultAZ: ver}
				node.Children = nil
			} else {
				if node.AzVersions == nil {
					node.AzVersions = map[string]string{}
				}
				node.AzVersions[zone] = ver
				// per AZ deployment need update the same AZ for all existing subtree nodes
				updateAzVersion(node, zone, ver)
			}
			continue
		}
		if err := mergeScope(v, zone, ver, node); err != nil {
			return err
		}
	}
	return nil
}

// 1. make sure each node is either empty AzVersions (using fallback) or complete AZVersions (merged with fallback)
// 2. normalize AzVersions so that there is no duplicate AZ version same as the default AZ version ("" AZ)
// 3. cut subtree when all the AzVersions in the subtree are empty (fallback from parent node)
func reduceTree(root *pinconf.ScopeNode, fallback map[string]string) bool {
	vers := root.AzVersions
	if len(vers) > 0 {
		// when default version exists, it will cover all AZ not in the map,
		// otherwise we need merge with fallback AZ versions
		if _, ok := vers[defaultAZ]; !ok {
			for k, v := range fallback {
				if _, ok := vers[k]; !ok {
					vers[k] = v
				}
			}
		}
		// remove duplicate AZ versions when default version can cover it
		if defaultVer, ok := vers[defaultAZ]; ok {
			for k, v := range vers {
				if k != defaultAZ && v == defaultVer {
					delete(vers, k)
				}
			}
		}
		// if same as fallback, clear again
		if isSameVers(vers, fallback) {
			root.AzVersions = nil
		} else {
			// this will be the new fallback in this subtree
			fallback = vers
		}
	}
	for k, node := range root.Children {
		// subtree all use fallback, we can cut it
		if reduceTree(node, fallback) {
			delete(root.Children, k)
		}
	}
	return len(root.Children) == 0 && len(root.AzVersions) == 0
}

func isSameVers(vers map[string]string, fallback map[string]string) bool {
	if len(vers) != len(fallback) {
		return false
	}
	for k, v := range vers {
		if fallback[k] != v {
			return false
		}
	}
	return true
}

func updateAzVersion(root *pinconf.ScopeNode, zone string, ver string) {
	for _, node := range root.GetChildren() {
		// if it's empty means this node just use fallback version from parent
		if len(node.AzVersions) > 0 {
			node.AzVersions[zone] = ver
		}
		updateAzVersion(node, zone, ver)
	}
}

func (p *pczp) artifactStatPath(name string) string {
	return fmt.Sprintf("%s/%s", p.artifactPath, strings.Replace(name, ".", "/", 1))
}

func (p *pczp) artifactVersionPath(name string, version string) string {
	return fmt.Sprintf("%s/%s/%s", p.artifactPath, strings.Replace(name, ".", "/", 1), version)
}

func (p *pczp) artifactScopeDeployPath(name string) string {
	return fmt.Sprintf("%s/%s", p.scopeDeployPath, strings.Replace(name, ".", "/", 1))
}

func (p *pczp) artifactScopeHistoryPath(name string, num uint64) string {
	return fmt.Sprintf("%s/%s/%016d", p.scopeDeployPath, strings.Replace(name, ".", "/", 1), num)
}

func (p *pczp) artifactScopeReportPath(name string) string {
	return fmt.Sprintf("%s/%s", p.reportPath, strings.Replace(name, ".", "/", 1))
}

func (p *pczp) artifactScopeReportInstancePath(name string, instance string) string {
	return fmt.Sprintf("%s/%s/%s", p.reportPath, strings.Replace(name, ".", "/", 1), instance)
}

func (p *pczp) artifactEventPath(name string) string {
	return fmt.Sprintf("%s/%s", p.eventPath, strings.Replace(name, ".", "/", 1))
}

func (p *pczp) targetConfigPath(name string, digest string) string {
	return fmt.Sprintf("%s/%s/%s", p.cfg.S3.Root, name, digest)
}

func (p *pczp) targetConfigLocation(name string) string {
	return fmt.Sprintf("s3://%s/%s/%s/", p.cfg.S3.Bucket, p.cfg.S3.Root, name)
}

func (p *pczp) commitSnapshotPath(version string) string {
	return fmt.Sprintf("%s/%s", p.commitPath, version)
}

// make sure the artifact name won't cause wrong znode path creation
func validateArtifactName(name string) error {
	if name == "" {
		return errors.New("artifact name is required")
	}
	// we need at least one "." in the middle
	i := strings.Index(name, ".")
	if i <= 0 || i >= len(name)-1 {
		return errors.New("artifact name must contain domain")
	}
	if strings.Index(name, "/") >= 0 {
		return errors.New("artifact name must not contain slash character")
	}
	return nil
}

func (p *pczp) artifactNameFromPath(path string) string {
	// find the second last index of '/'
	first := -1
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if first < 0 {
				first = i
			} else {
				return path[i+1:first] + "." + path[first+1:]
			}
		}
	}
	return path
}

func (p *pczp) evictArtifactVersions(
	ctx context.Context,
	tx *zk.Transaction,
	name string,
	vers []string,
) ([]string, error) {
	// get current deployed versions
	dvers := map[string]bool{}
	dpath := p.artifactScopeDeployPath(name)
	ds := &pinconf.ArtifactScopeDeploymentStatus{}
	val, _, err := p.zkMgr.Get(ctx, dpath)
	switch err {
	case nil:
		if err = proto.Unmarshal(val, ds); err != nil {
			return nil, fmt.Errorf("failed to unmarshal status of artifact %s: %v", name, err)
		}
		walkVersions(ds.Root, dvers)
	case zk.ErrNoNode: // means no deployment for this artifact
	default:
		return nil, fmt.Errorf("failed to get status of artifact %s: %v", name, err)
	}

	// delete old versions
	ret := make([]string, 0, len(vers))
	startIdx := len(vers) - p.cfg.Zookeeper.VersionsToKeep
	for i := 0; i < startIdx; i++ {
		// keep this version if it's still in current deployment tree
		if dvers[vers[i]] {
			ret = append(ret, vers[i])
			continue
		}
		path := p.artifactVersionPath(name, vers[i])
		v, _, err := p.zkMgr.Stat(ctx, path)
		if err == zk.ErrNoNode {
			// it maybe already deleted
			continue
		} else if err != nil {
			return nil, fmt.Errorf("failed to check artifact version %s: %v", path, err)
		}
		tx.Delete(path, v)
	}
	// keep all the newer versions
	ret = append(ret, vers[startIdx:]...)
	return ret, nil
}

func updateRefs(cfgRefs map[string]uint32, atf *pinconf.Artifact, prefixLen int) {
	for _, meta := range atf.Configs {
		// only handle S3 case
		u, err := url.Parse(meta.Location)
		if err != nil {
			// ignore it since it's not a S3 path anyway
			continue
		}
		if constants.S3SchemaName != strings.ToLower(u.Scheme) {
			continue
		}
		if meta.Location[len(meta.Location)-1] == '/' {
			meta.Location += meta.Digest
		}
		// remove the "s3://bucket/" prefix
		cfgRefs[meta.Location[prefixLen:]]++
	}
}

func walkVersions(root *pinconf.ScopeNode, vers map[string]bool) {
	if root == nil {
		return
	}
	for _, v := range root.AzVersions {
		vers[v] = true
	}
	for _, c := range root.Children {
		walkVersions(c, vers)
	}
}

func (p *pczp) evictDeploymentHistory(ctx context.Context, tx *zk.Transaction, name string) error {
	dpath := p.artifactScopeDeployPath(name)
	hist, err := p.zkMgr.Children(ctx, dpath)
	// new artifact have no history
	if err == zk.ErrNoNode {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to list scoped deployment history of artifact %s: %v", name, err)
	}
	if len(hist) >= p.cfg.Zookeeper.VersionsToKeep {
		// we use fixed length number string, so it can be sorted directly
		sort.Strings(hist)
		for i := 0; i <= len(hist)-p.cfg.Zookeeper.VersionsToKeep; i++ {
			path := dpath + "/" + hist[i]
			v, _, err := p.zkMgr.Stat(ctx, path)
			if err != nil {
				// it maybe already deleted, just skip it this time.
				continue
			}
			tx.Delete(path, v)
		}
	}
	return nil
}
