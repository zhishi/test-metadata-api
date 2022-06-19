package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v2"

	svc "appmeta/api/service/v1"
	"appmeta/pkg/data"
	"appmeta/pkg/storage"
)

type ma struct {
	svc.UnimplementedApiServiceServer

	logger  *zap.Logger
	scope   tally.Scope
	hserver *http.Server
	st      storage.MetadataPersistence
}

func NewServer(
	logger *zap.Logger,
	scope tally.Scope,
	address string,
	st storage.MetadataPersistence,
) *ma {
	return &ma{
		logger:  logger,
		scope:   scope,
		hserver: &http.Server{Addr: address},
		st:      st,
	}
}

func (m *ma) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	gwmux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.HTTPBodyMarshaler{
			Marshaler: &runtime.JSONPb{
				MarshalOptions: protojson.MarshalOptions{
					UseProtoNames:   true,
					EmitUnpopulated: true,
				},
				UnmarshalOptions: protojson.UnmarshalOptions{
					DiscardUnknown: false,
				},
			},
		}),
	)
	if err := svc.RegisterApiServiceHandlerServer(ctx, gwmux, m); err != nil {
		panic("failed to start service")
	}

	mux.Handle("/", gwmux)
	m.hserver.Handler = mux

	// Start serving
	m.logger.Debug("Start serving application metadata API gateway over HTTP 1.1 ...",
		zap.String("address", m.hserver.Addr))
	if err := m.hserver.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		m.logger.Fatal("Failed to start gateway", zap.Error(err))
	}

	if err := m.hserver.Close(); err != nil {
		m.logger.Error("Failed to close gateway.", zap.Error(err))
	}

	return nil
}

func (m *ma) UploadMetadata(
	ctx context.Context,
	req *svc.UploadMetadataRequest,
) (resp *svc.UploadMetadataResponse, err error) {
	resp = &svc.UploadMetadataResponse{}
	err = func() error {
		if req == nil {
			return errors.New("request is required")
		}
		if req.Metadata == "" {
			return errors.New("metadata content is required")
		}

		md := &data.AppMetadata{}
		if err := yaml.Unmarshal([]byte(req.Metadata), md); err != nil {
			return fmt.Errorf("failed to unmarshal yaml content: %v", err)
		}
		if err := md.Validate(); err != nil {
			return err
		}

		return m.st.UploadMetadata(
			ctx,
			md.Website,
			req.Metadata,
			time.Now(),
			md,
		)
	}()

	return
}

type byWebsite []*svc.MetadataRecord

func (w byWebsite) Len() int {
	return len(w)
}
func (w byWebsite) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}
func (w byWebsite) Less(i, j int) bool {
	return w[i].Website < w[j].Website
}

func (m *ma) SearchMetadata(
	ctx context.Context,
	req *svc.SearchMetadataRequest,
) (resp *svc.SearchMetadataResponse, err error) {
	resp = &svc.SearchMetadataResponse{}
	err = func() error {
		if req == nil {
			return errors.New("request is required")
		}
		// TODO: do strict syntax validation on the query string here
		if req.Query == "" {
			return errors.New("query string is required")
		}

		res, err := m.st.SearchMetadata(ctx, req.Query)
		if err != nil {
			return err
		}
		// sort the result based on website key
		sort.Sort(byWebsite(res))
		// handle paging
		nps := ""
		end := len(res)
		// if can't meet the PageStart then make sure it return empty slice
		start := end
		// TODO: may use binary search if the list is very big
		for i := 0; i < end; i++ {
			if strings.Compare(res[i].Website, req.PageStart) >= 0 {
				start = i
				if req.PageSize > 0 && int(req.PageSize) < end-start {
					end = start + int(req.PageSize)
					nps = res[end].Website
				}
				break
			}
		}
		resp.NextPageStart = nps
		resp.Results = res[start:end]
		return nil
	}()

	return
}
