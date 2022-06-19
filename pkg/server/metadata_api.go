package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v2"

	svc "appmeta/api/service/v1"
	"appmeta/pkg/data"
)

type ma struct {
	svc.UnimplementedApiServiceServer

	logger  *zap.Logger
	scope   tally.Scope
	hserver *http.Server
}

func NewServer(
	logger *zap.Logger,
	scope tally.Scope,
	address string,
) *ma {
	return &ma{
		logger:  logger,
		scope:   scope,
		hserver: &http.Server{Addr: address},
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
	m.logger.Debug("Start serving application metadata API gateway...",
		zap.String("address", m.hserver.Addr))
	if err := m.hserver.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		m.logger.Fatal("Failed to start gateway", zap.Error(err))
	}

	if err := m.hserver.Close(); err != nil {
		m.logger.Error("Failed to close gateway.", zap.Error(err))
	}

	return nil
}

func (m *ma) AddMetadata(
	ctx context.Context,
	req *svc.AddMetadataRequest,
) (resp *svc.AddMetadataResponse, err error) {
	resp = &svc.AddMetadataResponse{}
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
		return nil
	}()

	return
}
