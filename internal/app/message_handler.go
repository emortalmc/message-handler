package app

import (
	"context"
	"fmt"
	"github.com/emortalmc/proto-specs/gen/go/grpc/messagehandler"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"message-handler/internal/clients"
	"message-handler/internal/config"
	"message-handler/internal/kafka"
	"message-handler/internal/service"
	"net"
)

func Run(cfg *config.Config, logger *zap.SugaredLogger) {
	relClient, err := clients.NewRelationshipClient(cfg.RelationshipService)
	if err != nil {
		logger.Fatalw("failed to connect to relationship service", err)
	}

	permClient, err := clients.NewPermissionClient(cfg.PermissionService)
	if err != nil {
		logger.Fatalw("failed to connect to permission service", err)
	}

	badgeClient, err := clients.NewBadgeClient(cfg.BadgeService)
	if err != nil {
		logger.Fatalw("failed to connect to badge service", err)
	}

	notif := kafka.NewKafkaNotifier(cfg.Kafka, logger)

	ctx := context.Background()
	kafka.NewConsumer(ctx, cfg.Kafka, logger, notif, permClient, badgeClient)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		logger.Fatalw("failed to listen", err)
	}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpczap.UnaryServerInterceptor(logger.Desugar(), grpczap.WithLevels(func(code codes.Code) zapcore.Level {
			if code != codes.Internal && code != codes.Unavailable && code != codes.Unknown {
				return zapcore.DebugLevel
			} else {
				return zapcore.ErrorLevel
			}
		})),
	))

	if cfg.Development {
		reflection.Register(s)
	}

	messagehandler.RegisterMessageHandlerServer(s, service.NewMessageHandlerService(notif, relClient))
	logger.Infow("listening on port", "port", cfg.Port)

	err = s.Serve(lis)
	if err != nil {
		logger.Fatalw("failed to serve", err)
		return
	}
}
