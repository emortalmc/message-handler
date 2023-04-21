package kafka

import (
	"bytes"
	"context"
	"fmt"
	"github.com/emortalmc/proto-specs/gen/go/grpc/badge"
	"github.com/emortalmc/proto-specs/gen/go/grpc/permission"
	"github.com/emortalmc/proto-specs/gen/go/message/common"
	permmsg "github.com/emortalmc/proto-specs/gen/go/message/permission"
	pbmodel "github.com/emortalmc/proto-specs/gen/go/model/messagehandler"
	permmodel "github.com/emortalmc/proto-specs/gen/go/model/permission"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"message-handler/internal/config"
	"text/template"
	"time"
)

const consumeMessagesTopic = "mc-messages"
const consumePermissionsTopic = "permissions"

type consumer struct {
	logger *zap.SugaredLogger

	msgReader  *kafka.Reader
	permReader *kafka.Reader
	notifier   Notifier

	permClient  permission.PermissionServiceClient
	badgeClient badge.BadgeManagerClient

	roleCache map[string]*permmodel.Role
}

func NewConsumer(ctx context.Context, cfg *config.KafkaConfig, logger *zap.SugaredLogger, notifier Notifier,
	permClient permission.PermissionServiceClient, badgeClient badge.BadgeManagerClient) {

	msgReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		GroupID: "message-handler-service",
		Topic:   consumeMessagesTopic,
	})

	permReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		GroupID: "message-handler-service",
		Topic:   consumePermissionsTopic,
	})

	c := &consumer{
		logger: logger,

		msgReader:  msgReader,
		permReader: permReader,
		notifier:   notifier,

		permClient:  permClient,
		badgeClient: badgeClient,

		roleCache: make(map[string]*permmodel.Role),
	}

	c.cacheRoles(ctx)
	logger.Infow("cached roles", "count", len(c.roleCache))

	logger.Infow("listening for messages on topic", "topic", consumeMessagesTopic)
	go c.consumeMessages(ctx)
	logger.Infow("listening for permissions on topic", "topic", consumePermissionsTopic)
	go c.consumePermissions(ctx)
}

func (c *consumer) cacheRoles(ctx context.Context) {
	res, err := c.permClient.GetAllRoles(ctx, &permission.GetAllRolesRequest{})
	if err != nil {
		c.logger.Panicf("failed to get all roles: %v", err)
	}

	for _, role := range res.Roles {
		c.roleCache[role.Id] = role
	}
}

func (c *consumer) consumeMessages(ctx context.Context) {
	for {
		m, err := c.msgReader.ReadMessage(ctx)
		if err != nil {
			panic(err)
		}

		var protoType string
		for _, header := range m.Headers {
			if header.Key == "X-Proto-Type" {
				protoType = string(header.Value)
			}
		}
		if protoType == "" {
			c.logger.Warnw("no proto type found in message headers")
			continue
		}

		if protoType != string((&common.PlayerChatMessageMessage{}).ProtoReflect().Descriptor().FullName()) {
			continue
		}

		c.handlePlayerChatMessage(ctx, &m)
	}
}

var chatTemplate = template.Must(template.New("chat").Parse("{{if .Badge}}{{.Badge}} {{end}}{{.DisplayName}}: {{.Text}}"))

type chatTemplateData struct {
	Badge       string
	DisplayName string
	Text        string
}

func (c *consumer) handlePlayerChatMessage(ctx context.Context, m *kafka.Message) {
	var msg common.PlayerChatMessageMessage
	if err := proto.Unmarshal(m.Value, &msg); err != nil {
		c.logger.Errorw("failed to unmarshal message", err)
		return
	}

	originalMessage := msg.Message

	// 1. Get active badge
	// 2. Get active prefix
	// 3. Get active username
	// Process in chat template :D
	badgePart, err := c.getPlayerBadge(ctx, originalMessage.SenderId)
	if err != nil {
		c.logger.Errorw("failed to get player badgePart", err) // Log but continue
	}

	displayNamePart, err := c.getDisplayUsername(ctx, originalMessage.SenderId, originalMessage.SenderUsername)
	if err != nil {
		c.logger.Errorw("failed to get player displayNamePart", err) // Log but continue
	}

	content, err := createMessage(&chatTemplateData{
		Badge:       badgePart,
		DisplayName: displayNamePart,
		Text:        originalMessage.Message,
	})

	if err != nil {
		c.logger.Errorw("failed to create chat message", err)
		return
	}

	if err := c.notifier.ChatMessageCreated(ctx, &pbmodel.ChatMessage{
		SenderId: originalMessage.SenderId,
		Message:  content,
	}); err != nil {
		c.logger.Errorw("failed to notify chat message created", err)
	}
}

func createMessage(data *chatTemplateData) (string, error) {
	var buf bytes.Buffer
	if err := chatTemplate.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute chat template: %w", err)
	}
	return buf.String(), nil
}

// getPlayerBadge returns a string (including a space if a badge is present) representing the player's active badge.
// If no badge is present, an empty string is returned.
// If there is an error, the string "?? " is returned.
func (c *consumer) getPlayerBadge(ctx context.Context, playerId string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	res, err := c.badgeClient.GetActivePlayerBadge(ctx, &badge.GetActivePlayerBadgeRequest{
		PlayerId: playerId,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok {
			if s.Code() == codes.NotFound {
				return "", nil
			}
			return "??", fmt.Errorf("failed to get player badge (status: %s): %w", s.Code(), err)
		}
		return "??", fmt.Errorf("failed to get player badge (status: unknown): %w", err)
	}

	if res.Badge == nil {
		return "", nil
	}

	return res.Badge.ChatCharacter, nil
}

func (c *consumer) getDisplayUsername(ctx context.Context, playerId string, username string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	res, err := c.permClient.GetPlayerRoles(ctx, &permission.GetPlayerRolesRequest{
		PlayerId: playerId,
	})
	if err != nil {
		return username, fmt.Errorf("failed to get player roles: %w", err)
	}

	if res.ActiveDisplayNameRoleId == nil {
		return username, nil
	}

	role, ok := c.roleCache[*res.ActiveDisplayNameRoleId]
	if !ok {
		return username, fmt.Errorf("failed to find role with id %s", *res.ActiveDisplayNameRoleId)
	}
	if role.DisplayName == nil {
		return username, fmt.Errorf("role with id %s has no display name (but should have?)", *res.ActiveDisplayNameRoleId)
	}

	t, err := template.New("displayname").Parse(*role.DisplayName)
	if err != nil {
		return username, fmt.Errorf("failed to parse display name template: %w", err)
	}

	var buf bytes.Buffer
	if err := t.Execute(&buf, struct{ Username string }{Username: username}); err != nil {
		return username, fmt.Errorf("failed to execute display name template: %w", err)
	}

	return buf.String(), nil
}

func (c *consumer) consumePermissions(ctx context.Context) {
	for {
		m, err := c.permReader.ReadMessage(ctx)
		if err != nil {
			panic(err)
		}

		var protoType string
		for _, header := range m.Headers {
			if header.Key == "X-Proto-Type" {
				protoType = string(header.Value)
			}
		}
		if protoType == "" {
			c.logger.Warnw("no proto type found in message headers")
			continue
		}

		if protoType != string((&permmsg.RoleUpdateMessage{}).ProtoReflect().Descriptor().FullName()) {
			continue
		}

		var msg permmsg.RoleUpdateMessage
		if err := proto.Unmarshal(m.Value, &msg); err != nil {
			c.logger.Errorw("failed to unmarshal RoleUpdateMessage", err)
			return
		}

		switch msg.ChangeType {
		case permmsg.RoleUpdateMessage_CREATE, permmsg.RoleUpdateMessage_MODIFY:
			c.roleCache[msg.Role.Id] = msg.Role
		case permmsg.RoleUpdateMessage_DELETE:
			delete(c.roleCache, msg.Role.Id)
		}
	}
}
