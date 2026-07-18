package api

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	clusterv1 "github.com/zattera-dev/zattera/api/gen/zattera/cluster/v1"
	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
	"github.com/zattera-dev/zattera/internal/pkgutil/ids"
	"github.com/zattera-dev/zattera/internal/state"
)

// AlertServer implements AlertService: CRUD for alert rules and notification
// channels (T-74). The notify engine evaluates the rules on the leader. Channel
// secrets are sealed here; list responses redact them.
type AlertServer struct {
	zatterav1.UnimplementedAlertServiceServer
	store *state.Store
	raft  Applier
	vault *secrets.Vault
	clock clock.Clock
}

// NewAlertServer builds the alert service. sealer may be nil on a sealed node
// (channel writes that carry secrets then fail with FailedPrecondition).
func NewAlertServer(store *state.Store, raft Applier, vault *secrets.Vault, clk clock.Clock) *AlertServer {
	if clk == nil {
		clk = clock.Real{}
	}
	return &AlertServer{store: store, raft: raft, vault: vault, clock: clk}
}

// PutRule creates or updates an alert rule.
func (s *AlertServer) PutRule(ctx context.Context, req *zatterav1.PutRuleRequest) (*zatterav1.AlertRule, error) {
	rule := req.GetRule()
	if strings.TrimSpace(rule.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "rule name is required")
	}
	if err := validateRule(rule); err != nil {
		return nil, err
	}
	now := timestamppb.New(s.clock.Now())
	if rule.GetMeta().GetId() == "" {
		rule.Meta = &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now}
	} else {
		if rule.Meta == nil {
			rule.Meta = &zatterav1.Meta{Id: rule.GetMeta().GetId()}
		}
		rule.GetMeta().UpdatedAt = now
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutAlertRule{PutAlertRule: &clusterv1.PutAlertRule{Rule: rule}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return rule, nil
}

// ListRules returns all alert rules.
func (s *AlertServer) ListRules(_ context.Context, _ *emptypb.Empty) (*zatterav1.ListRulesResponse, error) {
	return &zatterav1.ListRulesResponse{Rules: s.store.ListAlertRules()}, nil
}

// DeleteRule removes an alert rule.
func (s *AlertServer) DeleteRule(ctx context.Context, req *zatterav1.DeleteRuleRequest) (*emptypb.Empty, error) {
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_DeleteAlertRule{DeleteAlertRule: &clusterv1.DeleteByID{Id: req.GetRuleId()}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// PutChannel creates or updates a notification channel, sealing any plaintext
// secrets supplied in the request.
func (s *AlertServer) PutChannel(ctx context.Context, req *zatterav1.PutChannelRequest) (*zatterav1.NotificationChannel, error) {
	ch := req.GetChannel()
	if strings.TrimSpace(ch.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "channel name is required")
	}
	switch ch.GetType() {
	case "webhook", "slack", "email", "telegram":
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown channel type %q (want webhook|slack|email|telegram)", ch.GetType())
	}

	// Carry existing sealed secrets forward on update; seal any new plaintext.
	var prev *zatterav1.NotificationChannel
	if id := ch.GetMeta().GetId(); id != "" {
		for _, c := range s.store.ListNotificationChannels() {
			if c.GetMeta().GetId() == id {
				prev = c
				break
			}
		}
	}
	if err := s.sealChannelSecrets(ch, prev, req); err != nil {
		return nil, err
	}

	now := timestamppb.New(s.clock.Now())
	if ch.GetMeta().GetId() == "" {
		ch.Meta = &zatterav1.Meta{Id: ids.New(), CreatedAt: now, UpdatedAt: now}
	} else {
		if ch.Meta == nil {
			ch.Meta = &zatterav1.Meta{Id: ch.GetMeta().GetId()}
		}
		ch.GetMeta().UpdatedAt = now
	}
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_PutNotificationChannel{PutNotificationChannel: &clusterv1.PutNotificationChannel{Channel: ch}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return redactChannel(ch), nil
}

// ListChannels returns channels with secrets redacted.
func (s *AlertServer) ListChannels(_ context.Context, _ *emptypb.Empty) (*zatterav1.ListChannelsResponse, error) {
	chs := s.store.ListNotificationChannels()
	out := make([]*zatterav1.NotificationChannel, 0, len(chs))
	for _, c := range chs {
		out = append(out, redactChannel(c))
	}
	return &zatterav1.ListChannelsResponse{Channels: out}, nil
}

// DeleteChannel removes a notification channel.
func (s *AlertServer) DeleteChannel(ctx context.Context, req *zatterav1.DeleteChannelRequest) (*emptypb.Empty, error) {
	if err := s.apply(ctx, &clusterv1.Command{
		Mutation: &clusterv1.Command_DeleteNotificationChannel{DeleteNotificationChannel: &clusterv1.DeleteByID{Id: req.GetChannelId()}},
	}); err != nil {
		return nil, toStatus(err)
	}
	return &emptypb.Empty{}, nil
}

// sealChannelSecrets seals plaintext secrets from the request into ch, preserving
// prior sealed values when no new plaintext is supplied.
func (s *AlertServer) sealChannelSecrets(ch, prev *zatterav1.NotificationChannel, req *zatterav1.PutChannelRequest) error {
	seal := func(plain string, prior *zatterav1.EncryptedValue) (*zatterav1.EncryptedValue, error) {
		if plain == "" {
			return prior, nil // keep whatever was already stored
		}
		if !s.vault.Unsealed() {
			return nil, status.Error(codes.FailedPrecondition, "cluster key is not unsealed; cannot store channel secrets")
		}
		return s.vault.Seal([]byte(plain))
	}

	switch ch.GetType() {
	case "webhook":
		v, err := seal(req.GetWebhookSecretPlain(), prev.GetWebhookSecret())
		if err != nil {
			return err
		}
		ch.WebhookSecret = v
	case "slack":
		v, err := seal(req.GetSlackWebhookUrlPlain(), prev.GetSlackWebhookUrl())
		if err != nil {
			return err
		}
		if v == nil {
			return status.Error(codes.InvalidArgument, "slack channel requires slack_webhook_url_plain")
		}
		ch.SlackWebhookUrl = v
	case "email":
		if ch.GetSmtp() == nil {
			return status.Error(codes.InvalidArgument, "email channel requires smtp settings")
		}
		v, err := seal(req.GetSmtpPasswordPlain(), prev.GetSmtp().GetPassword())
		if err != nil {
			return err
		}
		ch.GetSmtp().Password = v
	case "telegram":
		if ch.GetTelegramChatId() == "" {
			return status.Error(codes.InvalidArgument, "telegram channel requires telegram_chat_id")
		}
		v, err := seal(req.GetTelegramBotTokenPlain(), prev.GetTelegramBotToken())
		if err != nil {
			return err
		}
		if v == nil {
			return status.Error(codes.InvalidArgument, "telegram channel requires telegram_bot_token_plain")
		}
		ch.TelegramBotToken = v
	}
	return nil
}

// validateRule checks a rule is a well-formed metric or event rule.
func validateRule(r *zatterav1.AlertRule) error {
	hasMetric := r.GetMetric().GetMetric() != ""
	hasEvent := r.GetEventKind() != ""
	if hasMetric == hasEvent {
		return status.Error(codes.InvalidArgument, "a rule must set exactly one of a metric condition or an event_kind")
	}
	if hasMetric {
		switch r.GetMetric().GetOp() {
		case ">", ">=", "<", "<=":
		default:
			return status.Errorf(codes.InvalidArgument, "invalid metric op %q (want > >= < <=)", r.GetMetric().GetOp())
		}
	}
	return nil
}

// redactChannel returns a copy of ch with secret values stripped (list/put echo).
func redactChannel(c *zatterav1.NotificationChannel) *zatterav1.NotificationChannel {
	out := clone(c)
	out.WebhookSecret = nil
	out.SlackWebhookUrl = nil
	out.TelegramBotToken = nil
	if out.GetSmtp() != nil {
		out.GetSmtp().Password = nil
	}
	return out
}

func (s *AlertServer) apply(ctx context.Context, cmd *clusterv1.Command) error {
	id, _ := IdentityFrom(ctx)
	cmd.RequestId = ids.New()
	cmd.Actor = id.Actor()
	cmd.Time = timestamppb.New(s.clock.Now())
	return s.raft.Apply(ctx, cmd)
}
