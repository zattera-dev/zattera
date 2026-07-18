package api

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	zatterav1 "github.com/zattera-dev/zattera/api/gen/zattera/v1"
	"github.com/zattera-dev/zattera/internal/daemon/raftstore"
	"github.com/zattera-dev/zattera/internal/daemon/secrets"
	"github.com/zattera-dev/zattera/internal/pkgutil/clock"
)

func newAlertSrv(t *testing.T) (*AlertServer, context.Context) {
	t.Helper()
	rs := raftstore.NewTestStore(t)
	dataKey, _ := secrets.GenerateDataKey()
	vault := mustVault(mustKeyring(dataKey, 1))
	srv := NewAlertServer(rs.State(), rs, vault, clock.NewFake())
	return srv, withIdentity(context.Background(), Identity{UserID: "u1"})
}

func TestAlertRuleCRUD(t *testing.T) {
	srv, ctx := newAlertSrv(t)

	// Metric rule.
	r, err := srv.PutRule(ctx, &zatterav1.PutRuleRequest{Rule: &zatterav1.AlertRule{
		Name:   "disk",
		Metric: &zatterav1.MetricCondition{Metric: "disk_percent", Scope: "cluster", Op: ">", Threshold: 90},
	}})
	if err != nil {
		t.Fatalf("put rule: %v", err)
	}
	if r.GetMeta().GetId() == "" {
		t.Fatal("rule id not assigned")
	}
	list, _ := srv.ListRules(ctx, nil)
	if len(list.GetRules()) != 1 {
		t.Fatalf("want 1 rule, got %d", len(list.GetRules()))
	}
	if _, err := srv.DeleteRule(ctx, &zatterav1.DeleteRuleRequest{RuleId: r.GetMeta().GetId()}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	list, _ = srv.ListRules(ctx, nil)
	if len(list.GetRules()) != 0 {
		t.Fatalf("rule not deleted: %d", len(list.GetRules()))
	}
}

func TestAlertRuleValidation(t *testing.T) {
	srv, ctx := newAlertSrv(t)
	// Both metric and event → invalid.
	_, err := srv.PutRule(ctx, &zatterav1.PutRuleRequest{Rule: &zatterav1.AlertRule{
		Name: "bad", EventKind: "deploy.failed",
		Metric: &zatterav1.MetricCondition{Metric: "cpu_percent", Op: ">", Threshold: 1},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for dual rule, got %v", status.Code(err))
	}
	// Bad op.
	_, err = srv.PutRule(ctx, &zatterav1.PutRuleRequest{Rule: &zatterav1.AlertRule{
		Name: "bad2", Metric: &zatterav1.MetricCondition{Metric: "cpu_percent", Op: "=~", Threshold: 1},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for bad op, got %v", status.Code(err))
	}
}

func TestAlertChannelSealsAndRedacts(t *testing.T) {
	srv, ctx := newAlertSrv(t)

	ch, err := srv.PutChannel(ctx, &zatterav1.PutChannelRequest{
		Channel:            &zatterav1.NotificationChannel{Name: "ops", Type: "webhook", WebhookUrlPlain: "https://hooks.example/x"},
		WebhookSecretPlain: "topsecret",
	})
	if err != nil {
		t.Fatalf("put channel: %v", err)
	}
	// Response is redacted.
	if ch.GetWebhookSecret() != nil {
		t.Fatal("PutChannel response leaked the sealed secret")
	}
	// Stored channel has a sealed (non-plaintext) secret.
	var stored *zatterav1.NotificationChannel
	for _, c := range srv.store.ListNotificationChannels() {
		if c.GetMeta().GetId() == ch.GetMeta().GetId() {
			stored = c
		}
	}
	if stored.GetWebhookSecret() == nil {
		t.Fatal("secret not stored")
	}
	if string(stored.GetWebhookSecret().GetCiphertext()) == "topsecret" {
		t.Fatal("secret stored in plaintext")
	}
	// ListChannels redacts.
	lc, _ := srv.ListChannels(ctx, nil)
	if len(lc.GetChannels()) != 1 || lc.GetChannels()[0].GetWebhookSecret() != nil {
		t.Fatal("ListChannels leaked the secret")
	}

	// Update without a new plaintext keeps the sealed value.
	up, err := srv.PutChannel(ctx, &zatterav1.PutChannelRequest{
		Channel: &zatterav1.NotificationChannel{Meta: &zatterav1.Meta{Id: ch.GetMeta().GetId()}, Name: "ops2", Type: "webhook", WebhookUrlPlain: "https://hooks.example/y"},
	})
	if err != nil {
		t.Fatalf("update channel: %v", err)
	}
	for _, c := range srv.store.ListNotificationChannels() {
		if c.GetMeta().GetId() == up.GetMeta().GetId() && c.GetWebhookSecret() == nil {
			t.Fatal("update dropped the existing sealed secret")
		}
	}
}

func TestAlertTelegramChannel(t *testing.T) {
	srv, ctx := newAlertSrv(t)

	// Missing chat id → rejected.
	if _, err := srv.PutChannel(ctx, &zatterav1.PutChannelRequest{
		Channel:               &zatterav1.NotificationChannel{Name: "tg", Type: "telegram"},
		TelegramBotTokenPlain: "123:abc",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument without chat id, got %v", status.Code(err))
	}

	ch, err := srv.PutChannel(ctx, &zatterav1.PutChannelRequest{
		Channel:               &zatterav1.NotificationChannel{Name: "tg", Type: "telegram", TelegramChatId: "-1001"},
		TelegramBotTokenPlain: "123:abc",
	})
	if err != nil {
		t.Fatalf("put telegram channel: %v", err)
	}
	if ch.GetTelegramBotToken() != nil {
		t.Fatal("PutChannel leaked the telegram bot token")
	}
	// Stored token is sealed (not plaintext); chat id is retained.
	var stored *zatterav1.NotificationChannel
	for _, c := range srv.store.ListNotificationChannels() {
		if c.GetMeta().GetId() == ch.GetMeta().GetId() {
			stored = c
		}
	}
	if stored.GetTelegramBotToken() == nil || string(stored.GetTelegramBotToken().GetCiphertext()) == "123:abc" {
		t.Fatal("telegram token not sealed")
	}
	if stored.GetTelegramChatId() != "-1001" {
		t.Fatalf("chat id not stored: %q", stored.GetTelegramChatId())
	}
}

func TestAlertChannelUnknownType(t *testing.T) {
	srv, ctx := newAlertSrv(t)
	_, err := srv.PutChannel(ctx, &zatterav1.PutChannelRequest{
		Channel: &zatterav1.NotificationChannel{Name: "x", Type: "carrier-pigeon"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument for unknown type, got %v", status.Code(err))
	}
}
