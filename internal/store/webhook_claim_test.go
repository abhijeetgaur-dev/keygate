package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/tabloy/keygate/internal/model"
)

// Every replica runs the webhook retry loop. Two of them must never
// take the same delivery: the claim is part of the statement that reads
// it, so the second caller sees the rest of the queue instead.
func TestClaimPendingDeliveriesIsExclusive(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("skipping integration test: TEST_DATABASE_URL not set")
	}
	s, err := New(dsn)
	if err != nil {
		t.Skipf("skipping integration test: %v", err)
	}
	defer s.Close()
	if err := s.RunMigrations("../../db/migrations"); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	ctx := context.Background()
	suffix := time.Now().Format("150405.000000")
	prod := &model.Product{Name: "Claim", Slug: "whclaim-" + suffix, Type: "desktop"}
	if err := s.CreateProduct(ctx, prod); err != nil {
		t.Fatal(err)
	}
	wh := &model.Webhook{ProductID: prod.ID, URL: "https://hooks.example.com/" + suffix, Secret: "s",
		Events: []string{"license.created"}, Active: true}
	if err := s.CreateWebhook(ctx, wh); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := s.DB.NewRaw("DELETE FROM webhook_deliveries WHERE webhook_id = ?", wh.ID).Exec(ctx); err != nil {
			t.Errorf("clean up: %v", err)
		}
	}()

	// A new delivery is held by its creator until its lease runs out.
	lease := time.Now().Add(time.Minute)
	for range 3 {
		d := &model.WebhookDelivery{WebhookID: wh.ID, Event: "license.created", Status: "pending", NextRetry: &lease}
		if err := s.CreateWebhookDelivery(ctx, d); err != nil {
			t.Fatal(err)
		}
		if d.ClaimToken == "" {
			t.Fatal("a new delivery is not held by its creator")
		}
	}
	// Sort them ahead of the rest of the shared queue, and hand back
	// any other row a claim happened to take.
	if _, err := s.DB.NewRaw("UPDATE webhook_deliveries SET created_at = '1970-01-01' WHERE webhook_id = ?", wh.ID).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	mine := func(got []*model.WebhookDelivery) []*model.WebhookDelivery {
		t.Helper()
		var out []*model.WebhookDelivery
		for _, d := range got {
			if d.WebhookID == wh.ID {
				out = append(out, d)
			} else if _, err := s.DB.NewRaw("UPDATE webhook_deliveries SET next_retry = NULL WHERE id = ?", d.ID).Exec(ctx); err != nil {
				t.Errorf("release %s: %v", d.ID, err)
			}
		}
		return out
	}
	claim := func(limit int) []*model.WebhookDelivery {
		t.Helper()
		got, err := s.ClaimPendingDeliveries(ctx, limit, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return mine(got)
	}

	if got := claim(10); len(got) != 0 {
		t.Fatalf("deliveries held by their creator were on offer: %d", len(got))
	}
	expire := func() {
		t.Helper()
		if _, err := s.DB.NewRaw("UPDATE webhook_deliveries SET next_retry = now() - interval '1 minute' WHERE webhook_id = ?", wh.ID).Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expire()
	a, b := claim(2), claim(10)
	if len(a) != 2 || len(b) != 1 {
		t.Fatalf("claims: first=%d second=%d want 2 and 1", len(a), len(b))
	}
	for _, x := range a {
		if x.ID == b[0].ID {
			t.Fatalf("both callers took %s", x.ID)
		}
		if x.ClaimToken == b[0].ClaimToken {
			t.Fatal("two claims share a token")
		}
	}
	if got := claim(10); len(got) != 0 {
		t.Fatalf("claimed deliveries were still on offer: %d", len(got))
	}

	// Only the holder's token renews the lease.
	if held, err := s.RenewWebhookClaim(ctx, b[0].ID, "not-the-holder", time.Minute); err != nil || held {
		t.Fatalf("renew by a stranger: held %v, %v", held, err)
	}
	if held, err := s.RenewWebhookClaim(ctx, b[0].ID, b[0].ClaimToken, time.Minute); err != nil || !held {
		t.Fatalf("renew by the holder: held %v, %v", held, err)
	}

	// A lease that runs out releases the row: it is a timestamp, not a
	// lock held by a connection.
	expire()
	again := claim(10)
	if len(again) != 3 {
		t.Fatalf("expired leases: got %d want 3", len(again))
	}
	// The earlier holder's result is refused; the current one closes it.
	old := b[0]
	old.Status = "delivered"
	if held, err := s.FinishWebhookDelivery(ctx, old); err != nil || held {
		t.Fatalf("finish by a holder whose lease ran out: held %v, %v", held, err)
	}
	for _, d := range again {
		if d.ID == old.ID {
			d.Status = "delivered"
			if held, err := s.FinishWebhookDelivery(ctx, d); err != nil || !held {
				t.Fatalf("finish by the holder: held %v, %v", held, err)
			}
		}
	}
	expire()
	if got := claim(10); len(got) != 2 {
		t.Fatalf("after one was delivered: got %d want 2", len(got))
	}
}
