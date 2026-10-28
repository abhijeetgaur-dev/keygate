-- Who holds a webhook delivery that is being sent. Every replica runs
-- the retry loop; the row is claimed in the statement that reads it,
-- with a lease in next_retry and this token. The token is what the
-- holder finishes the row by: a sender that stalled past its lease
-- finds the delivery taken over and changes nothing, instead of
-- writing its result over the new holder's and sending it again.
ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS claim_token TEXT NOT NULL DEFAULT '';
