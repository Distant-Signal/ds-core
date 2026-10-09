SET LOCAL lock_timeout = '5s';

-- -------------------------------------------------------------------------
-- enricher_llm_batches: the enricher's in-flight Claude Message Batches
-- (LLM_SWEEP_MODE=batch, docs/enricher-anthropic.md "Batch mode").
--
-- A sweep that finds enough stale incidents submits their primary
-- extraction calls as one Message Batch and records it here; the poll loop
-- reads every row, polls the batch, and once it has ended submits the
-- adversarial calls as a second batch (stage `adversarial`, a new row in the
-- same transaction that deletes the first) and finally writes the
-- extractions and deletes the row. So a restart resumes polling the
-- batches listed here instead of submitting them again, and the sweep skips
-- every incident listed in a row.
--
-- items: one object per incident, {index, incident_id, text_hash, summary,
-- description, reference_date, primary_content?}: the text the batch was
-- built from (the adversarial stage and the final write need exactly that
-- text) and, in the adversarial stage, the primary pass's raw output.
-- Public Knowledgebase incident text only; no personal data.
--
-- Expand-only: a new table nothing else reads.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS enricher_llm_batches (
    batch_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('primary', 'adversarial')),
    model_version TEXT NOT NULL,
    items JSONB NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
