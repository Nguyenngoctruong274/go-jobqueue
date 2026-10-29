-- Listing jobs by state, newest first, pages through this index with a
-- keyset on id. It mostly serves the dead-letter view, which must stay
-- fast however many finished jobs the table holds. Jobs change state on
-- every attempt, so this costs one more index entry per transition; the
-- claim index's state predicate already rules out HOT updates.
CREATE INDEX jobs_state_id ON jobs (state, id);
