-- Wake workers when a job becomes runnable, instead of making them poll.
--
-- NOTIFY is transactional: the message is delivered when the transaction
-- that raised it commits, and never if it rolls back. A producer that
-- enqueues inside its own transaction therefore cannot wake a worker for
-- a job that does not exist yet (or never will), and the worker's claim
-- will see the committed row. The payload is "<type> <run_at as Unix
-- microseconds>", so a worker can ignore types it has no handler for and
-- sleep until a delayed job or a retry is due instead of until its next
-- poll.
CREATE FUNCTION jobs_notify_pending() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('jobqueue_pending',
        NEW.type || ' ' || (extract(epoch FROM NEW.run_at) * 1000000)::bigint::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_notify_pending
    AFTER INSERT OR UPDATE OF state ON jobs
    FOR EACH ROW
    WHEN (NEW.state = 'pending')
    EXECUTE FUNCTION jobs_notify_pending();
