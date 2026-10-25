-- A separate database for `make test-db`, so tests never touch local data.
CREATE DATABASE jobs_test OWNER jobs;
