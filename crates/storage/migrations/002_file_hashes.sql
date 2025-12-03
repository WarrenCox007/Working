-- Add separate fast/full hash columns to files.
ALTER TABLE files ADD COLUMN fast_hash TEXT;
ALTER TABLE files ADD COLUMN full_hash TEXT;
