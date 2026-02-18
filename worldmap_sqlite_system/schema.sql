PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS persons (
  person_id INTEGER PRIMARY KEY AUTOINCREMENT,
  first_name TEXT,
  middle_name TEXT,
  last_name TEXT,
  full_name_original TEXT,
  full_name_normalized TEXT,
  name_key TEXT,
  phone_original TEXT,
  phone_normalized TEXT,
  national_id_original TEXT,
  national_id_normalized TEXT,
  country TEXT,
  church_name TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_persons_phone_norm ON persons(phone_normalized);
CREATE UNIQUE INDEX IF NOT EXISTS ux_persons_id_norm ON persons(national_id_normalized);

CREATE INDEX IF NOT EXISTS ix_persons_name_norm ON persons(full_name_normalized);
CREATE INDEX IF NOT EXISTS ix_persons_name_key ON persons(name_key);
CREATE INDEX IF NOT EXISTS ix_persons_country ON persons(country);

CREATE TABLE IF NOT EXISTS applications (
  application_id INTEGER PRIMARY KEY AUTOINCREMENT,
  submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
  source_file TEXT,

  full_name_original TEXT,
  phone_original TEXT,
  national_id_original TEXT,
  country TEXT,
  church_name TEXT,
  title TEXT,
  congregation_size INTEGER,
  requested_language TEXT,
  received_before TEXT,
  received_before_reason TEXT,

  full_name_normalized TEXT,
  name_key TEXT,
  phone_normalized TEXT,
  national_id_normalized TEXT,

  is_disqualified INTEGER NOT NULL DEFAULT 0,
  disqualify_reason TEXT,
  needs_review INTEGER NOT NULL DEFAULT 0,
  system_flags TEXT,

  status TEXT NOT NULL DEFAULT 'PENDING',
  admin_notes TEXT,
  matched_person_id INTEGER,

  FOREIGN KEY (matched_person_id) REFERENCES persons(person_id)
);

CREATE INDEX IF NOT EXISTS ix_applications_status ON applications(status);
CREATE INDEX IF NOT EXISTS ix_applications_phone_norm ON applications(phone_normalized);
CREATE INDEX IF NOT EXISTS ix_applications_id_norm ON applications(national_id_normalized);
CREATE INDEX IF NOT EXISTS ix_applications_name_norm ON applications(full_name_normalized);

CREATE TABLE IF NOT EXISTS issuances (
  issuance_id INTEGER PRIMARY KEY AUTOINCREMENT,
  person_id INTEGER NOT NULL,
  issued_at TEXT NOT NULL,
  book_name TEXT NOT NULL,
  language TEXT NOT NULL,
  issued_by TEXT,
  notes TEXT,
  is_exception INTEGER NOT NULL DEFAULT 0,
  exception_type TEXT,
  exception_reason TEXT,
  FOREIGN KEY (person_id) REFERENCES persons(person_id)
);

CREATE INDEX IF NOT EXISTS ix_issuances_person_date ON issuances(person_id, issued_at);


-- Batches: track each committed import batch
CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  source_label TEXT,
  source_files TEXT,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS ix_batches_created_at ON batches(created_at);
