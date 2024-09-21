CREATE TABLE users (
  id INT NOT NULL PRIMARY KEY,
  email TEXT NOT NULL
);

CREATE TABLE organizations (
  id INT NOT NULL PRIMARY KEY,
  name TEXT NOT NULL,
  address TEXT NOT NULL
);

CREATE TABLE organization_users (
  id INT NOT NULL PRIMARY KEY,
  organization_id INT NOT NULL REFERENCES organizations(id),
  user_id INT NOT NULL REFERENCES users(id),
  UNIQUE (organization_id, user_id)
);

-- This should not be joined unless explicitly called out.
CREATE TABLE deep_table (
  id INT NOT NULL PRIMARY KEY,
  email TEXT NOT NULL,
  name TEXT NOT NULL,
  address TEXT NOT NULL
);

CREATE TABLE deep_table_organization_users (
  id INT NOT NULL PRIMARY KEY,
  organization_user_id INT NOT NULL REFERENCES organization_users(id),
  deep_table_id INT NOT NULL REFERENCES deep_table(id)
);

-- Joins should work with multiple primary keys as well.
CREATE TABLE multiple_primary_keys_target (
  key1 TEXT NOT NULL,
  key2 TEXT NOT NULL,
  nugget TEXT NOT NULL,
  PRIMARY KEY (key1, key2)
);

CREATE TABLE multiple_primary_keys (
  key1 TEXT NOT NULL,
  key2 TEXT NOT NULL,
  FOREIGN KEY (key1, key2) REFERENCES multiple_primary_keys_target (key1, key2)
);
