-- join users -> organization_users -> organizations
SELECT name, address, email from users;

-- join organizations -> organization_users -> users
SELECT name, address, email from organizations;

-- join organization_users -> deep_table_organization_users -> deep_table due to explit alias behavior
-- normally, this would just join users and organizations
SELECT deep_table.name, deep_table.address, deep_table.email from organization_users;

-- join users -> organization_users -> organization_users -> deep_table_organization_users -> deep_table due to explit alias behavior
-- normally, this would just join organization_users and organizations
SELECT deep_table.name, deep_table.address, deep_table.email from users;

-- join multiple_primary_keys -> multiple_primary_keys_target, using multiple foreign keys
SELECT nugget FROM multiple_primary_keys;
