SELECT name, address, email FROM users
 JOIN organization_users ON organization_users.user_id = users.id
 JOIN organizations ON organization_users.organization_id = organizations.id;

SELECT name, address, email FROM organizations
 JOIN organization_users ON organization_users.organization_id = organizations.id
 JOIN users ON organization_users.user_id = users.id;

SELECT deep_table.name, deep_table.address, deep_table.email FROM organization_users
 JOIN deep_table_organization_users ON deep_table_organization_users.organization_user_id = organization_users.id
 JOIN deep_table ON deep_table_organization_users.deep_table_id = deep_table.id;

SELECT deep_table.name, deep_table.address, deep_table.email FROM users
 JOIN organization_users ON organization_users.user_id = users.id
 JOIN deep_table_organization_users ON deep_table_organization_users.organization_user_id = organization_users.id
 JOIN deep_table ON deep_table_organization_users.deep_table_id = deep_table.id;

SELECT nugget FROM multiple_primary_keys
 JOIN multiple_primary_keys_target ON multiple_primary_keys.key1 = multiple_primary_keys_target.key1 AND multiple_primary_keys.key2 = multiple_primary_keys_target.key2
