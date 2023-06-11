# API Reference

## Packages
- [postgresql.facets.cloud/v1alpha1](#postgresqlfacetscloudv1alpha1)


## postgresql.facets.cloud/v1alpha1

Package v1alpha1 contains API Schema definitions for the postgresql v1alpha1 API group

### Resource Types
- [Grant](#grant)
- [Role](#role)



#### Grant



Grant is the Schema for the grants API



| Field                                                                                                              | Description                                                     |
| ------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| `apiVersion` _string_                                                                                              | `postgresql.facets.cloud/v1alpha1`                              |
| `kind` _string_                                                                                                    | `Grant`                                                         |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |
| `spec` _[GrantSpec](#grantspec)_                                                                                   |                                                                 |


#### GrantSpec



GrantSpec defines the desired state of Grant

_Appears in:_
- [Grant](#grant)

| Field                                               | Description                                         |
| --------------------------------------------------- | --------------------------------------------------- |
| `roleRef` _[ResourceReference](#resourcereference)_ | Defines the role reference to grant permissions     |
| `privileges` _string array_                         | Defines the list of permissions to grant for a role |
| `database` _string_                                 | Defines the Database to grant permission for a role |
| `schema` _string_                                   | Defines the Schema to grant permission for a role   |
| `table` _string_                                    | Defines the Database to grant permission for a role |


#### Role



Role is the Schema for the roles API



| Field                                                                                                              | Description                                                     |
| ------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| `apiVersion` _string_                                                                                              | `postgresql.facets.cloud/v1alpha1`                              |
| `kind` _string_                                                                                                    | `Role`                                                          |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.23/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |
| `spec` _[RoleSpec](#rolespec)_                                                                                     |                                                                 |


#### RolePrivilege



RolePrivilege is the PostgreSQL identifier to add or remove a permission on a role. See https://www.postgresql.org/docs/current/sql-createrole.html for available privileges.

_Appears in:_
- [RoleSpec](#rolespec)

| Field                   | Description                                                                                                    |
| ----------------------- | -------------------------------------------------------------------------------------------------------------- |
| `superUser` _boolean_   | SuperUser grants SUPERUSER privilege when true.                                                                |
| `createDb` _boolean_    | CreateDb grants CREATEDB when true, allowing the role to create databases.                                     |
| `createRole` _boolean_  | CreateRole grants CREATEROLE when true, allowing this role to create other roles.                              |
| `login` _boolean_       | Login grants LOGIN when true, allowing the role to login to the server.                                        |
| `inherit` _boolean_     | Inherit grants INHERIT when true, allowing the role to inherit permissions from other roles it is a member of. |
| `replication` _boolean_ | Replication grants REPLICATION when true, allowing the role to connect in replication mode.                    |
| `bypassRls` _boolean_   | BypassRls grants BYPASSRLS when true, allowing the role to bypass row-level security policies.                 |


#### RoleSpec



RoleSpec defines the desired state of Role

_Appears in:_
- [Role](#role)

| Field                                                         | Description                                                                                        |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `connectSecretRef` _[ResourceReference](#resourcereference)_  | ConnectSecretRef references the secret that contains database details () used to create this role. |
| `passwordSecretRef` _[SecretKeySelector](#secretkeyselector)_ | PasswordSecretRef references the secret that contains the password used for this role.             |
| `connectionLimit` _integer_                                   | ConnectionLimit to be applied to the role.                                                         |
| `privileges` _[RolePrivilege](#roleprivilege)_                | Privileges to be granted.                                                                          |


##### ResourceReference

The Database Connection details secret selector

_Appears in:_
- [Role](#role)
- [Grant](#Grant)

| Field       | Description                                                                                                            |
| ----------- | ---------------------------------------------------------------------------------------------------------------------- |
| `name`      | The name of secret that contains PostgreSQL database details `username`, `password`, `endpoint`, `port` and `database` |
| `namespace` | The namespace of the secret                                                                                            |


#### SecretKeySelector

The Role password secret selector

_Appears in:_
- [Role](#role)

| Field                                     | Description                                     |
| ----------------------------------------- | ----------------------------------------------- |
| _[ResourceReference](#resourcereference)_ | Includes resource reference                     |
| `key`                                     | The key name in the secret to get role password |
