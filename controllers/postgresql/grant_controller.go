/*
Copyright 2023 Pramodh Ayyappan.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/Facets-cloud/postgresql-operator/apis/common"
	postgresql "github.com/Facets-cloud/postgresql-operator/apis/postgresql/v1alpha1"
	"github.com/google/go-cmp/cmp"
	"github.com/lib/pq"
)

const (
	grantFinalizer    = "grant.postgresql.facets.cloud/finalizer"
	GRANTCREATED      = "GrantCreated"
	GRANTEXISTS       = "GrantExists"
	GRANTSYNCED       = "GrantSynced"
	GRANTCREATEFAILED = "GrantCreateFailed"
	GRANTSYNCFAILED   = "GrantSyncFailed"
	GRANTREVOKED      = "GrantRevoked"
	GRANTREVOKEFAILED = "GrantRevokeFailed"
	GRANTGETFAILED    = "GrantGetFailed"
	GRANTDUPLICATED   = "DuplicateGrant"
)

var (
	grantLogger        = log.Log.WithName("grant_controller")
	grantDB            *sql.DB
	grantReconcileTime time.Duration
	check_all          = regexp.MustCompile(`\*|ALL`)
)

// GrantReconciler reconciles a Grant object
type GrantReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=grants,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=grants/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=grants/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Grant object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *GrantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	grantLogger.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)

	// Get reconcile time
	grantReconcileTime, err := time.ParseDuration(flag.Lookup("reconcile-period").Value.String())
	if err != nil {
		panic(err)
	}

	grant := &postgresql.Grant{}
	err = r.Get(ctx, req.NamespacedName, grant)
	if err != nil {
		return ctrl.Result{}, nil
	}

	// Get Role resource
	role := &postgresql.Role{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: grant.Spec.RoleRef.Namespace,
		Name:      grant.Spec.RoleRef.Name,
	}, role)
	if err != nil {
		reason := fmt.Sprintf(
			"Failed to get role resource `%s/%s` for grant `%s`",
			grant.Spec.RoleRef.Name,
			grant.Spec.RoleRef.Namespace,
			grant.Name,
		)
		r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, err.Error())
		grantLogger.Error(err, reason)
		return ctrl.Result{}, nil
	}

	// Get Database connection secret
	connectionSecret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: role.Spec.ConnectSecretRef.Namespace,
		Name:      role.Spec.ConnectSecretRef.Name,
	}, connectionSecret)
	if err != nil {
		reason := fmt.Sprintf(
			"Failed to get connection secret `%s/%s` for grant `%s`",
			role.Spec.ConnectSecretRef.Name,
			role.Spec.ConnectSecretRef.Namespace,
			grant.Name,
		)
		r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, err.Error())
		grantLogger.Error(err, reason)
		return ctrl.Result{}, nil
	}

	// Check if any Database Conenction secret value is empty
	isEmpty, requiredSecretKeys := common.IsSecretsValueEmtpy(connectionSecret)
	if isEmpty {
		message := fmt.Sprintf(
			"The value for required keys `%s` in secret `%s/%s` should not be empty or null.",
			requiredSecretKeys,
			role.Spec.ConnectSecretRef.Namespace,
			role.Spec.ConnectSecretRef.Name,
		)
		r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, message)
		grantLogger.Error(errors.New(message), "Please update the keys in secret with right values")
		return ctrl.Result{}, nil
	}

	var currentGrantType string

	roleName := role.Name
	grantName := grant.Name
	database := strings.TrimSpace(*grant.Spec.Database)
	schema := strings.TrimSpace(*grant.Spec.Schema)
	table := strings.TrimSpace(*grant.Spec.Table)
	privileges := grant.Spec.Privileges
	privilegesString := strings.Join(grant.Spec.Privileges, ", ")
	previousState := grant.Status.PreviousState
	previousDatabase := strings.TrimSpace(previousState.Database)
	previousSchema := strings.TrimSpace(previousState.Schema)
	previousTable := strings.TrimSpace(previousState.Table)
	previousPrivileges := previousState.Privileges
	previousPrivilegesString := strings.Join(previousState.Privileges, ", ")
	previousGrantType := previousState.Type

	// sort privileges list
	sortedPrivileges := privileges
	sortedpreviousPrivileges := previousPrivileges
	sort.Sort(sort.StringSlice(sortedPrivileges))
	sort.Sort(sort.StringSlice(sortedpreviousPrivileges))
	privilegesMap := map[string][]string{
		"currentPrivileges":       sortedPrivileges,
		"previousStatePrivileges": sortedpreviousPrivileges,
	}

	// Get Grant Type
	if len(database) > 0 && len(schema) <= 0 && len(table) <= 0 {
		currentGrantType = common.GRANTDATABASE
	} else {
		currentGrantType = common.GRANTTABLE
	}

	// Check if grant is duplicated
	existingGrants := &postgresql.GrantList{}
	err = r.List(ctx, existingGrants)
	if err != nil {
		grantLogger.Error(err, "Error getting Grants List")
		return ctrl.Result{}, nil
	}
	for _, existingGrant := range existingGrants.Items {
		if existingGrant.Spec.RoleRef.Name == grant.Spec.RoleRef.Name && existingGrant.Spec.RoleRef.Namespace == grant.Spec.RoleRef.Namespace {
			if len(existingGrant.Status.Conditions) > 0 && len(grant.Status.Conditions) > 0 {
				if existingGrant.Status.Conditions[len(existingGrant.Status.Conditions)-1].Reason != GRANTDUPLICATED && grant.Status.Conditions[len(grant.Status.Conditions)-1].Reason != GRANTDUPLICATED {
					if grant.Name != existingGrant.Name && grant.Namespace == existingGrant.Namespace {
						if currentGrantType == common.GRANTDATABASE {
							if check_all.MatchString(*existingGrant.Spec.Database) {
								reason := fmt.Sprintf(
									"Already a grant `%s/%s` created with ALL database permission for role `%s/%s`. So delete this grant `%s/%s`",
									existingGrant.Namespace,
									existingGrant.Name,
									grant.Spec.RoleRef.Namespace,
									grant.Spec.RoleRef.Name,
									grant.Namespace,
									grant.Name,
								)
								r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTDUPLICATED, reason)
								grantLogger.Error(err, reason)
								return ctrl.Result{}, nil
							} else if existingGrant.Spec.Database == grant.Spec.Database {
								reason := fmt.Sprintf(
									"Already a grant `%s/%s` created with `%s` database permission for role `%s/%s`. So delete this grant `%s/%s`",
									existingGrant.Namespace,
									existingGrant.Name,
									*grant.Spec.Database,
									grant.Spec.RoleRef.Namespace,
									grant.Spec.RoleRef.Name,
									grant.Namespace,
									grant.Name,
								)
								r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTDUPLICATED, reason)
								grantLogger.Error(err, reason)
								return ctrl.Result{}, nil
							}
						} else if currentGrantType == common.GRANTTABLE {
							if check_all.MatchString(*existingGrant.Spec.Table) && existingGrant.Spec.Schema != grant.Spec.Schema {
								reason := fmt.Sprintf(
									"Already a grant `%s/%s` created with ALL tables permission for role `%s/%s`. So delete this grant `%s/%s`",
									existingGrant.Namespace,
									existingGrant.Name,
									grant.Spec.RoleRef.Namespace,
									grant.Spec.RoleRef.Name,
									grant.Namespace,
									grant.Name,
								)
								r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTDUPLICATED, reason)
								grantLogger.Error(err, reason)
								return ctrl.Result{}, nil
							} else if existingGrant.Spec.Table == grant.Spec.Table && existingGrant.Spec.Schema != grant.Spec.Schema {
								reason := fmt.Sprintf(
									"Already a grant `%s/%s` created with `%s` table permission for role `%s/%s`. So delete this grant `%s/%s`",
									existingGrant.Namespace,
									existingGrant.Name,
									*grant.Spec.Table,
									grant.Spec.RoleRef.Namespace,
									grant.Spec.RoleRef.Name,
									grant.Namespace,
									grant.Name,
								)
								r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTDUPLICATED, reason)
								grantLogger.Error(err, reason)
								return ctrl.Result{}, nil
							}
						}
					}
				}
			}
		}
	}

	// Start creating grant only if role name exists
	if len(roleName) > 0 {
		// Connect to Postgres DB
		grantDB, err = common.ConnectToPostgres(connectionSecret, database)
		if err != nil {
			grantLogger.Error(err, fmt.Sprintf("Failed connecting to database `%s` for grant `%s`", database, grant.Name))
			r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, common.CONNECTIONFAILED, err.Error())
			return ctrl.Result{}, nil
		}
		defer grantDB.Close()

		// Add finalizers to handle delete scenario
		// Also revoke grant up on delete
		if grant.ObjectMeta.DeletionTimestamp.IsZero() {
			if !controllerutil.ContainsFinalizer(grant, grantFinalizer) {
				controllerutil.AddFinalizer(grant, grantFinalizer)
				err = r.Update(ctx, grant)
				if err != nil {
					return ctrl.Result{}, err
				}
			}
		} else {
			if controllerutil.ContainsFinalizer(grant, grantFinalizer) {
				if _, status, _, _ := r.RevokeGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString, privilegesMap, false); status == metav1.ConditionFalse {
					return ctrl.Result{}, err
				}

				controllerutil.RemoveFinalizer(grant, grantFinalizer)
				err := r.Update(ctx, grant)
				if err != nil {
					return ctrl.Result{}, err
				}
			}
			return ctrl.Result{}, nil
		}

		if currentGrantType == common.GRANTDATABASE {
			// Check if previous grant type is null
			if len(previousGrantType) == 0 {
				// Create database grant
				typeName, status, reason, message := r.CreateGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
				grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
				r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
				if status == metav1.ConditionFalse {
					return ctrl.Result{}, nil
				}
			} else if len(previousGrantType) > 0 && previousGrantType == common.GRANTTABLE {
				// Revoke previous state grant
				typeName, status, reason, message := r.RevokeGrant(ctx, previousGrantType, grantName, roleName, previousDatabase, previousSchema, previousTable, previousPrivilegesString, privilegesMap, false)
				r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, fmt.Sprintf("%s as there is a request to change from table `%s` grant to database `%s` grant", message, previousTable, database))
				if status == metav1.ConditionFalse {
					return ctrl.Result{}, nil
				}
				// Create database grant
				typeName, status, reason, message = r.CreateGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
				// Update Previous state status and status condition
				grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
				r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
				if status == metav1.ConditionFalse {
					return ctrl.Result{}, nil
				}
			} else if len(previousGrantType) > 0 && previousGrantType == common.GRANTDATABASE {
				// Observe and then sync grant
				isGrantStateChanged := r.ObserveGrantState(ctx, grant, currentGrantType, grantName, roleName, database, schema, table, privileges)
				if isGrantStateChanged {
					// Revoke grant as the grant is modified outside of database operator
					typeName, status, reason, message := r.RevokeGrant(ctx, previousGrantType, grantName, roleName, database, schema, table, privilegesString, privilegesMap, true)
					r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
					// Sync database grant
					typeName, status, reason, message = r.SyncGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
					grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
					r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
					if status == metav1.ConditionFalse {
						return ctrl.Result{}, nil
					}
				}
			}
		} else if currentGrantType == common.GRANTTABLE {
			// Check if previous grant type is null
			if len(previousGrantType) == 0 {
				// Create table grant
				typeName, status, reason, message := r.CreateGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
				grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
				r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
				if status == metav1.ConditionFalse {
					return ctrl.Result{}, nil
				}
			} else if len(previousGrantType) > 0 && previousGrantType == common.GRANTDATABASE {
				// Revoke previous state grant
				typeName, status, reason, message := r.RevokeGrant(ctx, previousGrantType, grantName, roleName, previousDatabase, previousSchema, previousTable, previousPrivilegesString, privilegesMap, false)
				r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, fmt.Sprintf("%s as there is a request to change from database `%s` grant to table `%s` grant", message, previousDatabase, table))
				if status == metav1.ConditionFalse {
					return ctrl.Result{}, nil
				}
				// Create table grant
				typeName, status, reason, message = r.CreateGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
				// Update Previous state status and status condition
				grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
				r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
				if status == metav1.ConditionFalse {
					return ctrl.Result{}, nil
				}
			} else if len(previousGrantType) > 0 && previousGrantType == common.GRANTTABLE {
				// If there is a no diff between previous state schema and current schema
				if previousSchema == schema {
					// If there is a diff between previous state table and current table
					if previousTable != table {
						// Revoke previous table grant
						typeName, status, reason, message := r.RevokeGrant(ctx, previousGrantType, grantName, roleName, previousDatabase, previousSchema, previousTable, previousPrivilegesString, privilegesMap, false)
						r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, fmt.Sprintf("%s as there is a request to change from table `%s` to table `%s`", message, previousTable, table))
						if status == metav1.ConditionFalse {
							return ctrl.Result{}, nil
						}
						// Create table grant
						typeName, status, reason, message = r.CreateGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
						// Update Previous state status and status condition
						grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
						r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
						if status == metav1.ConditionFalse {
							return ctrl.Result{}, nil
						}
					} else {
						// Observe and then sync grant
						isGrantStateChanged := r.ObserveGrantState(ctx, grant, currentGrantType, grantName, roleName, database, schema, table, privileges)
						if isGrantStateChanged {
							// Revoke grant as the grant is modified outside of database operator
							typeName, status, reason, message := r.RevokeGrant(ctx, previousGrantType, grantName, roleName, database, schema, table, privilegesString, privilegesMap, true)
							r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
							typeName, status, reason, message = r.SyncGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
							grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
							r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
							if status == metav1.ConditionFalse {
								return ctrl.Result{}, nil
							}
						}
					}
				} else if previousSchema != schema {
					// Revoke previous table grant
					typeName, status, reason, message := r.RevokeGrant(ctx, previousGrantType, grantName, roleName, previousDatabase, previousSchema, previousTable, previousPrivilegesString, privilegesMap, false)
					r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, fmt.Sprintf("%s as there is a request to change from schema `%s` to schema `%s`", message, previousSchema, schema))
					if status == metav1.ConditionFalse {
						return ctrl.Result{}, nil
					}
					// Create table grant
					typeName, status, reason, message = r.CreateGrant(ctx, currentGrantType, grantName, roleName, database, schema, table, privilegesString)
					// Update Previous state status and status condition
					grant = r.updatePreviousStateStatus(ctx, grant, currentGrantType, database, schema, table, privileges)
					r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
					if status == metav1.ConditionFalse {
						return ctrl.Result{}, nil
					}
				}
			}
		}
	}

	return ctrl.Result{RequeueAfter: grantReconcileTime}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GrantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresql.Grant{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

func (r *GrantReconciler) CreateGrant(ctx context.Context, grantType string, grantName string, roleName string, database string, schema string, table string, privileges string) (string, metav1.ConditionStatus, string, string) {
	var createGrantQuery string
	switch grantType {
	case common.GRANTDATABASE:
		createGrantQuery = fmt.Sprintf("GRANT %s ON DATABASE %s TO \"%s\"", privileges, database, roleName)
	case common.GRANTTABLE:
		if check_all.MatchString(table) {
			// Using `ALTER DEFAULT PRIVILEGES` allows you to set the privileges that will be applied to objects created in the future
			// https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html
			createGrantQueryForFutureTables := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT %s ON TABLES TO \"%s\"", schema, privileges, roleName)
			createGrantQueryForExistingTables := fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA %s TO \"%s\"", privileges, schema, roleName)
			createGrantSeqQueryForFutureTables := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT %s ON SEQUENCES TO \"%s\"", schema, privileges, roleName)
			createGrantSeqQueryForExistingTables := fmt.Sprintf("GRANT %s ON ALL SEQUENCES IN SCHEMA %s TO \"%s\"", privileges, schema, roleName)

			createGrantQuery = strings.Join([]string{createGrantQueryForFutureTables, createGrantQueryForExistingTables, createGrantSeqQueryForFutureTables, createGrantSeqQueryForExistingTables}, "; ")
		} else {
			createGrantQuery = fmt.Sprintf("GRANT %s ON %s.%s TO \"%s\"", privileges, schema, table, roleName)
		}
	}

	_, err := grantDB.Exec(createGrantQuery)
	if err != nil {
		grantLogger.Error(err, fmt.Sprintf("Failed to create grant `%s`", grantName))
		return grantType, metav1.ConditionFalse, GRANTCREATEFAILED, err.Error()
	}

	grantLogger.Info(fmt.Sprintf("Grant `%s` got created successfully", grantName))
	return grantType, metav1.ConditionTrue, GRANTCREATED, "Grant created successfully"
}

func (r *GrantReconciler) RevokeGrant(ctx context.Context, grantType string, grantName string, roleName string, database string, schema string, table string, privileges string, privilegesMap map[string][]string, notInSync bool) (string, metav1.ConditionStatus, string, string) {
	var revokeGrantQuery string
	var message string
	switch grantType {
	case common.GRANTDATABASE:
		if notInSync {
			revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM \"%s\"", database, roleName)
			// If previous state privileges and current privileges are same then there is a change in datavase outside of this operator
			if cmp.Equal(privilegesMap["currentPrivileges"], privilegesMap["previousStatePrivileges"]) {
				message = "Grant revoked successfully as grant got updated outside of database operator"
			} else {
				message = "Grant revoked successfully"
			}
		} else {
			revokeGrantQuery = fmt.Sprintf("REVOKE %s ON DATABASE %s FROM \"%s\"", privileges, database, roleName)
			message = "Grant revoked successfully"
		}

	case common.GRANTTABLE:
		if check_all.MatchString(table) {
			if notInSync {
				revokeGrantQueryForFutureTables := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON TABLES FROM \"%s\"", schema, roleName)
				revokeGrantQueryForExistingTables := fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM \"%s\"", schema, roleName)
				revokeGrantSeqQueryForFutureTables := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON SEQUENCES FROM \"%s\"", schema, roleName)
				revokeGrantSeqQueryForExistingTables := fmt.Sprintf("REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM \"%s\"", schema, roleName)
				revokeGrantQuery = strings.Join([]string{revokeGrantQueryForFutureTables, revokeGrantQueryForExistingTables, revokeGrantSeqQueryForFutureTables, revokeGrantSeqQueryForExistingTables}, "; ")
				// If previous state privileges and current privileges are same then there is a change in datavase outside of this operator
				if cmp.Equal(privilegesMap["currentPrivileges"], privilegesMap["previousStatePrivileges"]) {
					message = "Grant revoked successfully as grant got updated outside of database operator"
				} else {
					message = "Grant revoked successfully"
				}
			} else {
				revokeGrantQueryForFutureTables := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE %s ON TABLES FROM \"%s\"", schema, privileges, roleName)
				revokeGrantQueryForExistingTables := fmt.Sprintf("REVOKE %s ON ALL TABLES IN SCHEMA %s FROM \"%s\"", privileges, schema, roleName)
				revokeGrantQuery = strings.Join([]string{revokeGrantQueryForFutureTables, revokeGrantQueryForExistingTables}, "; ")
				message = "Grant revoked successfully"
			}
		} else {
			if notInSync {
				revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON %s.%s FROM \"%s\"", schema, table, roleName)
				// If previous state privileges and current privileges are same then there is a change in datavase outside of this operator
				if cmp.Equal(privilegesMap["currentPrivileges"], privilegesMap["previousStatePrivileges"]) {
					message = "Grant revoked successfully as grant got updated outside of database operator"
				} else {
					message = "Grant revoked successfully"
				}
			} else {
				revokeGrantQuery = fmt.Sprintf("REVOKE %s ON %s.%s FROM \"%s\"", privileges, schema, table, roleName)
				message = "Grant revoked successfully"
			}
		}
	}

	_, err := grantDB.Exec(revokeGrantQuery)
	if err != nil {
		grantLogger.Error(err, fmt.Sprintf("Failed to revoke grant `%s`", grantName))
		return grantType, metav1.ConditionFalse, GRANTREVOKEFAILED, err.Error()
	}

	grantLogger.Info(fmt.Sprintf("`%s` %s", grantName, message))
	return grantType, metav1.ConditionTrue, GRANTREVOKED, message
}

func (r *GrantReconciler) SyncGrant(ctx context.Context, grantType string, grantName string, roleName string, database string, schema string, table string, privileges string) (string, metav1.ConditionStatus, string, string) {
	var syncGrantQuery string
	switch grantType {
	case common.GRANTDATABASE:
		syncGrantQuery = fmt.Sprintf("GRANT %s ON DATABASE %s TO \"%s\"", privileges, database, roleName)
	case common.GRANTTABLE:
		if check_all.MatchString(table) {
			// Using `ALTER DEFAULT PRIVILEGES` allows you to set the privileges that will be applied to objects created in the future
			// https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html
			syncGrantQueryForFutureTables := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT %s ON TABLES TO \"%s\"", schema, privileges, roleName)
			syncGrantQueryForExistingTables := fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA %s TO \"%s\"", privileges, schema, roleName)
			syncGrantSeqQueryForFutureTables := fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT %s ON SEQUENCES TO \"%s\"", schema, privileges, roleName)
			syncGrantSeqQueryForExistingTables := fmt.Sprintf("GRANT %s ON ALL SEQUENCES IN SCHEMA %s TO \"%s\"", privileges, schema, roleName)
			syncGrantQuery = strings.Join([]string{syncGrantQueryForFutureTables, syncGrantQueryForExistingTables, syncGrantSeqQueryForFutureTables, syncGrantSeqQueryForExistingTables}, "; ")
		} else {
			syncGrantQuery = fmt.Sprintf("GRANT %s ON %s.%s TO \"%s\"", privileges, schema, table, roleName)
		}
	}

	_, err := grantDB.Exec(syncGrantQuery)
	if err != nil {
		grantLogger.Error(err, fmt.Sprintf("Failed to sync grant `%s`", grantName))
		return grantType, metav1.ConditionFalse, GRANTSYNCFAILED, err.Error()
	}

	grantLogger.Info(fmt.Sprintf("Grant `%s` got synced successfully", grantName))
	return grantType, metav1.ConditionTrue, GRANTSYNCED, "Grant synced successfully"
}

func (r *GrantReconciler) ObserveGrantState(ctx context.Context, grant *postgresql.Grant, grantType string, grantName string, roleName string, database string, schema string, table string, privileges []string) bool {
	var selectGrantQuery string
	var isGrantStateNotChanged bool

	switch grantType {
	case common.GRANTDATABASE:
		if check_all.MatchString(privileges[0]) {
			privileges = []string{"CREATE", "CONNECT", "TEMPORARY"}
		}

		selectGrantQuery = "SELECT EXISTS(SELECT 1 FROM pg_database db, aclexplode(datacl) as acl INNER JOIN pg_roles s ON acl.grantee = s.oid WHERE db.datname = $1 AND s.rolname = $2 GROUP BY db.datname, s.rolname, acl.is_grantable HAVING array_agg(acl.privilege_type ORDER BY privilege_type ASC) = (SELECT array(SELECT unnest($3::text[]) as perms ORDER BY perms ASC)))"
		err := grantDB.QueryRow(
			selectGrantQuery,
			database,
			roleName,
			pq.Array(privileges),
		).Scan(&isGrantStateNotChanged)
		if err != nil {
			grantLogger.Error(err, fmt.Sprintf("Failed to get grant `%s` when observing ", grantName))
			r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTGETFAILED, err.Error())
		}

	case common.GRANTTABLE:
		if check_all.MatchString(privileges[0]) {
			privileges = []string{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}
		}

		if check_all.MatchString(table) {
			selectGrantQuery = "SELECT privilege_type FROM (" +
				" SELECT (acl).grantee AS grantee, (acl).grantor AS grantor, (acl).is_grantable AS is_grantable, (acl).privilege_type AS privilege_type, defaclnamespace FROM (" +
				" SELECT aclexplode(defaclacl) as acl, defaclnamespace AS defaclnamespace, defaclacl as defaclacl " +
				" FROM pg_default_acl" +
				" ) acl " +
				" ) da " +
				" INNER JOIN pg_namespace ns ON da.defaclnamespace = ns.oid" +
				" INNER JOIN pg_roles r ON r.oid = da.grantee" +
				" WHERE r.rolname = $1 AND ns.nspname = $2"

			rows, err := grantDB.Query(
				selectGrantQuery,
				roleName,
				schema,
			)

			var futureTablesResult string
			var futureTablesResults []string
			for rows.Next() {
				err := rows.Scan(&futureTablesResult)
				if err != nil {
					grantLogger.Error(err, "Scanning rows failed")
				}
				futureTablesResults = append(futureTablesResults, futureTablesResult)
			}
			sort.Sort(sort.StringSlice(privileges))
			sort.Sort(sort.StringSlice(futureTablesResults))
			if cmp.Equal(privileges, futureTablesResults) {
				isGrantStateNotChanged = true
			} else {
				isGrantStateNotChanged = false
			}
			if err != nil {
				grantLogger.Error(err, fmt.Sprintf("Failed to get grant `%s` when observing", grantName))
				r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTGETFAILED, err.Error())
			}
		} else {
			selectGrantQuery = "SELECT EXISTS (SELECT 1" +
				" FROM information_schema.role_table_grants" +
				" WHERE grantee=$1" +
				" AND table_schema=$2 " +
				" AND table_name=$3" +
				" GROUP BY grantee, table_schema, table_name" +
				" HAVING array_agg(privilege_type::text" +
				" ORDER BY privilege_type ASC) = (SELECT array(SELECT unnest($4::text[]) as perms ORDER BY perms ASC)))"
			err := grantDB.QueryRow(
				selectGrantQuery,
				roleName,
				schema,
				table,
				pq.Array(privileges),
			).Scan(&isGrantStateNotChanged)
			if err != nil {
				grantLogger.Error(err, fmt.Sprintf("Failed to get grant `%s` when observing", grant.Name))
				r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTGETFAILED, err.Error())
			}
		}
	}

	return !isGrantStateNotChanged
}

func (r *GrantReconciler) appendGrantStatusCondition(ctx context.Context, grant *postgresql.Grant, typeName string, status metav1.ConditionStatus, reason string, message string) {
	time := metav1.Time{Time: time.Now()}
	condition := metav1.Condition{Type: typeName, Status: status, Reason: reason, Message: message, LastTransitionTime: time}

	grantStatusConditions := grant.Status.Conditions

	if len(grantStatusConditions) > 0 {
		// Only keep 5 statuses
		if len(grantStatusConditions) >= 5 {
			if len(grantStatusConditions) > 5 {
				grant.Status.Conditions = grantStatusConditions[len(grantStatusConditions)-5:]
			}
		}

		grant.Status.Conditions = append(grant.Status.Conditions, condition)
		err := r.Status().Update(ctx, grant)
		if err != nil {
			grantLogger.Error(err, fmt.Sprintf("Resource status update failed for grant `%s`", grant.Name))
		}
	} else {
		grant.Status.Conditions = append(grant.Status.Conditions, condition)
		err := r.Status().Update(ctx, grant)
		if err != nil {
			grantLogger.Error(err, fmt.Sprintf("Resource status update failed for grant `%s`", grant.Name))
		}
	}
}

func (r *GrantReconciler) updatePreviousStateStatus(ctx context.Context, grant *postgresql.Grant, grantType string, database string, schema string, table string, privileges []string) *postgresql.Grant {

	grant.Status.PreviousState = postgresql.PreviousState{
		Type:       grantType,
		Privileges: privileges,
		Database:   database,
		Schema:     schema,
		Table:      table,
	}

	return grant
}
