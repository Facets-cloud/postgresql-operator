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
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/lib/pq"
	"github.com/pramodh-ayyappan/database-operator/apis/common"
	"github.com/pramodh-ayyappan/database-operator/apis/postgresql/v1alpha1"
	postgresql "github.com/pramodh-ayyappan/database-operator/apis/postgresql/v1alpha1"
)

const (
	grantFinalizer         = "grant.postgresql.facets.cloud/finalizer"
	grantReconcileTime     = time.Duration(20 * time.Second)
	errGrantAlreadyExists  = "Grant already exists"
	errGrantDeletedOutside = "Grant deleted outside of database operator"
	errGrantUpdatedOutside = "Grant manually updated outside of database operator. So revoking privileges"
	GRANTCREATED           = "GrantCreated"
	GRANTEXISTS            = "GrantExists"
	GRANTSYNCED            = "GrantSynced"
	GRANTCREATEFAILED      = "GrantCreateFailed"
	GRANTSYNCFAILED        = "GrantSyncFailed"
	GRANTREVOKED           = "GrantRevoked"
	GRANTREVOKEFAILED      = "GrantRevokeFailed"
	GRANTGETFAILED         = "GrantGetFailed"
)

var (
	grantLogger = log.Log.WithName("grant_controller")
	grantDB     *sql.DB
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

	grant := &postgresql.Grant{}
	err := r.Get(ctx, req.NamespacedName, grant)
	if err != nil {
		return ctrl.Result{}, nil
	}

	// Get Database connection secret
	connectionSecret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: grant.Spec.ConnectSecretRef.Namespace,
		Name:      grant.Spec.ConnectSecretRef.Name,
	}, connectionSecret)
	if err != nil {
		reason := fmt.Sprintf(
			"Failed to get connection secret `%s/%s` for grant `%s`",
			grant.Spec.ConnectSecretRef.Name,
			grant.Spec.ConnectSecretRef.Namespace,
			grant.Name,
		)
		r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, err.Error())
		grantLogger.Error(err, reason)
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
	}

	var grantType string
	if len(*grant.Spec.Database) > 0 && len(*grant.Spec.Schema) <= 0 && len(*grant.Spec.Table) <= 0 {
		grantType = common.GRANTDATABSE
	} else {
		grantType = common.GRANTTABLE
	}

	roleName := role.Name

	if len(roleName) > 0 {
		// Connect to Postgres DB
		defaultDatabase := grant.Spec.Database
		grantDB, err = common.ConnectToPostgres(connectionSecret, *defaultDatabase)
		if err != nil {
			reason := fmt.Sprintf("Failed connecting to database for grant `%s`", grant.Name)
			grantLogger.Error(err, reason)
			r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, common.CONNECTIONFAILED, err.Error())
		}
		defer grantDB.Close()

		// Add finalizers to handle delete scenario
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
				if _, _, _, _, err := r.RevokeGrant(ctx, grant, roleName, grantType, false, false); err != nil {
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

		conditions := grant.Status.Conditions
		if !(len(grant.Status.Conditions) > 0) {
			typeName, status, reason, message := r.CreateGrant(ctx, grant, roleName, grantType)
			r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
		} else {
			// Observe Grant State logic
			lastItem := conditions[len(conditions)-1]
			previousState := grant.Status.PreviousState
			grantLogger.Info("++++++++++++++++++++++++++")
			grantLogger.Info(fmt.Sprintf("Last Grant Type -  %s, Current Grant Type - %s", lastItem.Type, grantType))
			grantLogger.Info("++++++++++++++++++++++++++")

			if lastItem.Status == metav1.ConditionTrue {
				if previousState.Type != grantType {
					typeName, status, reason, _, _ := r.RevokeGrant(ctx, grant, roleName, previousState.Type, true, true)
					r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, fmt.Sprintf("Revoked previous grant for `%s` as new changes requesting grant for `%s`", previousState.Type, grantType))
				}
				isGrantStateSync := r.ObserveGrantState(ctx, grant, roleName, grantType)
				if !isGrantStateSync {
					typeName, status, reason, message, _ := r.RevokeGrant(ctx, grant, roleName, grantType, true, false)
					r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, errGrantUpdatedOutside)
					typeName, status, reason, message = r.SyncGrant(ctx, grant, roleName, grantType)
					r.appendGrantStatusCondition(ctx, grant, typeName, status, reason, message)
				}
			}
		}
	}

	return ctrl.Result{RequeueAfter: grantReconcileTime}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *GrantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresql.Grant{}).
		Complete(r)
}

func (r *GrantReconciler) CreateGrant(ctx context.Context, grant *postgresql.Grant, roleName string, grantType string) (string, metav1.ConditionStatus, string, string) {
	var createGrantQuery string
	privileges := strings.Join(grant.Spec.Privileges, ", ")
	database := *grant.Spec.Database
	schema := *grant.Spec.Schema
	table := *grant.Spec.Table

	switch grantType {
	case common.GRANTDATABSE:
		createGrantQuery = fmt.Sprintf("GRANT %s ON DATABASE %s TO \"%s\"", privileges, database, roleName)
	case common.GRANTTABLE:
		if table == "ALL" {
			// Using `ALTER DEFAULT PRIVILEGES` allows you to set the privileges that will be applied to objects created in the future
			// https://www.postgresql.org/docs/current/sql-alterdefaultprivileges.html
			createGrantQuery = fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE \"%s\" IN SCHEMA %s GRANT %s ON TABLES TO \"%s\"", roleName, schema, privileges, roleName)
		} else {
			createGrantQuery = fmt.Sprintf("GRANT %s ON %s.%s TO \"%s\"", privileges, schema, table, roleName)
			grantLogger.Info(createGrantQuery)
		}
	}

	_, err := grantDB.Exec(createGrantQuery)
	if err != nil {
		grantLogger.Error(err, fmt.Sprintf("Failed to create grant `%s`", grant.Name))
		return grantType, metav1.ConditionFalse, GRANTCREATEFAILED, err.Error()
	}

	grantLogger.Info(fmt.Sprintf("Grant `%s` got created successfully", grant.Name))
	return grantType, metav1.ConditionTrue, GRANTCREATED, "Grant created successfully"
}

func (r *GrantReconciler) SyncGrant(ctx context.Context, grant *postgresql.Grant, roleName string, grantType string) (string, metav1.ConditionStatus, string, string) {
	var createGrantQuery string
	privileges := strings.Join(grant.Spec.Privileges, ", ")
	database := *grant.Spec.Database
	schema := *grant.Spec.Schema
	table := *grant.Spec.Table

	switch grantType {
	case common.GRANTDATABSE:
		createGrantQuery = fmt.Sprintf("GRANT %s ON DATABASE %s TO \"%s\"", privileges, database, roleName)

	case common.GRANTTABLE:
		if table == "ALL" {
			createGrantQuery = fmt.Sprintf("GRANT %s ON ALL TABLES IN SCHEMA %s TO \"%s\"", privileges, schema, roleName)
		} else {
			createGrantQuery = fmt.Sprintf("GRANT %s ON %s.%s TO \"%s\"", privileges, schema, table, roleName)
		}
	}

	_, err := grantDB.Exec(createGrantQuery)
	if err != nil {
		grantLogger.Error(err, fmt.Sprintf("Failed to sync grant `%s`", grant.Name))
		return grantType, metav1.ConditionFalse, GRANTSYNCFAILED, err.Error()
	}

	grantLogger.Info(fmt.Sprintf("Grant `%s` got synced successfully", grant.Name))
	return grantType, metav1.ConditionTrue, GRANTSYNCED, "Grant synced successfully"
}

func (r *GrantReconciler) RevokeGrant(ctx context.Context, grant *v1alpha1.Grant, roleName string, grantType string, notInSync bool, previousGrant bool) (string, metav1.ConditionStatus, string, string, error) {
	var revokeGrantQuery string
	privileges := strings.Join(grant.Spec.Privileges, ", ")
	database := *grant.Spec.Database
	schema := *grant.Spec.Schema
	table := *grant.Spec.Table

	switch grantType {
	case common.GRANTDATABSE:
		if notInSync {
			revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM \"%s\"", database, roleName)
		} else if previousGrant {
			revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM \"%s\"", grant.Status.PreviousState.Database, roleName)
		} else {
			revokeGrantQuery = fmt.Sprintf("REVOKE %s ON DATABASE %s FROM \"%s\"", privileges, database, roleName)
		}

	case common.GRANTTABLE:
		if table == "ALL" {
			if notInSync {
				revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM \"%s\"", schema, roleName)
			} else if previousGrant {
				revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM \"%s\"", grant.Status.PreviousState.Schema, roleName)
			} else {
				revokeGrantQuery = fmt.Sprintf("REVOKE %s ON ALL TABLES IN SCHEMA %s FROM \"%s\"", privileges, schema, roleName)
			}
		} else {
			if notInSync {
				revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON %s.%s FROM \"%s\"", schema, table, roleName)
			} else if previousGrant {
				revokeGrantQuery = fmt.Sprintf("REVOKE ALL ON %s.%s FROM \"%s\"", grant.Status.PreviousState.Schema, grant.Status.PreviousState.Table, roleName)
			} else {
				revokeGrantQuery = fmt.Sprintf("REVOKE %s ON %s.%s FROM \"%s\"", privileges, schema, table, roleName)
			}
		}
	}

	_, err := grantDB.Exec(revokeGrantQuery)
	if err != nil {
		grantLogger.Error(err, fmt.Sprintf("Failed to revoke grant `%s`", grant.Name))
		return grantType, metav1.ConditionFalse, GRANTREVOKEFAILED, err.Error(), err
	}

	grantLogger.Info(fmt.Sprintf("Grant `%s` got revoked successfully", grant.Name))
	return grantType, metav1.ConditionTrue, GRANTREVOKED, "Grant revoked successfully", err
}

func (r *GrantReconciler) ObserveGrantState(ctx context.Context, grant *postgresql.Grant, roleName string, grantType string) bool {
	var privileges interface {
		driver.Valuer
		sql.Scanner
	}
	var selectGrantQuery string
	database := *grant.Spec.Database
	schema := *grant.Spec.Schema
	table := *grant.Spec.Table
	var isGrantStateChanged bool

	switch grantType {
	case common.GRANTDATABSE:
		if grant.Spec.Privileges[0] == "ALL" {
			privileges = pq.Array([]string{"CREATE", "CONNECT", "TEMPORARY"})
		} else {
			privileges = pq.Array(grant.Spec.Privileges)
		}

		selectGrantQuery = "SELECT EXISTS(SELECT 1 FROM pg_database db, aclexplode(datacl) as acl INNER JOIN pg_roles s ON acl.grantee = s.oid WHERE db.datname = $1 AND s.rolname = $2 GROUP BY db.datname, s.rolname, acl.is_grantable HAVING array_agg(acl.privilege_type ORDER BY privilege_type ASC) = (SELECT array(SELECT unnest($3::text[]) as perms ORDER BY perms ASC)))"
		err := grantDB.QueryRow(
			selectGrantQuery,
			database,
			roleName,
			privileges,
		).Scan(&isGrantStateChanged)
		if err != nil {
			grantLogger.Error(err, fmt.Sprintf("Failed to get grant `%s` when observing ", grant.Name))
			r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTGETFAILED, err.Error())
		}

	case common.GRANTTABLE:
		if grant.Spec.Privileges[0] == "ALL" {
			privileges = pq.Array([]string{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"})
		} else {
			privileges = pq.Array(grant.Spec.Privileges)
		}

		include_table := ""
		if table == "ALL" {
			include_table = ""
		} else {
			include_table = fmt.Sprintf("AND table_name='%s'", table)
		}

		selectGrantQuery = "SELECT EXISTS (SELECT 1" +
			" FROM information_schema.role_table_grants" +
			" WHERE grantee=$1" +
			" AND table_schema=$2 " +
			include_table +
			" GROUP BY grantee, table_schema, table_name" +
			" HAVING array_agg(privilege_type::text" +
			" ORDER BY privilege_type ASC) = (SELECT array(SELECT unnest($3::text[]) as perms ORDER BY perms ASC)))"
		err := grantDB.QueryRow(
			selectGrantQuery,
			roleName,
			schema,
			privileges,
		).Scan(&isGrantStateChanged)
		if err != nil {
			grantLogger.Error(err, fmt.Sprintf("Failed to get grant `%s` when observing", grant.Name))
			r.appendGrantStatusCondition(ctx, grant, common.FAIL, metav1.ConditionFalse, GRANTGETFAILED, err.Error())
		}
	}

	grantLogger.Info("------------------------")
	grantLogger.Info(fmt.Sprintf("Is Grant Change %t", !isGrantStateChanged))
	grantLogger.Info("------------------------")

	return isGrantStateChanged
}

func (r *GrantReconciler) appendGrantStatusCondition(ctx context.Context, grant *postgresql.Grant, typeName string, status metav1.ConditionStatus, reason string, message string) {
	time := metav1.Time{Time: time.Now()}
	condition := metav1.Condition{Type: typeName, Status: status, Reason: reason, Message: message, LastTransitionTime: time}

	var previousState postgresql.PreviousState
	if typeName == common.GRANTDATABSE {
		previousState = postgresql.PreviousState{Type: typeName, Database: *grant.Spec.Database}
	} else {
		previousState = postgresql.PreviousState{Type: typeName, Schema: *grant.Spec.Schema, Table: *grant.Spec.Table}
	}

	grantStatusConditions := grant.Status.Conditions

	if len(grantStatusConditions) > 0 {
		// Only keep 5 statuses
		if len(grantStatusConditions) >= 5 {
			if len(grantStatusConditions) > 5 {
				grant.Status.Conditions = grantStatusConditions[len(grantStatusConditions)-5:]
			}
		}

		getLastItem := grantStatusConditions[len(grantStatusConditions)-1]
		if getLastItem.Reason != condition.Reason && getLastItem.Status != metav1.ConditionFalse {
			grant.Status.Conditions = append(grant.Status.Conditions, condition)
			grant.Status.PreviousState = previousState
			err := r.Status().Update(ctx, grant)
			if err != nil {
				grantLogger.Error(err, fmt.Sprintf("Resource status update failed for grant `%s`", grant.Name))
			}
		}
	} else {
		grant.Status.Conditions = append(grant.Status.Conditions, condition)
		grant.Status.PreviousState = previousState
		err := r.Status().Update(ctx, grant)
		if err != nil {
			grantLogger.Error(err, fmt.Sprintf("Resource status update failed for grant `%s`", grant.Name))
		}
	}
}
