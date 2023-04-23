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
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/pramodh-ayyappan/database-operator/apis/common"
	"github.com/pramodh-ayyappan/database-operator/apis/postgresql/v1alpha1"
	postgresql "github.com/pramodh-ayyappan/database-operator/apis/postgresql/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	roleFinalizer           = "role.postgresql.facets.cloud/finalizer"
	roleReconcileTime       = time.Duration(300 * time.Second)
	passwordSecretNameField = ".spec.passwordSecretRef.name"
	connectSecretNameField  = ".spec.connectSecretRef.name"
	errRoleAlreadyExists    = "Role already exists"
	errRoleCreatedOutside   = "Role created outside of database operator"
	errRoleDeletedOutside   = "Role deleted outside of database operator"
	ROLECREATED             = "RoleCreated"
	ROLEEXISTS              = "RoleExists"
	ROLESYNCED              = "RoleSynced"
	ROLEPASSWORDSYNCED      = "RolePasswordSynced"
	ROLECREATEFAILED        = "RoleCreateFailed"
	ROLESYNCFAILED          = "RoleSyncFailed"
	ROLEPASSWORDSYNCFAILED  = "RolePasswordSyncFailed"
	ROLEDELETED             = "RoleDeleted"
	ROLEDELETEFAILED        = "RoleDeleteFailed"
	RESOURCENOTFOUND        = "ResourceNotFound"
	CONNECTIONFAILED        = "ConnectionFailed"
	ROLEGETFAILED           = "RoleGetFailed"
)

var (
	roleLogger              = log.Log.WithName("role_controller")
	roleDB                  *sql.DB
	passwordSecretVersion   string
	connectionSecretVersion string
)

// RoleReconciler reconciles a Role object
type RoleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=roles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=roles/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=roles/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Role object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *RoleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {

	roleLogger.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)

	role := &postgresql.Role{}
	err := r.Get(ctx, req.NamespacedName, role)
	if err != nil {
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
			"Failed to get connection secret `%s/%s` for role `%s`",
			role.Spec.ConnectSecretRef.Name,
			role.Spec.ConnectSecretRef.Namespace,
			role.Name,
		)
		r.appendRoleStatusCondition(ctx, role, common.FAIL, metav1.ConditionFalse, RESOURCENOTFOUND, err.Error())
		roleLogger.Error(err, reason)
	}

	// Get Role password secret
	passwordSecret := &corev1.Secret{}
	err = r.Get(
		ctx, types.NamespacedName{
			Namespace: role.Spec.PasswordSecretRef.Namespace,
			Name:      role.Spec.PasswordSecretRef.Name,
		},
		passwordSecret,
	)
	if err != nil {
		reason := fmt.Sprintf(
			"Failed to get password secret `%s/%s` for role `%s`",
			role.Spec.PasswordSecretRef.Name,
			role.Spec.PasswordSecretRef.Namespace,
			role.Name,
		)
		r.appendRoleStatusCondition(ctx, role, common.FAIL, metav1.ConditionFalse, RESOURCENOTFOUND, err.Error())
		roleLogger.Error(err, reason)
	}

	// Connect to Postgres DB
	common.ConnectToPostgres(connectionSecret)
	if err != nil {
		reason := fmt.Sprintf("Failed connecting to database for role `%s`", role.Name)
		roleLogger.Error(err, reason)
		r.appendRoleStatusCondition(ctx, role, common.FAIL, metav1.ConditionFalse, CONNECTIONFAILED, err.Error())

	}
	defer roleDB.Close()

	rolePassword := string(passwordSecret.Data[role.Spec.PasswordSecretRef.Key])
	roleAnnotationPwdSecretVer := role.Annotations["passwordSecretVersion"]
	if roleAnnotationPwdSecretVer != "" {
		if roleAnnotationPwdSecretVer != passwordSecret.ResourceVersion {
			typeName, status, reason, message := r.SyncRole(ctx, role, rolePassword, true)
			if status == metav1.ConditionTrue {
				r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
			} else {
				if reason == ROLESYNCFAILED && message == errRoleDeletedOutside {
					r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
					typeName, status, reason, message = r.CreateRole(ctx, role, rolePassword)
					r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
				} else {
					r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
				}
			}
		}
	}

	// Add annotation for external secret update to be detected
	passwordSecretVersion = passwordSecret.ResourceVersion
	role.Annotations["passwordSecretVersion"] = passwordSecretVersion
	r.Update(ctx, role)

	if role.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(role, roleFinalizer) {
			controllerutil.AddFinalizer(role, roleFinalizer)
			err = r.Update(ctx, role)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		if controllerutil.ContainsFinalizer(role, roleFinalizer) {
			if _, _, _, _, err := r.DeletRole(ctx, role); err != nil {
				return ctrl.Result{}, err
			}

			controllerutil.RemoveFinalizer(role, roleFinalizer)
			err := r.Update(ctx, role)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Manage Role based on conditions
	if !(len(role.Status.Conditions) > 0) {
		typeName, status, reason, message := r.CreateRole(ctx, role, rolePassword)
		if status == metav1.ConditionTrue {
			r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
		} else {
			if reason == ROLECREATEFAILED && message == errRoleCreatedOutside {
				r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
				typeName, status, reason, message, _ := r.DeletRole(ctx, role)
				r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
				typeName, status, reason, message = r.CreateRole(ctx, role, rolePassword)
				r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
			} else {
				r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
			}
		}
	} else {
		isRoleStateInSync := r.ObserveRoleState(ctx, role)
		if !isRoleStateInSync {
			typeName, status, reason, message := r.SyncRole(ctx, role, rolePassword, false)
			if status == metav1.ConditionTrue {
				r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
			} else {
				if reason == ROLESYNCFAILED && message == errRoleDeletedOutside {
					r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
					typeName, status, reason, message = r.CreateRole(ctx, role, rolePassword)
					r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
				} else {
					r.appendRoleStatusCondition(ctx, role, typeName, status, reason, message)
				}
			}
		}
	}

	return ctrl.Result{RequeueAfter: roleReconcileTime}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &postgresql.Role{}, passwordSecretNameField, func(rawObj client.Object) []string {
		roleData := rawObj.(*postgresql.Role)
		if roleData.Spec.PasswordSecretRef.Name == "" {
			return nil
		}
		return []string{roleData.Spec.PasswordSecretRef.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresql.Role{}).
		Watches(
			&source.Kind{Type: &corev1.Secret{}},
			handler.EnqueueRequestsFromMapFunc(r.findObjectsForSecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Complete(r)
}

func (r *RoleReconciler) findObjectsForSecret(secret client.Object) []reconcile.Request {
	roles := &postgresql.RoleList{}
	passwordSecretlistOps := &client.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(passwordSecretNameField, secret.GetName()),
		Namespace:     secret.GetNamespace(),
	}

	err := r.List(context.TODO(), roles, passwordSecretlistOps)
	if err != nil {
		return []reconcile.Request{}
	}

	requests := make([]reconcile.Request, len(roles.Items))
	for i, item := range roles.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}

	return requests
}

func (r *RoleReconciler) CreateRole(ctx context.Context, role *postgresql.Role, rolePassword string) (string, metav1.ConditionStatus, string, string) {
	privileges := strings.Join(PrivilegesToClauses(role.Spec.Privileges), " ")
	createRoleQuery := fmt.Sprintf("CREATE ROLE \"%s\" WITH %s PASSWORD '%s' CONNECTION LIMIT %d", role.Name, privileges, rolePassword, *role.Spec.ConnectionLimit)
	_, err := roleDB.Exec(createRoleQuery)
	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("pq: role \"%s\" already exists", role.Name)) {
			roleLogger.Error(err, fmt.Sprintf("Role `%s` created outside of database operator.", role.Name))
			return common.FAIL, metav1.ConditionFalse, ROLECREATEFAILED, errRoleCreatedOutside
		} else {
			roleLogger.Error(err, fmt.Sprintf("Failed to create role `%s`, Check if the secret `%s/%s` has valid database connection details", role.Name, role.Spec.ConnectSecretRef.Namespace, role.Spec.ConnectSecretRef.Name))
			return common.FAIL, metav1.ConditionFalse, ROLECREATEFAILED, fmt.Sprintf("%s, Check if the secret `%s/%s` has valid database connection details", err.Error(), role.Spec.ConnectSecretRef.Namespace, role.Spec.ConnectSecretRef.Name)
		}
	}

	roleLogger.Info(fmt.Sprintf("Role `%s` got created successfully", role.Name))
	return common.CREATE, metav1.ConditionTrue, ROLECREATED, "Role created successfully"
}

func (r *RoleReconciler) DeletRole(ctx context.Context, role *v1alpha1.Role) (string, metav1.ConditionStatus, string, string, error) {
	deleteRoleQuery := fmt.Sprintf("DROP ROLE IF EXISTS \"%s\"", role.Name)
	_, err := roleDB.Exec(deleteRoleQuery)
	if err != nil {
		roleLogger.Error(err, fmt.Sprintf("Failed to delete role `%s`", role.Name))
		return common.FAIL, metav1.ConditionFalse, ROLEDELETEFAILED, err.Error(), err
	}

	roleLogger.Info(fmt.Sprintf("Role `%s` got deleted successfully", role.Name))
	return common.DELETE, metav1.ConditionTrue, ROLEDELETED, "Role deleted successfully", err
}

func (r *RoleReconciler) SyncRole(ctx context.Context, role *postgresql.Role, rolePassword string, isPasswordSync bool) (string, metav1.ConditionStatus, string, string) {
	privileges := strings.Join(PrivilegesToClauses(role.Spec.Privileges), " ")

	alterRoleQuery := fmt.Sprintf("ALTER ROLE \"%s\" WITH %s PASSWORD '%s' CONNECTION LIMIT %d", role.Name, privileges, rolePassword, *role.Spec.ConnectionLimit)
	_, err := roleDB.Exec(alterRoleQuery)
	if err != nil {
		if strings.Contains(err.Error(), fmt.Sprintf("pq: role \"%s\" does not exist", role.Name)) {
			roleLogger.Error(err, fmt.Sprintf("Failed to sync role `%s`. Role deleted outside of database operator ", role.Name))
			return common.SYNC, metav1.ConditionFalse, ROLESYNCFAILED, errRoleDeletedOutside
		} else {
			roleLogger.Error(err, fmt.Sprintf("Failed to sync role `%s`", role.Name))
			return common.SYNC, metav1.ConditionFalse, ROLESYNCFAILED, err.Error()
		}
	}

	if isPasswordSync {
		roleLogger.Info(fmt.Sprintf("Role `%s` password got synced successfully", role.Name))
		return common.SYNC, metav1.ConditionTrue, ROLEPASSWORDSYNCED, "Role password synced successfully"
	}
	roleLogger.Info(fmt.Sprintf("Role `%s` got synced successfully", role.Name))
	return common.SYNC, metav1.ConditionTrue, ROLESYNCED, "Role synced successfully"
}

func (r *RoleReconciler) ObserveRoleState(ctx context.Context, role *postgresql.Role) bool {
	var isRoleStateChanged bool
	observeRoleStateQuery := "SELECT EXISTS (SELECT 1 " +
		"FROM pg_roles WHERE rolname = $1 " +
		"AND rolsuper = $2 " +
		"AND rolinherit = $3 " +
		"AND rolcreaterole = $4 " +
		"AND rolcreatedb = $5 " +
		"AND rolcanlogin = $6 " +
		"AND rolreplication = $7 " +
		"AND rolconnlimit = $8 " +
		"AND rolbypassrls = $9)"

	err := roleDB.QueryRow(
		observeRoleStateQuery,
		role.Name,
		&role.Spec.Privileges.SuperUser,
		&role.Spec.Privileges.Inherit,
		&role.Spec.Privileges.CreateRole,
		&role.Spec.Privileges.CreateDb,
		&role.Spec.Privileges.Login,
		&role.Spec.Privileges.Replication,
		&role.Spec.ConnectionLimit,
		&role.Spec.Privileges.BypassRls,
	).Scan(&isRoleStateChanged)
	if err != nil {
		roleLogger.Error(err, fmt.Sprintf("Failed to get role `%s` when observing ", role.Name))
		r.appendRoleStatusCondition(ctx, role, common.FAIL, metav1.ConditionFalse, ROLEGETFAILED, err.Error())
	}
	return isRoleStateChanged

}

func NegateClause(clause string, negate *bool, out *[]string) {
	// If clause boolean is not set (nil pointer), do not push a setting.
	// This means the postgres default is applied.
	if negate == nil {
		return
	}

	if !(*negate) {
		clause = "NO" + clause
	}
	*out = append(*out, clause)
}

func PrivilegesToClauses(p postgresql.RolePrivilege) []string {
	pc := []string{}

	NegateClause("SUPERUSER", p.SuperUser, &pc)
	NegateClause("INHERIT", p.Inherit, &pc)
	NegateClause("CREATEDB", p.CreateDb, &pc)
	NegateClause("CREATEROLE", p.CreateRole, &pc)
	NegateClause("LOGIN", p.Login, &pc)
	NegateClause("REPLICATION", p.Replication, &pc)
	NegateClause("BYPASSRLS", p.BypassRls, &pc)

	return pc
}

func (r *RoleReconciler) appendRoleStatusCondition(ctx context.Context, role *postgresql.Role, typeName string, status metav1.ConditionStatus, reason string, message string) {
	time := metav1.Time{Time: time.Now()}
	condition := metav1.Condition{Type: typeName, Status: status, Reason: reason, Message: message, LastTransitionTime: time}

	roleStatusConditions := role.Status.Conditions

	// Only keep 5 statuses
	if len(roleStatusConditions) >= 5 {
		if len(roleStatusConditions) > 5 {
			roleStatusConditions = roleStatusConditions[len(roleStatusConditions)-5:]
		}
	}

	getLastItem := roleStatusConditions[len(roleStatusConditions)-1]
	if getLastItem.Reason != condition.Reason && getLastItem.Status != metav1.ConditionFalse {
		role.Status.Conditions = append(role.Status.Conditions, condition)
		err := r.Status().Update(ctx, role)
		if err != nil {
			roleLogger.Error(err, fmt.Sprintf("Resource status update failed for role `%s`", role.Name))
		}
	}
}
