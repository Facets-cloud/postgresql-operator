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

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/pramodh-ayyappan/database-operator/api/common"
	"github.com/pramodh-ayyappan/database-operator/api/v1alpha1"
	postgresv1alpha1 "github.com/pramodh-ayyappan/database-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const finalizer = "role.postgres.facets.cloud/finalizer"
const reconcileTime = time.Duration(30 * time.Second)

var (
	logger = log.Log.WithName("role_controller")
	db     *sql.DB
)

// RoleReconciler reconciles a Role object
type RoleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=postgres.facets.cloud,resources=roles,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=postgres.facets.cloud,resources=roles/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=postgres.facets.cloud,resources=roles/finalizers,verbs=update

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

	logger.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)

	role := &v1alpha1.Role{}
	err := r.Get(ctx, req.NamespacedName, role)
	if err != nil {
		return ctrl.Result{}, nil
	}

	// Connect to Postgres DB
	db, err = r.Connect(ctx, role)
	if err != nil {
		reason := "Failed connecting to database."
		logger.Error(err, reason)
		r.appendCondition(ctx, role, common.FAIL, metav1.ConditionFalse, "ConnectionFailed", err.Error())

	}
	defer db.Close()

	// Get role password from secret
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
			"Failed to get password secret %s in namespace %s",
			role.Spec.PasswordSecretRef.Name,
			role.Spec.PasswordSecretRef.Namespace,
		)
		logger.Error(err, reason)
		r.appendCondition(ctx, role, common.FAIL, metav1.ConditionFalse, "ResourceNotFound", err.Error())
	}

	rolePassword := string(passwordSecret.Data[role.Spec.PasswordSecretRef.Key])

	if !controllerutil.ContainsFinalizer(role, finalizer) {
		controllerutil.AddFinalizer(role, finalizer)
		err = r.Update(ctx, role)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	isRoleMarkedToBeDeleted := role.GetDeletionTimestamp() != nil
	if isRoleMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(role, finalizer) {
			if err := r.DeletRole(ctx, role); err != nil {
				return ctrl.Result{}, err
			}

			controllerutil.RemoveFinalizer(role, finalizer)
			err := r.Update(ctx, role)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Check if Role exists and create/alter role
	isRoleExists, isObservedStateSame := r.IsRoleExists(ctx, role)
	if !isRoleExists {
		logger.Info(fmt.Sprintf("Creating Role: %s\n", role.Name))
		// Create Role
		isRoleCreated := r.CreateRole(ctx, role, rolePassword)
		if isRoleCreated {
			r.appendCondition(ctx, role, common.CREATE, metav1.ConditionTrue, "RoleCreated", "Role got created")
		}
	} else if !isObservedStateSame {
		logger.Info(fmt.Sprintf("The role `%s` is not in sync with database. Started role sync!!!", role.Name))
		isRoleSynced := r.SyncRole(ctx, role, rolePassword)
		if isRoleSynced {
			r.appendCondition(ctx, role, common.SYNC, metav1.ConditionTrue, "RoleSynced", "Synced role with database")
		}
	} else {
		logger.Info(fmt.Sprintf("Role `%s` already exists so skipping\n", role.Name))
		r.appendCondition(ctx, role, common.CREATE, metav1.ConditionTrue, "RoleCreated", "Role got created")
	}

	return ctrl.Result{RequeueAfter: reconcileTime}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresv1alpha1.Role{}).
		Complete(r)
}

func (r *RoleReconciler) Connect(ctx context.Context, role *postgresv1alpha1.Role) (*sql.DB, error) {
	connectionSecret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: role.Spec.ConnectSecretRef.Namespace,
		Name:      role.Spec.ConnectSecretRef.Name,
	}, connectionSecret)
	if err != nil {
		reason := fmt.Sprintf(
			"Failed to get connection secret %s in namespace %s",
			role.Spec.ConnectSecretRef.Name,
			role.Spec.ConnectSecretRef.Namespace,
		)
		logger.Error(err, reason)
		r.appendCondition(ctx, role, common.FAIL, metav1.ConditionFalse, "ResourceNotFound", err.Error())
	}

	// endpoint := string(connectionSecret.Data[common.ResourceCredentialsSecretEndpointKey])
	port := string(connectionSecret.Data[common.ResourceCredentialsSecretPortKey])
	username := string(connectionSecret.Data[common.ResourceCredentialsSecretUserKey])
	password := string(connectionSecret.Data[common.ResourceCredentialsSecretPasswordKey])
	defaultDatabase := *role.Spec.DefaultDatabase

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=%s",
		"127.0.0.1", // endpoint,
		port,
		username,
		password,
		defaultDatabase,
		*role.Spec.SSLMode,
	)

	db, err := sql.Open("postgres", psqlInfo)

	return db, err
}

func (r *RoleReconciler) IsRoleExists(ctx context.Context, role *postgresv1alpha1.Role) (bool, bool) {
	var isRoleExists bool
	var isObservedStateSame bool
	selectRoleQuery := "SELECT EXISTS (SELECT 1 " +
		"FROM pg_roles WHERE rolname = $1 )"

	// Check if Role exists
	err := db.QueryRow(
		selectRoleQuery,
		role.Name,
	).Scan(&isRoleExists)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to get role `%s`", role.Name))
		r.appendCondition(ctx, role, common.FAIL, metav1.ConditionFalse, "FailedToGetRole", err.Error())
	}

	// Check if the state of Role changed
	if isRoleExists {
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

		err := db.QueryRow(
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
		).Scan(&isObservedStateSame)
		if err != nil {
			logger.Error(err, fmt.Sprintf("Failed to get role `%s`", role.Name))
			r.appendCondition(ctx, role, common.FAIL, metav1.ConditionFalse, "FailedToGetRole", err.Error())
		}
	}

	return isRoleExists, isObservedStateSame
}

func (r *RoleReconciler) CreateRole(ctx context.Context, role *postgresv1alpha1.Role, rolePassword string) bool {
	privileges := strings.Join(PrivilegesToClauses(role.Spec.Privileges), " ")

	createRoleQuery := fmt.Sprintf("CREATE ROLE %s WITH %s PASSWORD '%s'", role.Name, privileges, rolePassword)
	_, err := db.Exec(createRoleQuery)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to create role `%s`", role.Name))
		r.appendCondition(ctx, role, "FailedToCreateRole", metav1.ConditionFalse, "FailedToCreateRole", err.Error())
		return false
	}

	logger.Info(fmt.Sprintf("Role `%s` got created successfully", role.Name))
	return true
}

func (r *RoleReconciler) SyncRole(ctx context.Context, role *postgresv1alpha1.Role, rolePassword string) bool {
	privileges := strings.Join(PrivilegesToClauses(role.Spec.Privileges), " ")

	createRoleQuery := fmt.Sprintf("ALTER ROLE %s WITH %s PASSWORD '%s'", role.Name, privileges, rolePassword)
	_, err := db.Exec(createRoleQuery)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to sync role `%s`", role.Name))
		r.appendCondition(ctx, role, common.FAIL, metav1.ConditionFalse, "FailedToSyncRole", err.Error())
		return false
	}

	logger.Info(fmt.Sprintf("Role `%s` got synced successfully", role.Name))
	return true
}

func (r *RoleReconciler) DeletRole(ctx context.Context, role *v1alpha1.Role) error {
	deleteRoleQuery := fmt.Sprintf("DROP ROLE IF EXISTS %s", role.Name)
	_, err := db.Exec(deleteRoleQuery)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Failed to delete role `%s`", role.Name))
		r.appendCondition(ctx, role, common.FAIL, metav1.ConditionFalse, "FailedToDeleteRole", err.Error())
	}

	logger.Info(fmt.Sprintf("Role `%s` got deleted successfully", role.Name))
	r.appendCondition(ctx, role, common.DELETE, metav1.ConditionTrue, "RoleDeleted", "Role got successfully deleted")
	return err
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

func PrivilegesToClauses(p postgresv1alpha1.RolePrivilege) []string {
	// Never copy user inputted data to this string. These values are
	// passed directly into the query.
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

func (r *RoleReconciler) appendCondition(ctx context.Context, role *postgresv1alpha1.Role, typeName string, status metav1.ConditionStatus, reason string, message string) error {
	if !r.containsCondition(ctx, role, reason) {
		time := metav1.Time{Time: time.Now()}
		condition := metav1.Condition{Type: typeName, Status: status, Reason: reason, Message: message, LastTransitionTime: time}
		role.Status.Conditions = append(role.Status.Conditions, condition)
		err := r.Status().Update(ctx, role)
		if err != nil {
			logger.Error(err, "Role resource status update failed")
		}
	}
	return nil
}

func (r *RoleReconciler) containsCondition(ctx context.Context, role *postgresv1alpha1.Role, reason string) bool {
	output := false
	for _, condition := range role.Status.Conditions {
		if condition.Reason == reason {
			output = true
		}
	}
	return output
}
