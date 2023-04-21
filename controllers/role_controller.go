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
	"sigs.k8s.io/controller-runtime/pkg/log"

	"database/sql"

	"github.com/go-logr/logr"
	_ "github.com/lib/pq"
	"github.com/pramodh-ayyappan/database-operator/api/common"
	"github.com/pramodh-ayyappan/database-operator/api/v1alpha1"
	postgresv1alpha1 "github.com/pramodh-ayyappan/database-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

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

	log := logger.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)

	role := &v1alpha1.Role{}
	err := r.Get(ctx, req.NamespacedName, role)
	if err != nil {
		return ctrl.Result{}, nil
	}

	// Connect to Postgres DB
	db, err = Connect(ctx, r, role, log)
	if err != nil {
		log.Error(err, "Failed connecting to database... Please check the connection details")
	}
	defer db.Close()

	// Ping database and check if connectivity is available
	err = db.Ping()
	if err != nil {
		log.Error(err, "Pinging to database failed... Please check the connection details")
	} else {
		log.Info("Pinging database successful!!!")
	}

	passwordSecret := &corev1.Secret{}
	err = r.Get(
		ctx, types.NamespacedName{
			Namespace: role.Spec.PasswordSecretRef.Namespace,
			Name:      role.Spec.PasswordSecretRef.Name,
		},
		passwordSecret,
	)
	if err != nil {
		log.Error(err,
			fmt.Sprintf(
				"Failed to get password secret %s in namespace %s",
				role.Spec.PasswordSecretRef.Name,
				role.Spec.PasswordSecretRef.Namespace,
			),
		)
	}

	rolePassword := string(passwordSecret.Data[role.Spec.PasswordSecretRef.Key])

	// Check if Role exists and create/alter role
	isRoleExists, isObservedStateSame := IsRoleExists(ctx, r, db, role, log)
	if !isRoleExists {
		log.Info(fmt.Sprintf("Creating Role: %s\n", role.Name))
		// Create Role
		CreateRole(ctx, r, db, role, log, rolePassword)
	} else if !isObservedStateSame {
		log.Info(fmt.Sprintf("Observed that the state of role `%s` is different. Started role sync!!!", role.Name))
		SyncRole(ctx, r, db, role, log, rolePassword)
	} else {
		log.Info(fmt.Sprintf("Role `%s` already exists so skipping\n", role.Name))
	}

	return ctrl.Result{RequeueAfter: time.Duration(30 * time.Second)}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresv1alpha1.Role{}).
		Complete(r)
}

func Connect(ctx context.Context, r *RoleReconciler, role *postgresv1alpha1.Role, log logr.Logger) (*sql.DB, error) {
	connectionSecret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: role.Spec.ConnectSecretRef.Namespace,
		Name:      role.Spec.ConnectSecretRef.Name,
	}, connectionSecret)
	if err != nil {
		log.Error(err,
			fmt.Sprintf(
				"Failed to get connection secret %s in namespace %s",
				role.Spec.ConnectSecretRef.Name,
				role.Spec.ConnectSecretRef.Namespace,
			),
		)
	}

	// endpoint := string(connectionSecret.Data[common.ResourceCredentialsSecretEndpointKey])
	port := string(connectionSecret.Data[common.ResourceCredentialsSecretPortKey])
	username := string(connectionSecret.Data[common.ResourceCredentialsSecretUserKey])
	password := string(connectionSecret.Data[common.ResourceCredentialsSecretPasswordKey])
	defaultDatabase := *role.Spec.DefaultDatabase

	// log.Info(fmt.Sprintf("Endpoint : %s\n", endpoint))
	// log.Info(fmt.Sprintf("Port : %s\n", port))
	// log.Info(fmt.Sprintf("Username : %s\n", username))
	// log.Info(fmt.Sprintf("Password : %s\n", password))
	// log.Info(fmt.Sprintf("Role Password : %s\n", passwordSecret.Data[role.Spec.PasswordSecretRef.Key]))
	// log.Info(fmt.Sprintf("Default Database : %s\n", defaultDatabase))

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

func IsRoleExists(ctx context.Context, r *RoleReconciler, db *sql.DB, role *postgresv1alpha1.Role, log logr.Logger) (bool, bool) {
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
		log.Error(err, fmt.Sprintf("Failed to get role `%s`", role.Name))
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
			log.Error(err, fmt.Sprintf("Failed to get role `%s`", role.Name))
		}
	}

	return isRoleExists, isObservedStateSame
}

func CreateRole(ctx context.Context, r *RoleReconciler, db *sql.DB, role *postgresv1alpha1.Role, log logr.Logger, rolePassword string) bool {
	privileges := strings.Join(PrivilegesToClauses(role.Spec.Privileges), " ")

	createRoleQuery := fmt.Sprintf("CREATE ROLE %s WITH %s PASSWORD '%s'", role.Name, privileges, rolePassword)
	_, execErr := db.Exec(createRoleQuery)
	if execErr != nil {
		log.Error(execErr, fmt.Sprintf("Failed to create role `%s`", role.Name))
		return false
	}

	log.Info(fmt.Sprintf("Role `%s` got created successfully", role.Name))
	return true
}

func SyncRole(ctx context.Context, r *RoleReconciler, db *sql.DB, role *postgresv1alpha1.Role, log logr.Logger, rolePassword string) bool {
	privileges := strings.Join(PrivilegesToClauses(role.Spec.Privileges), " ")

	createRoleQuery := fmt.Sprintf("ALTER ROLE %s WITH %s PASSWORD '%s'", role.Name, privileges, rolePassword)
	_, execErr := db.Exec(createRoleQuery)
	if execErr != nil {
		log.Error(execErr, fmt.Sprintf("Failed to sync role `%s`", role.Name))
		return false
	}

	log.Info(fmt.Sprintf("Role `%s` got synced successfully", role.Name))
	return true
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
