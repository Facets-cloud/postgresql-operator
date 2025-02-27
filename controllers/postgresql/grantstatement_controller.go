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
	"errors"
	"flag"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/Facets-cloud/postgresql-operator/apis/common"
	postgresqlv1alpha1 "github.com/Facets-cloud/postgresql-operator/apis/postgresql/v1alpha1"
	"github.com/Facets-cloud/postgresql-operator/utility"
	"github.com/Facets-cloud/postgresql-operator/validations"
)

const (
	CURRENT                   = "current"
	PREVIOUS                  = "previous"
	SUCCESS                   = "Success"
	GRANTSTATEMENT_EXECUTED   = "GrantStatementExecuted"
	GRANTSTATEMENT_DUPLICATED = "DuplicatGrantStatement"
	GRANTSTATEMENT_FINALIZER  = "grantstatement.postgresql.facets.cloud/finalizer"
	FAIL_FINALIZATION         = "FinalizationFailed"
)

var (
	logger           = log.Log.WithName("controllers").WithName("GrantStatement")
	EXCLUDED_SCHEMAS = []string{"information_schema", "pg_catalog", "pg_toast", "pg_temp", "pg_public"}
)

// GrantStatementReconciler reconciles a GrantStatement object
type GrantStatementReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=grantstatements,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=grantstatements/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=postgresql.facets.cloud,resources=grantstatements/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the GrantStatement object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *GrantStatementReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reconcileTime, err := time.ParseDuration(flag.Lookup("reconcile-period").Value.String())
	if err != nil {
		panic(err)
	}

	// get grantstatement resource
	grantStatement := &postgresqlv1alpha1.GrantStatement{}
	err = r.Get(ctx, req.NamespacedName, grantStatement)
	if err != nil {
		return ctrl.Result{}, nil
	}

	if grantStatement.GetDeletionTimestamp() == nil && !containsString(grantStatement.GetFinalizers(), GRANTSTATEMENT_FINALIZER) {
		controllerutil.AddFinalizer(grantStatement, GRANTSTATEMENT_FINALIZER)
		if err := r.Update(ctx, grantStatement); err != nil {
			message := fmt.Sprintf("Failed to add finalizer for GrantStatement %s/%s", grantStatement.Namespace, grantStatement.Name)
			logger.Error(err, message)
			r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, message)
			return ctrl.Result{RequeueAfter: reconcileTime}, err
		}
	}

	if grantStatement.GetDeletionTimestamp() != nil {
		if containsString(grantStatement.GetFinalizers(), GRANTSTATEMENT_FINALIZER) {
			err := r.finalizeGrantStatement(ctx, grantStatement)
			if err != nil && !strings.Contains(err.Error(), "not found") {
				message := fmt.Sprintf("Failed while finalising for GrantStatement %s/%s", grantStatement.Namespace, grantStatement.Name)
				logger.Error(err, fmt.Sprintf("Failed to finalize GrantStatement %s/%s", grantStatement.Namespace, grantStatement.Name))
				r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, FAIL_FINALIZATION, message)
				return ctrl.Result{RequeueAfter: reconcileTime}, nil
			}

			controllerutil.RemoveFinalizer(grantStatement, GRANTSTATEMENT_FINALIZER)
			if err := r.Update(ctx, grantStatement); err != nil {
				message := fmt.Sprintf("Failed to remove finaliser for GrantStatement %s/%s", grantStatement.Namespace, grantStatement.Name)
				logger.Error(err, message)
				r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, FAIL_FINALIZATION, message)
				return ctrl.Result{RequeueAfter: reconcileTime}, nil
			}
		}
		return ctrl.Result{}, nil
	}

	// validate grant statement
	err = validations.ValidateGrantStatement(grantStatement)
	if err != nil {
		message := fmt.Sprintf("Validations failed for GrantStatement %s", grantStatement.Name)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, err.Error())
		logger.Error(err, message)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, FAIL_FINALIZATION, message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}

	// get role resource
	role := &postgresqlv1alpha1.Role{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: grantStatement.Spec.RoleRef.Namespace,
		Name:      grantStatement.Spec.RoleRef.Name,
	}, role)
	if err != nil {
		message := fmt.Sprintf("Failed to get role resource %s/%s for GrantStatement %s", grantStatement.Spec.RoleRef.Namespace, grantStatement.Spec.RoleRef.Name, grantStatement.Name)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, err.Error())
		logger.Error(err, message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}

	// get database connection secret
	connectionSecret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: role.Spec.ConnectSecretRef.Namespace,
		Name:      role.Spec.ConnectSecretRef.Name,
	}, connectionSecret)
	if err != nil {
		message := fmt.Sprintf("Failed to get connection secret %s/%s for GrantStatement %s", role.Spec.ConnectSecretRef.Name, role.Spec.ConnectSecretRef.Namespace, grantStatement.Name)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, err.Error())
		logger.Error(err, message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}

	// Check if any Database Connection secret value is empty
	isEmpty, requiredSecretKeys := common.IsSecretsValueEmtpy(connectionSecret)
	if isEmpty {
		message := fmt.Sprintf("The value for required keys %s in secret %s/%s should not be empty or null.", requiredSecretKeys, role.Spec.ConnectSecretRef.Namespace, role.Spec.ConnectSecretRef.Name)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, message)
		logger.Error(fmt.Errorf("bad secret reference %s/%s for GrantStatement %s", role.Spec.ConnectSecretRef.Name, role.Spec.ConnectSecretRef.Namespace, grantStatement.Name), message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}

	// check if grantstatement is duplicated
	existingGrantStatements := &postgresqlv1alpha1.GrantStatementList{}
	err = r.List(ctx, existingGrantStatements)
	if err != nil {
		message := "Failed to list GrantStatements"
		logger.Error(err, message)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}
	for _, existingGS := range existingGrantStatements.Items {
		if reflect.DeepEqual(grantStatement.Spec.RoleRef, existingGS.Spec.RoleRef) && reflect.DeepEqual(grantStatement.Spec.Database, existingGS.Spec.Database) && !(grantStatement.Name == existingGS.Name && grantStatement.Namespace == existingGS.Namespace) {
			message := fmt.Sprintf("Already a GrantStatement %s/%s with RoleRef %s/%s and Database %s exists. Delete this GrantStatement.", existingGS.Namespace, existingGS.Name, grantStatement.Spec.RoleRef.Namespace, grantStatement.Spec.RoleRef.Name, grantStatement.Spec.Database)
			r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, GRANTSTATEMENT_DUPLICATED, message)
			logger.Error(errors.New(message), message)
			return ctrl.Result{RequeueAfter: reconcileTime}, nil
		}
	}

	// Check if the statements are same and last condition is SUCCESS
	if len(grantStatement.Status.Conditions) > 0 && isLastConditionSuccess(grantStatement.Status.Conditions) && !hasGrantStatementChanged(grantStatement) && grantStatement.GetDeletionTimestamp() == nil {
		message := fmt.Sprintf("GrantStatement %s has already been successfully executed, skipping execution", grantStatement.Name)
		logger.Info(message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}

	if len(grantStatement.Status.Conditions) > 0 && hasGrantStatementChanged(grantStatement) {

		prevDatabase := grantStatement.Status.PreviousGrantStatementState.Database
		prevRoleRef := grantStatement.Status.PreviousGrantStatementState.RoleRef
		secret, err, message := r.extractConnectionSecret(ctx, grantStatement)
		if err != nil {
			message = fmt.Sprintf("Failed to retrieve secret from previous state for GrasntStatement %s/%s: %s", grantStatement.Namespace, grantStatement.Name, message)
			r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, fmt.Sprintf("%s: %s", message, err.Error()))
			logger.Error(err, message)
			return ctrl.Result{RequeueAfter: reconcileTime}, nil
		}

		// Revoke all privileges from the role before granting new privileges
		err, message = r.revokeAllPrivileges(grantStatement, prevDatabase, prevRoleRef, secret)
		if err != nil {
			message := fmt.Sprintf("Failed to revoke all privileges for GrantStatement %s/%s: %s", grantStatement.Namespace, grantStatement.Name, message)
			r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, fmt.Sprintf("%s: %s", message, err.Error()))
			return ctrl.Result{RequeueAfter: reconcileTime}, nil
		}
	}

	database := grantStatement.Spec.Database
	statements := grantStatement.Spec.Statements
	// Connect to database
	db, err := common.ConnectToPostgres(connectionSecret, database)
	if err != nil {
		message := fmt.Sprintf("Error while connecting to database %s for GrantStatement %s/%s", database, grantStatement.Namespace, grantStatement.Name)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.CONNECTIONFAILED, err.Error())
		logger.Error(err, message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}
	defer db.Close()

	var ExecutionError error
	for _, statement := range statements {
		// Execute grant statements
		_, err = db.Exec(statement)
		if err != nil {
			ExecutionError = utility.AppendError(ExecutionError, fmt.Errorf("failed to execute statement %s for GrantStatement %s: %s", statement, grantStatement.Name, err.Error()))
		}
	}

	if ExecutionError != nil {
		message := fmt.Sprintf("Failed to execute grant statements for GrantStatement %s/%s", grantStatement.Namespace, grantStatement.Name)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, message)
		logger.Error(ExecutionError, message)
		return ctrl.Result{RequeueAfter: reconcileTime}, nil
	}

	r.appendGrantStatementStatusCondition(ctx, grantStatement, SUCCESS, metav1.ConditionTrue, GRANTSTATEMENT_EXECUTED, "Successfully executed grant statements")
	logger.Info(fmt.Sprintf("Successfully executed grant statements for GrantStatement %s", grantStatement.Name))

	return ctrl.Result{RequeueAfter: reconcileTime}, nil
}

func (r *GrantStatementReconciler) finalizeGrantStatement(ctx context.Context, grantStatement *postgresqlv1alpha1.GrantStatement) error {

	database := grantStatement.Spec.Database
	roleRef := grantStatement.Spec.RoleRef
	secret, err, message := r.extractConnectionSecret(ctx, grantStatement)
	if err != nil {
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, err.Error())
		logger.Error(err, message)
		return fmt.Errorf("failed to retrieve secret from current state for GrantStatement %s/%s: %s", grantStatement.Namespace, grantStatement.Name, err.Error())
	}
	err, message = r.revokeAllPrivileges(grantStatement, database, roleRef, secret)
	if err != nil {
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.FAIL, err.Error())
		logger.Error(err, message)
		return fmt.Errorf("failed to revoke all privileges while finalizing for GrantStatement %s/%s", grantStatement.Namespace, grantStatement.Name)
	}

	logger.Info(fmt.Sprintf("Successfully finalized GrantStatement %s", grantStatement.Name))
	return nil
}

func (r *GrantStatementReconciler) revokeAllPrivileges(grantStatement *postgresqlv1alpha1.GrantStatement, database string, roleRef common.ResourceReference, secret *corev1.Secret) (error, string) {
	var message string
	db, err := common.ConnectToPostgres(secret, database)
	if err != nil {
		message = fmt.Sprintf("Error while connecting to database %s for GrantStatement %s/%s", database, grantStatement.Namespace, grantStatement.Name)
		logger.Error(err, message)
		return err, message
	}
	defer db.Close()

	// Get all schemas
	rows, err := db.Query("SELECT schema_name FROM information_schema.schemata;")
	if err != nil {
		message = fmt.Sprintf("Error while getting schemas from database %s for GrantStatement %s/%s", database, grantStatement.Namespace, grantStatement.Name)
		logger.Error(err, message)
		return err, message
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			message = fmt.Sprintf("Error while copying schemas from database %s for GrantStatement %s/%s", database, grantStatement.Namespace, grantStatement.Name)
			logger.Error(err, message)
			return err, message
		}
		schemas = append(schemas, schema)
	}

	role := fmt.Sprintf("\"%s\"", roleRef.Name)
	rootUser := string(secret.Data[common.ResourceCredentialsSecretUserKey])

	// For each schema, execute each SQL command to revoke all privileges as a separate transaction
	tx, err := db.Begin()
	if err != nil {
		message = fmt.Sprintf("Error starting transaction for revoking privileges for GrantStatement %s/%s", grantStatement.Namespace, grantStatement.Name)
		logger.Error(err, message)
		return err, message
	}

	for _, schema := range schemas {

		queries := []string{
			fmt.Sprintf("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA \"%s\" FROM %s;", schema, role),
			fmt.Sprintf("REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA \"%s\" FROM %s;", schema, role),
			fmt.Sprintf("REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA \"%s\" FROM %s;", schema, role),
			fmt.Sprintf("REVOKE ALL ON SCHEMA \"%s\" FROM %s;", schema, role),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA \"%s\" REVOKE ALL ON TABLES FROM %s;", schema, role),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA \"%s\" REVOKE ALL ON SEQUENCES FROM %s;", schema, role),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA \"%s\" REVOKE ALL ON FUNCTIONS FROM %s;", schema, role),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA \"%s\" REVOKE ALL ON TABLES FROM %s;", rootUser, schema, role),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA \"%s\" REVOKE ALL ON SEQUENCES FROM %s;", rootUser, schema, role),
			fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA \"%s\" REVOKE ALL ON FUNCTIONS FROM %s;", rootUser, schema, role),
		}

		if containsString(EXCLUDED_SCHEMAS, schema) {
			queries = queries[4:]
		}

		for _, query := range queries {
			logger.Info(fmt.Sprintf("Executing query: %s", query))
			_, err = tx.Exec(query)
			if err != nil {
				tx.Rollback()
				message = fmt.Sprintf("Error while revoking privileges for role %s on schema %s from database %s for GrantStatement %s/%s", role, schema, database, grantStatement.Namespace, grantStatement.Name)
				logger.Error(err, message)
				return err, message
			}

		}
	}

	// Revoke all privileges on database
	query := fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE \"%s\" FROM %s", database, role)
	logger.Info(fmt.Sprintf("Executing query: %s", query))
	_, err = tx.Exec(query)
	if err != nil {
		tx.Rollback()
		message = fmt.Sprintf("Error while revoking privileges for role %s on database %s for GrantStatement %s/%s", role, database, grantStatement.Namespace, grantStatement.Name)
		logger.Error(err, message)
		return err, message
	}

	err = tx.Commit()
	if err != nil {
		message = fmt.Sprintf("Error while committing revoke queries for role %s on database %s for GrantStatement %s/%s", role, database, grantStatement.Namespace, grantStatement.Name)
		logger.Error(err, message)
		return err, message
	}

	return nil, "Successfully revoked all privileges"
}

func (r *GrantStatementReconciler) appendGrantStatementStatusCondition(ctx context.Context, grantStatement *postgresqlv1alpha1.GrantStatement, typeName string, status metav1.ConditionStatus, reason string, message string) {
	time := metav1.Time{Time: time.Now()}
	condition := metav1.Condition{Type: typeName, Status: status, Reason: reason, Message: message, LastTransitionTime: time}
	grantStatement.Status.Conditions = append(grantStatement.Status.Conditions, condition)
	// Keep only the last 5 conditions
	if len(grantStatement.Status.Conditions) > 5 {
		grantStatement.Status.Conditions = grantStatement.Status.Conditions[len(grantStatement.Status.Conditions)-5:]
	}
	// update previous state in the status
	grantStatement.Status.PreviousGrantStatementState.Database = grantStatement.Spec.Database
	grantStatement.Status.PreviousGrantStatementState.RoleRef = grantStatement.Spec.RoleRef
	grantStatement.Status.PreviousGrantStatementState.Statements = grantStatement.Spec.Statements
	err := r.Status().Update(ctx, grantStatement)
	if err != nil {
		logger.Error(err, fmt.Sprintf("Resource status update failed for GrantStatement %s", grantStatement.Name))
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *GrantStatementReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresqlv1alpha1.GrantStatement{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

func hasGrantStatementChanged(grantStatement *postgresqlv1alpha1.GrantStatement) bool {
	previousState := grantStatement.Status.PreviousGrantStatementState
	sort.Sort(sort.StringSlice(previousState.Statements))
	sort.Sort(sort.StringSlice(grantStatement.Spec.Statements))
	if reflect.DeepEqual(grantStatement.Spec.Database, previousState.Database) && reflect.DeepEqual(grantStatement.Spec.RoleRef, previousState.RoleRef) && reflect.DeepEqual(grantStatement.Spec.Statements, previousState.Statements) {
		return false
	}
	return true
}

func (r *GrantStatementReconciler) extractConnectionSecret(ctx context.Context, grantStatement *postgresqlv1alpha1.GrantStatement) (*corev1.Secret, error, string) {
	var roleRef common.ResourceReference
	roleRef = grantStatement.Spec.RoleRef
	var message string
	// get role resource
	role := &postgresqlv1alpha1.Role{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: roleRef.Namespace,
		Name:      roleRef.Name,
	}, role)
	if err != nil {
		message = fmt.Sprintf("Failed to get role resource %s/%s for GrantStatement %s", roleRef.Namespace, roleRef.Name, grantStatement.Name)
		r.appendGrantStatementStatusCondition(ctx, grantStatement, common.FAIL, metav1.ConditionFalse, common.RESOURCENOTFOUND, err.Error())
		return nil, err, message
	}

	// get database connection secret
	connectionSecret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: role.Spec.ConnectSecretRef.Namespace,
		Name:      role.Spec.ConnectSecretRef.Name,
	}, connectionSecret)
	if err != nil {
		message = fmt.Sprintf("Failed to get connection secret %s/%s for GrantStatement %s", role.Spec.ConnectSecretRef.Name, role.Spec.ConnectSecretRef.Namespace, grantStatement.Name)
		return nil, err, message
	}
	return connectionSecret, nil, ""
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func isLastConditionSuccess(conditions []metav1.Condition) bool {
	if len(conditions) == 0 {
		return false
	}
	lastCondition := conditions[len(conditions)-1]
	return lastCondition.Type == SUCCESS && lastCondition.Status == metav1.ConditionTrue
}
