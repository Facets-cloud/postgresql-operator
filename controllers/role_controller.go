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
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/pramodh-ayyappan/database-operator/api/common"
	"github.com/pramodh-ayyappan/database-operator/api/v1alpha1"
	postgresv1alpha1 "github.com/pramodh-ayyappan/database-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

var logger = log.Log.WithName("role_controller")

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

	rs := &v1alpha1.Role{}
	err := r.Get(ctx, req.NamespacedName, rs)
	if err != nil {
		return ctrl.Result{}, nil
	}

	secret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: rs.Spec.ConnectSecretRef.Namespace,
		Name:      rs.Spec.ConnectSecretRef.Name,
	}, secret)

	endpoint := string(secret.Data[common.ResourceCredentialsSecretEndpointKey])
	port := string(secret.Data[common.ResourceCredentialsSecretPortKey])
	username := string(secret.Data[common.ResourceCredentialsSecretUserKey])
	password := string(secret.Data[common.ResourceCredentialsSecretPasswordKey])

	log.Info(fmt.Sprintf("current time in hour : %s\n", endpoint))
	log.Info(fmt.Sprintf("current time in hour : %s\n", port))
	log.Info(fmt.Sprintf("current time in hour : %s\n", username))
	log.Info(fmt.Sprintf("current time in hour : %s\n", password))

	return ctrl.Result{RequeueAfter: time.Duration(30 * time.Second)}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresv1alpha1.Role{}).
		Complete(r)
}
