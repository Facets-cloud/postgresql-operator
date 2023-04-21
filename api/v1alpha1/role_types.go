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

package v1alpha1

import (
	common "github.com/pramodh-ayyappan/database-operator/api/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RolePrivilege is the PostgreSQL identifier to add or remove a permission
// on a role.
// See https://www.postgresql.org/docs/current/sql-createrole.html for available privileges.
type RolePrivilege struct {
	// SuperUser grants SUPERUSER privilege when true.
	// +optional
	// +kubebuilder:default=false
	SuperUser *bool `json:"superUser,omitempty"`

	// CreateDb grants CREATEDB when true, allowing the role to create databases.
	// +optional
	// +kubebuilder:default=false
	CreateDb *bool `json:"createDb,omitempty"`

	// CreateRole grants CREATEROLE when true, allowing this role to create other roles.
	// +optional
	// +kubebuilder:default=false
	CreateRole *bool `json:"createRole,omitempty"`

	// Login grants LOGIN when true, allowing the role to login to the server.
	// +optional
	// +kubebuilder:default=true
	Login *bool `json:"login,omitempty"`

	// Inherit grants INHERIT when true, allowing the role to inherit permissions
	// from other roles it is a member of.
	// +optional
	// +kubebuilder:default=false
	Inherit *bool `json:"inherit,omitempty"`

	// Replication grants REPLICATION when true, allowing the role to connect in replication mode.
	// +optional
	// +kubebuilder:default=false
	Replication *bool `json:"replication,omitempty"`

	// BypassRls grants BYPASSRLS when true, allowing the role to bypass row-level security policies.
	// +optional
	// +kubebuilder:default=false
	BypassRls *bool `json:"bypassRls,omitempty"`
}

// RoleSpec defines the desired state of Role
type RoleSpec struct {
	// ConnectSecretRef references the secret that contains database details () used
	// to create this role.
	// +kubebuilder:validation:Required
	ConnectSecretRef common.SecretReference `json:"connectSecretRef,omitempty"`

	// PasswordSecretRef references the secret that contains the password used
	// for this role.
	// +kubebuilder:validation:Required
	PasswordSecretRef common.SecretKeySelector `json:"passwordSecretRef,omitempty"`

	// Defines the Default Database used to set up a connection to the provided
	// PostgreSQL instance
	// +kubebuilder:default=postgres
	// +kubebuilder:validation:Optional
	DefaultDatabase *string `json:"defaultDatabase,omitempty"`

	// Defines the SSL mode used to set up a connection to the provided
	// PostgreSQL instance
	// +kubebuilder:validation:Enum=disable;allow;prefer;require;verify-ca;verify-full
	// +kubebuilder:default=disable
	// +kubebuilder:validation:Optional
	SSLMode *string `json:"sslMode,omitempty"`

	// ConnectionLimit to be applied to the role.
	// +kubebuilder:validation:Min=-1
	// +optional
	// +kubebuilder:default=100
	ConnectionLimit *int32 `json:"connectionLimit,omitempty"`

	// Privileges to be granted.
	// +optional
	Privileges RolePrivilege `json:"privileges,omitempty"`
}

// RoleStatus defines the observed state of Role
type RoleStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Status  string `json:"status,omitempty"`
	Message string `json:"message,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Role is the Schema for the roles API
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.status`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Role struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RoleSpec   `json:"spec,omitempty"`
	Status RoleStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RoleList contains a list of Role
type RoleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Role `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Role{}, &RoleList{})
}
