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
	"github.com/pramodh-ayyappan/database-operator/apis/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// GrantSpec defines the desired state of Grant
type GrantSpec struct {
	// Defines the role reference to grant permissions
	// +kubebuilder:validation:Required
	RoleRef common.ResourceReference `json:"roleRef,omitempty"`

	// Defines the list of permissions to grant for a role
	// +kubebuilder:validation:Required
	Privileges []string `json:"privileges,omitempty"`

	// Defines the Database to grant permission for a role
	// +kubebuilder:validation:Required
	Database *string `json:"database,omitempty"`

	// Defines the Schema to grant permission for a role
	// +optional
	// +kubebuilder:default=""
	Schema *string `json:"schema,omitempty"`

	// Defines the Database to grant permission for a role
	// +optional
	// +kubebuilder:default=""
	Table *string `json:"table,omitempty"`
}

// Store Previous state
type PreviousState struct {
	Type     string `json:"type,omitempty"`
	Database string `json:"database,omitempty"`
	Schema   string `json:"schema,omitempty"`
	Table    string `json:"table,omitempty"`
}

// GrantStatus defines the observed state of Grant
type GrantStatus struct {
	Conditions    []metav1.Condition `json:"conditions"`
	PreviousState PreviousState      `json:"previousState"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// Grant is the Schema for the grants API
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[-1:].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[-1:].reason`
// +kubebuilder:printcolumn:name="Message",type=string,priority=1,JSONPath=`.status.conditions[-1:].message`
// +kubebuilder:printcolumn:name="Last Transition Time",type=string,priority=1,JSONPath=`.status.conditions[-1:].lastTransitionTime`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type Grant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GrantSpec   `json:"spec,omitempty"`
	Status GrantStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// GrantList contains a list of Grant
type GrantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Grant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Grant{}, &GrantList{})
}
