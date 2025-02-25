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
	"github.com/Facets-cloud/postgresql-operator/apis/common"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// GrantStatementSpec defines the desired state of GrantStatement
type GrantStatementSpec struct {
	// +kubebuilder:validation:Required
	Database string `json:"database"`

	// +kubebuilder:validation:Required
	RoleRef common.ResourceReference `json:"roleRef"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Statements []string `json:"statements"`
}

// GrantStatementStatus defines the observed state of GrantStatement
type GrantStatementStatus struct {
	Conditions                  []metav1.Condition          `json:"conditions,omitempty"`
	PreviousGrantStatementState PreviousGrantStatementState `json:"previousGrantStatementState,omitempty"`
}

type PreviousGrantStatementState struct {
	Database   string                   `json:"database"`
	RoleRef    common.ResourceReference `json:"roleRef"`
	Statements []string                 `json:"statements"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.database`
//+kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.roleRef.name`
//+kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[-1:].status`
//+kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[-1:].reason`
//+kubebuilder:printcolumn:name="Last Transition Time",type=string,priority=1,JSONPath=`.status.conditions[-1:].lastTransitionTime`
//+kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GrantStatement is the Schema for the grantstatements API
type GrantStatement struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GrantStatementSpec   `json:"spec,omitempty"`
	Status GrantStatementStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// GrantStatementList contains a list of GrantStatement
type GrantStatementList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GrantStatement `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GrantStatement{}, &GrantStatementList{})
}
