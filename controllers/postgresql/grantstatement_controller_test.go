package postgresql

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	postgresqlv1alpha1 "github.com/Facets-cloud/postgresql-operator/apis/postgresql/v1alpha1"
)

func TestGrantStatementReconcilerAppendGrantStatementStatusCondition(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = postgresqlv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name           string
		grantStatement *postgresqlv1alpha1.GrantStatement
		typeName       string
		status         metav1.ConditionStatus
		reason         string
		message        string
		wantLength     int
	}{
		{
			name: "append new condition",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Status: postgresqlv1alpha1.GrantStatementStatus{
					Conditions: []metav1.Condition{},
				},
			},
			typeName:   "TestType",
			status:     metav1.ConditionTrue,
			reason:     "TestReason",
			message:    "TestMessage",
			wantLength: 1,
		},
		{
			name: "append condition to existing conditions",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Status: postgresqlv1alpha1.GrantStatementStatus{
					Conditions: []metav1.Condition{
						{
							Type:   "OldType",
							Status: metav1.ConditionFalse,
						},
					},
				},
			},
			typeName:   "TestType",
			status:     metav1.ConditionTrue,
			reason:     "TestReason",
			message:    "TestMessage",
			wantLength: 2,
		},
		{
			name: "keep only last 5 conditions",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Status: postgresqlv1alpha1.GrantStatementStatus{
					Conditions: []metav1.Condition{
						{Type: "Type1"}, {Type: "Type2"}, {Type: "Type3"}, {Type: "Type4"}, {Type: "Type5"},
					},
				},
			},
			typeName:   "TestType",
			status:     metav1.ConditionTrue,
			reason:     "TestReason",
			message:    "TestMessage",
			wantLength: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &GrantStatementReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
			}
			ctx := context.TODO()
			r.appendGrantStatementStatusCondition(ctx, tt.grantStatement, tt.typeName, tt.status, tt.reason, tt.message)
			if len(tt.grantStatement.Status.Conditions) != tt.wantLength {
				t.Errorf("appendGrantStatementStatusCondition() = %v, want %v", len(tt.grantStatement.Status.Conditions), tt.wantLength)
			}
		})
	}
}
