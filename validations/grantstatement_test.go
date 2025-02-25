package validations

import (
	"testing"

	postgresqlv1alpha1 "github.com/Facets-cloud/postgresql-operator/apis/postgresql/v1alpha1"
)

func TestValidateGrantStatement(t *testing.T) {
	tests := []struct {
		name           string
		grantStatement *postgresqlv1alpha1.GrantStatement
		wantErr        bool
	}{
		{
			name: "valid grant statement",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Spec: postgresqlv1alpha1.GrantStatementSpec{
					Database: "testdb",
					Statements: []string{
						"GRANT SELECT ON ALL TABLES IN SCHEMA public TO myrole;",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty database string",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Spec: postgresqlv1alpha1.GrantStatementSpec{
					Database: "",
					Statements: []string{
						"GRANT SELECT ON ALL TABLES IN SCHEMA public TO myrole;",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty statements",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Spec: postgresqlv1alpha1.GrantStatementSpec{
					Database:   "testdb",
					Statements: []string{},
				},
			},
			wantErr: true,
		},
		{
			name: "empty statement at index 0",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Spec: postgresqlv1alpha1.GrantStatementSpec{
					Database: "testdb",
					Statements: []string{
						"",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "statement without semicolon",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Spec: postgresqlv1alpha1.GrantStatementSpec{
					Database: "testdb",
					Statements: []string{
						"GRANT SELECT ON ALL TABLES IN SCHEMA public TO myrole",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "multiple statements in one string",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Spec: postgresqlv1alpha1.GrantStatementSpec{
					Database: "testdb",
					Statements: []string{
						"GRANT SELECT ON ALL TABLES IN SCHEMA public TO myrole; GRANT INSERT ON ALL TABLES IN SCHEMA public TO myrole;",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid grant statement",
			grantStatement: &postgresqlv1alpha1.GrantStatement{
				Spec: postgresqlv1alpha1.GrantStatementSpec{
					Database: "testdb",
					Statements: []string{
						"SELECT * FROM mytable;",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGrantStatement(tt.grantStatement)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateGrantStatement() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
