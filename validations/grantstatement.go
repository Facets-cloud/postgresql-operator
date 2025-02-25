package validations

import (
	"errors"
	"fmt"
	"strings"

	postgresqlv1alpha1 "github.com/Facets-cloud/postgresql-operator/apis/postgresql/v1alpha1"
	"github.com/Facets-cloud/postgresql-operator/utility"
)

func ValidateGrantStatement(grantStatement *postgresqlv1alpha1.GrantStatement) error {

	var ValidationError error

	if grantStatement.Spec.Database == "" {
		ValidationError = utility.AppendError(ValidationError, fmt.Errorf("GrantStatement must contain non empty database"))
	}

	if len(grantStatement.Spec.Statements) == 0 {
		ValidationError = utility.AppendError(ValidationError, fmt.Errorf("GrantStatement must contain at least one statement"))
	}

	for index, statement := range grantStatement.Spec.Statements {
		if statement == "" {
			ValidationError = utility.AppendError(ValidationError, errors.New(fmt.Sprintf("Statement at index %d is empty", index)))
		}

		trimmedStatement := strings.TrimSpace(statement)

		if !strings.HasSuffix(trimmedStatement, ";") {
			ValidationError = utility.AppendError(ValidationError, errors.New(fmt.Sprintf("Statement at index %d does not end with a semicolon", index)))
		}

		splitStatement := strings.Split(trimmedStatement, ";")

		if len(splitStatement) > 2 {
			ValidationError = utility.AppendError(ValidationError, errors.New(fmt.Sprintf("Statement at index %d contains multiple statements", index)))
		}

		lowerCaseStatement := strings.ToLower(trimmedStatement)

		if !strings.Contains(lowerCaseStatement, "grant") {
			ValidationError = utility.AppendError(ValidationError, errors.New(fmt.Sprintf("Statement at index %d is not a valid grant statement", index+1)))
		}

	}

	return ValidationError

}
