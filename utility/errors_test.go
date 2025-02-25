package utility

import (
	"errors"
	"testing"
)

func TestAppendError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		newErr  error
		wantErr string
	}{
		{
			name:    "append to nil error",
			err:     nil,
			newErr:  errors.New("new error"),
			wantErr: "new error",
		},
		{
			name:    "append to existing error",
			err:     errors.New("existing error"),
			newErr:  errors.New("new error"),
			wantErr: "existing error; new error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AppendError(tt.err, tt.newErr); got.Error() != tt.wantErr {
				t.Errorf("AppendError() = %v, want %v", got, tt.wantErr)
			}
		})
	}
}
