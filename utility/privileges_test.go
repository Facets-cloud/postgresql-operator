package utility

import "testing"

func TestGenerateSequencePrivileges(t *testing.T) {
	type args struct {
		privileges string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test has sequence privileges",
			args: args{
				privileges: " INSERT   , SELECT,UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, USAGE  ",
			},
			want: "SELECT, UPDATE, USAGE",
		},
		{
			name: "test has no sequence privileges",
			args: args{
				privileges: " INSERT, DELETE, TRUNCATE, REFERENCES, TRIGGER",
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateSequencePrivileges(tt.args.privileges); got != tt.want {
				t.Errorf("GenerateSequencePrivileges() = %v, want %v", got, tt.want)
			}
		})
	}
}
