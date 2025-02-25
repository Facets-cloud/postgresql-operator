package utility

import "strings"

func GenerateSequencePrivileges(privileges string) string {

	// map of valid privilges that can be granted on sequence
	allowedPrivileges := map[string]struct{}{"SELECT": {}, "UPDATE": {}, "USAGE": {}}

	// filtering the valid privileges
	splitStrings := strings.Split(privileges, ",")

	sequencePrivileges := make([]string, 0)
	for _, str := range splitStrings {
		trimmedStr := strings.TrimSpace(str)
		_, ok := allowedPrivileges[trimmedStr]
		if ok {
			sequencePrivileges = append(sequencePrivileges, trimmedStr)
		}
	}

	sequencePrivilegesStr := strings.Join(sequencePrivileges, ", ")

	return sequencePrivilegesStr
}
