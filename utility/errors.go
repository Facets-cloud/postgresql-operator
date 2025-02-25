package utility

import "fmt"

func AppendError(err error, newErr error) error {
	if err != nil {
		err = fmt.Errorf("%s; %s", err.Error(), newErr.Error())
	} else {
		err = newErr
	}
	return err
}
