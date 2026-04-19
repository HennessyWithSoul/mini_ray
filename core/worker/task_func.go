package worker

import (
	"errors"
	"mini-ray/common"

	"go.uber.org/zap"
)

func (w *worker) AddTowIntFunc(task common.Task) (int, error) {
	argNum := 0
	var intArgs []int
	for _, arg := range task.Args {
		val, err := DecodeInt(arg)
		if err != nil {
			return 0, err
		}
		intArgs = append(intArgs, val.(int))
		argNum++
	}
	if argNum != 2 {
		return 0, errors.New("add tow int func args num not equal to 2")
	}
	w.lg.Info("add tow int func", zap.Int("result", intArgs[0]+intArgs[1]))
	return intArgs[0] + intArgs[1], nil
}
