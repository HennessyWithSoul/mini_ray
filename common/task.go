package common

type Task struct {
	ID           string
	FuncName     string
	Args         [][]byte
	Dependencies []string
}

const (
	TaskFuncAddTowInt = "addTowInt"
)
