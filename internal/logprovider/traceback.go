package logprovider

import (
	"fmt"
	"os"
	"runtime/debug"
)
func DebugTraceback(errFuncName string) {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("panic in goroutine: %v\n%s", r, debug.Stack())
		f, _ := os.Create(errFuncName + ".log")
		fmt.Fprint(f, msg)
		f.Close()
	}
}