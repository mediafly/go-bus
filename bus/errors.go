package bus

import (
	"errors"
	"fmt"
	"runtime"
)

// ErrorWithTrace - struct that adheres to the "error" interface while also containing a stack trace for debugging purposes
type ErrorWithTrace struct {
	Message    string
	StackTrace string
}

// Error - prints error message and stack trace
func (err ErrorWithTrace) Error() string {
	return fmt.Sprintf("Error: %s \nStackTrace:\n%s\n", err.Message, err.StackTrace)
}

// String - return string representation of the error
func (err ErrorWithTrace) String() string {
	return err.Error()
}

// New - wrapper around errors.New from standard library
func New(text string) error {
	return errors.New(text)
}

// Newf - returns a new error using fmt.Printf conventions
func Newf(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

// NewTrace - returns a new error with trace using text as the error message
func NewTrace(text string) error {
	buf := make([]byte, 1<<16)
	len := runtime.Stack(buf, false)

	return ErrorWithTrace{Message: text, StackTrace: string(buf[:len])}
}

// NewTracef - returns a new error with trace using the conventions of fmt.Printf
func NewTracef(format string, a ...interface{}) error {
	buf := make([]byte, 1<<16)
	len := runtime.Stack(buf, false)

	return ErrorWithTrace{Message: fmt.Sprintf(format, a...), StackTrace: string(buf[:len])}
}

// Wrap - wrap an existing error with new text
func Wrap(err error, text string) error {
	if x, ok := err.(ErrorWithTrace); ok {
		return ErrorWithTrace{Message: fmt.Sprintf("%s: %s", text, x.Message), StackTrace: x.StackTrace}
	}

	return fmt.Errorf("%s: %v", text, err)
}

// Wrapf - wrap an existing error with formatted text following the conventions of fmt.Printf
func Wrapf(err error, format string, a ...interface{}) error {
	text := fmt.Sprintf(format, a...)
	if x, ok := err.(ErrorWithTrace); ok {
		return ErrorWithTrace{Message: fmt.Sprintf("%s: %s", text, x.Message), StackTrace: x.StackTrace}
	}

	return fmt.Errorf("%s: %v", text, err)
}

// Trace - add current stack trace to exiting error
func Trace(err error) error {
	buf := make([]byte, 1<<16)
	len := runtime.Stack(buf, false)

	return ErrorWithTrace{Message: err.Error(), StackTrace: string(buf[:len])}
}

// WrapTrace - wrap the error with new text and add the current stack trace
func WrapTrace(err error, text string) error {
	buf := make([]byte, 1<<16)
	len := runtime.Stack(buf, false)

	return ErrorWithTrace{Message: fmt.Sprintf("%s: %v", text, err), StackTrace: string(buf[:len])}
}

// WrapTracef - wrap the error with nex text following the conventaions of fmt.Printf and add the current stack trace
func WrapTracef(err error, format string, a ...interface{}) error {
	text := fmt.Sprintf(format, a...)
	buf := make([]byte, 1<<16)
	len := runtime.Stack(buf, false)

	return ErrorWithTrace{Message: fmt.Sprintf("%s: %v", text, err), StackTrace: string(buf[:len])}
}
