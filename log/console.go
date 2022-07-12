package log

import "fmt"

const (
	textBlack = iota + 30
	textRed
	textGreen
	textYellow
	textBlue
	textPurple
	textCyan
	textWhite
)

// Black Black
func Black(str string) string {
	return textColor(textBlack, str)
}

// Red Red
func Red(str string) string {
	return textColor(textRed, str)
}

// Yellow Yellow
func Yellow(str string) string {
	return textColor(textYellow, str)
}

// Green Green
func Green(str string) string {
	return textColor(textGreen, str)
}

// Cyan Cyan
func Cyan(str string) string {
	return textColor(textCyan, str)
}

// Blue Blue
func Blue(str string) string {
	return textColor(textBlue, str)
}

// Purple Purple
func Purple(str string) string {
	return textColor(textPurple, str)
}

// White White
func White(str string) string {
	return textColor(textWhite, str)
}

func textColor(color int, str string) string {
	return fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", color, str)
}
