package assist

import (
	"fmt"
	"github.com/satori/go.uuid"
	"regexp"
	"strings"
)

var sanitizeItems = []string{
	"\\",
	"^",
	"$",
	".",
	"+",
	"{",
	"}",
	"(",
	")",
	"[",
	"]",
	"|",
}

type wildcardMasker struct {
	wildcard    string
	target      string
	placeHolder string
}

var sanitizePrefix = "\\"

func newWildcardMasker(wildcard, target string) *wildcardMasker {
	w := &wildcardMasker{}
	w.wildcard = wildcard
	w.target = target
	if placeHolder, err := uuid.NewV4(); err == nil {
		w.placeHolder = placeHolder.String()
	}
	return w
}

func (w wildcardMasker) mask(input string) string {
	if w.placeHolder == "" {
		return input
	}
	return strings.Replace(input, sanitizePrefix+w.wildcard, w.placeHolder, -1)
}

func (w wildcardMasker) transfer(input string) string {
	return strings.Replace(input, w.wildcard, w.target, -1)
}

func (w wildcardMasker) unmask(input string) string {
	if w.placeHolder == "" {
		return input
	}
	return strings.Replace(input, w.placeHolder, sanitizePrefix+w.wildcard, -1)
}

func SanitizeInput(input string) string {
	for _, item := range sanitizeItems {
		input = strings.Replace(input, item, sanitizePrefix+item, -1)
	}
	return input
}

func AdaptWildcards(input string) string {
	wildcardMaskers := []*wildcardMasker{
		newWildcardMasker("?", "."),
		newWildcardMasker("*", ".*?"),
	}
	for _, w := range wildcardMaskers {
		input = w.mask(input)
	}

	input = SanitizeInput(input)

	for _, w := range wildcardMaskers {
		input = w.transfer(input)
	}

	for _, w := range wildcardMaskers {
		input = w.unmask(input)
	}

	return fmt.Sprintf("^%s$", input)
}

func CompileWildcardInput(input string) (*regexp.Regexp, error) {
	return regexp.Compile(AdaptWildcards(input))
}
