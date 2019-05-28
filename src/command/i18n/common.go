package i18n

import (
	"fmt"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"sync"
)

var printers map[language.Tag]*PrinterWrapper
var currentTag = language.BritishEnglish
var tagLock sync.Mutex

type messageBuilder interface {
	buildMessage(printers map[language.Tag]*PrinterWrapper)
}

func SetI18nStrings() {
	printers = make(map[language.Tag]*PrinterWrapper)
	(&messageBuilderEn{}).buildMessage(printers)
	(&messageBuilderCn{}).buildMessage(printers)
}

type PrinterWrapper struct {
	p *message.Printer
}

func newPrinterWrapper(p *message.Printer) *PrinterWrapper {
	pw := &PrinterWrapper{
		p: p,
	}
	return pw
}

func (pw PrinterWrapper) Printf(format string, a ...interface{}) (n int, err error) {
	n, err = pw.p.Printf(format, a...)
	fmt.Println()
	return
}

func (pw PrinterWrapper) Sprintf(format string, a ...interface{}) string {
	return pw.p.Sprintf(format, a...)
}

func getPrinter(tag language.Tag) *PrinterWrapper {
	if printer, ok := printers[tag]; ok {
		return printer
	}
	return printers[language.BritishEnglish]
}

func SetCurrentTag(tag language.Tag) {
	tagLock.Lock()
	currentTag = tag
	tagLock.Unlock()
}

func SetCurrentLanguage(lan string) {
	if lan == "chinese" {
		SetCurrentTag(language.Chinese)
	} else {
		SetCurrentTag(language.BritishEnglish)
	}
}

func GetCurrentPrinter() *PrinterWrapper {
	tagLock.Lock()
	defer tagLock.Unlock()
	return getPrinter(currentTag)
}
