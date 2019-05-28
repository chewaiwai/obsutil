package i18n

import (
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type messageBuilderEn struct {
}

func (m messageBuilderEn) buildMessage(printers map[language.Tag]*PrinterWrapper) {
	printers[language.BritishEnglish] = newPrinterWrapper(message.NewPrinter(language.BritishEnglish))
}
