package assist

import (
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Hint struct {
	Message    string
	Interval   time.Duration
	Writers    []io.Writer
	Terminator string
	stop       int32
	suffixes   map[int]string
	index      int
	wg         *sync.WaitGroup
}

type HintV2 struct {
	Hint
	MessageFunc func() string
	lastLength  int
}

func NewHint(message string, interval time.Duration) *Hint {
	return &Hint{Message: message, Interval: interval}
}

func (h *Hint) start(writers ...io.Writer) {
	if h.suffixes == nil {
		h.suffixes = map[int]string{0: ".", 1: "..", 2: "..."}
	}

	if h.Interval <= 0 {
		h.Interval = time.Second
	}

	if h.Writers == nil {
		h.Writers = make([]io.Writer, 0, len(writers))
	}
	for _, writer := range writers {
		if writer != nil {
			h.Writers = append(h.Writers, writer)
		}
	}

	if len(h.Writers) == 0 {
		h.Writers = append(h.Writers, os.Stdout)
	}
}

func (h *Hint) Start(writers ...io.Writer) {
	h.wg = new(sync.WaitGroup)
	h.start(writers...)
	h.action()
}

func (h *Hint) End() {
	if atomic.CompareAndSwapInt32(&h.stop, 0, 1) {
		h.wg.Wait()
		terminator := h.Terminator
		if terminator == "" {
			terminator = "\n\n"
		}
		for _, writer := range h.Writers {
			writer.Write(StringToBytes(terminator))
		}
	}
}

func (h *Hint) doPrint() {
	message := StringToBytes("\r" + h.Message + h.suffixes[h.index%3])
	for _, writer := range h.Writers {
		writer.Write(message)
	}
	h.index++
}

func (h *Hint) action() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			if atomic.LoadInt32(&h.stop) == 1 {
				break
			}
			h.doPrint()
			time.Sleep(h.Interval)
		}
	}()
}

func (h *HintV2) Start(writers ...io.Writer) {
	h.wg = new(sync.WaitGroup)
	h.start(writers...)
	h.action()
}

func (h *HintV2) doPrint() {
	message := h.Message
	if h.MessageFunc != nil {
		message = h.MessageFunc()
	}
	message += h.suffixes[h.index%3]
	for _, writer := range h.Writers {
		if h.lastLength > 0 {
			writer.Write(StringToBytes("\r" + strings.Repeat(" ", h.lastLength)))
		}
		writer.Write(StringToBytes("\r" + message))
	}
	h.lastLength = len(message)
	h.index++
}

func (h *HintV2) action() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			if atomic.LoadInt32(&h.stop) == 1 {
				break
			}
			h.doPrint()
			time.Sleep(h.Interval)
		}
	}()
}
