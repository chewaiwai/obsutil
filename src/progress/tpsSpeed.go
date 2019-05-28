package progress

import (
	"gopkg.in/VividCortex/ewma.v1"
	"gopkg.in/cheggaaa/pb.v2"
	"math"
	"time"
)

type tpsSpeed struct {
	ewma        ewma.MovingAverage
	lastStateId uint64
	prevValue   int64
	startValue  int64
	prevTime    time.Time
	startTime   time.Time
}

func (s *tpsSpeed) value(state *pb.State) float64 {
	if s.ewma == nil {
		s.ewma = ewma.NewMovingAverage()
	}
	if state.IsFirst() || state.Id() < s.lastStateId {
		s.reset(state)
		return 0
	}
	if state.Id() == s.lastStateId {
		return s.ewma.Value()
	}
	if state.IsFinished() {
		return s.absValue(state)
	}
	dur := state.Time().Sub(s.prevTime)
	if dur < speedAddLimit {
		return s.ewma.Value()
	}
	diff := math.Abs(float64(state.Value() - s.prevValue))
	lastSpeed := diff / dur.Seconds()
	s.prevTime = state.Time()
	s.prevValue = state.Value()
	s.lastStateId = state.Id()
	s.ewma.Add(lastSpeed)
	return s.ewma.Value()
}

func (s *tpsSpeed) reset(state *pb.State) {
	s.lastStateId = state.Id()
	s.startTime = state.Time()
	s.prevTime = state.Time()
	s.startValue = state.Value()
	s.prevValue = s.startValue
	s.ewma = ewma.NewMovingAverage()
}

func (s *tpsSpeed) absValue(state *pb.State) float64 {
	if dur := state.Time().Truncate(time.Millisecond).Sub(s.startTime).Truncate(time.Millisecond); dur > 0 {
		return float64(state.Value()) / dur.Seconds()
	}
	return 0
}
