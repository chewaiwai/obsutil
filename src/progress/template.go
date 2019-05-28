package progress

import (
	"assist"
	"fmt"
	"gopkg.in/cheggaaa/pb.v2"
	"time"
)

const (
	Standard = `{{bar . }} {{percent . }} {{speed .}} {{rtime . }}`

	Simple = `{{bar .}} {{percent . }} {{counter . }} {{crtime . }}`

	TpsOnly = `{{bar . | tpsBarColor}} {{percent . }} {{tps .}} {{counter . }} {{crtime . }}`

	SpeedOnly = `{{bar . | speedBarColor }} {{percent . }} {{cspeed . | speedColor }} {{counter . "stream"}} {{crtime . "stream"}}`

	TpsAndSpeed = `{{bar . | speedBarColor}} {{percent . }} {{tps .}} {{cspeed . | speedColor}} {{counter . "stream"}} {{crtime . "stream" }}`
)

func InitCustomizeElements(colorful bool) {

	tps := &tps{}
	var tpsEl pb.ElementFunc = func(state *pb.State, args ...string) string {
		return tps.eval(state, args...)
	}
	registerElement("tps", tpsEl, false)

	speed := &speed{}
	var speedEl pb.ElementFunc = func(state *pb.State, args ...string) string {
		return speed.eval(state, args...)
	}

	registerElement("cspeed", speedEl, false)

	unsafeFuncs := pb.UnsafeDefaultTemplateFuncs
	unsafeFuncs["tpsBarColor"] = tpsBarColorFunc(colorful)
	unsafeFuncs["speedBarColor"] = speedBarColorFunc(colorful)
	unsafeFuncs["speedColor"] = speedColorFunc(colorful, speed)

	tpsSpeed := &tpsSpeed{}
	var rtimeEl pb.ElementFunc = func(state *pb.State, args ...string) string {
		if !state.IsFinished() {
			if len(args) > 0 && args[0] == "stream" {
				sp := speed.value(state)
				if sp > 0 && state.Total() > 0 {
					remain := float64(state.Total() - state.Value())
					remainDur := time.Duration(remain/sp) * time.Second
					return fmt.Sprintf("%s", remainDur.String())
				}
				return "?"
			}
			sp := tpsSpeed.value(state)
			if sp > 0 && state.Total() > 0 {
				remain := float64(state.Total() - state.Value())
				remainDur := time.Duration(remain/sp) * time.Second
				return fmt.Sprintf("%s", remainDur.String())
			}
			return "?"
		}
		rts := state.Time().Truncate(time.Millisecond).Sub(state.StartTime().Truncate(time.Millisecond)).String()
		return fmt.Sprintf("%s", rts)
	}

	registerElement("crtime", rtimeEl, false)

	var counterEl pb.ElementFunc = func(state *pb.State, args ...string) string {
		if len(args) > 0 && args[0] == "stream" {
			if total := ctx.GetTotalStream(); total >= 0 {
				return fmt.Sprintf("%s/%s", assist.NormalizeBytes(ctx.GetFinishedStream()), assist.NormalizeBytes(total))
			}
			return fmt.Sprintf("%s/%s", assist.NormalizeBytes(ctx.GetFinishedStream()), "?")
		}

		if total := ctx.GetTotalCount(); total >= 0 {
			return fmt.Sprintf("%s/%s", assist.NormalizeCount(ctx.GetFinishedCount(), ""), assist.NormalizeCount(total, ""))
		}
		return fmt.Sprintf("%s/%s", assist.NormalizeCount(ctx.GetFinishedCount(), ""), "?")
	}
	registerElement("counter", counterEl, false)

}

func registerElement(name string, el pb.Element, adaptive bool) {
	pb.RegisterElement(name, el, adaptive)
}
