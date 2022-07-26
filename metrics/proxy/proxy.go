package proxy

import (
	"context"
	"reflect"

	"go.opencensus.io/tag"

	"titan/api"
	"titan/metrics"
)

func MetricedValidatorPI(a api.Validator) api.Validator {
	var out api.ValidatorStruct
	proxy(a, &out)
	return &out
}

func MetricedCandidateAPI(a api.Candidate) api.Candidate {
	var out api.CandidateStruct
	proxy(a, &out)
	return &out
}

func MetricedSchedulerAPI(a api.Scheduler) api.Scheduler {
	var out api.SchedulerStruct
	proxy(a, &out)
	return &out
}

func MetricedEdgePI(a api.Edge) api.Edge {
	var out api.EdgeStruct
	proxy(a, &out)
	return &out
}

func proxy(in interface{}, outstr interface{}) {
	outs := api.GetInternalStructs(outstr)
	for _, out := range outs {
		rint := reflect.ValueOf(out).Elem()
		ra := reflect.ValueOf(in)

		for f := 0; f < rint.NumField(); f++ {
			field := rint.Type().Field(f)
			fn := ra.MethodByName(field.Name)

			rint.Field(f).Set(reflect.MakeFunc(field.Type, func(args []reflect.Value) (results []reflect.Value) {
				ctx := args[0].Interface().(context.Context)
				// upsert function name into context
				ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, field.Name))
				stop := metrics.Timer(ctx, metrics.APIRequestDuration)
				defer stop()
				// pass tagged ctx back into function call
				args[0] = reflect.ValueOf(ctx)
				return fn.Call(args)
			}))
		}
	}
}
