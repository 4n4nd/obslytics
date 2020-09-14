package ingest

// This file contains implementation of the aggregation ingestion logic.

import (
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/thanos-community/obslytics/pkg/dataframe"
	"github.com/thanos-community/obslytics/pkg/input"
)

// AggrOption defines options for a single aggregation.
type AggrOption struct {
	// Should the aggregation be used?
	Enabled bool
	// Column to store the aggregation at.
	Column string
}

// AggrsOptions ia a collections of aggregations-related options. Determines
// what aggregations are enabled etc..
type AggrsOptions struct {
	// Function to suggest the time of the first sample based on the resolution
	// and the initial time of a series. By default, it uses time.Truncate(resolution)
	// to normalize against the beginning of epoch.
	initSampleTimeFunc func(time.Duration, time.Time) time.Time

	Sum   AggrOption
	Count AggrOption
	Min   AggrOption
	Max   AggrOption
}

// By default, all aggregations are disabled and target columns set with `_` prefix.
func defaultAggrsOptions() AggrsOptions {
	return AggrsOptions{
		initSampleTimeFunc: func(res time.Duration, t time.Time) time.Time {
			return t.Truncate(res)
		},

		Sum:   AggrOption{Column: "_sum"},
		Count: AggrOption{Column: "_count"},
		Min:   AggrOption{Column: "_min"},
		Max:   AggrOption{Column: "_max"},
	}
}

type AggrOptionFunc func(*AggrsOptions)

func evalOptions(optFuncs []AggrOptionFunc) *AggrsOptions {
	opt := defaultAggrsOptions()
	for _, o := range optFuncs {
		o(&opt)
	}
	return &opt
}

type aggregatedSeries struct {
	labels      labels.Labels
	hash        uint64
	sampleStart time.Time
	sampleEnd   time.Time
	minTime     time.Time
	maxTime     time.Time
	count       uint64
	min         float64
	max         float64
	sum         float64
}

type aggregator struct {
	resolution   time.Duration
	options      AggrsOptions
	activeSeries map[uint64]*aggregatedSeries
	df           AggrDf
}

// Aggregator ingests the data and produces aggregations at the specified resolution.
type Aggregator struct{ aggregator }

func NewAggregator(resolution time.Duration, optFuncs ...AggrOptionFunc) *aggregator {
	// TODO(bwplotka): What if resolution is 0?
	options := *evalOptions(optFuncs)
	return &aggregator{
		resolution:   resolution,
		options:      options,
		activeSeries: make(map[uint64]*aggregatedSeries),
		df:           newAggrDf(options),
	}
}

func newAggrDf(AggrsOptions) AggrDf {
	return AggrDf{seriesRecordSets: make(map[uint64]*SeriesRecordSet)}
}

// ingestChunk ingests a single chunk provided via an iterator. For a specific searies, We
// assume the iterator returns values ordered by the timestamp.
// The iterator is expected to already be at the point of the first sample after as.sampleStart.
func (a *aggregator) ingestChunk(as *aggregatedSeries, i input.ChunkIterator) error {
	var (
		ts int64
		v  float64
		t  time.Time
	)
	for {
		ts, v = i.At()
		t = timestamp.Time(ts)
		if t.Before(as.sampleStart) {
			return errors.Errorf("Chunk timestamp %s is less than the sampleStart %s", t, as.sampleStart)
		}
		if t.After(as.sampleEnd) {
			as = a.finalizeSample(as, t)
		}

		if as.count == 0 {
			as.minTime = t
			as.maxTime = t
			as.min = v
			as.max = v
		}
		if as.maxTime.After(t) {
			return errors.Errorf("Incoming chunks are not sorted by timestamp: expected %s after %s", t, as.maxTime)
		}
		as.maxTime = t
		as.count += 1
		as.sum += v
		if as.max < v {
			as.max = v
		}
		if as.min > v {
			as.min = v
		}
		if !i.Next() {
			break
		}
	}
	return nil
}

func (a *aggregator) addSeriesToDf(as *aggregatedSeries) {
	rs, ok := a.df.seriesRecordSets[as.hash]
	if !ok {
		rs = a.df.addRecordSet(as.labels)
	}

	vals := map[string]interface{}{
		"_sample_start": as.sampleStart,
		"_sample_end":   as.sampleEnd,
		"_min_time":     as.minTime,
		"_max_time":     as.maxTime,
	}

	for _, l := range as.labels {
		if l.Name == "__name__" {
			continue
		}
		vals[l.Name] = l.Value
	}

	if a.options.Count.Enabled {
		vals[a.options.Count.Column] = as.count
	}
	if a.options.Sum.Enabled {
		vals[a.options.Sum.Column] = as.sum
	}
	if a.options.Min.Enabled {
		vals[a.options.Min.Column] = as.min
	}
	if a.options.Max.Enabled {
		vals[a.options.Max.Column] = as.max
	}
	rs.Records = append(rs.Records, Record{Values: vals})
}

// finalizeSample adds the active aggregated series into the final dataframe when we've reached the
// sample end time. Returns pointer to a new instance of the aggregatedSeries.
func (a *aggregator) finalizeSample(as *aggregatedSeries, nextT time.Time) *aggregatedSeries {
	if as.count > 0 {
		a.addSeriesToDf(as)
	}

	// calculate the next sample cycle to contain the nextT time. First calculate how many
	// whole resolution cycles are between current sampleStart and nextT and then add
	// those cycles to the current sampleStart.
	nextSampleCycle := ((nextT.Unix() - as.sampleStart.Unix()) / (int64)(a.resolution/time.Second))
	nextSampleStart := as.sampleStart.Add((time.Duration(nextSampleCycle)) * a.resolution)

	as = &aggregatedSeries{
		labels:      as.labels,
		hash:        as.hash,
		sampleStart: nextSampleStart,
		sampleEnd:   nextSampleStart.Add(a.resolution),
	}
	a.activeSeries[as.hash] = as
	return as
}

// Ingest the data to an aggregated set.
func (a *aggregator) Ingest(s input.Series) error {
	if s.MinTime().IsZero() {
		// Zero means no chunks in the series.
		return nil
	}
	ls := s.Labels()
	seriesHash := ls.Hash()

	as, ok := a.activeSeries[seriesHash]
	if !ok {
		minTime := s.MinTime()
		sampleStart := a.options.initSampleTimeFunc(a.resolution, minTime)
		sampleEnd := sampleStart.Add(a.resolution)
		as = &aggregatedSeries{labels: ls, hash: seriesHash, sampleStart: sampleStart, sampleEnd: sampleEnd}
		a.activeSeries[seriesHash] = as
	}

	i, err := s.ChunkIterator()
	if err != nil {
		return errors.Wrap(err, "error while decoding a chunk")
	}

	if !i.Seek(timestamp.FromTime(as.sampleStart)) {
		// No chunks after the sampleStart to process.
		return nil
	}

	err = a.ingestChunk(as, i)
	if err != nil {
		return errors.Wrap(err, "error while ingesting a chunk")
	}
	return nil
}

func (a *aggregator) Finalize() error {
	for _, as := range a.activeSeries {
		a.finalizeSample(as, as.sampleEnd)
	}
	return nil
}

// Flush returns already aggregated data from previous samples while cleaning
// the buffer.
func (a *aggregator) Flush() (dataframe.Dataframe, bool) {
	if len(a.df.seriesRecordSets) == 0 {
		return a.df, false
	}

	df := a.df

	// We postpone the schema calculation to the time just before sending the df out
	// so that we can use the ingested data to determine the labels to be exported.
	df.schema = a.getSchema()

	a.df = newAggrDf(a.options)
	return df, true
}

// getLabelNames assumes all series having the same labels and just takes the first
// series to get the label names.
// The returned strings are always sorted alphabetically.
func (a *aggregator) getLabelNames() []string {
	// TODO(inecas): The labels can be changing over time: to workaround
	// this problem, it could have to add an option to explicitly provide the list
	// of labels we want to export and fill in NULLs in case the label would be missing.
	var (
		ls  labels.Labels
		ret []string
	)
	for _, s := range a.df.seriesRecordSets {
		ls = s.Labels
		break
	}

	// We've not found labels in df, look at the active series instead.
	if len(ls) == 0 {
		for _, s := range a.activeSeries {
			ls = s.labels
			break
		}
	}
	for _, l := range ls {
		if l.Name == "__name__" {
			continue
		}
		ret = append(ret, l.Name)
	}
	sort.Strings(ret)
	return ret
}

func (a *aggregator) getSchema() dataframe.Schema {
	ao := a.options
	schema := dataframe.Schema{}

	for _, l := range a.getLabelNames() {
		schema = append(schema, dataframe.Column{Name: l, Type: dataframe.TypeString})
	}

	timeColumns := []dataframe.Column{
		{Name: "_sample_start", Type: dataframe.TypeTime},
		{Name: "_sample_end", Type: dataframe.TypeTime},
		{Name: "_min_time", Type: dataframe.TypeTime},
		{Name: "_max_time", Type: dataframe.TypeTime},
	}
	schema = append(schema, timeColumns...)

	if ao.Count.Enabled {
		schema = append(schema, dataframe.Column{Name: ao.Count.Column, Type: dataframe.TypeUint})
	}
	if ao.Sum.Enabled {
		schema = append(schema, dataframe.Column{Name: ao.Sum.Column, Type: dataframe.TypeFloat})
	}
	if ao.Min.Enabled {
		schema = append(schema, dataframe.Column{Name: ao.Min.Column, Type: dataframe.TypeFloat})
	}
	if ao.Max.Enabled {
		schema = append(schema, dataframe.Column{Name: ao.Max.Column, Type: dataframe.TypeFloat})
	}

	return schema
}