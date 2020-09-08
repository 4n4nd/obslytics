package main

// TBD, help wanted!
import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-io/thanos/pkg/extgrpc"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	tracing "github.com/thanos-io/thanos/pkg/tracing/client"
	"google.golang.org/grpc"
	"io"

	"net/url"

	"time"
)

type remoteReadInput struct {
	logger log.Logger
	conf   input.InputConfig
}

func NewRemoteReadInput(logger log.Logger, conf input.InputConfig) (remoteReadInput, error) {
	return remoteReadInput{logger: logger, conf: conf}, nil
}

func (i remoteReadInput) Open(ctx context.Context, params input.SeriesParams) (input.SeriesIterator, error) {

	//dialOpts, err := extgrpc.StoreClientGRPCOpts(i.logger, nil, tracing.NoopTracer(),
	//	!i.conf.TLSConfig.InsecureSkipVerify,
	//	i.conf.TLSConfig.CertFile,
	//	i.conf.TLSConfig.KeyFile,
	//	i.conf.TLSConfig.CAFile,
	//	i.conf.Endpoint)
	//
	//if err != nil {
	//	return nil, errors.Wrap(err, "Error initializing GRPC options")
	//}
	//
	//conn, err := grpc.DialContext(ctx, i.conf.Endpoint, dialOpts...)
	//if err != nil {
	//	return nil, errors.Wrap(err, "Error initializing GRPC dial context")
	//}

	//client := storepb.NewStoreClient(conn)

	myUrl := i.conf.Endpoint
	tlsConfig := config_util.TLSConfig{
		CAFile:             i.conf.TLSConfig.CAFile,
		CertFile:           i.conf.TLSConfig.CertFile,
		KeyFile:            i.conf.TLSConfig.KeyFile,
		ServerName:         i.conf.TLSConfig.ServerName,
		InsecureSkipVerify: i.conf.TLSConfig.InsecureSkipVerify,
	}

	httpConfig := config_util.HTTPClientConfig{
		//BasicAuth:       nil,
		//BearerToken:     "",
		//BearerTokenFile: "",
		//ProxyURL:        config_util.URL{},
		TLSConfig: tlsConfig,
	}

	parsedUrl, err := url.Parse(i.conf.Endpoint)
	if err != nil {
		return nil, err
	}

	endpointUrl := &config_util.URL{URL: parsedUrl}

	clientConfig := &remote.ClientConfig{
		URL:              endpointUrl,
		Timeout:          0,
		HTTPClientConfig: httpConfig,
	}

	client, err := remote.NewReadClient("name", clientConfig)
	if err != nil {
		return nil, err
	}

	readResponse, err := client.Read(ctx, nil)

	//readResponse.Get
	//timeseries := readResponse.GetTimeseries()
	//timeseries[0].GetSamples()[0].XXX_Marshal()
	//samples := timeseries[0].Samples[0].
	//ser := storepb.Series{
	//	Labels: readResponse.Timeseries[0].Labels,
	//	Chunks: nil,
	//}

	//prompb.Chunk{
	//	MinTimeMs:            0,
	//	MaxTimeMs:            0,
	//	Type:                 0,
	//	Data:                 nil,
	//	XXX_NoUnkeyedLiteral: struct{}{},
	//	XXX_unrecognized:     nil,
	//	XXX_sizecache:        0,
	//}
	//storepb.Chunk{
	//	Type: 0,
	//	Data: nil,
	//}
	//storepb.AggrChunk{
	//	MinTime: 0,
	//	MaxTime: 0,
	//	Raw:     nil,
	//	Count:   nil,
	//	Sum:     nil,
	//	Min:     nil,
	//	Max:     nil,
	//	Counter: nil,
	//}
	//seriesClient, err := client.Series(ctx, &storepb.SeriesRequest{
	//	MinTime: timestamp.FromTime(params.MinTime),
	//	MaxTime: timestamp.FromTime(params.MaxTime),
	//	Matchers: []storepb.LabelMatcher{
	//		{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: params.Metric},
	//	},
	//	PartialResponseStrategy: storepb.PartialResponseStrategy_ABORT,
	//})

	return &readSeriesIterator{
		logger: i.logger,
		ctx:    ctx,
		client: client}, nil
}

// readSeriesIterator implements input.SeriesIterator
type readSeriesIterator struct {
	logger             log.Logger
	ctx                context.Context
	client             remote.ReadClient
	seriesList         []ReadSeries
	currentSeriesIndex int
	maxSeriesIndex     int
}

func (i *readSeriesIterator) Next() bool {

	// return false if the last index is already reached
	if i.currentSeriesIndex+1 > i.maxSeriesIndex {
		return false
	} else {
		i.currentSeriesIndex++
		return true
	}
}

func (i *readSeriesIterator) At() input.Series {
	return i.seriesList[i.currentSeriesIndex]
}

func (i *readSeriesIterator) Close() error {
	return nil
}

// ReadSeries implements input.Series
type ReadSeries struct {
	input.Series
	timeseries prompb.TimeSeries
}

func (r ReadSeries) Labels() labels.Labels {

	var labelList []labels.Label
	for i := range r.timeseries.Labels {
		labelList[i] = labels.Label{
			Name:  r.timeseries.Labels[i].Name,
			Value: r.timeseries.Labels[i].Value,
		}

	}
	return labels.New(labelList...)
}

func (r ReadSeries) MinTime() time.Time {

	seconds := r.timeseries.Samples[0].Timestamp / 1000
	nanoseconds := r.timeseries.Samples[0].Timestamp % 1000 * 1000000

	return time.Unix(seconds, nanoseconds)
}

func (r ReadSeries) ChunkIterator() (input.ChunkIterator, error) {
	//panic("implement me")
	chunk := ReadChunk{
		series: r.timeseries,
	}
	return chunk.Iterator(), nil
}

// ReadChunkIterator implements input.ChunkIterator
type ReadChunkIterator struct {
	input.ChunkIterator
	Chunk              ReadChunk
	currentSampleIndex int
	maxSampleIndex     int
}

func (c ReadChunkIterator) Next() bool {
	// return false if the last index is reached
	if c.currentSampleIndex+1 > c.maxSampleIndex {
		return false
	} else {
		c.currentSampleIndex++
		return true
	}
}

// Seek advances the iterator forward to the first sample with the timestamp equal or greater than t.
// If current sample found by previous `Next` or `Seek` operation already has this property, Seek has no effect.
// Seek returns true, if such sample exists, false otherwise.
// Iterator is exhausted when the Seek returns false.
func (c ReadChunkIterator) Seek(t int64) bool {
	for c.Chunk.series.Samples[c.currentSampleIndex].Timestamp < t {
		if !c.Next() {
			return false
		}
	}
	return true
}

// At returns the current timestamp/value pair.
// Before the iterator has advanced At behaviour is unspecified.
func (c ReadChunkIterator) At() (int64, float64) {
	return c.Chunk.series.Samples[c.currentSampleIndex].Timestamp, c.Chunk.series.Samples[c.currentSampleIndex].Value
}

// Err returns the current error. It should be used only after iterator is
// exhausted, that is `Next` or `Seek` returns false.
func (c ReadChunkIterator) Err() error {
	return nil
}

type ReadChunk struct {
	chunkenc.Chunk
	series prompb.TimeSeries
}

func (c ReadChunk) Iterator() ReadChunkIterator {
	return ReadChunkIterator{Chunk: c, currentSampleIndex: 0, maxSampleIndex: len(c.series.Samples) - 1}
}

var userAgent = fmt.Sprintf("Obslytics/%s", 0.1)

func main() {
	//myClient := http.Client{}
	myUrl := &url.URL{
		Scheme: "http",
		Host:   "localhost:9090",
		Path:   "api/v1/read",
	}

	myClientConfig := &remote.ClientConfig{
		URL:              &config_util.URL{URL: myUrl},
		Timeout:          model.Duration(10 * time.Second),
		HTTPClientConfig: config_util.HTTPClientConfig{},
	}
	myReadClient, err := remote.NewReadClient(userAgent, myClientConfig)

	var myLabelsList = make([]*prompb.LabelMatcher, 1)

	myLabels := prompb.LabelMatcher{
		Type:  0,
		Name:  "__name__",
		Value: "up",
	}
	myLabelsList[0] = &myLabels

	myQuery := prompb.Query{
		StartTimestampMs: 1598626000000,
		EndTimestampMs:   1598626163000,
		Matchers:         myLabelsList,
	}
	print(myQuery.String())
	ctx := context.Background()
	//out, err := c.Read(ctx, &myQuery)
	//

	req := &prompb.ReadRequest{
		// TODO: Support batching multiple queries into one read request,
		// as the protobuf interface allows for it.
		Queries: []*prompb.Query{
			&myQuery,
		},
	}
	println(req.String())

	if err != nil {
		print("error")
	}

	readResponse, err := myReadClient.Read(ctx, &myQuery)
	println(readResponse.Timeseries)
	if err != nil {
		print("error: ", err.Error())
	} else {
		//println("Query Response: ", readResponse.String())
	}
}
