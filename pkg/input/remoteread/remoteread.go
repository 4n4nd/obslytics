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

func NewStoreAPIInput(logger log.Logger, conf input.InputConfig) (remoteReadInput, error) {
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

	endpointUrl := &config_util.URL{URL: i.conf.Endpoint}

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
	timeseries := readResponse.GetTimeseries()
	samples := timeseries[0].Samples[0]
	ser := storepb.Series{
		Labels: readResponse.Timeseries[0].Labels,
		Chunks: nil,
	}

	prompb.Chunk{
		MinTimeMs:            0,
		MaxTimeMs:            0,
		Type:                 0,
		Data:                 nil,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	storepb.Chunk{
		Type: 0,
		Data: nil,
	}
	storepb.AggrChunk{
		MinTime: 0,
		MaxTime: 0,
		Raw:     nil,
		Count:   nil,
		Sum:     nil,
		Min:     nil,
		Max:     nil,
		Counter: nil,
	}
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
		conn:   nil,
		client: client}, nil
}

func ConvertTimeseriesToStoreSeries() {

}

// readSeriesIterator implements input.SeriesIterator
type readSeriesIterator struct {
	logger        log.Logger
	ctx           context.Context
	conn          *grpc.ClientConn
	client        remote.ReadClient
	currentSeries *storepb.Series
}

func (i *readSeriesIterator) Next() bool {
	return true
}

func (i *readSeriesIterator) At() input.Series {
	return ReadSeries{StoreS: i.currentSeries}
}

func (i *readSeriesIterator) Close() error {
	return nil
}

// ReadSeries implements input.Series
type ReadSeries struct{ StoreS *storepb.Series }

func (r ReadSeries) Labels() labels.Labels {
	panic("implement me")
}

func (r ReadSeries) MinTime() time.Time {
	panic("implement me")
}

func (r ReadSeries) ChunkIterator() (input.ChunkIterator, error) {
	panic("implement me")
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
