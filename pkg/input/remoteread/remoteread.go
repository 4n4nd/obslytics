package main

// TBD, help wanted!
import (
	"context"
	"fmt"
	"github.com/prometheus/common/model"

	//"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/prompb"
	//"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage/remote"

	"net/url"

	"time"
)

var userAgent = fmt.Sprintf("Obslytics/%s", 0.1)

//type Client struct {
//	remoteName string // Used to differentiate clients in metrics.
//	url        *config_util.URL
//	Client     *http.Client
//	timeout    time.Duration
//}
// ReadClient uses the SAMPLES method of remote read to read series samples from remote server.
// TODO(bwplotka): Add streamed chunked remote read method as well (https://github.com/prometheus/prometheus/issues/5926).
//type ReadClient interface {
//	Read(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error)
//}

// NewReadClient creates a new client for remote read.
//func NewReadClient(name string, conf *remote.ClientConfig) (remote.ReadClient, error) {
//
//	if err != nil {
//		return nil, err
//	}
//
//	//return &remote.Client{
//	//	remoteName:          name,
//	//	url:                 conf.URL,
//	//	Client:              httpClient,
//	//	timeout:             time.Duration(conf.Timeout),
//	//	readQueries:         remoteReadQueries.WithLabelValues(name, conf.URL.String()),
//	//	readQueriesTotal:    remoteReadQueriesTotal.MustCurryWith(prometheus.Labels{remoteName: name, endpoint: conf.URL.String()}),
//	//	readQueriesDuration: remoteReadQueryDuration.WithLabelValues(name, conf.URL.String()),
//	//}, nil
//}
//httpClient, err := config_util.NewClientFromConfig(conf.HTTPClientConfig, "remote_storage_read_client", false)

// ClientConfig configures a client.
//type ClientConfig struct {
//	URL              *config_util.URL
//	Timeout          model.Duration
//	HTTPClientConfig config_util.HTTPClientConfig
//}

//// Read reads from a remote endpoint.
//func (c *Client) Read(ctx context.Context, query *prompb.Query) (*prompb.QueryResult, error) {
//	//c.readQueries.Inc()
//	//defer c.readQueries.Dec()
//
//	req := &prompb.ReadRequest{
//		// TODO: Support batching multiple queries into one read request,
//		// as the protobuf interface allows for it.
//		Queries: []*prompb.Query{
//			query,
//		},
//	}
//	print("req")
//	print(req)
//
//	data, err := proto.Marshal(req)
//	if err != nil {
//		return nil, errors.Wrapf(err, "unable to marshal read request")
//	}
//	println("data ", data)
//	compressed := snappy.Encode(nil, data)
//	print("compressed: ", compressed)
//	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
//	if err != nil {
//		return nil, errors.Wrap(err, "unable to create request")
//	}
//	httpReq.Header.Add("Content-Encoding", "snappy")
//	httpReq.Header.Add("Accept-Encoding", "snappy")
//	httpReq.Header.Set("Content-Type", "application/x-protobuf")
//	httpReq.Header.Set("User-Agent", userAgent)
//	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")
//
//	ctx, cancel := context.WithTimeout(ctx, c.timeout)
//	defer cancel()
//
//	httpReq = httpReq.WithContext(ctx)
//
//	if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
//		var ht *nethttp.Tracer
//		httpReq, ht = nethttp.TraceRequest(
//			parentSpan.Tracer(),
//			httpReq,
//			nethttp.OperationName("Remote Read"),
//			nethttp.ClientTrace(false),
//		)
//		defer ht.Finish()
//	}
//
//	//start := time.Now()
//	httpResp, err := c.Client.Do(httpReq)
//	if err != nil {
//		return nil, errors.Wrap(err, "error sending request")
//	}
//	defer func() {
//		io.Copy(ioutil.Discard, httpResp.Body)
//		httpResp.Body.Close()
//	}()
//	//c.readQueriesDuration.Observe(time.Since(start).Seconds())
//	//c.readQueriesTotal.WithLabelValues(strconv.Itoa(httpResp.StatusCode)).Inc()
//
//	compressed, err = ioutil.ReadAll(httpResp.Body)
//	if err != nil {
//		return nil, errors.Wrap(err, fmt.Sprintf("error reading response. HTTP status code: %s", httpResp.Status))
//	}
//
//	if httpResp.StatusCode/100 != 2 {
//		return nil, errors.Errorf("remote server %s returned HTTP status %s: %s", c.url.String(), httpResp.Status, strings.TrimSpace(string(compressed)))
//	}
//
//	uncompressed, err := snappy.Decode(nil, compressed)
//	if err != nil {
//		return nil, errors.Wrap(err, "error reading response")
//	}
//
//	var resp prompb.ReadResponse
//	err = proto.Unmarshal(uncompressed, &resp)
//	if err != nil {
//		return nil, errors.Wrap(err, "unable to unmarshal response body")
//	}
//
//	if len(resp.Results) != len(req.Queries) {
//		return nil, errors.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
//	}
//
//	return resp.Results[0], nil
//}

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
	print(req.String())

	if err != nil {
		print("error")
	}

	readResponse, err := myReadClient.Read(ctx, &myQuery)
	if err != nil {
		print("error: ", err.Error())
	} else {
		println("Query Response: ", readResponse.String())

	}

	//mT := model.TimeFromUnix(time.Now().Unix())
	//mT, _ := model.ParseDuration("10s")
	//
	//var myClientConfig = remote.ClientConfig{
	//	URL:              &hostUrl,
	//	Timeout:          mT,
	//	HTTPClientConfig: config_util.HTTPClientConfig{},
	//}
	//var myC = remote.ReadClient{
	//
	//} //myC := remote.New
	//myC, _ := remote.New(userAgent, &myClientConfig)

	//myClientConfig2 := remote.ClientConfig{
	//	URL:              &hostUrl,
	//	HTTPClientConfig: config_util.HTTPClientConfig{},
	//}

	//myClient2, err := remote.newReadClient(userAgent, &myClientConfig2)

}

//func test()  {
//	//readClient := remote.ReadClient.Read()
//	//prompb.
//	store.NewPrometheusStore()
//}

//func (p *PrometheusStore) handleStreamedPrometheusResponse(s storepb.Store_SeriesServer, httpResp *http.Response, externalLabels labels.Labels) error {
//	//level.Debug(p.logger).Log("msg", "started handling ReadRequest_STREAMED_XOR_CHUNKS streamed read response.")
//
//	framesNum := 0
//	seriesNum := 0
//
//	//defer runutil.CloseWithLogOnErr(p.logger, httpResp.Body, "prom series request body")
//
//	var (
//		lastSeries string
//		currSeries string
//		tmp        []string
//		data       = p.getBuffer()
//	)
//	defer p.putBuffer(data)
//
//	// TODO(bwplotka): Put read limit as a flag.
//	stream := remote.NewChunkedReader(httpResp.Body, remote.DefaultChunkedReadLimit, *data)
//	for {
//		res := &prompb.ChunkedReadResponse{}
//		err := stream.NextProto(res)
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			return errors.Wrap(err, "next proto")
//		}
//
//		if len(res.ChunkedSeries) != 1 {
//			//level.Warn(p.logger).Log("msg", "Prometheus ReadRequest_STREAMED_XOR_CHUNKS returned non 1 series in frame", "series", len(res.ChunkedSeries))
//
//		}
//
//		framesNum++
//		for _, series := range res.ChunkedSeries {
//			{
//				// Calculate hash of series for counting.
//				tmp = tmp[:0]
//				for _, l := range series.Labels {
//					tmp = append(tmp, l.String())
//				}
//				currSeries = strings.Join(tmp, ";")
//				if currSeries != lastSeries {
//					seriesNum++
//					lastSeries = currSeries
//				}
//			}
//
//			thanosChks := make([]storepb.AggrChunk, len(series.Chunks))
//			for i, chk := range series.Chunks {
//				thanosChks[i] = storepb.AggrChunk{
//					MaxTime: chk.MaxTimeMs,
//					MinTime: chk.MinTimeMs,
//					Raw: &storepb.Chunk{
//						Data: chk.Data,
//						// Prometheus ChunkEncoding vs ours https://github.com/thanos-io/thanos/blob/master/pkg/store/storepb/types.proto#L19
//						// has one difference. Prometheus has Chunk_UNKNOWN Chunk_Encoding = 0 vs we start from
//						// XOR as 0. Compensate for that here:
//						Type: storepb.Chunk_Encoding(chk.Type - 1),
//					},
//				}
//				// Drop the reference to data from non protobuf for GC.
//				series.Chunks[i].Data = nil
//			}
//
//			if err := s.Send(storepb.NewSeriesResponse(&storepb.Series{
//				Labels: p.translateAndExtendLabels(series.Labels, externalLabels),
//				Chunks: thanosChks,
//			})); err != nil {
//				return err
//			}
//		}
//	}
//	//level.Debug(p.logger).Log	("msg", "handled ReadRequest_STREAMED_XOR_CHUNKS request.", "frames", framesNum, "series", seriesNum)
//	return nil
//}
