package main

// TBD, help wanted!
import (
	"context"
	"fmt"
	"github.com/go-kit/kit/log"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/thanos-community/obslytics/pkg/input"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"

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

//func (i remoteReadInput) Open(ctx context.Context, params input.SeriesParams) (input.SeriesIterator, error) {
//
//
//	tlsConfig := config_util.TLSConfig{
//		CAFile:             i.conf.TLSConfig.CAFile,
//		CertFile:           i.conf.TLSConfig.CertFile,
//		KeyFile:            i.conf.TLSConfig.KeyFile,
//		ServerName:         i.conf.TLSConfig.ServerName,
//		InsecureSkipVerify: i.conf.TLSConfig.InsecureSkipVerify,
//	}
//
//	httpConfig := config_util.HTTPClientConfig{
//		//BasicAuth:       nil,
//		//BearerToken:     "",
//		//BearerTokenFile: "",
//		//ProxyURL:        config_util.URL{},
//		TLSConfig:       tlsConfig,
//	}
//
//	clientConfig := remote.ClientConfig{
//		URL:              nil,
//		Timeout:          0,
//		HTTPClientConfig: config_util.HTTPClientConfig{},
//	}
//
//	//client, err := remote.NewReadClient("name", clientConfig)
//	return nil, nil
//}

// readSeriesIterator implements input.SeriesIterator
type readSeriesIterator struct {
	logger        log.Logger
	ctx           context.Context
	conn          *grpc.ClientConn
	client        remote.ReadClient
	currentSeries *storepb.Series
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
