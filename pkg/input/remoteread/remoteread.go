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
}
