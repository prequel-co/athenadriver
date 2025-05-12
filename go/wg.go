// Copyright (c) 2022 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package athenadriver

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/shogo82148/memoize"
)

var (
    // memoizer for GetWorkGroup results: key is workgroup name, value is *athenatypes.WorkGroup
    getWGGroup memoize.Group[string, *athenatypes.WorkGroup]
)

// Workgroup is a wrapper of Athena Workgroup.
type Workgroup struct {
	Name   string
	Config *athenatypes.WorkGroupConfiguration
	Tags   *WGTags
}

// NewDefaultWG is to create new default Workgroup.
func NewDefaultWG(name string, config *athenatypes.WorkGroupConfiguration, tags *WGTags) *Workgroup {
	wg := Workgroup{
		Name:   name,
		Config: config,
	}
	if config == nil {
		wg.Config = GetDefaultWGConfig()
	}
	if tags != nil {
		wg.Tags = tags
	} else {
		wg.Tags = NewWGTags()
	}
	return &wg
}

// NewWG is to create a new Workgroup.
func NewWG(name string, config *athenatypes.WorkGroupConfiguration, tags *WGTags) *Workgroup {
	return &Workgroup{
		Name:   name,
		Config: config,
		Tags:   tags,
	}
}

// getWG retrieves an Athena WorkGroup from AWS remotely, caching the result for 10 minutes.
// Subsequent calls with the same name within the TTL return the cached *WorkGroup.
func getWG(ctx context.Context, client AthenaClient, name string) (*athenatypes.WorkGroup, error) {
    if client == nil {
        return nil, ErrAthenaNilClient
    }

    // Define the actual fetch logic: invoked on cache-miss or expired entry.
    fetch := func(ctx context.Context, key string) (wg *athenatypes.WorkGroup, expiresAt time.Time, err error) {
        out, err := client.GetWorkGroup(ctx, &athena.GetWorkGroupInput{
            WorkGroup: aws.String(key),
        })
        if err != nil {
            return nil, time.Time{}, err
        }

        wg = out.WorkGroup
        // Cache this result for 10 minutes.
        expiresAt = time.Now().Add(10 * time.Minute)
        return wg, expiresAt, nil
    }

    wg, _, err := getWGGroup.Do(ctx, name, fetch)
    if err != nil {
        return nil, err
    }
    return wg, nil
}

// CreateWGRemotely is to create a Workgroup remotely.
func (w *Workgroup) CreateWGRemotely(ctx context.Context, athenaClient AthenaClient) error {
	tags := w.Tags.Get()
	var err error
	if len(tags) == 0 {
		_, err = athenaClient.CreateWorkGroup(ctx, &athena.CreateWorkGroupInput{
			Configuration: w.Config,
			Name:          aws.String(w.Name),
		})
	} else {
		_, err = athenaClient.CreateWorkGroup(ctx, &athena.CreateWorkGroupInput{
			Configuration: w.Config,
			Name:          aws.String(w.Name),
			Tags:          w.Tags.Get(),
		})
	}
	return err
}
