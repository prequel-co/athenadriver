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

package main

import (
	"database/sql"
	"os"

	secret "github.com/uber/athenadriver/examples/constants"
	drv "github.com/prequel-co/athenadriver/go"
)

func main() {
	// 1. Set AWS Credential in Driver Config.
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
	conf, err := drv.NewDefaultConfig(secret.OutputBucket, secret.Region, secret.AccessID, secret.SecretAccessKey)
	conf.SetLogging(true)
	if err != nil {
		panic(err)
		return
	}

	// 2. Open Connection.
	dsn := conf.Stringify()
	db, _ := sql.Open(drv.DriverName, dsn)

	// 3. Query with pseudo command `pc:get_query_id`
	var qid string
	_ = db.QueryRow("pc:get_query_id select url from sampledb.elb_logs limit 2").Scan(&qid)
	println("Query ID: ", qid)
}

/*
Sample Output:
Query ID: c89088ab-595d-4ee6-a9ce-73b55aeb8953
*/
