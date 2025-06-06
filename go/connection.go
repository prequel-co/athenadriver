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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// timestampFormatDriverMicro is the string format we transform Go time.Time objects into. This is not meant for
// TIMESTAMP columns, as Athena timestamp columns only have a millisecond granularity.
const timestampFormatDriverMicro = "2006-01-02 15:04:05.000000"

// Connection is a connection to AWS Athena. It is not used concurrently by multiple goroutines.
// Connection is assumed to be stateful.
type Connection struct {
	athenaClient AthenaClient

	connector *SQLConnector
	numInput  int
}

// buildExecutionParams converts Go data types into strings for query arguments in parameterized queries.
func (c *Connection) buildExecutionParams(args []driver.Value) ([]string, error) {
	if len(args) == 0 {
		return nil, nil
	}

	executionParams := []string{}
	for _, arg := range args {
		if arg == nil {
			executionParams = append(executionParams, "NULL")
			continue
		}
		// type switches of arg to handle different query parameter types
		val := ""
		switch v := arg.(type) {
		case int64:
			val = strconv.FormatInt(v, 10)
		case uint64:
			val = strconv.FormatUint(v, 10)
		case float64:
			val = strconv.FormatFloat(v, 'g', -1, 64)
		case bool:
			if v {
				val = "1"
			} else {
				val = "0"
			}
		case time.Time:
			// Note: time.Time objects are transformed into strings for a STRING/CHAR/VARCHAR type column.
			// To maintain compatibility with the current interpolateParams() behavior, this function produces a string
			// up to microsecond granularity, which Athena does not support in TIMESTAMP columns (up to milliseconds).
			// For DATE/TIME/TIMESTAMP, it is better to pass in string arguments with a typecast. Refer to the string
			// case below.
			// Matches interpolateParams() behavior.
			val = "'0000-00-00'" // Special-cased.
			if !v.IsZero() {
				v := v.In(time.UTC)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				dateFormat := timestampFormatDriverMicro
				if v.Nanosecond()/1000 == 0 {
					// Omit microseconds if that part is zero
					dateFormat = time.DateTime
				}
				val = fmt.Sprintf("'%s'", v.Format(dateFormat))
			}
		case []byte:
			// Note: Different from interpolateParams() behavior.
			// Like the string case below, enclosing in single quotes would prevent typecasting or function calls in
			// execution parameters. Prior to passing in query arguments, Format* functions in utils.go can be used.
			val = string(v)
		case string:
			// Note: Different from interpolateParams() behavior.
			// For parameterized queries, typecasting or function calls go in the execution parameters. For example,
			// `WHERE created = TIMESTAMP '2024-07-01 00:00:00'` should be formatted as: `WHERE created = ?` (query) and
			// `TIMESTAMP '2024-07-01 00:00:00.000'` (arg). Therefore, we cannot simply enclose the full string with
			// single quotes here. Users should use the Format* functions in utils.go to format input string arguments.
			val = v
		default:
			return []string{}, ErrQueryUnknownType
		}
		executionParams = append(executionParams, val)
	}
	return executionParams, nil
}

func (c *Connection) interpolateParams(query string, args []driver.Value) (string, error) {
	c.numInput = len(args)
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != c.numInput {
		return "", ErrInvalidQuery
	}

	queryBuffer := make([]byte, MAXQueryStringLength)
	queryBuffer = queryBuffer[:0]
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			queryBuffer = append(queryBuffer, query[i:]...)
			break
		}
		queryBuffer = append(queryBuffer, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			queryBuffer = append(queryBuffer, "NULL"...)
			continue
		}
		// type switches of arg to handle different query parameter types
		switch v := arg.(type) {
		case int64:
			queryBuffer = strconv.AppendInt(queryBuffer, v, 10)
		case uint64:
			queryBuffer = strconv.AppendUint(queryBuffer, v, 10)
		case float64:
			queryBuffer = strconv.AppendFloat(queryBuffer, v, 'g', -1, 64)
		case bool:
			if v {
				queryBuffer = append(queryBuffer, '1')
			} else {
				queryBuffer = append(queryBuffer, '0')
			}
		case time.Time:
			if v.IsZero() {
				queryBuffer = append(queryBuffer, "'0000-00-00'"...)
			} else {
				v := v.In(time.UTC)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				queryBuffer = append(queryBuffer, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					queryBuffer = append(queryBuffer, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				queryBuffer = append(queryBuffer, '\'')
			}
		case []byte:
			queryBuffer = append(queryBuffer, "_binary'"...)
			queryBuffer = escapeBytesBackslash(queryBuffer, v)
			queryBuffer = append(queryBuffer, '\'')
		case string:
			queryBuffer = append(queryBuffer, '\'')
			queryBuffer = escapeStringBackslash(queryBuffer, v)
			queryBuffer = append(queryBuffer, '\'')
		default:
			return "", ErrQueryUnknownType
		}

		if len(queryBuffer)+4 > 10*MAXQueryStringLength {
			return "", ErrQueryBufferOF
		}
	}
	return string(queryBuffer), nil
}

// CheckNamedValue is to implement interface driver.NamedValueChecker.
func (c *Connection) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = driver.DefaultParameterConverter.ConvertValue(nv.Value)
	return
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (c *Connection) ExecContext(ctx context.Context, query string, namedArgs []driver.NamedValue) (driver.Result, error) {
	var obs = c.connector.tracer
	var err error
	args := namedValueToValue(namedArgs)
	if len(namedArgs) > 0 {
		query, err = c.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		obs.Scope().Counter(DriverName + ".execcontext").Inc(1)
	}
	if !isQueryValid(query) {
		return nil, ErrInvalidQuery
	}
	rows, err := c.QueryContext(ctx, query, []driver.NamedValue{})
	if err != nil {
		return nil, err
	}
	var rowAffected int64 = 0
	r := rows.(*Rows)
	if r != nil && r.ResultOutput != nil && r.ResultOutput.UpdateCount != nil {
		rowAffected = *r.ResultOutput.UpdateCount
	}
	var lastInsertedID int64 = -1
	result := AthenaResult{
		lastInsertedID: lastInsertedID,
		rowAffected:    rowAffected,
	}
	return result, nil
}

func (c *Connection) cachedQuery(ctx context.Context, QID string) (driver.Rows, error) {
	if c.connector.config.IsMoneyWise() {
		dataScanned := int64(0)
		printCost(&athena.GetQueryExecutionOutput{
			QueryExecution: &athenatypes.QueryExecution{
				QueryExecutionId: &QID,
				Statistics: &athenatypes.QueryExecutionStatistics{
					DataScannedInBytes: &dataScanned,
				},
			},
		})
	}
	wg := c.connector.config.GetWorkgroup()
	if wg.Name == "" {
		wg.Name = DefaultWGName
	}
	return NewRows(ctx, c.athenaClient, QID, c.connector.config, c.connector.tracer)
}

func (c *Connection) getHeaderlessSingleRowResultPage(ctx context.Context, qid string) (driver.Rows, error) {
	r, err := NewNonOpsRows(ctx, c.athenaClient, qid, c.connector.config, c.connector.tracer)
	colName := "_col0"
	columnNames := []string{colName}
	columnTypes := []string{"string"}
	data := make([][]*string, 1)
	data[0] = []*string{&qid}
	r.ResultOutput = newHeaderlessResultPage(columnNames, columnTypes, data)
	return r, err
}

// QueryContext is implemented to be called by `DB.Query` (QueryerContext interface).
//
// "QueryerContext is an optional interface that may be implemented by a Conn.
// If a Conn does not implement QueryerContext, the sql package's DB.Query
// will fall back to Queryer; if the Conn does not implement Queryer either,
// DB.Query will first prepare a query, execute the statement, and then
// close the statement."
//
// With QueryContext implemented, we don't need Queryer.
// QueryerContext must honor the context timeout and return when the context is canceled.
func (c *Connection) QueryContext(ctx context.Context, query string, namedArgs []driver.NamedValue) (driver.Rows, error) {
	var obs = c.connector.tracer
	var pseudoCommand = ""
	if strings.HasPrefix(query, "pc:") {
		query = strings.Trim(query[3:], " ")
		if pseudoCommand = PCGetQID; strings.HasPrefix(query, pseudoCommand+" ") {
			query = strings.Trim(query[len(pseudoCommand):], " ")
		} else if pseudoCommand = PCGetQIDStatus; strings.HasPrefix(query, pseudoCommand+" ") {
			query = strings.Trim(query[len(pseudoCommand):], " ")
		} else if pseudoCommand = PCStopQID; strings.HasPrefix(query, pseudoCommand+" ") {
			query = strings.Trim(query[len(pseudoCommand):], " ")
		} else if pseudoCommand = PCGetDriverVersion; strings.HasPrefix(query, pseudoCommand) {
			return c.getHeaderlessSingleRowResultPage(ctx, DriverVersion)
		} else {
			return nil, fmt.Errorf("pseudo command " + query + "doesn't exist")
		}
	}
	if c.connector.config.IsReadOnly() {
		if !isReadOnlyStatement(query) {
			obs.Scope().Counter(DriverName + ".failure.querycontext.writeviolation").Inc(1)
			obs.Log(WarnLevel, "write db violation", zap.String("query", query))
			return nil, fmt.Errorf("writing to Athena database is disallowed in read-only mode")
		}
	}
	now := time.Now()
	args := namedValueToValue(namedArgs)
	queryWithPlaceholders := query // For parameterized queries
	var err error
	if len(namedArgs) > 0 {
		query, err = c.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		obs.Scope().Counter(DriverName + ".prepared.querycontext").Inc(1)
	}
	if !isQueryValid(query) {
		return nil, ErrInvalidQuery
	}
	wg := c.connector.config.GetWorkgroup()
	if wg.Name == "" {
		wg.Name = DefaultWGName
	} else if wg.Name != DefaultWGName {
		athenaWG, err := getWG(ctx, c.athenaClient, wg.Name)
		if err != nil {
			obs.Scope().Counter(DriverName + ".failure.querycontext.getwg").Inc(1)
			obs.Log(WarnLevel, "Didn't find workgroup "+wg.Name+" due to: "+err.Error())
			if c.connector.config.IsWGRemoteCreationAllowed() {
				err = wg.CreateWGRemotely(ctx, c.athenaClient)
				if err != nil {
					obs.Scope().Counter(DriverName + ".failure.querycontext.createwgremotely").Inc(1)
					return nil, err
				}
				obs.Log(DebugLevel, "workgroup "+wg.Name+" is created successfully.")
			} else {
				obs.Log(WarnLevel, "workgroup "+DefaultWGName+" is used for "+wg.Name+".")
				return nil,
					fmt.Errorf("workgroup %q doesn't exist and workgroup remote creation is disabled", wg.Name)
			}
		} else {
			if athenaWG.State != athenatypes.WorkGroupStateEnabled {
				obs.Log(WarnLevel, "workgroup "+DefaultWGName+" is disabled.")
				obs.Scope().Counter(DriverName + ".failure.querycontext.wgdisabled").Inc(1)
				return nil, fmt.Errorf("workgroup %q is disabled", wg.Name)
			}
			obs.Log(DebugLevel, "workgroup "+DefaultWGName+" is enabled.")
		}
	}

	timeWorkgroup := time.Since(now)
	startOfStartQueryExecution := time.Now()
	obs.Scope().Timer(DriverName + ".query.workgroup").Record(timeWorkgroup)

	// case 1 - query directly using QID
	if IsQID(query) {
		if pseudoCommand == PCGetQIDStatus {
			statusResp, err := c.athenaClient.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
				QueryExecutionId: aws.String(query),
			})
			if err != nil {
				obs.Log(ErrorLevel, "GetQueryExecutionWithContext failed",
					zap.String("workgroup", wg.Name),
					zap.String("queryID", query),
					zap.String("error", err.Error()))
				obs.Scope().Counter(DriverName + ".failure.querycontext.getqueryexecutionwithcontext").Inc(1)
				return nil, err
			}
			return c.getHeaderlessSingleRowResultPage(ctx, string(statusResp.QueryExecution.Status.State))
		}
		if pseudoCommand == PCStopQID {
			_, err := c.athenaClient.StopQueryExecution(ctx, &athena.StopQueryExecutionInput{
				QueryExecutionId: aws.String(query),
			})
			if err != nil {
				obs.Log(ErrorLevel, "StopQueryExecution failed",
					zap.String("workgroup", wg.Name),
					zap.String("queryID", query),
					zap.String("query", query))
				obs.Scope().Counter(DriverName + ".failure.querycontext.stopqueryexecution.failed").Inc(1)
				return nil, err
			}
			return c.getHeaderlessSingleRowResultPage(ctx, "OK")
		}
		return c.cachedQuery(ctx, query)
	}

	//  case 2 - TODO
	executionParams, err := c.buildExecutionParams(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.athenaClient.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString:         aws.String(queryWithPlaceholders),
		ExecutionParameters: executionParams,
		QueryExecutionContext: &athenatypes.QueryExecutionContext{
			Database: aws.String(c.connector.config.GetDB()),
		},
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String(c.connector.config.GetOutputBucket()),
		},
		WorkGroup: aws.String(wg.Name),
	})
	if err != nil {
		if pseudoCommand == PCGetQID {
			var re *awshttp.ResponseError
			if errors.As(err, &re) {
				return c.getHeaderlessSingleRowResultPage(ctx, re.ServiceRequestID())
			}
		}
		return nil, err
	}

	timeStartQueryExecution := time.Since(startOfStartQueryExecution)
	now = time.Now()
	obs.Scope().Timer(DriverName + ".query.startqueryexecution").Record(timeStartQueryExecution)

	queryID := *resp.QueryExecutionId
	if pseudoCommand == PCGetQID {
		return c.getHeaderlessSingleRowResultPage(ctx, queryID)
	}
WAITING_FOR_RESULT:
	for {
		pollInterval := c.connector.config.GetResultPollIntervalSeconds()
		statusResp, err := c.athenaClient.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			obs.Log(ErrorLevel, "GetQueryExecutionWithContext failed",
				zap.String("workgroup", wg.Name),
				zap.String("queryID", queryID),
				zap.String("error", err.Error()))
			obs.Scope().Counter(DriverName + ".failure.querycontext.getqueryexecutionwithcontext").Inc(1)
			return nil, err
		}
		//statementType = statusResp.QueryExecution.StatementType
		switch statusResp.QueryExecution.Status.State {
		case athenatypes.QueryExecutionStateCancelled:
			timeCanceled := time.Since(now)
			obs.Log(ErrorLevel, "QueryExecutionStateCancelled",
				zap.String("workgroup", wg.Name),
				zap.String("queryID", queryID))
			obs.Scope().Timer(DriverName + ".query.canceled").Record(timeCanceled)
			if c.connector.config.IsMoneyWise() {
				printCost(statusResp)
			}
			return nil, context.Canceled
		case athenatypes.QueryExecutionStateFailed:
			reason := *statusResp.QueryExecution.Status.StateChangeReason
			timeQueryExecutionStateFailed := time.Since(now)
			obs.Log(ErrorLevel, "QueryExecutionStateFailed",
				zap.String("workgroup", wg.Name),
				zap.String("queryID", queryID),
				zap.String("reason", reason))
			obs.Scope().Timer(DriverName + ".query.queryexecutionstatefailed").Record(timeQueryExecutionStateFailed)
			return nil, errors.New(reason)
		case athenatypes.QueryExecutionStateSucceeded:
			if c.connector.config.IsMoneyWise() {
				printCost(statusResp)
			}
			timeQueryExecutionStateSucceeded := time.Since(now)
			obs.Scope().Timer(DriverName + ".query.queryexecutionstatesucceeded").Record(timeQueryExecutionStateSucceeded)
			break WAITING_FOR_RESULT
		// for athena.QueryExecutionStateQueued and athena.QueryExecutionStateRunning
		default:
		}

		select {
		case <-ctx.Done():
			_, err := c.athenaClient.
				StopQueryExecution(context.Background(), &athena.StopQueryExecutionInput{
					QueryExecutionId: aws.String(queryID),
				})
			if err != nil {
				obs.Log(ErrorLevel, "StopQueryExecution failed",
					zap.String("workgroup", wg.Name),
					zap.String("queryID", queryID),
					zap.String("query", query))
				obs.Scope().Counter(DriverName + ".failure.querycontext.stopqueryexecution.failed").Inc(1)
				return nil, err
			}
			if c.connector.config.IsMoneyWise() {
				statusRespFinal, _ := c.athenaClient.GetQueryExecution(context.Background(), &athena.GetQueryExecutionInput{
					QueryExecutionId: aws.String(queryID),
				})
				printCost(statusRespFinal)
			}
			obs.Scope().Counter(DriverName + ".failure.querycontext.stopqueryexecution.succeeded").Inc(1)
			timeStopQueryExecution := time.Since(now)
			obs.Scope().Timer(DriverName + ".query.StopQueryExecution").Record(timeStopQueryExecution)
			obs.Log(ErrorLevel, "query canceled", zap.String("queryID", queryID))
			return nil, ctx.Err()
		case <-time.After(pollInterval):
			if isQueryTimeOut(startOfStartQueryExecution, statusResp.QueryExecution.StatementType, c.connector.config.GetServiceLimitOverride()) {
				obs.Log(ErrorLevel, "Query timeout failure",
					zap.String("workgroup", wg.Name),
					zap.String("queryID", queryID),
					zap.String("query", query))
				obs.Scope().Counter(DriverName + ".failure.querycontext.timeout").Inc(1)
				return nil, ErrQueryTimeout
			}
			continue
		}
	}

	return NewRows(ctx, c.athenaClient, queryID, c.connector.config, obs)
}

// Ping implements driver.Pinger interface.
// Ping is a good first step in a health check: If the Ping succeeds,
// make a simple query, then make a complex query which depends on proper
// DB scheme. This will make troubleshooting simpler as the error now is:
// "We've got network connectivity, we can Ping the DB, so we have valid
// credentials for a SELECT xxx; but ...".
func (c *Connection) Ping(ctx context.Context) error {
	rows, err := c.QueryContext(ctx, "SELECT 1", nil)
	if err != nil {
		return driver.ErrBadConn // https://golang.org/pkg/database/sql/driver/#Pinger
	}
	defer rows.Close()
	return nil
}

// Prepare is inherited from Conn interface.
func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	if !isQueryValid(query) {
		return nil, ErrInvalidQuery
	}
	stmt := &Statement{
		connection: c,
		query:      query,
		closed:     false,
		numInput:   strings.Count(query, "?"),
	}
	return stmt, nil
}

// Begin is from Conn interface, but no implementation for AWS Athena.
func (c *Connection) Begin() (driver.Tx, error) {
	return nil, ErrAthenaTransactionUnsupported
}

// BeginTx is to replace Begin as it is deprecated.
func (c *Connection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, ErrAthenaTransactionUnsupported
}

// Close is from Conn interface, but no implementation for AWS Athena.
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *Connection) Close() error {
	c.connector = nil
	c.athenaClient = nil
	c.numInput = -1
	return nil
}

var _ driver.QueryerContext = (*Connection)(nil)
var _ driver.ExecerContext = (*Connection)(nil)
