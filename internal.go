package internalClient

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/go-tron/base-error"
	"github.com/go-tron/config"
	"github.com/go-tron/types/mapUtil"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/tidwall/gjson"
	"reflect"
	"strings"
)

var (
	ErrorParams   = baseError.SystemFactory("1700", "params error:{}")
	ErrorRequest  = baseError.SystemFactory("1702", "connect failed:{}")
	ErrorInternal = baseError.SystemFactory("1703", "internal error:{}")
	ErrorConvert  = baseError.SystemFactory("1704", "convert error:{}")
)

func NewWithConfig(name string, label string, port string, c *config.Config) *Client {
	url := "http://127.0.0.1:" + port
	if dnsSuffix := c.GetString("cluster.dnsSuffix"); dnsSuffix != "" {
		host := name + "." + c.GetString("cluster.namespace") + "." + dnsSuffix
		port = "80"
		url = "http://" + host + ":" + port
	}
	return New(&Config{
		Name:  name,
		Label: label,
		Url:   url,
		BasicAuth: &BasicAuth{
			Username: c.GetString("application.id"),
			Password: c.GetString("application.secret"),
		},
	})
}

func New(config *Config) *Client {
	if config == nil {
		panic("config 必须设置")
	}
	if config.Name == "" {
		panic("Name 必须设置")
	}
	if config.Label == "" {
		panic("Label 必须设置")
	}
	if config.CodeProperty == "" {
		config.CodeProperty = "code"
	}
	if config.MessageProperty == "" {
		config.MessageProperty = "message"
	}
	if config.DataProperty == "" {
		config.DataProperty = "data"
	}
	if config.SystemProperty == "" {
		config.SystemProperty = "system"
	}
	if config.ChainProperty == "" {
		config.ChainProperty = "chain"
	}
	if config.SucceedCode == nil {
		config.SucceedCode = "00"
	}
	if config.InternalErrorCode == nil {
		config.InternalErrorCode = "500"
	}
	return &Client{config}
}

type Signer interface {
	Sign(map[string]interface{}) error
}

type BasicAuth struct{ Username, Password string }

type Config struct {
	Signer
	*BasicAuth
	ClientId          string
	Name              string
	Url               string
	Label             string
	CodeProperty      string
	MessageProperty   string
	DataProperty      string
	SystemProperty    string
	ChainProperty     string
	SucceedCode       interface{}
	InternalErrorCode interface{}
}

type RequestConfig struct {
	Ctx context.Context
}

type RequestOption func(*RequestConfig)

func WithCtx(val context.Context) RequestOption {
	return func(opts *RequestConfig) {
		opts.Ctx = val
	}
}

type Client struct {
	*Config
}

func (s *Client) Post(path string, req interface{}, res interface{}, opts ...RequestOption) (interface{}, error) {
	return s.Request("POST", path, req, res, opts...)
}
func (s *Client) Get(path string, req interface{}, res interface{}, opts ...RequestOption) (interface{}, error) {
	return s.Request("GET", path, req, res, opts...)
}
func (s *Client) Request(method string, path string, req interface{}, res interface{}, opts ...RequestOption) (val interface{}, err error) {
	config := &RequestConfig{}
	for _, apply := range opts {
		apply(config)
	}
	headers, span := s.Trace(config.Ctx, method, path)
	defer func() {
		if span != nil {
			if err != nil {
				if reflect.TypeOf(err).String() == "*baseError.Error" && !err.(*baseError.Error).System {
					span.LogFields(log.String("error", err.Error()))
				} else {
					ext.LogError(span, err)
				}
			}
			span.Finish()
		}
	}()

	request := resty.New().R()
	request = request.SetHeaders(headers)
	if s.BasicAuth != nil {
		request = request.SetBasicAuth(s.Username, s.Password)
	}
	if s.Signer != nil {
		var data map[string]interface{}
		switch v := req.(type) {
		case map[string]interface{}:
			data = v
		default:
			var err error
			data, err = mapUtil.FromStruct(v)
			if err != nil {
				return nil, ErrorParams(s.Label)
			}
		}

		if s.ClientId != "" {
			data["appId"] = s.ClientId
		}
		s.Sign(data)
		request = request.SetBody(data)
	} else {
		request = request.SetBody(req)
	}

	resp, err := request.Execute(strings.ToUpper(method), s.Url+path)
	if err != nil {
		return nil, ErrorRequest(s.Label).WithChain(s.Name)
	}

	body := string(resp.Body())
	code := gjson.Get(body, s.CodeProperty).Value()

	if code == nil {
		return nil, ErrorRequest(s.Label).WithChain(s.Name)
	}

	message := gjson.Get(body, s.MessageProperty).String()
	system := gjson.Get(body, s.SystemProperty).Bool()
	chain := gjson.Get(body, s.ChainProperty).String()
	var chainArr = make([]string, 0)
	if chain != "" {
		chainArr = strings.Split(chain, "<-")
	}
	chainArr = append(chainArr, s.Name)
	if code != s.SucceedCode {
		var e *baseError.Error
		if code == s.InternalErrorCode {
			e = ErrorInternal(s.Label)
		} else {
			if system {
				e = baseError.System(fmt.Sprint(code), message)
			} else {
				e = baseError.New(fmt.Sprint(code), message)
			}
		}
		return nil, e.WithChain(chainArr...)
	}

	bodyData := gjson.Get(body, s.DataProperty).Value()

	if res == nil {
		return bodyData, nil
	}

	if err := mapUtil.ToStruct(bodyData, res); err != nil {
		return nil, ErrorConvert().WithChain(chainArr...)
	}

	return res, nil
}

func (s *Client) Trace(ctx context.Context, method, path string) (map[string]string, opentracing.Span) {
	var headers = make(map[string]string)
	if ctx != nil {
		var requestId = ""
		if id := ctx.Value("x-request-id"); id != nil {
			requestId = id.(string)
		}
		headers["x-request-id"] = requestId

		span, _ := opentracing.StartSpanFromContext(ctx, path+":C")
		if span != nil {
			span.SetTag("x-request-id", requestId)
			ext.SpanKindRPCClient.Set(span)
			ext.HTTPMethod.Set(span, method)
			ext.HTTPUrl.Set(span, path)
			if err := span.Tracer().Inject(
				span.Context(),
				opentracing.TextMap,
				opentracing.TextMapCarrier(headers),
			); err != nil {
				fmt.Println("RPCClientTrace Inject Err", err)
			}
		}
		return headers, span
	}
	return headers, nil
}
