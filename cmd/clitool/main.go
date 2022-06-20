package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	svc "appmeta/api/service/v1"
)

var (
	svrUrl string
	file   string
	query  string

	rootCmd = &cobra.Command{
		Use:   "clitool",
		Short: "For application metadata API tests",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := checkGlobalFlags(); err != nil {
				return err
			}
			return nil
		},
	}

	uploadCmd = &cobra.Command{
		Use:   "upload",
		Short: "upload application metadata",
		Run: func(cmd *cobra.Command, args []string) {
			client := &http.Client{Timeout: 30 * time.Second}
			data, err := ioutil.ReadFile(file)
			if err != nil {
				die("failed to load file %q: %v", file, err)
			}
			req := &svc.UploadMetadataRequest{
				Metadata: string(data),
			}
			m, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(req)
			if err != nil {
				die("failed to marshal request object: %v", err)
			}
			buf := bytes.NewBuffer(m)

			uri := fmt.Sprintf("%s/v1/app/metadata", svrUrl)
			hreq := newRequest(http.MethodPost, uri, buf)
			resp, err := retryableHttpDo(5, client, hreq)
			if err != nil {
				die("failed to send http request: %v", err)
			}
			body, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				die("http request not succeed for %q: %s", uri, string(body))
			}
			println("Successfully uploaded application metadata.")
		},
	}

	searchCmd = &cobra.Command{
		Use:   "search",
		Short: "search application metadata",
		Run: func(cmd *cobra.Command, args []string) {
			client := &http.Client{Timeout: 30 * time.Second}
			uri := fmt.Sprintf("%s/v1/app/metadata/search?query=%s", svrUrl, url.QueryEscape(query))
			hreq := newRequest(http.MethodGet, uri, nil)
			resp, err := retryableHttpDo(5, client, hreq)
			if err != nil {
				die("failed to send http request: %v", err)
			}
			body, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				die("http request not succeed %q: %s", uri, string(body))
			}
			result := &svc.SearchMetadataResponse{}
			if err = protojson.Unmarshal(body, result); err != nil {
				die("failed to unmarshal http response: %s, %v", string(body), err)
			}
			println("found %d results:", len(result.Results))
			for _, rd := range result.Results {
				println("%s:\n%s\n", rd.Website, rd.Metadata)
			}
		},
	}
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(
		&svrUrl,
		"url",
		"",
		"url of api server")
	uploadCmd.Flags().StringVarP(
		&file,
		"file",
		"f",
		"",
		"metadata file")
	searchCmd.Flags().StringVarP(
		&query,
		"query",
		"q",
		"",
		"query string")

	rootCmd.AddCommand(uploadCmd)
	rootCmd.AddCommand(searchCmd)

}

func checkGlobalFlags() error {
	if svrUrl == "" {
		return fmt.Errorf("invalid url (--url): %q", svrUrl)
	}

	return nil
}

func die(msg string, args ...interface{}) {
	_, _ = os.Stderr.Write([]byte(fmt.Sprintf("%s\t%s", time.Now().Format(time.RFC3339), fmt.Sprintf(msg, args...))))
	os.Exit(1)
}

func println(msg string, args ...interface{}) {
	fmt.Printf("%s\t%s\n", time.Now().Format(time.RFC3339), fmt.Sprintf(msg, args...))
}

func newRequest(method, url string, body io.Reader) *http.Request {
	hreq, err := http.NewRequest(method, url, body)
	if err != nil {
		die("failed to create http request: %v", err)
	}
	hreq.Header.Set("User-Agent", "mc_test")
	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("X-Forwarded-User", "mc_test")
	hreq.Header.Set("X-Forwarded-Groups", "e2e-test")
	return hreq
}

func retryableHttpDo(
	maxAttempts int,
	client *http.Client,
	req *http.Request,
) (*http.Response, error) {
	var resp *http.Response
	var err error
	for i := 0; i < maxAttempts; i++ {
		if resp, err = client.Do(req); err == nil {
			return resp, nil
		}
		<-time.After(time.Second)
	}

	return resp, err
}
