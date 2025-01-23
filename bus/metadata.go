package bus

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"net/http"
	"net/url"
)

type queueResponse struct {
	VHost, Name string
}

type QueueDetails struct {
	MessageReadyCount int `json:"messages_ready"`
	MessageCount      int `json:"messages"`
}

func (b bus) ListQueues() ([]string, error) {
	url := fmt.Sprintf("%s://%s:%d/api/queues", b.config.ServerConfig.APIConfig.Protocol, b.config.ServerConfig.Host, b.config.ServerConfig.APIConfig.Port)
	body, err := b.httpRequest(http.MethodGet, url)

	if err != nil {
		return nil, err
	}

	queues := []queueResponse{}
	err = json.Unmarshal(body, &queues)
	if err != nil {
		return nil, Wrap(err, "error deserializing response body")
	}

	queueNames := []string{}
	for _, q := range queues {
		if q.VHost == b.config.ServerConfig.VirtualHost {
			queueNames = append(queueNames, q.Name)
		}
	}

	return queueNames, nil
}

func (b bus) QueueDetails(queue string) (QueueDetails, error) {
	url := fmt.Sprintf("%s://%s:%d/api/queues/%v/%v", b.config.ServerConfig.APIConfig.Protocol, b.config.ServerConfig.Host, b.config.ServerConfig.APIConfig.Port, url.PathEscape(b.config.ServerConfig.VirtualHost), url.PathEscape(queue))

	body, err := b.httpRequest(http.MethodGet, url)

	if err != nil {
		return QueueDetails{}, err
	}

	queueDetails := QueueDetails{}
	err = json.Unmarshal(body, &queueDetails)
	if err != nil {
		return QueueDetails{}, Wrap(err, "error deserializing response body")
	}

	return queueDetails, nil
}

func (b bus) PurgeQueue(queue string) error {
	url := fmt.Sprintf("%s://%s:%d/api/queues/%v/%v/contents", b.config.ServerConfig.APIConfig.Protocol, b.config.ServerConfig.Host, b.config.ServerConfig.APIConfig.Port, url.PathEscape(b.config.ServerConfig.VirtualHost), url.PathEscape(queue))

	body, err := b.httpRequest(http.MethodDelete, url)

	if err != nil {
		return err
	}

	log.Printf("body: %v", string(body))

	return nil
}

func (b bus) httpRequest(method string, url string) ([]byte, error) {
	req, err := http.NewRequest(method, url, nil)

	if err != nil {
		return nil, Wrap(err, "error building new request")
	}

	req.SetBasicAuth(b.config.ServerConfig.APIConfig.Username, b.config.ServerConfig.APIConfig.Password)
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}
	res, err := client.Do(req)
	if err != nil {
		return nil, Wrap(err, "error calling rabbitmq api")
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, Wrap(err, "error reading response body")
	}
	return body, nil
}
