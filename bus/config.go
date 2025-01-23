package bus

import (
	"fmt"
)

// ServerConfig - config for connecting to the rabbit mq server
type ServerConfig struct {
	Protocol    string    `json:"protocol,omitempty"`
	Host        string    `json:"host,omitempty"`
	VirtualHost string    `json:"virtualhost,omitempty"`
	Port        int       `json:"port,omitempty"`
	Username    string    `json:"username,omitempty"`
	Password    string    `json:"password,omitempty"`
	APIConfig   APIConfig `json:"apiconfig,omitempty"`
}

// GetAMQPUrl - gets the AQMP URL based on server config parameters
func (s ServerConfig) GetAMQPUrl() string {
	return fmt.Sprintf("%s://%s:%s@%s:%d/%s", s.Protocol, s.Username, s.Password, s.Host, s.Port, s.VirtualHost)
}

// APIConfig - config for interacting with rabbit mq HTTP api
type APIConfig struct {
	Protocol string `json:"protocol,omitempty"`
	Port     int    `json:"port,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// Config - config for the rabbitmq messge bus
type Config struct {
	ExchangeType         string       `json:"exchangetype,omitempty"`
	MaxConnectionRetries int          `json:"maxconnectionretries"`
	ServerConfig         ServerConfig `json:"serverconfig,omitempty"`
	Durable              bool         `json:"durable"`
}
