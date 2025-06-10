package config

type InitSpec struct {
	ServerPort int `json:"port"`
}

type Config struct {
	ServerPort int `json:"port"`
}

func NewConfig(initSpec InitSpec) (*Config, error) {
	return &Config{
		ServerPort: initSpec.ServerPort,
	}, nil
}
