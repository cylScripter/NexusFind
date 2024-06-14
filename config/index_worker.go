package config

type ServiceConfig struct {
	ServiceName string   `mapstructure:"service_name" json:"service_name" yaml:"service_name"`
	WorkerHost  string   `json:"worker_host" yaml:"worker_host" mapstructure:"worker_host"`
	WorkerPort  int      `yaml:"worker_port" json:"worker_port" mapstructure:"worker_port"`
	MasterHost  string   `json:"master_host" yaml:"master_host" mapstructure:"master_host"`
	MasterPort  int      `json:"master_port" yaml:"master_port" mapstructure:"master_port"`
	NodeName    string   `json:"node_name" yaml:"node_name" mapstructure:"node_name"`
	Etcd        []string `yaml:"etcd" json:"etcd" mapstructure:"etcd"`
}
