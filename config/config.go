package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/dev-mockingbird/logf"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	DBMysql  = "mysql"
	DBPgsql  = "pgsql"
	DBSqlite = "sqlite"
)

type DBConfig struct {
	DBMS     string `json:"dbms" yaml:"dbms"`
	Database string `json:"database" yaml:"database"`
	Host     string `json:"host" yaml:"host"`
	Port     int64  `json:"port" yaml:"port"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
}

type Config struct {
	Http     string    `json:"http" yaml:"http"`
	Tcp      string    `json:"tcp" yaml:"tcp"`
	Logpath  string    `json:"logpath" yaml:"logpath"`
	Loglevel int       `json:"loglevel" yaml:"loglevel"`
	DB       *DBConfig `json:"db" yaml:"db"`
}

func (cfg DBConfig) MysqlDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
}

func (cfg DBConfig) SqliteDSN() string {
	return cfg.Database
}

func (cfg DBConfig) PgsqlDSN() string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=Asia/Shanghai",
		cfg.Host, cfg.User, cfg.Password, cfg.Database, cfg.Port)
}

func (cfg DBConfig) DSN() string {
	if cfg.DBMS == "" {
		cfg.DBMS = DBMysql
	}
	switch cfg.DBMS {
	case DBMysql:
		return cfg.MysqlDSN()
	case DBSqlite:
		return cfg.SqliteDSN()
	case DBPgsql:
		return cfg.PgsqlDSN()
	default:
		panic("not support dbms: " + cfg.DBMS)
	}
}

func ReadInput(cfg *Config, logger logf.Logger) error {
	viper.SetEnvPrefix("mb")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	var (
		jsonFile string
		yamlFile string
	)
	pflag.StringVar(&jsonFile, "json", "", "json config path file")
	pflag.StringVar(&yamlFile, "yaml", "/etc/mockingbird/config.yaml", "yaml config path file")
	pflag.Bool("db.disable", false, "enable db or not")
	pflag.String("db.dbms", "mysql", "dbms")
	pflag.Int("loglevel", 0, "log level, from 0 to 5")
	pflag.String("logpath", "", "log file path, default: ./mockingbird.log")
	pflag.String("db.host", "127.0.0.1", "db host")
	pflag.String("db.database", "mockingbird", "database to use")
	pflag.Int("db.port", 3306, "db port")
	pflag.String("db.user", "root", "db user")
	pflag.String("db.password", "", "db password")
	pflag.String("jaeger.url", "", "jaeger url")
	pflag.String("http.disable", "", "enable http or not")
	pflag.String("http.docroot", "./static/", "docment root of static files")
	pflag.Int("http.port", 7000, "http port")
	pflag.String("mail.from", "", "mail from")
	pflag.String("mail.user", "", "mail user")
	pflag.String("mail.password", "", "mail password")
	pflag.String("mail.smtphost", "", "mail smtp host")
	pflag.Int("mail.smtpport", 0, "mail smtp host")
	pflag.Parse()
	if jsonFile != "" {
		viper.SetConfigType("json")
		f, err := os.Open(jsonFile)
		if err != nil {
			logger.Logf(logf.Error, "open file [%s]: %s", jsonFile, err.Error())
			return err
		}
		if err := viper.ReadConfig(f); err != nil {
			logger.Logf(logf.Error, "read json config: %s", err.Error())
			return err
		}
	}
	if yamlFile != "" {
		viper.SetConfigType("yaml")
		f, err := os.Open(yamlFile)
		if err != nil {
			logger.Logf(logf.Error, "open file [%s]: %s", yamlFile, err.Error())
			return err
		}
		if err := viper.ReadConfig(f); err != nil {
			logger.Logf(logf.Error, "read json config: %s", err.Error())
			return err
		}
	}
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		logger.Logf(logf.Error, "read command line: %s", err.Error())
		return err
	}
	if err := viper.Unmarshal(cfg); err != nil {
		return err
	}
	return nil
}
