package main

import (
	"fmt"
	"github.com/bigkevmcd/go-configparser"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const CONFIG_PATH = "/Users/jklinger/Nesta/ojd_daps/ojd_daps/config/mysqldb.config" // Need to resolve this dynamically
const DB_NAME = "production"                                                        // Need to set defaults somehow

// RawJobAdvert ORM - this specifies which fields are extracted from the table "raw_job_adverts".
type RawJobAdvert struct {
	ID          string `gorm:"primaryKey"`
	DataSource  string `gorm:"primaryKey"`
	JobTitleRaw string // e.g. unpacked into job_title_raw
}

func load_db_config() *configparser.ConfigParser {
	// load_db_config will load the database configuration file
	config, err := configparser.NewConfigParserFromFile(CONFIG_PATH)
	if err != nil {
		panic(err)
	}
	return config
}

func get_config_value(option string) string {
	// get_config_value will get the value for the key "option" from the section "mysqldb" from the globally specified mysqldb.config
	config := load_db_config() // TODO: not cached, should be
	value, err := config.Get("mysqldb", option)
	if err != nil {
		panic(err)
	}
	return value
}

func make_connection_string() string {
	// make_connection_string will generate the connection string for accessing the database
	conn := get_config_value("user") + ":" + get_config_value("password") + "@tcp(" + get_config_value("host") + ")/" + DB_NAME + "?charset=utf8mb4&parseTime=True&loc=Local"
	return conn
}

func init() {
	// Validate config exists
	load_db_config()
}

func main() {
	// Connect to the database
	conn := make_connection_string()
	db, err := gorm.Open(mysql.Open(conn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	// Retrieve all data
	var raw_job_ads []RawJobAdvert
	result := db.Find(&raw_job_ads)

	// Print some stats
	fmt.Printf("%v, %v\n", result.RowsAffected, result.Error)
	fmt.Printf("%v\n", len(raw_job_ads))
	fmt.Printf("%v\n", raw_job_ads[0])

}
