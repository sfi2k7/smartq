package smartq

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Url      string `json:"url"`
	Password string `json:"password"`
	Token    string `json:"token"`
}

var defaulturl = "localhost:6379"
var defaultpassword = "passme"

var redisurl string
var redispassword string

func LoadConfig(up ...string) error {

	fmt.Println("loading configuration...")
	defer func() {
		fmt.Println("done loading configuration")
	}()

	if len(up) > 0 {
		if len(up) > 0 {
			redispassword = up[0]
		}

		if len(up) > 1 {
			redisurl = up[1]
		}
	}

	var configfilename string
	if len(os.Args) > 1 {
		configfilename = os.Args[1]
		if strings.HasPrefix(configfilename, "--") {
			configfilename = ""
		}
	}

	if len(configfilename) == 0 {
		fmt.Println("config filename", configfilename)
		redisurl = defaulturl
		redispassword = defaultpassword
		fmt.Println("using default user/password")
		return nil
	}

	ext := filepath.Ext(configfilename)
	if ext == ".json" {

		var config = make(map[string]string)
		bytes, err := os.ReadFile(configfilename)
		if err != nil {
			panic(err)
		}

		err = json.Unmarshal(bytes, &config)
		if err != nil {
			panic(err)
		}

		url, ok := config["url"]
		if !ok {
			url = defaulturl
		}

		password, ok := config["password"]
		if !ok {
			password = defaultpassword
		}

		redisurl = url
		redispassword = password
		return nil
	}

	// if ext == ".yaml" {
	// 	loadYamlConfig(filename)
	// } else if ext == ".json" {
	// 	loadJsonConfig(filename)
	// } else {
	// 	loadDefaultConfig()
	// }

	return nil
}
