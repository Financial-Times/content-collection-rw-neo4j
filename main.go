package main

import (
	_ "net/http/pprof"
	"os"

	"time"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/content-collection-rw-neo4j/collection"
	"github.com/Financial-Times/go-fthealth/v1_1"
	cli "github.com/jawher/mow.cli"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	log "github.com/sirupsen/logrus"
)

var appDescription = "A RESTful API for managing Content Collections in neo4j"

func main() {
	app := cli.App("content-collection-rw-neo4j", "A RESTful API for managing Content Collections in neo4j")

	appName := app.String(cli.StringOpt{
		Name:   "app-name",
		Value:  "content-collection-rw-neo4j",
		Desc:   "Name of the application",
		EnvVar: "APP_NAME",
	})

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "upp-content-collection-rw-neo4j",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})

	neoURL := app.String(cli.StringOpt{
		Name:   "neo-url",
		Value:  "http://localhost:7474/db/data",
		Desc:   "neo4j endpoint URL",
		EnvVar: "NEO_URL",
	})

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	app.Action = func() {
		driver, err := neo4j.NewDriver(*neoURL, neo4j.NoAuth())
		if err != nil {
			log.Fatalf("Could not create a new instance of neo4j driver: %s", err)
		}
		defer func() {
			err := driver.Close()
			if err != nil {
				log.Errorf("failed to close neo4j driver: %s", err)
			}
		}()

		spServiceUrl := "content-collection/story-package"
		cpServiceUrl := "content-collection/content-package"
		services := map[string]baseftrwapp.Service{
			spServiceUrl: collection.NewContentCollectionService(driver, []string{"Curation", "StoryPackage"}, "SELECTS", "IS_CURATED_FOR"),
			cpServiceUrl: collection.NewContentCollectionService(driver, []string{}, "CONTAINS", ""),
		}

		for _, service := range services {
			if err := service.Initialise(); err != nil {
				log.Fatalf("could not initialise cc service: %s", err)
			}
		}

		checks := []v1_1.Check{checkNeo4J(services[spServiceUrl], spServiceUrl), checkNeo4J(services[cpServiceUrl], cpServiceUrl)}
		hc := v1_1.TimedHealthCheck{
			HealthCheck: v1_1.HealthCheck{
				SystemCode:  *appSystemCode,
				Name:        *appName,
				Description: appDescription,
				Checks:      checks,
			},
			Timeout: 10 * time.Second,
		}
		baseftrwapp.RunServerWithConf(baseftrwapp.RWConf{
			Services:      services,
			HealthHandler: v1_1.Handler(&hc),
			Port:          *port,
			ServiceName:   *appName,
			Env:           "local",
			EnableReqLog:  true,
		})
	}

	log.SetLevel(log.InfoLevel)
	log.Infof("Application started with args %s", os.Args)
	app.Run(os.Args)
}

func checkNeo4J(service baseftrwapp.Service, serviceUrl string) v1_1.Check {
	return v1_1.Check{
		BusinessImpact:   "Cannot read/write content via this writer",
		Name:             "Check connectivity to Neo4j",
		PanicGuide:       "https://dewey.ft.com/upp-content-collection-rw-neo4j.html",
		Severity:         1,
		TechnicalSummary: "Service mapped on URL " + serviceUrl + " cannot connect to Neo4j",
		Checker:          func() (string, error) { return "", service.Check() },
	}
}
