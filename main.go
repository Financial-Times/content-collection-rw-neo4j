package main

import (
	"os"

	"time"

	"github.com/Financial-Times/base-ft-rw-app-go/v2/baseftrwapp"
	"github.com/Financial-Times/content-collection-rw-neo4j/collection"
	"github.com/Financial-Times/go-fthealth/v1_1"
	logger "github.com/Financial-Times/go-logger/v2"
	cli "github.com/jawher/mow.cli"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
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
		Value:  "bolt://localhost:7687",
		Desc:   "neo4j endpoint URL",
		EnvVar: "NEO_URL",
	})

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	batchSize := app.Int(cli.IntOpt{
		Name:   "batchSize",
		Value:  1024,
		Desc:   "Maximum number of statements to execute per batch",
		EnvVar: "BATCH_SIZE",
	})

	log := logger.NewUPPInfoLogger(*appName)
	log.WithFields(map[string]interface{}{
		"appName":       *appName,
		"appSystemCode": *appSystemCode,
		"neoURL":        *neoURL,
		"port":          *port,
		"batchSize":     *batchSize,
	}).Info("Application staring...")

	app.Action = func() {
		driver, err := cmneo4j.NewDefaultDriver(*neoURL, log)
		if err != nil {
			log.WithError(err).Fatal("Could not create a new instance of cmneo4j driver")
		}
		defer driver.Close()

		spServiceURL := "content-collection/story-package"
		cpServiceURL := "content-collection/content-package"
		services := map[string]baseftrwapp.Service{
			spServiceURL: collection.NewContentCollectionService(driver, []string{"Curation", "StoryPackage"}, "SELECTS", "IS_CURATED_FOR"),
			cpServiceURL: collection.NewContentCollectionService(driver, []string{}, "CONTAINS", ""),
		}

		checks := []v1_1.Check{checkNeo4J(services[spServiceURL], spServiceURL), checkNeo4J(services[cpServiceURL], cpServiceURL)}
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

	err := app.Run(os.Args)
	if err != nil {
		log.WithError(err).Error("app.Run returned an error")
	}
}

func checkNeo4J(service baseftrwapp.Service, serviceURL string) v1_1.Check {
	return v1_1.Check{
		BusinessImpact:   "Cannot read/write content via this writer",
		Name:             "Check connectivity to Neo4j",
		PanicGuide:       "https://dewey.ft.com/upp-content-collection-rw-neo4j.html",
		Severity:         1,
		TechnicalSummary: "Service mapped on URL " + serviceURL + " cannot connect to Neo4j",
		Checker:          func() (string, error) { return "", service.Check() },
	}
}
