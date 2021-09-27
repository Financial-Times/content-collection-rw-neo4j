package collection

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
	"github.com/stretchr/testify/assert"

	logger "github.com/Financial-Times/go-logger/v2"
)

var (
	ccUUID            = "cc-12345"
	cpLabels          = []string{}
	cpRelation        = "CONTAINS"
	spLabels          = []string{"Curation", "StoryPackage"}
	spRelation        = "SELECTS"
	extraRelForDelete = "IS_CURATED_FOR"
	extraRelThingUUID = "t-12345"
)

func TestWrite(t *testing.T) {
	assert := assert.New(t)
	d := getDriverAndCheckClean(t, assert)
	defer d.Close()

	testService := getContentCollectionService(t, d, spLabels, spRelation, "")
	defer cleanDB(d, assert)

	err := testService.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testService.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)
}

func TestUpdate(t *testing.T) {
	assert := assert.New(t)
	d := getDriverAndCheckClean(t, assert)
	defer d.Close()

	testService := getContentCollectionService(t, d, spLabels, spRelation, "")
	defer cleanDB(d, assert)

	err := testService.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testService.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)

	err = testService.Write(createContentCollection(3), "tID")
	assert.NoError(err)

	result, found, err = testService.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 3)
}

func TestDeleteSP(t *testing.T) {
	assert := assert.New(t)
	d := getDriverAndCheckClean(t, assert)
	defer d.Close()

	testService := getContentCollectionService(t, d, spLabels, spRelation, "")
	defer cleanDB(d, assert)

	err := testService.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testService.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)

	deleted, err := testService.Delete(ccUUID, "tID")
	assert.NoError(err)
	assert.Equal(true, deleted)

	result, found, err = testService.Read(ccUUID, "tID")
	assert.NoError(err)
	assert.False(found)
	assert.Equal(contentCollection{}, result)
}

func TestDeleteCP(t *testing.T) {
	assert := assert.New(t)
	d := getDriverAndCheckClean(t, assert)
	defer d.Close()

	testService := getContentCollectionService(t, d, cpLabels, cpRelation, "")
	defer cleanDB(d, assert)

	err := testService.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testService.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)

	deleted, err := testService.Delete(ccUUID, "tID")
	assert.NoError(err)
	assert.Equal(true, deleted)

	result, found, err = testService.Read(ccUUID, "tID")
	assert.NoError(err)
	assert.False(found)
	assert.Equal(contentCollection{}, result)
}

func TestDeleteWithExtraRelation(t *testing.T) {
	assert := assert.New(t)
	d := getDriverAndCheckClean(t, assert)
	defer d.Close()

	testServiceNoExtraRelHandle := getContentCollectionService(t, d, spLabels, spRelation, "")
	testServiceExtraRelHandle := getContentCollectionService(t, d, spLabels, spRelation, extraRelForDelete)
	defer cleanDB(d, assert)

	err := testServiceNoExtraRelHandle.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testServiceNoExtraRelHandle.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)

	err = createExtraRelation(d, ccUUID)
	assert.NoError(err)

	deleted, err := testServiceNoExtraRelHandle.Delete(ccUUID, "tID")
	assert.NoError(err)
	assert.Equal(false, deleted)

	deleted, err = testServiceExtraRelHandle.Delete(ccUUID, "tID")
	assert.NoError(err)
	assert.Equal(true, deleted)

	result, found, err = testServiceNoExtraRelHandle.Read(ccUUID, "tID")
	assert.NoError(err)
	assert.False(found)
	assert.Equal(contentCollection{}, result)
}

func createContentCollection(itemCount int) contentCollection {
	items := []item{}
	for count := 0; count < itemCount; count++ {
		items = append(items, item{fmt.Sprint("Item", count)})
	}

	c := contentCollection{
		UUID:             ccUUID,
		PublishReference: "test12345",
		LastModified:     "2016-08-25T06:06:23.532Z",
		Items:            items,
	}

	return c
}

func validateResult(assert *assert.Assertions, result interface{}, found bool, err error, itemCount int) {
	assert.NoError(err)
	assert.True(found)

	collection := result.(contentCollection)
	assert.Equal(ccUUID, collection.UUID)
	assert.Equal(itemCount, len(collection.Items))
}

func getDriverAndCheckClean(t *testing.T, assert *assert.Assertions) *cmneo4j.Driver {
	d := getNeoDriver(assert)
	cleanDB(d, assert)
	checkDBClean(d, t)
	return d
}

func getNeoDriver(assert *assert.Assertions) *cmneo4j.Driver {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "bolt://localhost:7687"
	}

	log := logger.NewUPPLogger("cc-rw-neo4j-tests", "INFO")
	d, err := cmneo4j.NewDefaultDriver(url, log)
	assert.NoError(err, "Failed to connect to Neo4j")

	return d
}

func cleanDB(d *cmneo4j.Driver, assert *assert.Assertions) {
	qs := []*cmneo4j.Query{
		{
			Cypher: `MATCH (mc:Thing {uuid: $uuid})
			DETACH DELETE mc`,
			Params: map[string]interface{}{
				"uuid": ccUUID,
			},
		},
		{
			Cypher: `MATCH (mc:Thing {uuid: $uuid})
			DETACH DELETE mc`,
			Params: map[string]interface{}{
				"uuid": extraRelThingUUID,
			},
		},
	}

	err := d.Write(qs...)
	assert.NoError(err)
}

func checkDBClean(d *cmneo4j.Driver, t *testing.T) {
	assert := assert.New(t)

	result := []struct {
		UUID string `json:"uuid"`
	}{}

	checkGraph := &cmneo4j.Query{
		Cypher: `MATCH (n:Thing) WHERE n.uuid in $uuids RETURN n.uuid`,
		Params: map[string]interface{}{
			"uuids": []string{ccUUID},
		},
		Result: &result,
	}
	err := d.Read(checkGraph)
	if errors.Is(err, cmneo4j.ErrNoResultsFound) {
		assert.Empty(result)
	} else {
		assert.NoError(err)
	}
}

func getContentCollectionService(t *testing.T, d *cmneo4j.Driver, labels []string, rel, extraRelForDelete string) baseftrwapp.Service {
	assert := assert.New(t)

	s := NewContentCollectionService(d, labels, rel, extraRelForDelete)
	err := s.Initialise()
	if errors.Is(err, cmneo4j.ErrNeo4jVersionNotSupported) {
		return s
	}

	assert.NoError(err)
	return s
}

func createExtraRelation(d *cmneo4j.Driver, ccUUID string) error {
	params := map[string]interface{}{
		"uuid": extraRelThingUUID,
	}

	extraRelThingQuery := &cmneo4j.Query{
		Cypher: `MERGE (n:Thing {uuid: $uuid}) set n=$allprops`,
		Params: map[string]interface{}{
			"uuid":     extraRelThingUUID,
			"allprops": params,
		},
	}

	extraRelQuery := &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MATCH (cc:Thing {uuid:$ccUuid})
			MERGE (content:Thing {uuid: $thingUuid})
			MERGE (cc)-[rel:%s]->(content)`, extraRelForDelete),
		Params: map[string]interface{}{
			"ccUuid":    ccUUID,
			"thingUuid": extraRelThingUUID,
		},
	}

	return d.Write(extraRelThingQuery, extraRelQuery)
}
