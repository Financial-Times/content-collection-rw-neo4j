package collection

import (
	"fmt"
	"os"
	"testing"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
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
	db := getDatabaseConnectionAndCheckClean(t, assert)
	testService := getContentCollectionService(db, spLabels, spRelation, "")
	defer cleanDB(db, assert)

	err := testService.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testService.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)
}

func TestUpdate(t *testing.T) {
	assert := assert.New(t)
	db := getDatabaseConnectionAndCheckClean(t, assert)
	testService := getContentCollectionService(db, spLabels, spRelation, "")
	defer cleanDB(db, assert)

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
	db := getDatabaseConnectionAndCheckClean(t, assert)
	testService := getContentCollectionService(db, spLabels, spRelation, "")
	defer cleanDB(db, assert)

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
	db := getDatabaseConnectionAndCheckClean(t, assert)
	testService := getContentCollectionService(db, cpLabels, cpRelation, "")
	defer cleanDB(db, assert)

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
	db := getDatabaseConnectionAndCheckClean(t, assert)
	testServiceNoExtraRelHandle := getContentCollectionService(db, spLabels, spRelation, "")
	testServiceExtraRelHandle := getContentCollectionService(db, spLabels, spRelation, extraRelForDelete)
	defer cleanDB(db, assert)

	err := testServiceNoExtraRelHandle.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testServiceNoExtraRelHandle.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)

	err = createExtraRelation(db, ccUUID)
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

func getDatabaseConnectionAndCheckClean(t *testing.T, assert *assert.Assertions) neoutils.NeoConnection {
	db := getDatabaseConnection(assert)
	cleanDB(db, assert)
	checkDbClean(db, t)
	return db
}

func getDatabaseConnection(assert *assert.Assertions) neoutils.NeoConnection {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(url, conf)
	assert.NoError(err, "Failed to connect to Neo4j")
	return db
}

func cleanDB(db neoutils.CypherRunner, assert *assert.Assertions) {
	qs := []*neoism.CypherQuery{
		{
			Statement: `MATCH (mc:Thing {uuid: {uuid}})
			DETACH DELETE mc`,
			Parameters: map[string]interface{}{
				"uuid": ccUUID,
			},
		},
		{
			Statement: `MATCH (mc:Thing {uuid: {uuid}})
			DETACH DELETE mc`,
			Parameters: map[string]interface{}{
				"uuid": extraRelThingUUID,
			},
		},
	}

	err := db.CypherBatch(qs)
	assert.NoError(err)
}

func checkDbClean(db neoutils.CypherRunner, t *testing.T) {
	assert := assert.New(t)

	result := []struct {
		Uuid string `json:"uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `MATCH (n:Thing) WHERE n.uuid in {uuids} RETURN n.uuid`,
		Parameters: neoism.Props{
			"uuids": []string{ccUUID},
		},
		Result: &result,
	}
	err := db.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)
	assert.Empty(result)
}

func getContentCollectionService(db neoutils.NeoConnection, labels []string, relation string, extraRelForDelete string) service {
	s := NewContentCollectionService(db, labels, relation, extraRelForDelete)
	s.Initialise()
	return s
}

func createExtraRelation(cypherRunner neoutils.NeoConnection, ccUUID string) error {
	params := map[string]interface{}{
		"uuid": extraRelThingUUID,
	}

	extraRelThingQuery := &neoism.CypherQuery{
		Statement: fmt.Sprint(`MERGE (n:Thing {uuid: {uuid}})
		    set n={allprops}`),
		Parameters: map[string]interface{}{
			"uuid":     extraRelThingUUID,
			"allprops": params,
		},
	}

	extraRelQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (cc:Thing {uuid:{ccUuid}})
			MERGE (content:Thing {uuid: {thingUuid}})
			MERGE (cc)-[rel:%s]->(content)`, extraRelForDelete),
		Parameters: map[string]interface{}{
			"ccUuid":    ccUUID,
			"thingUuid": extraRelThingUUID,
		},
	}

	queries := []*neoism.CypherQuery{extraRelThingQuery, extraRelQuery}

	return cypherRunner.CypherBatch(queries)
}
