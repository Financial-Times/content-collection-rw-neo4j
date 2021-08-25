package collection

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
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
	driver := getDriverAndCheckClean(t, assert)
	testService, err := getContentCollectionService(driver, spLabels, spRelation, "")
	assert.NoError(err, "could not get new ccService")
	defer cleanDB(driver, assert)

	err = testService.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testService.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)
}

func TestUpdate(t *testing.T) {
	assert := assert.New(t)
	driver := getDriverAndCheckClean(t, assert)
	testService, err := getContentCollectionService(driver, spLabels, spRelation, "")
	assert.NoError(err, "could not get new ccService")
	defer cleanDB(driver, assert)

	err = testService.Write(createContentCollection(2), "tID")
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
	driver := getDriverAndCheckClean(t, assert)
	testService, err := getContentCollectionService(driver, spLabels, spRelation, "")
	assert.NoError(err, "could not get new ccService")
	defer cleanDB(driver, assert)

	err = testService.Write(createContentCollection(2), "tID")
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
	driver := getDriverAndCheckClean(t, assert)
	testService, err := getContentCollectionService(driver, cpLabels, cpRelation, "")
	assert.NoError(err, "could not get new ccService")
	defer cleanDB(driver, assert)

	err = testService.Write(createContentCollection(2), "tID")
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
	driver := getDriverAndCheckClean(t, assert)
	testServiceNoExtraRelHandle, err := getContentCollectionService(driver, spLabels, spRelation, "")
	assert.NoError(err, "could not get new ccService")
	testServiceExtraRelHandle, err := getContentCollectionService(driver, spLabels, spRelation, extraRelForDelete)
	assert.NoError(err, "could not get new ccService with extraRelForDelete")
	defer cleanDB(driver, assert)

	err = testServiceNoExtraRelHandle.Write(createContentCollection(2), "tID")
	assert.NoError(err)

	result, found, err := testServiceNoExtraRelHandle.Read(ccUUID, "tID")
	validateResult(assert, result, found, err, 2)

	err = createExtraRelation(driver, ccUUID)
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

func getDriverAndCheckClean(t *testing.T, assert *assert.Assertions) neo4j.Driver {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "bolt://localhost:7687"
	}
	driver, err := neo4j.NewDriver(url, neo4j.NoAuth())
	assert.NoError(err, "Failed to create a new neo4j driver")
	cleanDB(driver, assert)
	checkDBClean(driver, assert)
	return driver
}

func cleanDB(driver neo4j.Driver, assert *assert.Assertions) {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		_, err := tx.Run(
			`MATCH (mc:Thing) WHERE mc.uuid IN {uuids} DETACH DELETE mc`,
			map[string]interface{}{
				"uuids": []string{ccUUID, extraRelThingUUID},
			})
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	assert.NoError(err, "Could not clean database")
}

func checkDBClean(driver neo4j.Driver, assert *assert.Assertions) {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	responseNotEmptyErr := errors.New("the query was not expected to return any results")
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run(
			`MATCH (n:Thing) WHERE n.uuid in {uuids} RETURN n.uuid`,
			map[string]interface{}{
				"uuids": []string{ccUUID},
			})
		if err != nil {
			return nil, err
		}
		records, err := result.Collect()
		if err != nil {
			return nil, err
		}
		if len(records) != 0 {
			return nil, responseNotEmptyErr
		}
		return nil, nil
	})
	assert.NoError(err)
}

func getContentCollectionService(driver neo4j.Driver, labels []string, relation string, extraRelForDelete string) (baseftrwapp.Service, error) {
	s := NewContentCollectionService(driver, labels, relation, extraRelForDelete)
	err := s.Initialise()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func createExtraRelation(driver neo4j.Driver, ccUUID string) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	tx, err := session.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin a new transaction: %w", err)
	}
	defer tx.Close()

	cypher := fmt.Sprint(`MERGE (n:Thing {uuid: {uuid}}) set n={allprops}`)
	params := map[string]interface{}{
		"uuid": extraRelThingUUID,
		"allprops": map[string]interface{}{
			"uuid": extraRelThingUUID,
		},
	}
	err = runCypherInTx(tx, cypher, params, nil)
	if err != nil {
		return fmt.Errorf("failed to create a new node, reverting transaction: %w", err)
	}

	cypher = fmt.Sprintf(`MATCH (cc:Thing {uuid:{ccUuid}})
			MERGE (content:Thing {uuid: {thingUuid}})
			MERGE (cc)-[rel:%s]->(content)`, extraRelForDelete)
	params = map[string]interface{}{
		"ccUuid":    ccUUID,
		"thingUuid": extraRelThingUUID,
	}
	err = runCypherInTx(tx, cypher, params, nil)
	if err != nil {
		return fmt.Errorf("failed to create a new extra relation, reverting transaction: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit transaction for creating a new extra relation failed: %w", err)
	}
	return nil
}
