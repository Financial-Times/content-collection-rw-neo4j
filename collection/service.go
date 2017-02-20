package collection

import (
	"encoding/json"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	//	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"fmt"
)

type Service interface {
	Write(thing interface{}, collectionType string, relationType string) error
	Read(uuid string, collectionType string, relationType string) (thing interface{}, found bool, err error)
	Delete(uuid string, relationType string) (found bool, err error)
	DecodeJSON(*json.Decoder) (thing interface{}, identity string, err error)
	Count(collectionType string) (int, error)
	Check() error
	Initialise() error
}

type service struct {
	conn neoutils.NeoConnection
}

//instantiate service
func NewContentCollectionService(cypherRunner neoutils.NeoConnection) service {
	return service{cypherRunner}
}

//Initialise initialisation of the indexes
func (cd service) Initialise() error {
	err := cd.conn.EnsureIndexes(map[string]string{
		"Identifier": "value",
	})

	if err != nil {
		return err
	}

	return cd.conn.EnsureConstraints(map[string]string{
		"StoryPackage": "uuid"})
}

// Check - Feeds into the Healthcheck and checks whether we can connect to Neo and that the datastore isn't empty
func (pcd service) Check() error {
	return neoutils.Check(pcd.conn)
}

// Read - reads a content collection given a UUID
func (pcd service) Read(uuid string, collectionType string, relationType string) (interface{}, bool, error) {
	results := []struct {
		contentCollection
	}{}

	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n {uuid:{uuid}}) WHERE {label} IN labels(n)
				OPTIONAL MATCH (n)-[rel:%s]->(t:Thing)
				WITH n, rel, t
				ORDER BY rel.order
				RETURN n.uuid as uuid, n.publishReference as publishReference, n.lastModified as lastModified, collect({uuid:t.uuid}) as items`, relationType),
		Parameters: map[string]interface{}{
			"label": collectionType,
			"uuid":  uuid,
		},
		Result: &results,
	}

	err := pcd.conn.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return contentCollection{}, false, err
	}

	if len(results) == 0 {
		return contentCollection{}, false, nil
	}

	result := results[0]
	if len(result.Items) == 1 && (result.Items[0].UUID == "") {
		result.Items = []item{}
	}

	contentCollectionResult := contentCollection{
		UUID:             result.UUID,
		PublishReference: result.PublishReference,
		LastModified:     result.LastModified,
		Items:            result.Items,
	}

	return contentCollectionResult, true, nil
}

//Write - Writes a content collection node
func (pcd service) Write(thing interface{}, collectionType string, relationType string) error {
	newContentCollection := thing.(contentCollection)

	deleteRelationshipsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n:Thing {uuid: {uuid}})
			MATCH (item:Thing)<-[rel:%s]-(n)
			DELETE rel`, relationType),
		Parameters: map[string]interface{}{
			"uuid": newContentCollection.UUID,
		},
	}

	params := map[string]interface{}{
		"uuid":             newContentCollection.UUID,
		"publishReference": newContentCollection.PublishReference,
		"lastModified":     newContentCollection.LastModified,
	}

	writeContentCollectionQuery := &neoism.CypherQuery{
		Statement: `MERGE (n:Thing {uuid: {uuid}})
		    set n={allprops}
		    set n :Curation:` + collectionType,
		Parameters: map[string]interface{}{
			"uuid":     newContentCollection.UUID,
			"allprops": params,
		},
	}

	queries := []*neoism.CypherQuery{deleteRelationshipsQuery, writeContentCollectionQuery}

	for i, item := range newContentCollection.Items {
		addItemQuery := addStoryPackageItemQuery(collectionType, relationType, newContentCollection.UUID, item.UUID, i + 1)
		queries = append(queries, addItemQuery)
	}

	return pcd.conn.CypherBatch(queries)
}

func addStoryPackageItemQuery(contentCollectionType string, relationType string, contentCollectionUuid string, itemUuid string, order int) *neoism.CypherQuery {
	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n {uuid:{contentCollectionUuid}}) WHERE {label} IN labels(n)
			MERGE (content:Thing {uuid: {contentUuid}})
			MERGE (n)-[rel:%s {order: {itemOrder}}]->(content)`, relationType),
		Parameters: map[string]interface{}{
			"label":                 contentCollectionType,
			"contentCollectionUuid": contentCollectionUuid,
			"contentUuid":           itemUuid,
			"itemOrder":             order,
		},
	}

	return query
}

//Delete - Deletes a content collection
func (pcd service) Delete(uuid string, relationType string) (bool, error) {
	removeRelationships := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n:Thing {uuid: {uuid}})
			OPTIONAL MATCH (item:Thing)<-[rel:%s]-(n)
			DELETE rel`, relationType),
		Parameters: map[string]interface{}{
			"uuid":         uuid,
		},
	}

	removeNode := &neoism.CypherQuery{
		Statement: `MATCH (n:Thing {uuid: {uuid}}) DELETE n`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}

	err := pcd.conn.CypherBatch([]*neoism.CypherQuery{removeRelationships, removeNode})

	s1, err := removeNode.Stats()
	if err != nil {
		return false, err
	}

	var deleted bool
	if s1.NodesDeleted > 0 {
		deleted = true
	}

	return deleted, err
}

// DecodeJSON - Decodes JSON into story package
func (pcd service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	c := contentCollection{}
	err := dec.Decode(&c)

	return c, c.UUID, err
}

// Count - Returns a count of the number of content in this Neo instance
func (pcd service) Count(collectionType string) (int, error) {
	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n) WHERE {label} IN labels(n) RETURN count(n) as c`,
		Parameters: map[string]interface{}{
			"label": collectionType,
		},
		Result: &results,
	}

	err := pcd.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}
