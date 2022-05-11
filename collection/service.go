package collection

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	cmneo4j "github.com/Financial-Times/cm-neo4j-driver"
)

var defaultLabels = []string{"ContentCollection"}

type Service struct {
	driver            *cmneo4j.Driver
	joinedLabels      string
	relation          string
	extraRelForDelete string
}

// NewContentCollectionService returns an implementation of rwapi.Service interface
// defined in github.com/Financial-Times/up-rw-app-api-go
func NewContentCollectionService(d *cmneo4j.Driver, labels []string, rel, extraRelForDelete string) Service {
	labels = append(defaultLabels, labels...)
	joinedLabels := strings.Join(labels, ":")

	return Service{
		driver:            d,
		joinedLabels:      joinedLabels,
		relation:          rel,
		extraRelForDelete: extraRelForDelete,
	}
}

// Initialise is invoked right after NewContentCollectionService and it makes
// sure that the database has Constraints on "uuid" field for the labels passed
// in NewContentCollectionService
func (pcd Service) Initialise() error {
	labels := strings.Split(pcd.joinedLabels, ":")

	constraintMap := map[string]string{}
	for _, label := range labels {
		constraintMap[label] = "uuid"
	}

	err := pcd.driver.EnsureConstraints(constraintMap)

	return err
}

// Check feeds into the Healthcheck and checks whether we can connect to Neo
func (pcd Service) Check() error {
	return pcd.driver.VerifyConnectivity()
}

// Read - reads a content collection given a UUID
func (pcd Service) Read(uuid string, transID string) (interface{}, bool, error) {
	results := []struct {
		contentCollection
	}{}

	query := &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MATCH (n:%s {uuid:$uuid})
				OPTIONAL MATCH (n)-[rel:%s]->(t:Thing)
				WITH n, rel, t
				ORDER BY rel.order
				RETURN  n.uuid as uuid,
					n.publishReference as publishReference,
					n.lastModified as lastModified,
					collect({uuid:t.uuid}) as items`, pcd.joinedLabels, pcd.relation),
		Params: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := pcd.driver.Read(query)

	if errors.Is(err, cmneo4j.ErrNoResultsFound) {
		return contentCollection{}, false, nil
	}
	if err != nil {
		return contentCollection{}, false, err
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
func (pcd Service) Write(newThing interface{}, transID string) error {
	newContentCollection := newThing.(contentCollection)

	// nolint:gosec
	deleteRelationshipsQuery := &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MATCH (n:%s {uuid: $uuid})
			OPTIONAL MATCH (item:Thing)<-[rel:%s]-(n)
			DELETE rel`, pcd.joinedLabels, pcd.relation),
		Params: map[string]interface{}{
			"uuid": newContentCollection.UUID,
		},
	}

	params := map[string]interface{}{
		"uuid":             newContentCollection.UUID,
		"publishReference": newContentCollection.PublishReference,
		"lastModified":     newContentCollection.LastModified,
	}

	writeContentCollectionQuery := &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MERGE (n:Thing {uuid: $uuid})
		    set n=$allprops
		    set n:%s`, pcd.joinedLabels),
		Params: map[string]interface{}{
			"uuid":     newContentCollection.UUID,
			"allprops": params,
		},
	}

	queries := []*cmneo4j.Query{deleteRelationshipsQuery, writeContentCollectionQuery}

	for i, item := range newContentCollection.Items {
		addItemQuery := addCollectionItemQuery(pcd.joinedLabels, pcd.relation, newContentCollection.UUID, item.UUID, i+1)
		queries = append(queries, addItemQuery)
	}

	return pcd.driver.Write(queries...)
}

func addCollectionItemQuery(joinedLabels, relation, contentCollectionUUID, itemUUID string, order int) *cmneo4j.Query {
	// nolint:gosec
	return &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MATCH (n:%s {uuid:$contentCollectionUuid})
			MERGE (content:Thing {uuid: $contentUuid})
			MERGE (n)-[rel:%s {order: $itemOrder}]->(content)`, joinedLabels, relation),
		Params: map[string]interface{}{
			"contentCollectionUuid": contentCollectionUUID,
			"contentUuid":           itemUUID,
			"itemOrder":             order,
		},
	}
}

//Delete - Deletes a content collection
func (pcd Service) Delete(uuid string, transID string) (bool, error) {
	relsToDelete := pcd.relation
	if pcd.extraRelForDelete != "" {
		relsToDelete = fmt.Sprintf("%s|%s", pcd.relation, pcd.extraRelForDelete)
	}

	// nolint:gosec
	removeRelationships := &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MATCH (cc:Thing {uuid: $uuid})
			OPTIONAL MATCH (item:Thing)<-[rel:%s]-(cc)
			DELETE rel`, relsToDelete),
		Params: map[string]interface{}{
			"uuid": uuid,
		},
	}

	removeLabel := &cmneo4j.Query{
		Cypher: `MATCH (cc:Thing {uuid: $uuid})
			REMOVE cc:ContentCollection`,
		Params: map[string]interface{}{
			"uuid": uuid,
		},
	}

	deleteNode := &cmneo4j.Query{
		Cypher: `MATCH (cc:Thing {uuid: $uuid})
			OPTIONAL MATCH (cc)-[rel]-()
			WITH cc, count(rel) AS relCount
			WHERE relCount = 0
			DELETE cc`,
		Params: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeSummary: true,
	}

	err := pcd.driver.Write(removeRelationships, removeLabel, deleteNode)
	if err != nil {
		return false, err
	}

	s1, err := deleteNode.Summary()
	if err != nil {
		return false, err
	}

	return s1.Counters().NodesDeleted() > 0, nil
}

// DecodeJSON - Decodes JSON into a content collection
func (pcd Service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	c := contentCollection{}
	err := dec.Decode(&c)

	return c, c.UUID, err
}

// Count - Returns a count of the number of content in this Neo instance
func (pcd Service) Count() (int, error) {
	results := []struct {
		Count int `json:"c"`
	}{}

	query := &cmneo4j.Query{
		Cypher: fmt.Sprintf(`MATCH (n:%s) RETURN count(n) as c`, pcd.joinedLabels),
		Result: &results,
	}

	err := pcd.driver.Read(query)
	if errors.Is(err, cmneo4j.ErrNoResultsFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}
