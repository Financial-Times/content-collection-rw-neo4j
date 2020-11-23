package collection

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

var defaultLabels = []string{"ContentCollection"}

type service struct {
	conn              neoutils.NeoConnection
	joinedLabels      string
	relation          string
	extraRelForDelete string
}

//instantiate service
func NewContentCollectionService(cypherRunner neoutils.NeoConnection, labels []string, relation string, extraRelForDelete string) service {
	labels = append(defaultLabels, labels...)
	joinedLabels := strings.Join(labels, ":")

	return service{
		conn:              cypherRunner,
		joinedLabels:      joinedLabels,
		relation:          relation,
		extraRelForDelete: extraRelForDelete,
	}
}

//Initialise initialisation of the indexes
func (pcd service) Initialise() error {
	labels := strings.Split(pcd.joinedLabels, ":")

	constraintMap := map[string]string{}
	for _, label := range labels {
		constraintMap[label] = "uuid"
	}

	return pcd.conn.EnsureConstraints(constraintMap)
}

// Check feeds into the Healthcheck and checks whether we can connect to Neo and that we are connected to the leader
func (pcd service) Check() error {
	writableErr := neoutils.CheckWritable(pcd.conn)
	if writableErr != nil {
		return writableErr
	}

	return neoutils.Check(pcd.conn)
}

// Read - reads a content collection given a UUID
func (pcd service) Read(uuid string, transID string) (interface{}, bool, error) {
	results := []struct {
		contentCollection
	}{}

	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n:%s {uuid:{uuid}})
				OPTIONAL MATCH (n)-[rel:%s]->(t:Thing)
				WITH n, rel, t
				ORDER BY rel.order
				RETURN  n.uuid as uuid,
					n.publishReference as publishReference,
					n.lastModified as lastModified,
					collect({uuid:t.uuid}) as items`, pcd.joinedLabels, pcd.relation),
		Parameters: map[string]interface{}{
			"uuid": uuid,
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
func (pcd service) Write(newThing interface{}, transID string) error {
	newContentCollection := newThing.(contentCollection)

	deleteRelationshipsQuery := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n:%s {uuid: {uuid}})
			OPTIONAL MATCH (item:Thing)<-[rel:%s]-(n)
			DELETE rel`, pcd.joinedLabels, pcd.relation),
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
		Statement: fmt.Sprintf(`MERGE (n:Thing {uuid: {uuid}})
		    set n={allprops}
		    set n:%s`, pcd.joinedLabels),
		Parameters: map[string]interface{}{
			"uuid":     newContentCollection.UUID,
			"allprops": params,
		},
	}

	queries := []*neoism.CypherQuery{deleteRelationshipsQuery, writeContentCollectionQuery}

	for i, item := range newContentCollection.Items {
		addItemQuery := addCollectionItemQuery(pcd.joinedLabels, pcd.relation, newContentCollection.UUID, item.UUID, i+1)
		queries = append(queries, addItemQuery)
	}

	return pcd.conn.CypherBatch(queries)
}

func addCollectionItemQuery(joinedLabels string, relation string, contentCollectionUuid string, itemUuid string, order int) *neoism.CypherQuery {
	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n:%s {uuid:{contentCollectionUuid}})
			MERGE (content:Thing {uuid: {contentUuid}})
			MERGE (n)-[rel:%s {order: {itemOrder}}]->(content)`, joinedLabels, relation),
		Parameters: map[string]interface{}{
			"contentCollectionUuid": contentCollectionUuid,
			"contentUuid":           itemUuid,
			"itemOrder":             order,
		},
	}

	return query
}

//Delete - Deletes a content collection
func (pcd service) Delete(uuid string, transID string) (bool, error) {
	var queries []*neoism.CypherQuery

	removeRelationships := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (cc:Thing {uuid: {uuid}})
			OPTIONAL MATCH (item:Thing)<-[rel:%s]-(cc)
			DELETE rel`, pcd.relation),
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}
	queries = append(queries, removeRelationships)

	if pcd.extraRelForDelete != "" {
		removeExtraRelationships := &neoism.CypherQuery{
			Statement: fmt.Sprintf(`MATCH (cc:Thing {uuid: {uuid}})
				OPTIONAL MATCH (t:Thing)<-[rel:%s]-(cc)
				DELETE rel`, pcd.extraRelForDelete),
			Parameters: map[string]interface{}{
				"uuid": uuid,
			},
		}
		queries = append(queries, removeExtraRelationships)
	}

	removeLabel := &neoism.CypherQuery{
		Statement: `MATCH (cc:Thing {uuid: {uuid}})
			REMOVE cc:ContentCollection`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}

	deleteNode := &neoism.CypherQuery{
		Statement: `MATCH (cc:Thing {uuid: {uuid}})
			OPTIONAL MATCH (cc)-[rel]-()
			WITH cc, count(rel) AS relCount
			WHERE relCount = 0
			DELETE cc`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		IncludeStats: true,
	}
	queries = append(queries, removeLabel, deleteNode)

	_ = pcd.conn.CypherBatch(queries)

	s1, err := deleteNode.Stats()
	if err != nil {
		return false, err
	}

	var deleted bool
	if s1.NodesDeleted > 0 {
		deleted = true
	}

	return deleted, err
}

// DecodeJSON - Decodes JSON into a content collection
func (pcd service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	c := contentCollection{}
	err := dec.Decode(&c)

	return c, c.UUID, err
}

// Count - Returns a count of the number of content in this Neo instance
func (pcd service) Count() (int, error) {
	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: fmt.Sprintf(`MATCH (n:%s) RETURN count(n) as c`, pcd.joinedLabels),
		Result:    &results,
	}

	err := pcd.conn.CypherBatch([]*neoism.CypherQuery{query})
	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}
