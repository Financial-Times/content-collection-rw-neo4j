package collection

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var defaultLabels = []string{"ContentCollection"}

type service struct {
	driver            neo4j.Driver
	joinedLabels      string
	relation          string
	extraRelForDelete string
}

//instantiate service
func NewContentCollectionService(driver neo4j.Driver, labels []string, relation string, extraRelForDelete string) baseftrwapp.Service {
	labels = append(defaultLabels, labels...)
	joinedLabels := strings.Join(labels, ":")

	return service{
		driver:            driver,
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

	return EnsureConstraints(pcd.driver, constraintMap)
}

func (pcd service) Close() error {
	return pcd.driver.Close()
}

// Check feeds into the Healthcheck and checks whether we can connect to Neo and that we are connected to the leader
func (pcd service) Check() error {
	return pcd.driver.VerifyConnectivity()
}

// Read - reads a content collection given a UUID
func (pcd service) Read(uuid string, transID string) (interface{}, bool, error) {
	results := []struct {
		contentCollection
	}{}
	//nolint
	cypher := fmt.Sprintf(`MATCH (n:%s {uuid:{uuid}})
				OPTIONAL MATCH (n)-[rel:%s]->(t:Thing)
				WITH n, rel, t
				ORDER BY rel.order
				RETURN  n.uuid as uuid,
					n.publishReference as publishReference,
					n.lastModified as lastModified,
					collect({uuid:t.uuid}) as items`,
		pcd.joinedLabels, pcd.relation)
	params := map[string]interface{}{"uuid": uuid}

	session := pcd.driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()
	_, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return nil, runCypherInTx(tx, cypher, params, &results)
	})

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
	newCC := newThing.(contentCollection)

	session := pcd.driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	tx, err := session.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin a new transaction: %w", err)
	}
	// Close will Rollback the tx if it's not committed
	defer tx.Close()

	//nolint
	cypher := fmt.Sprintf(`MATCH (n:%s {uuid: {uuid}})
			OPTIONAL MATCH (item:Thing)<-[rel:%s]-(n)
			DELETE rel`,
		pcd.joinedLabels, pcd.relation)
	params := map[string]interface{}{
		"uuid": newCC.UUID,
	}
	err = runCypherInTx(tx, cypher, params, nil)
	if err != nil {
		msg := "could not delete cc outgoing rels(%s), reverting transaction: %w"
		return fmt.Errorf(msg, pcd.relation, err)
	}

	//nolint
	cypher = fmt.Sprintf(`MERGE (n:Thing {uuid: {uuid}})
		    set n={allprops}
		    set n:%s`, pcd.joinedLabels)
	params = map[string]interface{}{
		"uuid": newCC.UUID,
		"allprops": map[string]interface{}{
			"uuid":             newCC.UUID,
			"publishReference": newCC.PublishReference,
			"lastModified":     newCC.LastModified,
		},
	}
	err = runCypherInTx(tx, cypher, params, nil)
	if err != nil {
		msg := "updating cc with uuid=%s failed, reverting transaction: %w"
		return fmt.Errorf(msg, newCC.UUID, err)
	}

	for i, item := range newCC.Items {
		//nolint
		cypher = fmt.Sprintf(
			`MATCH (n:%s {uuid:{contentCollectionUuid}})
			MERGE (content:Thing {uuid: {contentUuid}})
			MERGE (n)-[rel:%s {order: {itemOrder}}]->(content)`,
			pcd.joinedLabels, pcd.relation)
		params = map[string]interface{}{
			"contentCollectionUuid": newCC.UUID,
			"contentUuid":           item.UUID,
			"itemOrder":             i + 1,
		}
		err = runCypherInTx(tx, cypher, params, nil)
		if err != nil {
			msg := "adding content to cc with uuid=%s failed, reverting transaction: %w"
			return fmt.Errorf(msg, newCC.UUID, err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("could not write new cc, failed to commit transaction: %w", err)
	}
	return nil
}

//Delete - Deletes a content collection
func (pcd service) Delete(uuid string, transID string) (bool, error) {
	session := pcd.driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	tx, err := session.BeginTransaction()
	if err != nil {
		return false, fmt.Errorf("failed to begin a new transaction: %w", err)
	}
	defer tx.Close()

	//nolint
	cypher := fmt.Sprintf(`MATCH (cc:Thing {uuid: {uuid}})
			OPTIONAL MATCH (item:Thing)<-[rel:%s]-(cc)
			DELETE rel`, pcd.relation)
	params := map[string]interface{}{
		"uuid": uuid,
	}
	err = runCypherInTx(tx, cypher, params, nil)
	if err != nil {
		msg := "could not delete cc outgoing rels(%s), reverting transaction: %w"
		return false, fmt.Errorf(msg, pcd.relation, err)
	}

	if pcd.extraRelForDelete != "" {
		//nolint
		cypher = fmt.Sprintf(`MATCH (cc:Thing {uuid: {uuid}})
				OPTIONAL MATCH (t:Thing)<-[rel:%s]-(cc)
				DELETE rel`, pcd.extraRelForDelete)
		params = map[string]interface{}{
			"uuid": uuid,
		}
		err = runCypherInTx(tx, cypher, params, nil)
		if err != nil {
			msg := "could not delete cc outgoing rels(%s), reverting transaction: %w"
			return false, fmt.Errorf(msg, pcd.extraRelForDelete, err)
		}
	}

	//nolint
	cypher = `MATCH (cc:Thing {uuid: {uuid}})
			REMOVE cc:ContentCollection`
	params = map[string]interface{}{
		"uuid": uuid,
	}
	err = runCypherInTx(tx, cypher, params, nil)
	if err != nil {
		msg := "failed to delete label ContentCollection for %s, reverting transaction: %w"
		return false, fmt.Errorf(msg, uuid, err)
	}

	//nolint
	cypher = `MATCH (cc:Thing {uuid: {uuid}})
			OPTIONAL MATCH (cc)-[rel]-()
			WITH cc, count(rel) AS relCount
			WHERE relCount = 0
			DELETE cc`
	params = map[string]interface{}{
		"uuid": uuid,
	}
	result, err := tx.Run(cypher, params)
	if err != nil {
		msg := "failed to delete cc with uuid=%s, reverting transaction: %w"
		return false, fmt.Errorf(msg, uuid, err)
	}
	summary, err := result.Consume()
	if err != nil {
		msg := "could not get summary info for cc delete statememnt, reverting transaction: %w"
		return false, fmt.Errorf(msg, err)
	}

	var deleted bool
	if summary.Counters().NodesDeleted() > 0 {
		deleted = true
	}
	err = tx.Commit()
	if err != nil {
		return false, fmt.Errorf("failed to delete cc, committing delete transaction failed: %w", err)
	}
	return deleted, nil
}

// DecodeJSON - Decodes JSON into a content collection
func (pcd service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	c := contentCollection{}
	err := dec.Decode(&c)

	return c, c.UUID, err
}

// Count - Returns a count of the number of content in this Neo instance
func (pcd service) Count() (int, error) {
	session := pcd.driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	results := []struct {
		Count int `json:"c"`
	}{}
	cypher := fmt.Sprintf(`MATCH (n:%s) RETURN count(n) as c`, pcd.joinedLabels)
	_, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return nil, runCypherInTx(tx, cypher, nil, &results)
	})
	if err != nil {
		return 0, err
	}
	if len(results) != 1 {
		return 0, fmt.Errorf("unexpected response body, result contains more than one element")
	}
	return results[0].Count, nil
}

// Schema functions

// EnsureIndexes should look similar

func EnsureConstraints(driver neo4j.Driver, constraints map[string]string) error {
	existingConstraints := map[string][]string{}
	for label := range constraints {
		cs, err := GetUniqueNodePropertyConstraints(driver, label)
		if err != nil {
			return fmt.Errorf("could not get constraints for %s: %w", label, err)
		}
		existingConstraints[label] = cs
	}
	for label, prop := range constraints {
		labelConstraints := existingConstraints[label]
		var found bool
		for _, c := range labelConstraints {
			if c == prop {
				found = true
				break
			}
		}
		if found {
			continue
		}
		err := CreateUniqueNodePropertyConstraint(driver, label, prop)
		if err != nil {
			msg := "could not create constraint for label: %s on property: %s"
			return fmt.Errorf(msg, label, prop)
		}
	}
	return nil
}

func CreateUniqueNodePropertyConstraint(driver neo4j.Driver, label, property string) error {
	session := driver.NewSession(neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close()
	cypher := fmt.Sprintf(
		`CREATE CONSTRAINT ON (label:%s) ASSERT label.%s IS UNIQUE`, label, property)
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		result, err := tx.Run(cypher, nil)
		if err != nil {
			return nil, err
		}
		_, err = result.Consume()
		if err != nil {
			return nil, err
		}
		return nil, nil
	})

	return err
}

// For Neo4j 4.x this function will be refactored
//
// IMO for 3.5 our best option is using the rest api.
// The other approach would be to use the driver and execute `call db.constraints()`
// and then parse the description string, which seems more fragile to me.
func GetUniqueNodePropertyConstraints(driver neo4j.Driver, label string) ([]string, error) {
	if label == "" {
		return nil, fmt.Errorf("label is empty")
	}
	t := driver.Target()
	// need a url pointer in order to invoke .Hostname()
	tptr := &t
	// TODO: Hardcoding the port is error prone
	restAPIURL := fmt.Sprintf("http://%s:7474", tptr.Hostname())
	nodeConstraintsURL := fmt.Sprintf("%s/db/data/schema/constraint/%s/uniqueness/", restAPIURL, label)
	req, err := http.NewRequest("GET", nodeConstraintsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create new request object: %w", err)
	}
	// TODO: DefaultClient doesn't have a timeout
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request failed: %w", err)
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %w", err)
	}
	var response []struct {
		Properties []string `json:"property_keys"`
	}
	err = json.Unmarshal(data, &response)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal response body: %w", err)
	}
	props := []string{}
	for _, r := range response {
		props = append(props, r.Properties...)
	}
	return props, nil
}

// end of schema functions

// util functions

// parseTransactionArrResult iterates trough the records in the given neo4j.Result object and parses them into the given output object.
// The function relies that the neo4j.Result object contains array of records each of which with the same fields.
// TODO: could eventually be moved out in a common library
func parseTransactionArrResult(result neo4j.Result, output interface{}) error {
	// We are not interested in the result
	if output == nil {
		_, err := result.Consume()
		if err != nil {
			return err
		}
		return nil
	}

	var records []*neo4j.Record
	for result.Next() {
		records = append(records, result.Record())
	}

	// It is important to check Err() after Next() returning false to find out whether it is end of result stream or
	// an error that caused the end of result consumption.
	if err := result.Err(); err != nil {
		return fmt.Errorf("failed to consume the transaction result stream: %w", err)
	}

	if len(records) == 0 {
		return nil
	}

	// Get the keys of the records, we rely on that they are all the same for all the records in the result
	keys := records[0].Keys

	var recordsMaps []map[string]interface{}
	for _, rec := range records {
		recMap := make(map[string]interface{})
		for _, k := range keys {
			val, ok := rec.Get(k)
			if !ok {
				return fmt.Errorf("failed to parse transaction result: unknown key %s", k)
			}
			recMap[k] = val
		}
		recordsMaps = append(recordsMaps, recMap)
	}

	recordsMarshalled, err := json.Marshal(recordsMaps)
	if err != nil {
		return fmt.Errorf("failed to marshall parsed transaction results: %w", err)
	}

	err = json.Unmarshal(recordsMarshalled, output)
	if err != nil {
		return fmt.Errorf("failed to unmarshall parsed transaction results: %w", err)
	}

	return nil
}

func runCypherInTx(tx neo4j.Transaction, cypher string, params map[string]interface{}, results interface{}) error {
	result, err := tx.Run(cypher, params)
	if err != nil {
		return err
	}
	err = parseTransactionArrResult(result, results)
	if err != nil {
		return err
	}
	return nil
}
