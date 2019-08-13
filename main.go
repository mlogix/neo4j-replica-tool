package main

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/neo4j"
	"math"
	"runtime"
)

type Neo4jType struct {
	Name  string
	Count int64
}

func main() {
	var workers = runtime.NumCPU() / 2
	runtime.GOMAXPROCS(workers)

	var sourceSession neo4j.Session
	var destinationSession neo4j.Session

	var sourceNodes interface{}
	var sourceRelationships interface{}

	sourceSession = prepareNeo4jConnection("bolt://localhost:7687", "", "")
	defer sourceSession.Close()

	destinationSession = prepareNeo4jConnection("bolt://localhost:27687", "neo4j", "testpass")
	defer destinationSession.Close()

	sourceNodes, _ = sourceSession.ReadTransaction(readLabels)
	sourceRelationships, _ = sourceSession.ReadTransaction(readRelationshipsTypes)
	records := make(chan neo4j.Node)
	relations := make(chan neo4j.Relationship)

	go func() {
		for {
			select {
			case record := <-records:
				performRecord(destinationSession, record)
			case rel := <-relations:
				performRelationship(destinationSession, rel)
			}
		}
	}()

	for _, node := range sourceNodes.([]Neo4jType) {
		readLabelNodes(sourceSession, node, records)
	}
	close(records)

	for _, rel := range sourceRelationships.([]Neo4jType) {
		fmt.Printf("%s \n", rel)
		readRelationships(sourceSession, rel, relations)
	}
	close(relations)
}

func prepareNeo4jConnection(uri string, username string, password string) neo4j.Session {
	var driver neo4j.Driver
	var session neo4j.Session

	driver, _ = neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""), func(config *neo4j.Config) {
		config.Log = neo4j.ConsoleLogger(neo4j.ERROR)
	})

	session, _ = driver.Session(neo4j.AccessModeWrite)
	return session
}

func readLabels(tx neo4j.Transaction) (interface{}, error) {
	var err error
	var list []Neo4jType
	var result neo4j.Result

	if result, err = tx.Run("MATCH (n) RETURN distinct labels(n) as label, count(*) as count", nil); err != nil {
		return nil, err
	}

	for result.Next() {
		record := result.Record()
		labels, _ := record.Get("label")
		count, _ := record.Get("count")
		label := labels.([]interface{})[0].(string)
		list = append(list, Neo4jType{Name: label, Count: count.(int64)})
	}

	if err = result.Err(); err != nil {
		return nil, err
	}

	return list, nil
}

func readLabelNodes(session neo4j.Session, node Neo4jType, c chan neo4j.Node) {
	var batchSize = 50.0
	batches := int(math.Ceil(float64(node.Count) / batchSize))

	for i := 0; i < batches; i++ {
		reader := func(tx neo4j.Transaction) (interface{}, error) {
			var err error
			var list []interface{}
			var result neo4j.Result

			offset := i * int(batchSize)
			query := fmt.Sprintf("MATCH (n:`%s`) RETURN n SKIP %v LIMIT %v", node.Name, offset, batchSize)

			if result, err = tx.Run(query, nil); err != nil {
				return nil, err
			}

			for result.Next() {
				record := result.Record()
				rec := record.GetByIndex(0)
				c <- rec.(neo4j.Node)
				list = append(list, rec)
			}

			if err = result.Err(); err != nil {
				return nil, err
			}

			return list, nil
		}
		session.ReadTransaction(reader)
	}
}

func performRecord(session neo4j.Session, record neo4j.Node) error {
	var result neo4j.Result
	var err error

	id := record.Id()
	label := record.Labels()[0]
	props := record.Props()

	query := fmt.Sprintf("MERGE (n:`%s` { _id: $id }) ON CREATE SET n = $props, n._id = $id RETURN n", label)
	result, err = session.Run(query, map[string]interface{}{"id": id, "props": props})

	if err != nil {
		return err // handle error
	}

	if err = result.Err(); err != nil {
		return err // handle error
	}

	return nil
}

func readRelationshipsTypes(tx neo4j.Transaction) (interface{}, error) {
	var err error
	var list []Neo4jType
	var result neo4j.Result

	if result, err = tx.Run("match ()-[r]-() return distinct type(r) as type, COUNT(r) as count", nil); err != nil {
		return nil, err
	}

	for result.Next() {
		record := result.Record()
		relType, _ := record.Get("type")
		count, _ := record.Get("count")
		list = append(list, Neo4jType{Name: relType.(string), Count: count.(int64)})
	}

	if err = result.Err(); err != nil {
		return nil, err
	}

	return list, nil
}

func readRelationships(session neo4j.Session, rel Neo4jType, c chan neo4j.Relationship) {
	var batchSize = 50.0
	batches := int(math.Ceil(float64(rel.Count) / batchSize))

	for i := 0; i < batches; i++ {
		reader := func(tx neo4j.Transaction) (interface{}, error) {
			var err error
			var list []interface{}
			var result neo4j.Result

			offset := i * int(batchSize)
			query := fmt.Sprintf("MATCH ()-[r:`%s`]-() RETURN r SKIP %v LIMIT %v",
				rel.Name, offset, batchSize)

			if result, err = tx.Run(query, nil); err != nil {
				return nil, err
			}

			for result.Next() {
				record := result.Record()
				rel := record.GetByIndex(0)
				c <- rel.(neo4j.Relationship)
				list = append(list, rel)
			}

			if err = result.Err(); err != nil {
				return nil, err
			}

			return list, nil
		}
		session.ReadTransaction(reader)
	}
}

func performRelationship(session neo4j.Session, relationship neo4j.Relationship) error {
	var result neo4j.Result
	var err error

	id := relationship.Id()
	startId := relationship.StartId()
	endId := relationship.EndId()
	props := relationship.Props()
	relType := relationship.Type()

	query := fmt.Sprintf("MATCH (a),(b) WHERE a._id = %d AND b._id = %d CREATE (a)-[r:`%s` { _id: $id } ]->(b) SET r = $props, r._id = $id RETURN r",
		startId, endId, relType)

	result, err = session.Run(query, map[string]interface{}{"id": id, "props": props})

	if err != nil {
		return err // handle error
	}

	if result.Next() {
		result.Record()
	}

	if err = result.Err(); err != nil {
		return err // handle error
	}

	return nil
}
