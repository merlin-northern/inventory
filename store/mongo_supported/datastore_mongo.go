// Copyright 2019 Northern.tech AS
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package mongo_supported

import (
	"context"
	"crypto/tls"
	"fmt"

	// "net"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/mendersoftware/go-lib-micro/identity"
	"github.com/mendersoftware/go-lib-micro/log"
	"github.com/mendersoftware/go-lib-micro/mongo_supported/migrate"
	mstore "github.com/mendersoftware/go-lib-micro/store"
	"github.com/pkg/errors"

	"github.com/mendersoftware/inventory/model"
	"github.com/mendersoftware/inventory/store"
)

const (
	DbVersion = "0.1.0"

	DbConnectionTimeout_s = 10

	DbName        = "inventory"
	DbDevicesColl = "devices"

	DbDevId              = "_id"
	DbDevAttributes      = "attributes"
	DbDevGroup           = "group"
	DbDevAttributesDesc  = "description"
	DbDevAttributesValue = "value"
)

var (
	//with offcial mongodb supported driver we keep client
	clientGlobal *mongo.Client

	// once ensures client is created only once
	once sync.Once

	ErrNotFound = errors.New("mongo: no documents in result")
)

type DataStoreMongoConfig struct {
	// connection string
	ConnectionString string

	// SSL support
	SSL           bool
	SSLSkipVerify bool

	// Overwrites credentials provided in connection string if provided
	Username string
	Password string
}

type DataStoreMongo struct {
	client      *mongo.Client
	automigrate bool
}

func NewDataStoreMongoWithSession(client *mongo.Client) store.DataStore {
	return &DataStoreMongo{client: client}
}

//config.ConnectionString must contain a valid
func NewDataStoreMongo(config DataStoreMongoConfig, l *log.Logger) (store.DataStore, error) {
	//init master session
	var err error
	once.Do(func() {
		if !strings.Contains(config.ConnectionString, "://") {
			config.ConnectionString = "mongodb://" + config.ConnectionString
		}
		clientOptions := options.Client().ApplyURI(config.ConnectionString)

		if config.Username != "" {
			clientOptions.SetAuth(options.Credential{
				Username: config.Username,
				Password: config.Password,
			})
		}

		if config.SSL {
			tlsConfig := &tls.Config{}
			tlsConfig.InsecureSkipVerify = config.SSLSkipVerify
			clientOptions.SetTLSConfig(tlsConfig)
		}

		l.Infof("mongo_supported: connecting to mongo '%v'", clientOptions)
		ctx, _ := context.WithTimeout(context.Background(), DbConnectionTimeout_s*time.Second)
		// client, err := mongo.NewClient(options.Client().ApplyURI(uri))
		// err = client.Connect(ctx,clientOptions)
		var err error
		clientGlobal, err = mongo.Connect(ctx, clientOptions)
		if err != nil {
			l.Errorf("mongo_supported: error connecting to mongo '%s'", err.Error())
			return
		}
		if clientGlobal == nil {
			l.Errorf("mongo_supported: client is nil. wow.")
			return
		}
		// from: https://www.mongodb.com/blog/post/mongodb-go-driver-tutorial
		/*
			It is best practice to keep a client that is connected to MongoDB around so that the application can make use of connection pooling - you don't want to open and close a connection for each query. However, if your application no longer requires a connection, the connection can be closed with client.Disconnect() like so:
		*/
		//err = client.Disconnect(context.TODO()) should be called when the connection is no longer needed
		err = clientGlobal.Ping(context.TODO(), nil)
		if err != nil {
			l.Errorf("mongo_supported: error pigning mongo '%s'", err.Error())
			return
		}
		if clientGlobal == nil {
			l.Errorf("mongo_supported: client is nil. wow. on exit from Once.")
			return
		}
	})

	if err != nil {
		l.Errorf("mongo_supported: error: %s.", err.Error())
		return nil, errors.Wrap(err, "failed to open mongo-driver session")
	}

	if clientGlobal == nil {
		l.Errorf("mongo_supported: client is nil. wow. right out of Once.")
	}
	db := &DataStoreMongo{client: clientGlobal}
	if db.client == nil {
		l.Errorf("mongo_supported: client is nil. wow. right before return.")
	}
	l.Infof("mongo_supported: returning db:%v", db)
	return db, nil
}

func (db *DataStoreMongo) GetDevices(ctx context.Context, q store.ListQuery) ([]model.Device, int, error) {
	// l := log.FromContext(ctx)

	// l.Infof("GetDevices: backed up by mongo-supported driver; dbname=%s.", mstore.DbFromContext(ctx, DbName))
	if db.client == nil {
		// l.Error("db client if nil. wow.")
	}
	if db.client.Database(mstore.DbFromContext(ctx, DbName)) == nil {
		// l.Errorf("db.client.Database(%s) if nil. wow.", mstore.DbFromContext(ctx, DbName))
	}

	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	queryFilters := make([]bson.M, 0)
	for _, filter := range q.Filters {
		op := mongoOperator(filter.Operator)
		field := fmt.Sprintf("%s.%s.%s", DbDevAttributes, filter.AttrName, DbDevAttributesValue)
		switch filter.Operator {
		default:
			if filter.ValueFloat != nil {
				queryFilters = append(queryFilters, bson.M{"$or": []bson.M{
					{field: bson.M{op: filter.Value}},
					{field: bson.M{op: filter.ValueFloat}},
				}})
			} else {
				queryFilters = append(queryFilters, bson.M{field: bson.M{op: filter.Value}})
			}
		}
	}
	findQuery := bson.M{}
	if len(queryFilters) > 0 {
		findQuery["$and"] = queryFilters
	}
	groupFilter := bson.M{}
	if q.GroupName != "" {
		groupFilter = bson.M{DbDevGroup: q.GroupName}
	}
	groupExistenceFilter := bson.M{}
	if q.HasGroup != nil {
		groupExistenceFilter = bson.M{DbDevGroup: bson.M{"$exists": *q.HasGroup}}
	}
	filter := bson.M{
		"$match": bson.M{
			"$and": []bson.M{
				groupFilter,
				groupExistenceFilter,
				findQuery,
			},
		},
	}

	// since the sorting step will have to be executable we have to use a noop here instead of just
	// an empty query object, as unsorted queries would fail otherwise
	sortQuery := bson.M{"$skip": 0}
	if q.Sort != nil {
		sortField := fmt.Sprintf("%s.%s.%s", DbDevAttributes, q.Sort.AttrName, DbDevAttributesValue)
		sortFieldQuery := bson.M{}
		sortFieldQuery[sortField] = 1
		if !q.Sort.Ascending {
			sortFieldQuery[sortField] = -1
		}
		sortQuery = bson.M{"$sort": sortFieldQuery}
	}
	limitQuery := bson.M{"$skip": 0}
	// exchange the limit query only if limit is set, as limits need to be positive in an aggregation pipeline
	if q.Limit > 0 {
		limitQuery = bson.M{"$limit": q.Limit}
	}
	combinedQuery := bson.M{
		"$facet": bson.M{
			"results": []bson.M{
				sortQuery,
				bson.M{"$skip": q.Skip},
				limitQuery,
			},
			"totalCount": []bson.M{
				bson.M{"$count": "count"},
			},
		},
	}
	resultMap := bson.M{
		"$project": bson.M{
			"results": 1,
			"totalCount": bson.M{
				"$ifNull": []interface{}{
					bson.M{
						"$arrayElemAt": []interface{}{"$totalCount.count", 0},
					},
					0,
				},
			},
		},
	}

	cursor, err := c.Aggregate(ctx, []bson.M{
		filter,
		combinedQuery,
		resultMap,
	})
	defer cursor.Close(ctx)

	cursor.Next(ctx)
	elem := &bson.D{}
	if err = cursor.Decode(elem); err != nil {
		return nil, -1, errors.Wrap(err, "failed to fetch device list")
	}
	m := elem.Map()
	count := m["totalCount"].(int32)
	results := m["results"].([]interface{})
	devices := make([]model.Device, len(results))
	for i, d := range results {
		var device model.Device
		bsonBytes, e := bson.Marshal(d.(bson.D))
		if e != nil {
			return nil, int(count), errors.Wrap(e, "failed to parse device in device list")
		}
		bson.Unmarshal(bsonBytes, &device)
		devices[i] = device
	}

	return devices, int(count), nil

	// // filter devices - skip, limit + get count afterwards
	// // followed by pretty printing
	// pipe := c.Pipe([]bson.M{
	// 	filter,
	// 	combinedQuery,
	// 	resultMap,
	// })

	// var res bson.M
	// err := pipe.One(&res)
	// if err != nil {
	// 	return nil, -1, errors.Wrap(err, "failed to fetch device list")
	// }
	// count := res["totalCount"].(int)
	// results := res["results"].([]interface{})
	// devices := make([]model.Device, len(results))
	// for i, d := range results {
	// 	var device model.Device
	// 	bsonBytes, e := bson.Marshal(d.(bson.M))
	// 	if e != nil {
	// 		return nil, count, errors.Wrap(e, "failed to parse device in device list")
	// 	}
	// 	bson.Unmarshal(bsonBytes, &device)
	// 	devices[i] = device
	// }
	// return devices, count, nil
}

func (db *DataStoreMongo) GetDevice(ctx context.Context, id model.DeviceID) (*model.Device, error) {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	res := model.Device{}

	result := c.FindOne(ctx, bson.M{DbDevId: id})
	if result == nil {
		return nil, errors.Wrap(result.Err(), "failed to fetch device")
	}

	elem := &bson.D{}
	err := result.Decode(elem)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		} else {
			// return nil, errors.Wrap(err, "failed to decode device data")
			return nil, errors.Wrap(err, "failed to fetch device")
		}
	}

	// m := elem.Map()
	// var device model.Device
	bsonBytes, e := bson.Marshal(elem) // .(bson.M))
	if e != nil {
		// return nil, errors.Wrap(e, "failed to get device data")
		return nil, errors.Wrap(e, "failed to fetch device")
	}
	bson.Unmarshal(bsonBytes, &res)
	// err := c.FindId(id).One(&res)

	// if err != nil {
	// 	if err == mgo.ErrNotFound {
	// 		return nil, nil
	// 	} else {
	// 		return nil, errors.Wrap(err, "failed to fetch device")
	// 	}
	// }

	return &res, nil
}

func (db *DataStoreMongo) AddDevice(ctx context.Context, dev *model.Device) error {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	// id, err := primitive.ObjectIDFromHex("5d100d9c23affb7006dd9cff")
	// if err != nil {
	// 	return errors.Wrap(err, "failed to store device")
	// }

	filter := bson.M{"_id": primitive.ObjectID(dev.ID)} // id} // dev.ID.String()} // id}
	update := makeAttrUpsert(dev.Attributes)
	now := time.Now()
	update["updated_ts"] = now
	update = bson.M{"$set": update,
		"$setOnInsert": bson.M{"created_ts": now}}
	l := log.FromContext(ctx)
	l.Infof("upserting: '%s'->'%s'.", filter, update)
	res, err := c.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true)) //this does not insert anything else than ID from model.Device
	if err != nil {
		return errors.Wrap(err, "failed to store device")
	}
	if res.ModifiedCount > 0 {
	} else {
		return errors.Wrap(err, "failed to store device")
	} // to check the update count

	// s := db.session.Copy()
	// defer s.Close()
	// c := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl)

	// update := makeAttrUpsert(dev.Attributes)
	// now := time.Now()
	// update["updated_ts"] = now
	// update = bson.M{"$set": update,
	// 	"$setOnInsert": bson.M{"created_ts": now}}

	// _, err := c.UpsertId(dev.ID, update)
	// if err != nil {
	// 	return errors.Wrap(err, "failed to store device")
	// }
	return nil
}

func (db *DataStoreMongo) UpsertAttributes(ctx context.Context, id model.DeviceID, attrs model.DeviceAttributes) error {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	// idDev, err := primitive.ObjectIDFromHex(id)
	// if err != nil {
	// 	return errors.Wrap(err, "failed to store device")
	// }

	filter := bson.M{"_id": id} // idDev}
	update := makeAttrUpsert(attrs)
	now := time.Now()
	update["updated_ts"] = now
	update = bson.M{"$set": update,
		"$setOnInsert": bson.M{"created_ts": now}}
	res, err := c.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
	if err != nil {
		return err
	}
	if res.ModifiedCount > 0 {
	} else {
	} // to check the update count

	// s := db.session.Copy()
	// defer s.Close()
	// c := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl)

	// update := makeAttrUpsert(attrs)

	// //set update time and optionally created time
	// now := time.Now()
	// update["updated_ts"] = now
	// update = bson.M{"$set": update,
	// 	"$setOnInsert": bson.M{"created_ts": now}}

	// _, err := c.UpsertId(id, update)

	// return err
	return nil
}

// prepare an attribute upsert doc based on DeviceAttributes map
func makeAttrUpsert(attrs model.DeviceAttributes) map[string]interface{} {
	var fieldName string
	upsert := map[string]interface{}{}

	for name, a := range attrs {
		if a.Description != nil {
			fieldName =
				fmt.Sprintf("%s.%s.%s", DbDevAttributes, name, DbDevAttributesDesc)
			upsert[fieldName] = a.Description

		}

		if a.Value != nil {
			fieldName =
				fmt.Sprintf("%s.%s.%s", DbDevAttributes, name, DbDevAttributesValue)
			upsert[fieldName] = a.Value
		}

		if a.Name != "" {
			fieldName =
				fmt.Sprintf("%s.%s.%s", DbDevAttributes, name, "name")
			upsert[fieldName] = a.Name
		}
	}

	return upsert
}

func mongoOperator(co store.ComparisonOperator) string {
	switch co {
	case store.Eq:
		return "$eq"
	case store.Regex:
		return "$regex"
	}
	return ""
}

func (db *DataStoreMongo) UnsetDeviceGroup(ctx context.Context, id model.DeviceID, groupName model.GroupName) error {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	filter := bson.M{
		"_id":   id,
		"group": groupName,
	}
	update := bson.M{
		"$unset": bson.M{
			"group": 1,
		},
	}

	res, err := c.UpdateMany(ctx, filter, update) //Update one or update many?
	if err != nil {
		return err
	}
	if res.ModifiedCount > 0 {
	} else {
	} // to check the update count

	// s := db.session.Copy()
	// defer s.Close()

	// query := bson.M{
	// 	"_id":   id,
	// 	"group": groupName,
	// }
	// update := mgo.Change{
	// 	Update: bson.M{
	// 		"$unset": bson.M{
	// 			"group": 1,
	// 		},
	// 	},
	// }
	// if _, err := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl).Find(query).Apply(update, nil); err != nil {
	// 	if err.Error() == mgo.ErrNotFound.Error() {
	// 		return store.ErrDevNotFound
	// 	}
	// 	return err
	// }
	return nil
}

func (db *DataStoreMongo) UpdateDeviceGroup(ctx context.Context, devId model.DeviceID, newGroup model.GroupName) error {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	filter := bson.M{
		"_id": devId,
	}
	update := bson.M{
		"$set": &model.Device{Group: newGroup}, //FIXME: why not just newGroup?
	}

	res, err := c.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}

	if res.ModifiedCount > 0 {
	} else {
	} // to check the update count

	// s := db.session.Copy()
	// defer s.Close()
	// c := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl)

	// err := c.UpdateId(devId, bson.M{"$set": &model.Device{Group: newGroup}})
	// if err != nil {
	// 	if err == mgo.ErrNotFound {
	// 		return store.ErrDevNotFound
	// 	}
	// 	return errors.Wrap(err, "failed to update device group")
	// }
	return nil
}

func (db *DataStoreMongo) ListGroups(ctx context.Context) ([]model.GroupName, error) {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	filter := bson.M{}
	results, err := c.Distinct(ctx, "group", filter)
	if err != nil {
		return nil, err
	}

	groups := make([]model.GroupName, len(results))
	for i, d := range results {
		groups[i] = model.GroupName(d.(string))
	}
	return groups, nil

	// s := db.session.Copy()
	// defer s.Close()
	// c := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl)

	// var groups []model.GroupName
	// err := c.Find(bson.M{}).Distinct("group", &groups)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "failed to list device groups")
	// }
	// return groups, nil
}

func (db *DataStoreMongo) GetDevicesByGroup(ctx context.Context, group model.GroupName, skip, limit int) ([]model.DeviceID, int, error) {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	filter := bson.M{DbDevGroup: group}
	result := c.FindOne(ctx, filter)
	if result == nil {
		return nil, -1, store.ErrGroupNotFound
	}

	element := &bson.D{}
	err := result.Decode(element)
	if err != nil {
		return nil, -1, errors.Wrap(err, "failed to fetch device")
	}

	dev := model.Device{}
	bsonBytes, e := bson.Marshal(element)
	if e != nil {
		return nil, -1, errors.Wrap(err, "failed to get a single device for group")
	}
	bson.Unmarshal(bsonBytes, &dev)

	hasGroup := group != ""
	devices, totalDevices, e := db.GetDevices(ctx,
		store.ListQuery{
			Skip:      skip,
			Limit:     limit,
			Filters:   nil,
			Sort:      nil,
			HasGroup:  &hasGroup,
			GroupName: string(group)})
	if e != nil {
		return nil, -1, errors.Wrap(e, "failed to get device list for group")
	}

	resIds := make([]model.DeviceID, len(devices))
	for i, d := range devices {
		resIds[i] = d.ID
	}
	return resIds, totalDevices, nil

	// s := db.session.Copy()
	// defer s.Close()
	// // compose aggregation pipeline
	// c := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl)

	// //first, find if the group exists at all, i.e. if any dev is assigned
	// var dev model.Device
	// filter := bson.M{DbDevGroup: group}
	// err := c.Find(filter).One(&dev)
	// if err != nil {
	// 	if err == mgo.ErrNotFound {
	// 		return nil, -1, store.ErrGroupNotFound
	// 	} else {
	// 		return nil, -1, errors.Wrap(err, "failed to get a single device for group")
	// 	}
	// }

	// hasGroup := group != ""
	// devices, totalDevices, e := db.GetDevices(ctx,
	// 	store.ListQuery{
	// 		Skip:      skip,
	// 		Limit:     limit,
	// 		Filters:   nil,
	// 		Sort:      nil,
	// 		HasGroup:  &hasGroup,
	// 		GroupName: string(group)})
	// if e != nil {
	// 	return nil, -1, errors.Wrap(e, "failed to get device list for group")
	// }

	// resIds := make([]model.DeviceID, len(devices))
	// for i, d := range devices {
	// 	resIds[i] = d.ID
	// }
	// return resIds, totalDevices, nil
	// return nil, 0, nil
}

func (db *DataStoreMongo) GetDeviceGroup(ctx context.Context, id model.DeviceID) (model.GroupName, error) {
	// l := log.FromContext(ctx)
	dev, err := db.GetDevice(ctx, id)
	if err != nil || dev == nil {
		return "", store.ErrDevNotFound
	}
	if err != nil || dev == nil {
		return "", errors.Wrap(err, "failed to get device")
	}
	// l.Infof("got dev: %v", dev)

	return dev.Group, nil

	// s := db.session.Copy()
	// defer s.Close()
	// c := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl)

	// var dev model.Device

	// err := c.FindId(id).Select(bson.M{"group": 1}).One(&dev)
	// if err != nil {
	// 	if err == mgo.ErrNotFound {
	// 		return "", store.ErrDevNotFound
	// 	} else {
	// 		return "", errors.Wrap(err, "failed to get device")
	// 	}
	// }

	// return dev.Group, nil
}

func (db *DataStoreMongo) DeleteDevice(ctx context.Context, id model.DeviceID) error {
	// l := log.FromContext(ctx)
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	filter := bson.M{DbDevId: id}
	result, err := c.DeleteOne(ctx, filter)
	if err != nil {
		// l.Infof("here0: err=" + err.Error())
		return err
	}
	if result.DeletedCount > 0 {
		// l.Infof("here1: results.DeletedCount=%d", result.DeletedCount)
	} else {
		// l.Infof("here2: returning error=%s", store.ErrDevNotFound.Error())
		return store.ErrDevNotFound
	} // to check the update count

	return nil
	// s := db.session.Copy()
	// defer s.Close()

	// if err := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl).RemoveId(id); err != nil {
	// 	if err.Error() == mgo.ErrNotFound.Error() {
	// 		return store.ErrDevNotFound
	// 	}
	// 	return err
	// }
}

func (db *DataStoreMongo) GetAllAttributeNames(ctx context.Context) ([]string, error) {
	c := db.client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)

	project := bson.M{
		"$project": bson.M{
			"arrayofkeyvalue": bson.M{
				"$objectToArray": "$$ROOT.attributes",
			},
		},
	}

	unwind := bson.M{
		"$unwind": "$arrayofkeyvalue",
	}

	group := bson.M{
		"$group": bson.M{
			"_id": nil,
			"allkeys": bson.M{
				"$addToSet": "$arrayofkeyvalue.k",
			},
		},
	}

	cursor, err := c.Aggregate(ctx, []bson.M{
		project,
		unwind,
		group,
	})
	defer cursor.Close(ctx)

	cursor.Next(ctx)
	elem := &bson.D{}
	if err = cursor.Decode(elem); err != nil {
		return nil, errors.Wrap(err, "failed to fetch device list")
	}
	m := elem.Map()
	results := m["allkeys"].([]interface{})
	attributeNames := make([]string, len(results))
	for i, d := range results {
		results[i] = d.(string)
	}

	return attributeNames, nil
	// s := db.session.Copy()
	// defer s.Close()

	// project := bson.M{
	// 	"$project": bson.M{
	// 		"arrayofkeyvalue": bson.M{
	// 			"$objectToArray": "$$ROOT.attributes",
	// 		},
	// 	},
	// }

	// unwind := bson.M{
	// 	"$unwind": "$arrayofkeyvalue",
	// }

	// group := bson.M{
	// 	"$group": bson.M{
	// 		"_id": nil,
	// 		"allkeys": bson.M{
	// 			"$addToSet": "$arrayofkeyvalue.k",
	// 		},
	// 	},
	// }

	// c := s.DB(mstore.DbFromContext(ctx, DbName)).C(DbDevicesColl)
	// pipe := c.Pipe([]bson.M{
	// 	project,
	// 	unwind,
	// 	group,
	// })

	// type Res struct {
	// 	AllKeys []string `bson:"allkeys"`
	// }

	// var res Res

	// err := pipe.One(&res)
	// switch err {
	// case nil, mgo.ErrNotFound:
	// 	return res.AllKeys, nil
	// default:
	// 	return nil, errors.Wrap(err, "failed to get attributes")
	// }
	return nil, nil
}

func (db *DataStoreMongo) MigrateTenant(ctx context.Context, version string, tenantId string) error {
	l := log.FromContext(ctx)

	database := mstore.DbNameForTenant(tenantId, DbName)

	l.Infof("migrating %s", database)

	m := migrate.SimpleMigrator{
		Session:     db.client,
		Db:          database,
		Automigrate: db.automigrate,
	}

	ver, err := migrate.NewVersion(version)
	if err != nil {
		return errors.Wrap(err, "failed to parse service version")
	}

	ctx = identity.WithContext(ctx, &identity.Identity{
		Tenant: tenantId,
	})

	migrations := []migrate.Migration{}

	err = m.Apply(ctx, *ver, migrations)
	if err != nil {
		return errors.Wrap(err, "failed to apply migrations")
	}

	return nil
}

func (db *DataStoreMongo) Migrate(ctx context.Context, version string) error {
	l := log.FromContext(ctx)

	dbs, err := migrate.GetTenantDbs(ctx, db.client, mstore.IsTenantDb(DbName))
	if err != nil {
		return errors.Wrap(err, "failed go retrieve tenant DBs")
	}
	l.Infof("(2) got dbs from GetTenantDbs '%v'/%d", dbs, len(dbs))

	if len(dbs) == 0 {
		dbs = []string{DbName}
	}
	l.Infof("(2) now dbs from GetTenantDbs '%v'/%d", dbs, len(dbs))

	if db.automigrate {
		l.Infof("automigrate is ON, will apply migrations")
	} else {
		l.Infof("automigrate is OFF, will check db version compatibility")
	}

	for _, d := range dbs {
		l.Infof("migrating %s", d)

		tenantId := mstore.TenantFromDbName(d, DbName)

		if err := db.MigrateTenant(ctx, version, tenantId); err != nil {
			return err
		}
	}
	return nil
}

// WithAutomigrate enables automatic migration and returns a new datastore based
// on current one
func (db *DataStoreMongo) WithAutomigrate() store.DataStore {
	return &DataStoreMongo{
		client:      db.client,
		automigrate: true,
	}
}

func indexAttr(s *mongo.Client, ctx context.Context, attr string) error {
	l := log.FromContext(ctx)
	c := s.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl)
	indexField := fmt.Sprintf("attributes.%s.values", attr)

	indexView := c.Indexes()
	cursor, err := indexView.List(ctx)
	//if index on indexField does not exist, then create:
	//indexView.CreateOne(ctx, mongo.IndexModel{Keys: bson.M{indexField: 1,}, Options: nil,})

	for cursor.Next(ctx) {
		index := bson.D{}
		if err := cursor.Decode(&index); err != nil {
			// fmt.Printf("cant decode index: %s\n",err.Error())
		} else {
			// fmt.Printf("index: %v\n",index)
		}
	}
	_, err = indexView.CreateOne(ctx, mongo.IndexModel{Keys: bson.M{indexField: 1}, Options: nil})

	// err := c.EnsureIndex(mgo.Index{
	// 	Key: []string{fmt.Sprintf("attributes.%s.values", attr)},
	// })

	if err != nil {
		if isTooManyIndexes(err) {
			l.Warnf("failed to index attr %s in db %s: too many indexes", attr, mstore.DbFromContext(ctx, DbName))
		} else {
			return errors.Wrapf(err, "failed to index attr %s in db %s", attr, mstore.DbFromContext(ctx, DbName))
		}
	}

	return nil
}

func isTooManyIndexes(e error) bool {
	return strings.HasPrefix(e.Error(), "add index fails, too many indexes for inventory.devices")
}
