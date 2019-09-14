// Copyright 2018 Northern.tech AS
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
package migrate

import (
	"context"
	"time"

	"github.com/mendersoftware/go-lib-micro/log"
	"github.com/mendersoftware/go-lib-micro/store"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// this is a small internal data layer for the migration utils, may be shared by diff migrators
const (
	DbMigrationsColl = "migration_info"
)

type MigrationEntry struct {
	Version   Version   `bson:"version"`
	Timestamp time.Time `bson:"timestamp"`
}

// GetMigrationInfo retrieves a list of migrations applied to the db.
func GetMigrationInfo(ctx context.Context, sess *mongo.Client, db string) ([]MigrationEntry, error) {
	l := log.FromContext(ctx)

	c := sess.Database(db).Collection(DbMigrationsColl)

	cursor, err := c.Find(ctx, bson.M{})
	if cursor == nil || err != nil {
		return nil, errors.Wrap(err, "db: failed to get migration info")
	}

	var infoArray []MigrationEntry

	for cursor.Next(ctx) {
		var info MigrationEntry
		element := bson.D{}
		err := cursor.Decode(&element)
		if err != nil {
			return nil, errors.Wrap(err, "db: failed to decode migration info")
		}
		bsonBytes, e := bson.Marshal(element) // .(bson.M))
		if e != nil {
			return nil, errors.Wrap(err, "failed to get bson bytes")
		}

		bson.Unmarshal(bsonBytes, &info)
		l.Infof("got info: '%v'", info)
		infoArray = append(infoArray, info)
	}

	return infoArray, nil

	// elem := &bson.D{}
	// err = result.Decode(elem)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "db: failed to decode migration info")
	// }

	// bsonBytes, e := bson.Marshal(elem) // .(bson.M))
	// if e != nil {
	// 	return nil, errors.Wrap(err, "failed to get bson bytes")
	// }

	// bson.Unmarshal(bsonBytes, &info)
	// l.Infof("returning migration info: '%v'", info)
	// return info, nil

	// s := sess.Copy()
	// defer s.Close()
	// c := s.DB(db).C(DbMigrationsColl)

	// var info []MigrationEntry

	// var err = c.Find(nil).All(&info)
	// if err != nil {
	// 	return nil, errors.Wrap(err, "db: failed to get migration info")
	// }

	// return info, nil
}

// UpdateMigrationInfo inserts a migration entry in the migration info collection.
func UpdateMigrationInfo(ctx context.Context, version Version, sess *mongo.Client, db string) error {
	c := sess.Database(db).Collection(DbMigrationsColl)

	entry := MigrationEntry{
		Version:   version,
		Timestamp: time.Now(),
	}
	_, err := c.InsertOne(ctx, entry)
	if err != nil {
		return errors.Wrap(err, "db: failed to insert migration info")
	}
	// result.InsertedID (InsertOneResult)

	return nil

	// if err != nil {
	// 	return errors.Wrap(err, "db: failed to insert migration info")
	// }

	// s := sess.Copy()
	// defer s.Close()
	// c := s.DB(db).C(DbMigrationsColl)

	// entry := MigrationEntry{
	// 	Version:   version,
	// 	Timestamp: time.Now(),
	// }

	// err := c.Insert(entry)
	// if err != nil {
	// 	return errors.Wrap(err, "db: failed to insert migration info")
	// }
}

func GetTenantDbs(ctx context.Context, client *mongo.Client, matcher store.TenantDbMatchFunc) ([]string, error) {
	l := log.FromContext(ctx)
	result, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	tenantDbs := make([]string, len(result))
	j := 0
	for _, db := range result {
		if matcher(db) {
			tenantDbs[j] = db
			l.Infof("(1)got db: '%s'", db)
			j++
		}
	}
	tenantDbs = tenantDbs[:j]

	l.Infof("(1)returing dbs:'%v'", tenantDbs)
	return tenantDbs, nil

	// s := sess.Copy()
	// defer s.Close()

	// dbs, err := s.DatabaseNames()
	// if err != nil {
	// 	return nil, err
	// }

	// tenantDbs := []string{}
	// for _, db := range dbs {
	// 	if matcher(db) {
	// 		tenantDbs = append(tenantDbs, db)
	// 	}
	// }

	// return tenantDbs, err
}
