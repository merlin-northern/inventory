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

package mongo_supported_test

import (
	"io/ioutil"
	"os"
	"testing"

	"log"

	// mgo "github.com/globalsign/mgo"

	"github.com/mendersoftware/go-lib-micro/mongo_supported/dbtest"
	// "go.mongodb.org/mongo-driver/bson"
	// "github.com/globalsign/mgo/bson"
)

var db *dbtest.DBServer

// Overwrites test execution and allows for test database setup
func TestMain(m *testing.M) {
	// log.Println("truet")
	// mgo.SetStats(true)
	// b := bson.M{}
	// if b != nil {

	// }
	dbdir, _ := ioutil.TempDir("/tmp", "dbsetup-test")
	// os.Exit would ignore defers, workaround
	status := func() int {
		// Start test database server
		if !testing.Short() {
			db = &dbtest.DBServer{}
			db.SetPath(dbdir)
			db.SetTimeout(64)
			_ = db.Session()
			// client, err := mongo.NewClient(options.Client().ApplyURI(uri))
			// err = client.Connect(ctx,clientOptions)
			// Tear down databaser server
			// Note:
			// if test panics, it will require manual database tier down
			// testing package executes tests in goroutines therefore
			// we can't catch panics issued in tests.
			log.Printf("mongo_supported: started mock mongo (mgo/dbtest)")
			defer os.RemoveAll(dbdir)
			defer db.Stop()
		}
		return m.Run()
	}()

	os.Exit(status)
}
