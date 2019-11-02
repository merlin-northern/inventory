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
package mongo_supported_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/mendersoftware/go-lib-micro/log"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/mendersoftware/inventory/model"
	"github.com/mendersoftware/inventory/store"
	. "github.com/mendersoftware/inventory/store/mongo_supported"

	"github.com/mendersoftware/go-lib-micro/identity"
	"github.com/mendersoftware/go-lib-micro/mongo_supported/migrate"
	mstore "github.com/mendersoftware/go-lib-micro/store"
	"github.com/pkg/errors"
)

// test funcs
func TestMongoGetDevices(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoGetDevices in short mode.")
	}

	inputDevs := []model.Device{
		{ID: "000000000000000000000000"},                              // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{ID: "010000000000000000000000", Group: model.GroupName("1")}, // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Group: model.GroupName("1")},
		{ID: "020000000000000000000000", Group: model.GroupName("2")}, // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Group: model.GroupName("2")},
		{
			ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			Attributes: map[string]model.DeviceAttribute{
				"attrString": {Name: "attrString", Value: "val3", Description: strPtr("desc1")},
				"attrFloat":  {Name: "attrFloat", Value: 3.0, Description: strPtr("desc2")},
			},
		},
		{
			ID: "040000000000000000000000", // [12]byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			Attributes: map[string]model.DeviceAttribute{
				"attrString": {Name: "attrString", Value: "val4", Description: strPtr("desc1")},
				"attrFloat":  {Name: "attrFloat", Value: 4.0, Description: strPtr("desc2")},
			},
		},
		{
			ID: "050000000000000000000000", // [12]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			Attributes: map[string]model.DeviceAttribute{
				"attrString": {Name: "attrString", Value: "val5", Description: strPtr("desc1")},
				"attrFloat":  {Name: "attrFloat", Value: 5.0, Description: strPtr("desc2")},
			},
			Group: model.GroupName("2"),
		},
		{
			ID: "060000000000000000000000", // [12]byte{6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			Attributes: map[string]model.DeviceAttribute{
				"attrString": {Name: "attrString", Value: "val6", Description: strPtr("desc1")},
				"attrFloat":  {Name: "attrFloat", Value: 4.0, Description: strPtr("desc2")},
			},
		},
		{
			ID: "070000000000000000000000", // [12]byte{7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			Attributes: map[string]model.DeviceAttribute{
				"attrString": {Name: "attrString", Value: "val4", Description: strPtr("desc1")},
				"attrFloat":  {Name: "attrFloat", Value: 6.0, Description: strPtr("desc2")},
			},
		},
	}
	floatVal4 := 4.0
	floatVal5 := 5.0

	testCases := map[string]struct {
		expected  []model.Device
		devTotal  int
		skip      int
		limit     int
		filters   []store.Filter
		sort      *store.Sort
		hasGroup  *bool
		groupName string
		tenant    string
	}{
		"get device from group 1": {
			expected:  []model.Device{inputDevs[1]},
			devTotal:  1,
			skip:      0,
			filters:   nil,
			sort:      nil,
			groupName: "1",
		},
		"all devs, no skip, no limit": {
			expected: inputDevs,
			devTotal: len(inputDevs),
			skip:     0,
			limit:    20,
			filters:  nil,
			sort:     nil,
		},
		"all devs, no skip, no limit; with tenant": {
			expected: inputDevs,
			devTotal: len(inputDevs),
			skip:     0,
			limit:    20,
			filters:  nil,
			sort:     nil,
			tenant:   "foo",
		},
		"all devs, with skip": {
			expected: []model.Device{inputDevs[4], inputDevs[5], inputDevs[6], inputDevs[7]},
			devTotal: len(inputDevs),
			skip:     4,
			limit:    20,
			filters:  nil,
			sort:     nil,
		},
		"all devs, no skip, with limit": {
			expected: []model.Device{inputDevs[0], inputDevs[1], inputDevs[2]},
			devTotal: len(inputDevs),
			skip:     0,
			limit:    3,
			filters:  nil,
			sort:     nil,
		},
		"skip + limit": {
			expected: []model.Device{inputDevs[3], inputDevs[4]},
			devTotal: len(inputDevs),
			skip:     3,
			limit:    2,
			filters:  nil,
			sort:     nil,
		},
		"filter on attribute (equal attribute)": {
			expected: []model.Device{inputDevs[3]},
			devTotal: 1,
			skip:     0,
			limit:    20,
			filters: []store.Filter{
				{
					AttrName: "attrString",
					Value:    "val3", Operator: store.Eq,
				},
			},
			sort: nil,
		},
		"filter on attribute (equal attribute float)": {
			expected: []model.Device{inputDevs[5]},
			devTotal: 1,
			skip:     0,
			limit:    20,
			filters: []store.Filter{
				{
					AttrName:   "attrFloat",
					Value:      "5.0",
					ValueFloat: &floatVal5,
					Operator:   store.Eq,
				},
			},
			sort: nil,
		},
		"filter on two attributes (equal)": {
			expected: []model.Device{inputDevs[4]},
			devTotal: 1,
			skip:     0,
			limit:    20,
			filters: []store.Filter{
				{
					AttrName: "attrString",
					Value:    "val4",
					Operator: store.Eq,
				},
				{
					AttrName:   "attrFloat",
					Value:      "4.0",
					ValueFloat: &floatVal4,
					Operator:   store.Eq,
				},
			},
			sort: nil,
		},
		"sort, limit": {
			expected: []model.Device{inputDevs[5], inputDevs[4], inputDevs[3]},
			devTotal: len(inputDevs),
			skip:     0,
			limit:    3,
			filters:  nil,
			sort: &store.Sort{
				AttrName:  "attrFloat",
				Ascending: false,
			},
		},
		"hasGroup = true": {
			expected: []model.Device{inputDevs[1], inputDevs[2], inputDevs[5]},
			devTotal: 3,
			skip:     0,
			limit:    20,
			filters:  nil,
			sort:     nil,
			hasGroup: boolPtr(true),
		},
		"hasGroup = false": {
			expected: []model.Device{inputDevs[0], inputDevs[3], inputDevs[4], inputDevs[6], inputDevs[7]},
			devTotal: 5,
			skip:     0,
			limit:    20,
			filters:  nil,
			sort:     nil,
			hasGroup: boolPtr(false),
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, tc := range testCases {
		t.Logf("test case: %s", name)

		// Make sure we start test with empty database
		db.Wipe()

		if tc.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: tc.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		for _, d := range inputDevs {
			t.Logf("inserting %s", d.ID)
			_, err := client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl).InsertOne(db.Ctx, d) // bson.M{ID: [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}) // bson.M{d})
			if err != nil {
				t.Logf("inserting %s got: '%s'", d.ID, err.Error())
			}
			assert.NoError(t, err, "failed to setup input data")
		}

		mongoStore := NewDataStoreMongoWithSession(client)

		//test
		devs, totalCount, err := mongoStore.GetDevices(db.Ctx,
			store.ListQuery{
				Skip:      tc.skip,
				Limit:     tc.limit,
				Filters:   tc.filters,
				Sort:      tc.sort,
				HasGroup:  tc.hasGroup,
				GroupName: tc.groupName})
		assert.NoError(t, err, "failed to get devices")

		assert.Equal(t, len(tc.expected), len(devs))
		assert.Equal(t, tc.devTotal, totalCount)
	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestMongoGetAllAttributeNames(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoGetAllAttributeNames in short mode.")
	}

	testCases := map[string]struct {
		inDevs []model.Device
		tenant string

		outAttrs []string
	}{
		"single dev": {
			inDevs: []model.Device{
				{
					ID: "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac_usb0":  {Value: "12:c6:bb:1a:f5:2c", Name: "mac_usb0"},
						"cpu_model": {Value: "ARMv7 Processor rev 10 (v7l)", Name: "cpu_model"},
						// "mac": {Name: "mac", Value: "foo", Description: strPtr("desc")},
						// "sn":  {Name: "sn", Value: "bar", Description: strPtr("desc")},
					},
				},
			},
			outAttrs: []string{"mac_usb0", "cpu_model"},
			// outAttrs: []string{"mac", "sn"},
		},
		"two devs, non-overlapping attrs": {
			inDevs: []model.Device{
				{
					ID: "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {Name: "mac", Value: "foo", Description: strPtr("desc")},
						"sn":  {Name: "sn", Value: "bar", Description: strPtr("desc")},
					},
				},
				{
					ID: "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"foo": {Name: "foo", Value: "foo", Description: strPtr("desc")},
						"bar": {Name: "bar", Value: "bar", Description: strPtr("desc")},
					},
				},
			},
			outAttrs: []string{"mac", "sn", "foo", "bar"},
		},
		"two devs, overlapping attrs": {
			inDevs: []model.Device{
				{
					ID: "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {Name: "mac", Value: "foo", Description: strPtr("desc")},
						"sn":  {Name: "sn", Value: "bar", Description: strPtr("desc")},
					},
				},
				{
					ID: "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {Name: "mac", Value: "foo", Description: strPtr("desc")},
						"foo": {Name: "foo", Value: "foo", Description: strPtr("desc")},
						"bar": {Name: "bar", Value: "bar", Description: strPtr("desc")},
					},
				},
			},
			outAttrs: []string{"mac", "sn", "foo", "bar"},
		},
		"single dev, tenant": {
			inDevs: []model.Device{
				{
					ID: "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {Name: "mac", Value: "foo", Description: strPtr("desc")},
						"sn":  {Name: "sn", Value: "bar", Description: strPtr("desc")},
					},
				},
			},
			outAttrs: []string{"mac", "sn"},
			tenant:   "tenant1",
		},
		"no devs": {
			outAttrs: []string{},
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 32*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, tc := range testCases {
		t.Logf("test case: %s", name)

		// Make sure we start test with empty database
		db.Wipe()

		if tc.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: tc.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		for _, d := range tc.inDevs {
			_, err := client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl).InsertOne(db.Ctx, d)
			assert.NoError(t, err, "failed to setup input data")
		}

		mongoStore := NewDataStoreMongoWithSession(client)

		//test
		names, err := mongoStore.GetAllAttributeNames(db.Ctx)
		if err != nil {
			t.Logf("got error: '%s'", err.Error())
		}
		assert.NoError(t, err, "failed to get devices")

		assert.ElementsMatch(t, tc.outAttrs, names)
	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestMongoGetDevice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoGetDevice in short mode.")
	}

	testCases := map[string]struct {
		InputID     model.DeviceID
		InputDevice *model.Device
		tenant      string
		OutputError error
	}{
		"no device and no ID given": {
			InputID:     model.NilDeviceID, // primitive.NilObjectID, //nil,
			InputDevice: nil,
		},
		"no device and no ID given; with tenant": {
			InputID:     model.NilDeviceID, // primitive.NilObjectID, //nil,
			InputDevice: nil,
			tenant:      "foo",
		},
		"device with given ID not exists": {
			InputID:     "7d0000000000000000000000", // model.DeviceID([12]byte{123, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			InputDevice: nil,
		},
		"device with given ID not exists; with tenant": {
			InputID:     "7d0000000000000000000000", // model.DeviceID([12]byte{123, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			InputDevice: nil,
			tenant:      "foo",
		},
		"device with given ID exists, no error": {
			InputID: "020000000000000000000000", // model.DeviceID([12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			InputDevice: &model.Device{
				ID: "020000000000000000000000", // model.DeviceID([12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
				},
			},
		},
		"device with given ID exists, no error; with tenant": {
			InputID: "020000000000000000000000", // model.DeviceID([12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
			InputDevice: &model.Device{
				ID: "020000000000000000000000", // model.DeviceID([12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
				},
			},
			tenant: "foo",
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 32*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, testCase := range testCases {
		t.Logf("test case: %s", name)

		// Make sure we start test with empty database
		db.Wipe()

		store := NewDataStoreMongoWithSession(client)
		// _, err := client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl).DeleteMany(db.Ctx, bson.M{})

		if testCase.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: testCase.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		if testCase.InputDevice != nil {
			existingID := "020000000000000000000000" // primitive.ObjectIDFromHex("020000000000000000000000")

			inputDevice := model.Device{
				ID: model.DeviceID(existingID),
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0200-mac"},
				},
			}
			t.Logf("db:%s InsertOne(%s)", mstore.DbFromContext(db.Ctx, DbName), testCase.InputDevice.ID)
			client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl).InsertOne(db.Ctx, inputDevice)
		}

		t.Logf("calling db:%s GetDevice(%s)", mstore.DbFromContext(db.Ctx, DbName), testCase.InputID)
		dbdev, err := store.GetDevice(db.Ctx, testCase.InputID)
		if dbdev != nil {
			t.Logf("got device with id %s GetDevice", dbdev.ID)
		} else {
			t.Logf("got nil device from GetDevice")
		}

		if testCase.InputDevice != nil {
			assert.NotNil(t, dbdev, "expected to device of ID %s to be found", testCase.InputDevice.ID)
			assert.Equal(t, testCase.InputID, dbdev.ID)
		} else {
			assert.Nil(t, dbdev, "expected no device to be found")
		}

		assert.NoError(t, err, "expected no error")

	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestMongoAddDevice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoAddDevice in short mode.")
	}

	// existing := bson.M{
	// 	// "ID": primitive.NewObjectID(),
	// 	"attributes": model.DeviceAttributes{
	// 		"mac": {Name: "mac", Value: "0000-mac"},
	// 		"sn":  {Name: "sn", Value: "0000-sn"},
	// 	},
	// }
	existingID := "000000000000000000000000" // primitive.ObjectIDFromHex("000000000000000000000000")

	existing := model.Device{
		ID: model.DeviceID(existingID),
		Attributes: model.DeviceAttributes{
			"mac": {Name: "mac", Value: "0000-mac"},
			"sn":  {Name: "sn", Value: "0000-sn"},
		},
	}

	testCases := map[string]struct {
		InputDevice  *model.Device
		OutputDevice *model.Device
		tenant       string
		OutputError  error
	}{
		"valid device with one attribute, no error": {
			InputDevice: &model.Device{
				ID: "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
				},
			},
			OutputDevice: &model.Device{
				ID: "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
				},
			},
			OutputError: nil,
		},
		"valid device with one attribute, no error; with tenant": {
			InputDevice: &model.Device{
				ID: "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
				},
			},
			OutputDevice: &model.Device{
				ID: "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
				},
			},
			tenant:      "foo",
			OutputError: nil,
		},
		"valid device with two attributes, no error": {
			InputDevice: &model.Device{
				ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
					"sn":  {Name: "sn", Value: "0002-sn"},
				},
			},
			OutputDevice: &model.Device{
				ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0002-mac"},
					"sn":  {Name: "sn", Value: "0002-sn"},
				},
			},
			OutputError: nil,
		},
		"valid device with attribute without value, no error": {
			InputDevice: &model.Device{
				ID: "040000000000000000000000", // [12]byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac"},
				},
			},
			OutputDevice: &model.Device{
				ID: "040000000000000000000000", // [12]byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac"},
				},
			},
			OutputError: nil,
		},
		"valid device with array in attribute value, no error": {
			InputDevice: &model.Device{
				ID: "050000000000000000000000", //  [12]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: []interface{}{int32(123), int32(456)}},
				},
			},
			OutputDevice: &model.Device{
				ID: "050000000000000000000000", // [12]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: []interface{}{int32(123), int32(456)}},
				},
			},
			OutputError: nil,
		},
		"valid device without attributes, no error": {
			InputDevice: &model.Device{
				ID: "070000000000000000000000", // [12]byte{7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac"},
				},
			},
			OutputDevice: &model.Device{
				ID: "070000000000000000000000", // [12]byte{7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac"},
				},
			},
			OutputError: nil,
		},
		"valid device with upsert, all attrs updated, no error": {
			InputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0000-mac-new"},
					"sn":  {Name: "sn", Value: "0000-sn-new"},
				},
			},
			OutputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0000-mac-new"},
					"sn":  {Name: "sn", Value: "0000-sn-new"},
				},
			},
			OutputError: nil,
		},
		"valid device with upsert, one attr updated, no error": {
			InputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0000-mac-new"},
				},
			},
			OutputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"mac": {Name: "mac", Value: "0000-mac-new"},
					"sn":  {Name: "sn", Value: "0000-sn"},
				},
			},
			OutputError: nil,
		},
		"valid device with upsert, no attrs updated, new upserted, no error": {
			InputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"other-param": {Name: "other-param", Value: "other-param-value"},
				},
			},
			OutputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"other-param": {Name: "other-param", Value: "other-param-value"},
					"mac":         {Name: "mac", Value: "0000-mac"},
					"sn":          {Name: "sn", Value: "0000-sn"},
				},
			},
			OutputError: nil,
		},
		"valid device with upsert, no attrs updated, many new upserted, no error": {
			InputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"other-param":   {Name: "other-param", Value: "other-param-value"},
					"other-param-2": {Name: "other-param-2", Value: "other-param-2-value"},
				},
			},
			OutputDevice: &model.Device{
				ID: "000000000000000000000000", // [12]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Attributes: model.DeviceAttributes{
					"other-param":   {Name: "other-param", Value: "other-param-value"},
					"other-param-2": {Name: "other-param-2", Value: "other-param-2-value"},
					"mac":           {Name: "mac", Value: "0000-mac"},
					"sn":            {Name: "sn", Value: "0000-sn"},
				},
			},
			OutputError: nil,
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 128*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, testCase := range testCases {
		t.Logf("test case: %s", name)

		// Make sure we start test with empty database
		db.Wipe()

		store := NewDataStoreMongoWithSession(client)

		if testCase.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: testCase.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		c := client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl)
		result, err := c.InsertOne(db.Ctx, existing)
		assert.NoError(t, err)
		objectID := result.InsertedID.(string)
		// testCase.InputDevice.ID = model.DeviceID(objectID)
		// testCase.OutputDevice.ID = model.DeviceID(objectID)
		// for i := 0; i < len(objectID); i++ {
		// 	testCase.InputDevice.ID[i] = objectID[i]
		// 	testCase.OutputDevice.ID[i] = objectID[i]
		// }

		l := log.FromContext(db.Ctx)
		l.Infof("inserted id=%s calling AddDevice with device=%s.",
			objectID, testCase.InputDevice)
		err = store.AddDevice(db.Ctx, testCase.InputDevice)

		if testCase.OutputError != nil {
			assert.EqualError(t, err, testCase.OutputError.Error())
		} else {
			assert.NoError(t, err, "expected no error inserting to data store")

			var dbdev model.Device
			devsColl := client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl)
			t.Logf("looking for: '%s'", model.DeviceID(testCase.InputDevice.ID))

			cursor, e := devsColl.Find(db.Ctx, bson.M{DbDevId: model.DeviceID(testCase.InputDevice.ID)})
			if e != nil {
				t.Logf("and here now this is an error: '%s'.", e.Error())
			}
			count := 0
			l := log.FromContext(db.Ctx)
			cursor.Next(db.Ctx)
			elem := &bson.D{}
			if err = cursor.Decode(elem); err != nil {
			}
			m := elem.Map()
			d := model.Device{}
			d.ID = model.DeviceID(m["_id"].(string)) // .(primitive.ObjectID))
			// d.Attributes = m["Attributes"].(map[string]model.DeviceAttribute)

			attributes := m["attributes"].(interface{})
			var attrs model.DeviceAttributes
			bsonBytes, e := bson.Marshal(attributes.(bson.D))
			if e != nil {
			}
			bson.Unmarshal(bsonBytes, &attrs)
			l.Infof("unmarshaled via map: '%s'.", attrs)
			d.Attributes = attrs
			dbdev = d

			l.Infof(" by Map here is found: %s", d)
			// for cursor.Next(ctx) {
			// 	var d model.Device
			// 	cursor.Decode(&d)
			// 	l.Infof("   here is id found: %s", d)
			// 	count++
			// }
			t.Logf("and found %d by '%s'", count, objectID)

			// result := devsColl.FindOne(ctx, bson.M{DbDevId: objectID}) // objectID.String()}) // testCase.InputDevice.ID.String()})
			// if err = result.Err(); err != nil {
			// 	t.Logf("now this is an error: '%s'.", err.Error())
			// }
			// result.Decode(&dbdev)

			assert.NoError(t, err, "expected no error")

			compareDevsWithoutTimestamps(t, testCase.OutputDevice, &dbdev)
		}

	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func compareDevsWithoutTimestamps(t *testing.T, expected, actual *model.Device) {
	assert.Equal(t, expected.ID, actual.ID)
	assert.Equal(t, expected.Attributes, actual.Attributes)
	assert.Equal(t, expected.Group, actual.Group)
}

func TestNewDataStoreMongo(t *testing.T) {
	// if testing.Short() {
	t.Skip("skipping TestNewDataStoreMongo in short mode.")
	// }

	ds, err := NewDataStoreMongo(DataStoreMongoConfig{ConnectionString: "illegal url"}, nil)

	assert.Nil(t, ds)
	assert.EqualError(t, err, "failed to open mongo session: no reachable servers")
}

func TestMongoUpsertAttributes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoUpsertAttributes in short mode.")
	}

	//single create timestamp for all inserted devs
	createdTs := time.Now()

	testCases := map[string]struct {
		devs []model.Device

		inDevId model.DeviceID
		inAttrs model.DeviceAttributes

		tenant string

		outAttrs model.DeviceAttributes
	}{
		"dev exists, attributes exist, update both attrs (descr + val)": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("mac description"),
					Value:       "0003-newmac",
				},
				"sn": {
					Description: strPtr("sn description"),
					Value:       "0003-newsn",
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("mac description"),
					Value:       "0003-newmac",
				},
				"sn": {
					Description: strPtr("sn description"),
					Value:       "0003-newsn",
				},
			},
		},
		"dev exists, attributes exist, update both attrs (descr + val); with tenant": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("mac description"),
					Value:       "0003-newmac",
				},
				"sn": {
					Description: strPtr("sn description"),
					Value:       "0003-newsn",
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("mac description"),
					Value:       "0003-newmac",
				},
				"sn": {
					Description: strPtr("sn description"),
					Value:       "0003-newsn",
				},
			},
		},
		"dev exists, attributes exist, update one attr (descr + val)": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"sn": {
					Description: strPtr("sn description"),
					Value:       "0003-newsn",
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("descr"),
					Value:       "0003-mac",
				},
				"sn": {
					Description: strPtr("sn description"),
					Value:       "0003-newsn",
				},
			},
		},

		"dev exists, attributes exist, update one attr (descr only)": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"sn": {
					Description: strPtr("sn description"),
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("descr"),
					Value:       "0003-mac",
				},
				"sn": {
					Description: strPtr("sn description"),
					Value:       "0003-sn",
				},
			},
		},
		"dev exists, attributes exist, update one attr (value only)": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"sn": {
					Value: "0003-newsn",
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("descr"),
					Value:       "0003-mac",
				},
				"sn": {
					Description: strPtr("descr"),
					Value:       "0003-newsn",
				},
			},
		},
		"dev exists, attributes exist, update one attr (value only, change type)": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"sn": {
					Value: []string{"0003-sn-1", "0003-sn-2"},
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("descr"),
					Value:       "0003-mac",
				},
				"sn": {
					Description: strPtr("descr"),
					//[]interface{} instead of []string - otherwise DeepEquals fails where it really shouldn't
					Value: []interface{}{"0003-sn-1", "0003-sn-2"},
				},
			},
		},
		"dev exists, attributes exist, add(merge) new attrs": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"new-1": {
					Name:  "new-1",
					Value: []string{"new-1-0", "new-1-0"},
				},
				"new-2": {
					Name:        "new-2",
					Value:       "new-2-val",
					Description: strPtr("foo"),
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Description: strPtr("descr"),
					Value:       "0003-mac",
				},
				"sn": {
					Name:        "sn",
					Value:       "0003-sn",
					Description: strPtr("descr"),
				},
				"new-1": {
					Name:  "new-1",
					Value: []interface{}{"new-1-0", "new-1-0"},
				},
				"new-2": {
					Name:        "new-2",
					Value:       "new-2-val",
					Description: strPtr("foo"),
				},
			},
		},
		"dev exists, attributes exist, add(merge) new attrs + modify existing": {
			devs: []model.Device{
				{
					ID: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Attributes: map[string]model.DeviceAttribute{
						"mac": {
							Name:        "mac",
							Value:       "0003-mac",
							Description: strPtr("descr"),
						},
						"sn": {
							Name:        "sn",
							Value:       "0003-sn",
							Description: strPtr("descr"),
						},
					},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Name:        "mac",
					Value:       "0003-mac-new",
					Description: strPtr("descr-new"),
				},
				"new-1": {
					Name:  "new-1",
					Value: []string{"new-1-0", "new-1-0"},
				},
				"new-2": {
					Name:        "new-2",
					Value:       "new-2-val",
					Description: strPtr("foo"),
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"mac": {
					Name:        "mac",
					Value:       "0003-mac-new",
					Description: strPtr("descr-new"),
				},
				"sn": {
					Name:        "sn",
					Value:       "0003-sn",
					Description: strPtr("descr"),
				},
				"new-1": {
					Name:  "new-1",
					Value: []interface{}{"new-1-0", "new-1-0"},
				},
				"new-2": {
					Name:        "new-2",
					Value:       "new-2-val",
					Description: strPtr("foo"),
				},
			},
		},
		"dev exists, no attributes exist, upsert new attrs (val + descr)": {
			devs: []model.Device{
				{
					ID:        "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					CreatedTs: createdTs,
				},
			},
			inDevId: "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Value:       []string{"1.2.3.4", "1.2.3.5"},
					Description: strPtr("ip addr array"),
				},
				"mac": {
					Value:       "0006-mac",
					Description: strPtr("mac addr"),
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Value:       []interface{}{"1.2.3.4", "1.2.3.5"},
					Description: strPtr("ip addr array"),
				},
				"mac": {
					Value:       "0006-mac",
					Description: strPtr("mac addr"),
				},
			},
		},
		"dev doesn't exist, upsert new attr (descr + val)": {
			devs:    []model.Device{},
			inDevId: "630000000000000000000000", // [12]byte{99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Description: strPtr("ip addr array"),
					Value:       []string{"1.2.3.4", "1.2.3.5"},
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Description: strPtr("ip addr array"),
					Value:       []interface{}{"1.2.3.4", "1.2.3.5"},
				},
			},
		},
		"dev doesn't exist, upsert new attr (val only)": {
			devs:    []model.Device{},
			inDevId: "630000000000000000000000", // [12]byte{99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Value: []string{"1.2.3.4", "1.2.3.5"},
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Value: []interface{}{"1.2.3.4", "1.2.3.5"},
				},
			},
		},
		"dev doesn't exist, upsert with new attrs (val + descr)": {
			inDevId: "990000000000000000000000", // [12]byte{99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			inAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Value:       []string{"1.2.3.4", "1.2.3.5"},
					Description: strPtr("ip addr array"),
				},
				"mac": {
					Value:       "0099-mac",
					Description: strPtr("mac addr"),
				},
			},

			outAttrs: map[string]model.DeviceAttribute{
				"ip": {
					Value:       []interface{}{"1.2.3.4", "1.2.3.5"},
					Description: strPtr("ip addr array"),
				},
				"mac": {
					Value:       "0099-mac",
					Description: strPtr("mac addr"),
				},
			},
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, tc := range testCases {

		t.Logf("%s", name)
		//setup
		db.Wipe()
		// _, err := client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl).DeleteMany(db.Ctx, bson.M{})

		if tc.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: tc.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		for _, d := range tc.devs {
			_, err := client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl).InsertOne(db.Ctx, d)
			assert.NoError(t, err, "failed to setup input data")
		}

		//test
		d := NewDataStoreMongoWithSession(client)

		err := d.UpsertAttributes(db.Ctx, tc.inDevId, tc.inAttrs)
		assert.NoError(t, err, "UpsertAttributes failed")

		//get the device back
		var dev model.Device
		result := client.Database(DbName).Collection(DbDevicesColl).FindOne(db.Ctx, bson.M{DbDevId: tc.inDevId})
		elem := &bson.D{}
		err = result.Decode(elem)
		bsonBytes, _ := bson.Marshal(elem) // .(bson.M))
		bson.Unmarshal(bsonBytes, &dev)
		assert.NoError(t, err, "error getting device")

		if !compare(dev.Attributes, tc.outAttrs) {
			t.Errorf("attributes mismatch, have: %v\nwant: %v", dev.Attributes, tc.outAttrs)
		}

		//check timestamp validity
		//note that mongo stores time with lower precision- custom comparison
		assert.Condition(t,
			func() bool {
				return dev.UpdatedTs.After(dev.CreatedTs) ||
					dev.UpdatedTs.Unix() == dev.CreatedTs.Unix()
			})
	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestMongoUpdateDeviceGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoUpdateDeviceGroup in short mode.")
	}

	deviceID := "020000000000000000000000" // , _ := primitive.ObjectIDFromHex("020000000000000000000000")
	testCases := map[string]struct {
		InputDeviceID  model.DeviceID
		InputGroupName model.GroupName
		InputDevice    *model.Device
		tenant         string
		OutputError    error
	}{
		"update group for device with empty device id": {
			InputDeviceID:  model.DeviceID(""),
			InputGroupName: model.GroupName("abc"),
			InputDevice:    nil,
			OutputError:    store.ErrDevNotFound,
		},
		"update group for device with empty device id; with tenant": {
			InputDeviceID:  model.DeviceID(""),
			InputGroupName: model.GroupName("abc"),
			InputDevice:    nil,
			tenant:         "foo",
			OutputError:    store.ErrDevNotFound,
		},
		"update group for device, device not found": {
			InputDeviceID:  "020000000000000000000000",
			InputGroupName: model.GroupName("abc"),
			InputDevice:    nil,
			OutputError:    store.ErrDevNotFound,
		},
		"update group for device, group exists": {
			InputDeviceID:  model.DeviceID(deviceID), //model.NilDeviceID, // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			InputGroupName: model.GroupName("abc"),
			InputDevice: &model.Device{
				ID:    model.DeviceID(deviceID), // model.NilDeviceID, // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Group: model.GroupName("def"),
			},
		},
		"update group for device, group exists; with tenant": {
			InputDeviceID:  "010000000000000000000000",
			InputGroupName: model.GroupName("abc"),
			InputDevice: &model.Device{
				ID:    "010000000000000000000000",
				Group: model.GroupName("def"),
			},
			tenant: "foo",
		},
		"update group for device, group does not exist": {
			InputDeviceID:  "010000000000000000000000",
			InputGroupName: model.GroupName("abc"),
			InputDevice: &model.Device{
				ID:    "010000000000000000000000",
				Group: model.GroupName(""),
			},
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 32*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, testCase := range testCases {
		t.Logf("test case: %s", name)

		// Make sure we start test with empty database
		db.Wipe()

		store := NewDataStoreMongoWithSession(client)
		// _, err := client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl).DeleteMany(db.Ctx, bson.M{})

		if testCase.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: testCase.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		if testCase.InputDevice != nil {
			client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl).InsertOne(db.Ctx, testCase.InputDevice)
		}

		err := store.UpdateDeviceGroup(db.Ctx, testCase.InputDeviceID, testCase.InputGroupName)
		if testCase.OutputError != nil {
			assert.Error(t, err, "expected error")

			assert.EqualError(t, err, testCase.OutputError.Error())
		} else {
			assert.NoError(t, err, "expected no error")

			groupsColl := client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl)
			cursor, err := groupsColl.Find(db.Ctx, bson.M{"group": model.GroupName("abc")})
			assert.NoError(t, err, "expected no error")

			count := 0
			for cursor.Next(db.Ctx) {
				count++
			}
			assert.Equal(t, 1, count)
		}

	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func strPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func compare(a, b model.DeviceAttributes) bool {
	if len(a) != len(b) {
		return false
	}

	for k, va := range a {
		vb := b[k]

		if !reflect.DeepEqual(va.Value, vb.Value) {
			return false
		}

		if !reflect.DeepEqual(va.Description, vb.Description) {
			return false
		}
	}

	return true
}

func TestMongoUnsetDevicesGroupWithGroupName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoUnsetDevicesGroupWithmodel.GroupName in short mode.")
	}

	testCases := map[string]struct {
		InputDeviceID  model.DeviceID
		InputGroupName model.GroupName
		InputDevice    *model.Device
		tenant         string
		OutputError    error
	}{
		"unset group for device with group id, device not found": {
			InputDeviceID:  "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			InputGroupName: model.GroupName("e16c71ec"),
			InputDevice:    nil,
			OutputError:    store.ErrDevNotFound,
		},
		"unset group for device with group id, device not found; with tenant": {
			InputDeviceID:  "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			InputGroupName: model.GroupName("e16c71ec"),
			InputDevice:    nil,
			tenant:         "foo",
			OutputError:    store.ErrDevNotFound,
		},
		"unset group for device, ok": {
			InputDeviceID:  "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			InputGroupName: model.GroupName("e16c71ec"),
			InputDevice: &model.Device{
				ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Group: model.GroupName("e16c71ec"),
			},
		},
		"unset group for device, ok; with tenant": {
			InputDeviceID:  "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			InputGroupName: model.GroupName("e16c71ec"),
			InputDevice: &model.Device{
				ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Group: model.GroupName("e16c71ec"),
			},
			tenant: "foo",
		},
		"unset group for device with incorrect group name provided": {
			InputDeviceID:  "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			InputGroupName: model.GroupName("other-group-name"),
			InputDevice: &model.Device{
				ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Group: model.GroupName("e16c71ec"),
			},
			OutputError: store.ErrDevNotFound,
		},
		"unset group for device with incorrect group name provided; with tenant": {
			InputDeviceID:  "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			InputGroupName: model.GroupName("other-group-name"),
			InputDevice: &model.Device{
				ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Group: model.GroupName("e16c71ec"),
			},
			tenant:      "foo",
			OutputError: store.ErrDevNotFound,
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, testCase := range testCases {
		t.Logf("test case: %s", name)

		// Make sure we start test with empty database
		db.Wipe()

		store := NewDataStoreMongoWithSession(client)
		// _, err := client.Database(mstore.DbFromContext(ctx, DbName)).Collection(DbDevicesColl).DeleteMany(db.Ctx, bson.M{})
		// _ = client.Database(mstore.DbFromContext(ctx, DbName)).Drop(db.Ctx)

		if testCase.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: testCase.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		if testCase.InputDevice != nil {
			client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl).InsertOne(db.Ctx, testCase.InputDevice)
		}

		err := store.UnsetDeviceGroup(db.Ctx, testCase.InputDeviceID, testCase.InputGroupName)
		if testCase.OutputError != nil {
			assert.Error(t, err, "expected error")

			assert.EqualError(t, err, testCase.OutputError.Error())
		} else {
			assert.NoError(t, err, "expected no error")

			groupsColl := client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl)
			cursor, err := groupsColl.Find(db.Ctx, bson.M{"group": model.GroupName("e16c71ec")})
			assert.NoError(t, err, "expected no error")

			count := 0
			for cursor.Next(db.Ctx) {
				count++
			}
			assert.Equal(t, 0, count)
		}

	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestMongoListGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoListGroups in short mode.")
	}

	testCases := map[string]struct {
		InputDevices []model.Device
		tenant       string
		OutputGroups []model.GroupName
	}{
		"groups foo, bar": {
			InputDevices: []model.Device{
				{
					ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("foo"),
				},
				{
					ID:    "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("foo"),
				},
				{
					ID:    "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("foo"),
				},
				{
					ID:    "040000000000000000000000", // [12]byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("bar"),
				},
				{
					ID:    "050000000000000000000000", // [12]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
			},
			OutputGroups: []model.GroupName{"foo", "bar"},
		},
		"groups foo, bar; with tenant": {
			InputDevices: []model.Device{
				{
					ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("foo"),
				},
				{
					ID:    "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("foo"),
				},
				{
					ID:    "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("foo"),
				},
				{
					ID:    "040000000000000000000000", // [12]byte{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName("bar"),
				},
				{
					ID:    "050000000000000000000000", // [12]byte{5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
			},
			tenant:       "foo",
			OutputGroups: []model.GroupName{"foo", "bar"},
		},
		"no groups": {
			InputDevices: []model.Device{
				{
					ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
				{
					ID:    "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
				{
					ID:    "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
			},
			OutputGroups: []model.GroupName{},
		},
		"no groups; with tenant": {
			InputDevices: []model.Device{
				{
					ID:    "010000000000000000000000", // [12]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
				{
					ID:    "020000000000000000000000", // [12]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
				{
					ID:    "030000000000000000000000", // [12]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
					Group: model.GroupName(""),
				},
			},
			tenant:       "foo",
			OutputGroups: []model.GroupName{},
		},
	}

	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	client := db.Session()
	for name, testCase := range testCases {
		t.Logf("test case: %s", name)

		db.Wipe()

		if testCase.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: testCase.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		for _, d := range testCase.InputDevices {
			client.Database(mstore.DbFromContext(db.Ctx, DbName)).Collection(DbDevicesColl).InsertOne(db.Ctx, d)
		}

		// Make sure we start test with empty database
		store := NewDataStoreMongoWithSession(client)

		groups, err := store.ListGroups(db.Ctx)
		assert.NoError(t, err, "expected no error")

		t.Logf("groups: %v", groups)
		if testCase.OutputGroups != nil {
			assert.Len(t, groups, len(testCase.OutputGroups))
			for _, eg := range testCase.OutputGroups {
				assert.Contains(t, groups, eg)
			}
		} else {
			assert.Len(t, groups, 0)
		}

	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestGetDevicesByGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestGetDevicesByGroup in short mode.")
	}

	devDevices := []model.Device{
		{
			ID:    "010000000000000000000000",
			Group: model.GroupName("dev"),
		},
		{
			ID:    "060000000000000000000000",
			Group: model.GroupName("dev"),
		},
		{
			ID:    "080000000000000000000000",
			Group: model.GroupName("dev"),
		},
	}
	prodDevices := []model.Device{
		{
			ID:    "020000000000000000000000",
			Group: model.GroupName("prod"),
		},
		{
			ID:    "040000000000000000000000",
			Group: model.GroupName("prod"),
		},
		{
			ID:    "050000000000000000000000",
			Group: model.GroupName("prod"),
		},
	}
	testDevices := []model.Device{
		{
			ID:    "030000000000000000000000",
			Group: model.GroupName("test"),
		},
		{
			ID:    "070000000000000000000000",
			Group: model.GroupName("test"),
		},
	}

	inputDevices := make([]model.Device, 0, len(devDevices)+len(prodDevices)+len(testDevices))
	inputDevices = append(inputDevices, devDevices...)
	inputDevices = append(inputDevices, prodDevices...)
	inputDevices = append(inputDevices, testDevices...)

	testCases := map[string]struct {
		InputGroupName    model.GroupName
		InputSkip         int
		InputLimit        int
		OutputDevices     []model.DeviceID
		OutputDeviceCount int
		OutputError       error
	}{
		"no skip, no limit": {
			InputGroupName: "dev",
			InputSkip:      0,
			InputLimit:     0,
			OutputDevices: []model.DeviceID{
				"010000000000000000000000",
				"060000000000000000000000",
				"080000000000000000000000",
			},
			OutputDeviceCount: len(devDevices),
			OutputError:       nil,
		},
		"no skip, limit": {
			InputGroupName: "prod",
			InputSkip:      0,
			InputLimit:     2,
			OutputDevices: []model.DeviceID{
				"020000000000000000000000",
				"040000000000000000000000",
			},
			OutputDeviceCount: len(prodDevices),
			OutputError:       nil,
		},
		"skip, no limit": {
			InputGroupName: "dev",
			InputSkip:      2,
			InputLimit:     0,
			OutputDevices: []model.DeviceID{
				"080000000000000000000000",
			},
			OutputDeviceCount: len(devDevices),
			OutputError:       nil,
		},
		"skip + limit": {
			InputGroupName: "prod",
			InputSkip:      1,
			InputLimit:     1,
			OutputDevices: []model.DeviceID{
				"040000000000000000000000",
			},
			OutputDeviceCount: len(prodDevices),
			OutputError:       nil,
		},
		"no results (past last page)": {
			InputGroupName:    "dev",
			InputSkip:         10,
			InputLimit:        1,
			OutputDevices:     []model.DeviceID{},
			OutputDeviceCount: len(devDevices),
			OutputError:       nil,
		},
		"group doesn't exist": {
			InputGroupName:    "unknown",
			InputSkip:         0,
			InputLimit:        0,
			OutputDevices:     nil,
			OutputDeviceCount: -1,
			OutputError:       store.ErrGroupNotFound,
		},
		"dev group": {
			InputGroupName: "dev",
			InputSkip:      0,
			InputLimit:     10,
			OutputDevices: []model.DeviceID{
				"010000000000000000000000",
				"060000000000000000000000",
				"080000000000000000000000",
			},
			OutputDeviceCount: len(devDevices),
			OutputError:       nil,
		},
		"prod group": {
			InputGroupName: "prod",
			InputSkip:      0,
			InputLimit:     10,
			OutputDevices: []model.DeviceID{
				"020000000000000000000000",
				"040000000000000000000000",
				"050000000000000000000000",
			},
			OutputDeviceCount: len(prodDevices),
			OutputError:       nil,
		},
		"test group": {
			InputGroupName: "test",
			InputSkip:      0,
			InputLimit:     10,
			OutputDevices: []model.DeviceID{
				"030000000000000000000000",
				"070000000000000000000000",
			},
			OutputDeviceCount: len(testDevices),
			OutputError:       nil,
		},
	}

	client := db.Session()
	db.Wipe()
	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)

	for _, d := range inputDevices {
		_, err := client.Database(DbName).Collection(DbDevicesColl).InsertOne(db.Ctx, d)
		assert.NoError(t, err, "failed to setup input data")
	}

	for name, tc := range testCases {
		t.Logf("test case: %s", name)

		store := NewDataStoreMongoWithSession(client)

		devs, totalCount, err := store.GetDevicesByGroup(db.Ctx, tc.InputGroupName, tc.InputSkip, tc.InputLimit)

		if tc.OutputError != nil {
			assert.EqualError(t, err, tc.OutputError.Error())
		} else {
			assert.NoError(t, err, "expected no error")
			if !reflect.DeepEqual(tc.OutputDevices, devs) {
				assert.Fail(t, "expected outputDevices to match", fmt.Sprintf("Expected: %v but\n have:%v", tc.OutputDevices, devs))
			}
			if !reflect.DeepEqual(tc.OutputDeviceCount, totalCount) {
				assert.Fail(t, "expected outputDeviceCount to match", fmt.Sprintf("Expected: %v but\n have:%v", tc.OutputDeviceCount, totalCount))
			}
		}
	}
}

func TestGetDevicesByGroupWithTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestGetDevicesByGroupWithTenant in short mode.")
	}

	inputDevices := []model.Device{
		{
			ID:    "010000000000000000000000",
			Group: model.GroupName("dev"),
		},
		{
			ID:    "020000000000000000000000",
			Group: model.GroupName("prod"),
		},
		{
			ID:    "030000000000000000000000",
			Group: model.GroupName("test"),
		},
		{
			ID:    "040000000000000000000000",
			Group: model.GroupName("prod"),
		},
		{
			ID:    "050000000000000000000000",
			Group: model.GroupName("prod"),
		},
		{
			ID:    "060000000000000000000000",
			Group: model.GroupName("dev"),
		},
		{
			ID:    "070000000000000000000000",
			Group: model.GroupName("test"),
		},
		{
			ID:    "080000000000000000000000",
			Group: model.GroupName("dev"),
		},
	}

	testCases := map[string]struct {
		InputGroupName    model.GroupName
		InputSkip         int
		InputLimit        int
		OutputDevices     []model.DeviceID
		OutputDeviceCount int
		OutputError       error
	}{
		"no skip, no limit": {
			InputGroupName: "dev",
			InputSkip:      0,
			InputLimit:     0,
			OutputDevices: []model.DeviceID{
				"010000000000000000000000",
				"060000000000000000000000",
				"080000000000000000000000",
			},
			OutputDeviceCount: 3,
			OutputError:       nil,
		},
		"no skip, limit": {
			InputGroupName: "prod",
			InputSkip:      0,
			InputLimit:     2,
			OutputDevices: []model.DeviceID{
				"020000000000000000000000",
				"040000000000000000000000",
			},
			OutputDeviceCount: 3,
			OutputError:       nil,
		},
		"skip, no limit": {
			InputGroupName: "dev",
			InputSkip:      2,
			InputLimit:     0,
			OutputDevices: []model.DeviceID{
				"080000000000000000000000",
			},
			OutputDeviceCount: 3,
			OutputError:       nil,
		},
		"skip + limit": {
			InputGroupName: "prod",
			InputSkip:      1,
			InputLimit:     1,
			OutputDevices: []model.DeviceID{
				"040000000000000000000000",
			},
			OutputDeviceCount: 3,
			OutputError:       nil,
		},
		"no results (past last page)": {
			InputGroupName:    "dev",
			InputSkip:         10,
			InputLimit:        1,
			OutputDevices:     []model.DeviceID{},
			OutputDeviceCount: 3,
			OutputError:       nil,
		},
		"group doesn't exist": {
			InputGroupName:    "unknown",
			InputSkip:         0,
			InputLimit:        0,
			OutputDevices:     nil,
			OutputDeviceCount: -1,
			OutputError:       store.ErrGroupNotFound,
		},
	}

	client := db.Session()
	db.Wipe()
	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)

	for _, d := range inputDevices {
		_, err := client.Database(DbName+"-foo").Collection(DbDevicesColl).InsertOne(db.Ctx, d)
		assert.NoError(t, err, "failed to setup input data")
	}

	for name, tc := range testCases {
		t.Logf("test case: %s", name)

		store := NewDataStoreMongoWithSession(client)

		db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
			Tenant: "foo",
		})
		devs, totalCount, err := store.GetDevicesByGroup(db.Ctx, tc.InputGroupName, tc.InputSkip, tc.InputLimit)

		if tc.OutputError != nil {
			assert.EqualError(t, err, tc.OutputError.Error())
		} else {
			assert.NoError(t, err, "expected no error")
			if !reflect.DeepEqual(tc.OutputDevices, devs) {
				assert.Fail(t, "expected outputDevices to match", fmt.Sprintf("Expected: %v but\n have:%v", tc.OutputDevices, devs))
			}
			if !reflect.DeepEqual(tc.OutputDeviceCount, totalCount) {
				assert.Fail(t, "expected outputDeviceCount to match", fmt.Sprintf("Expected: %v but\n have:%v", tc.OutputDeviceCount, totalCount))
			}
		}
	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestGetDeviceGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestGetDeviceGroup in short mode.")
	}

	inputDevices := []model.Device{
		{
			ID:    "010000000000000000000000",
			Group: model.GroupName("dev"),
		},
		{
			ID: "020000000000000000000000",
		},
	}

	testCases := map[string]struct {
		InputDeviceID model.DeviceID
		OutputGroup   model.GroupName
		OutputError   error
	}{
		"dev has group": {
			InputDeviceID: "010000000000000000000000",
			OutputGroup:   model.GroupName("dev"),
			OutputError:   nil,
		},
		"dev has no group": {
			InputDeviceID: "020000000000000000000000",
			OutputGroup:   "",
			OutputError:   nil,
		},
		"dev doesn't exist": {
			InputDeviceID: "030000000000000000000000",
			OutputGroup:   "",
			OutputError:   store.ErrDevNotFound,
		},
	}

	client := db.Session()
	db.Wipe()
	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)

	for _, d := range inputDevices {
		_, err := client.Database(DbName).Collection(DbDevicesColl).InsertOne(db.Ctx, d)
		assert.NoError(t, err, "failed to setup input data")
	}

	for name, tc := range testCases {
		t.Logf("test case: %s", name)

		store := NewDataStoreMongoWithSession(client)

		group, err := store.GetDeviceGroup(db.Ctx, tc.InputDeviceID)

		if tc.OutputError != nil {
			assert.EqualError(t, err, tc.OutputError.Error())
		} else {
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tc.OutputGroup, group)
		}
	}
}

func TestGetDeviceGroupWithTenant(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestGetDeviceGroupWithTenant in short mode.")
	}

	inputDevices := []model.Device{
		{
			ID:    "010000000000000000000000",
			Group: model.GroupName("dev"),
		},
		{
			ID: "020000000000000000000000",
		},
	}

	testCases := map[string]struct {
		InputDeviceID model.DeviceID
		OutputGroup   model.GroupName
		OutputError   error
	}{
		"dev has group": {
			InputDeviceID: "010000000000000000000000",
			OutputGroup:   model.GroupName("dev"),
			OutputError:   nil,
		},
		"dev has no group": {
			InputDeviceID: "020000000000000000000000",
			OutputGroup:   "",
			OutputError:   nil,
		},
		"dev doesn't exist": {
			InputDeviceID: "030000000000000000000000",
			OutputGroup:   "",
			OutputError:   store.ErrDevNotFound,
		},
	}

	client := db.Session()
	db.Wipe()
	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)

	for _, d := range inputDevices {
		_, err := client.Database(DbName+"-foo").Collection(DbDevicesColl).InsertOne(db.Ctx, d)
		assert.NoError(t, err, "failed to setup input data")
	}

	for name, tc := range testCases {
		t.Logf("test case: %s", name)

		store := NewDataStoreMongoWithSession(client)

		db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
			Tenant: "foo",
		})
		group, err := store.GetDeviceGroup(db.Ctx, tc.InputDeviceID)

		if tc.OutputError != nil {
			assert.EqualError(t, err, tc.OutputError.Error())
		} else {
			assert.NoError(t, err, "expected no error")
			assert.Equal(t, tc.OutputGroup, group)
		}
	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMigrate in short mode.")
	}

	someDevs := []model.Device{
		{
			ID: "000000000000000000000000",
			Attributes: map[string]model.DeviceAttribute{
				"mac": {Name: "mac", Value: "foo", Description: strPtr("desc")},
				"sn":  {Name: "sn", Value: "bar", Description: strPtr("desc")},
			},
		},
		{
			ID: "010000000000000000000000",
			Attributes: map[string]model.DeviceAttribute{
				"mac": {Name: "mac", Value: "foo", Description: strPtr("desc")},
				"foo": {Name: "foo", Value: "foo", Description: strPtr("desc")},
				"bar": {Name: "bar", Value: "bar", Description: strPtr("desc")},
			},
		},
		{
			ID: "020000000000000000000000",
			Attributes: map[string]model.DeviceAttribute{
				"baz": {Name: "baz", Value: "baz", Description: strPtr("desc")},
			},
		},
	}

	testCases := map[string]struct {
		versionFrom string
		inDevs      []model.Device
		automigrate bool
		tenant      string

		outVers []string
		err     error
	}{
		"from no version (fresh db)": {
			versionFrom: "",
			automigrate: true,

			outVers: []string{
				"0.1.0",
			},
		},
		"from 0.1.0 (first, dummy migration)": {
			versionFrom: "0.1.0",
			automigrate: true,

			outVers: []string{
				"0.1.0",
			},
		},
		"from 0.1.0, no-automigrate": {
			versionFrom: "0.0.0",

			err: errors.New("failed to apply migrations: db needs migration: inventory has version 0.0.0, needs version 0.1.0"),
		},
		"with devices, from 0.1.0": {
			versionFrom: "0.1.0",
			inDevs:      someDevs,
			automigrate: true,

			outVers: []string{
				"0.1.0",
			},
		},
		"with devices, from 0.1.0, with tenant": {
			versionFrom: "",
			inDevs:      someDevs,
			automigrate: true,
			tenant:      "tenant",

			outVers: []string{
				"0.1.0",
			},
		},
		"with devices, from 0.1.0, with tenant, other devs": {
			versionFrom: "0.1.0",
			inDevs:      []model.Device{someDevs[0], someDevs[2]},
			automigrate: true,
			tenant:      "tenant",

			outVers: []string{
				"0.1.0",
			},
		},
	}

	client := db.Session()
	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	for name, tc := range testCases {
		t.Logf("case: %s", name)
		db.Wipe()

		if tc.tenant != "" {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: tc.tenant,
			})
		} else {
			db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
				Tenant: "",
			})
		}

		store := NewDataStoreMongoWithSession(client)

		if tc.automigrate {
			store = store.WithAutomigrate()
		}

		// prep input data
		// input migrations
		if tc.versionFrom != "" {
			v, err := migrate.NewVersion(tc.versionFrom)
			assert.NoError(t, err)

			entry := migrate.MigrationEntry{
				Version: *v,
			}
			assert.NoError(t, err)

			_, err = client.Database(mstore.DbFromContext(db.Ctx, DbName)).
				Collection(migrate.DbMigrationsColl).
				InsertOne(db.Ctx, entry)
			assert.NoError(t, err)
		}

		// input devices
		for _, d := range tc.inDevs {
			_, err := client.Database(mstore.DbFromContext(db.Ctx, DbName)).
				Collection(DbDevicesColl).
				InsertOne(db.Ctx, d)
			assert.NoError(t, err)
		}

		err := store.Migrate(db.Ctx, DbVersion)
		if tc.err == nil {
			assert.NoError(t, err)

			// verify migration entries
			var out []migrate.MigrationEntry
			cursor, _ := client.Database(mstore.DbFromContext(db.Ctx, DbName)).
				Collection(migrate.DbMigrationsColl).
				Find(db.Ctx, bson.M{})

			count := 0
			for cursor.Next(db.Ctx) {
				var res migrate.MigrationEntry
				count++
				elem := &bson.D{}
				err = cursor.Decode(elem)
				bsonBytes, _ := bson.Marshal(elem)
				bson.Unmarshal(bsonBytes, &res)
				out = append(out, res)
			}

			assert.Equal(t, len(tc.outVers), count)
			for i, v := range tc.outVers {
				assert.Equal(t, v, out[i].Version.String())
			}
		} else {
			assert.EqualError(t, err, tc.err.Error())
		}
	}
	db.Ctx = identity.WithContext(db.Ctx, &identity.Identity{
		Tenant: "",
	})
}

// test funcs
func TestMongoDeleteDevice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestMongoDeleteDevice in short mode.")
	}

	inputDevs := []model.Device{
		{ID: "000000000000000000000000"},
		{ID: "010000000000000000000000"},
	}

	testCases := map[string]struct {
		inputId  model.DeviceID
		expected []model.Device
		err      error
	}{
		"existing 1": {
			inputId: "000000000000000000000000",
			expected: []model.Device{
				{ID: "010000000000000000000000"},
			},
			err: nil,
		},
		"existing 2": {
			inputId: "010000000000000000000000",
			expected: []model.Device{
				{ID: "000000000000000000000000"},
			},
			err: nil,
		},
		"doesn't exist": {
			inputId: "030000000000000000000000",
			expected: []model.Device{
				{ID: "000000000000000000000000"},
				{ID: "010000000000000000000000"},
			},
			err: store.ErrDevNotFound,
		},
	}

	client := db.Session()
	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)
	for name, tc := range testCases {
		t.Logf("test case: %s", name)

		// Make sure we start test with empty database
		db.Wipe()

		for _, d := range inputDevs {
			_, err := client.Database(DbName).Collection(DbDevicesColl).InsertOne(db.Ctx, d)
			assert.NoError(t, err, "failed to setup input data")
		}

		store := NewDataStoreMongoWithSession(client)

		//test
		err := store.DeleteDevice(db.Ctx, tc.inputId)
		if tc.err != nil {
			assert.EqualError(t, err, tc.err.Error())
		} else {
			assert.NoError(t, err, "failed to delete device")

			var outDevs []model.Device
			cursor, err := client.Database(DbName).Collection(DbDevicesColl).Find(db.Ctx, bson.M{})
			assert.NoError(t, err, "failed to verify devices")
			for cursor.Next(db.Ctx) {
				var d model.Device
				cursor.Decode(&d)
				outDevs = append(outDevs, d)
			}
			assert.True(t, reflect.DeepEqual(tc.expected, outDevs))
		}

	}
}

func TestWithAutomigrate(t *testing.T) {
	client := db.Session()
	db.Wipe()
	// clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")
	// ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
	// t.Logf("mongo_supported: connecting to mongo '%v'", clientOptions)
	// client, _ := mongo.Connect(ctx, clientOptions)

	store := NewDataStoreMongoWithSession(client)

	newStore := store.WithAutomigrate()

	assert.NotEqual(t, store, newStore)
	_ = client.Database(mstore.DbFromContext(db.Ctx, DbName)).Drop(db.Ctx)
}
