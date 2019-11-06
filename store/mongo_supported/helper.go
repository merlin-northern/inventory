package mongo_supported

import (
	"context"

	"github.com/mendersoftware/inventory/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func FindId(ctx context.Context, c *mongo.Collection, id model.DeviceID, dst *model.Device) error {
	result := c.FindOne(ctx, bson.M{"_id": id})
	elem := &bson.D{}
	err := result.Decode(elem)
	if err != nil {
		return err
	}

	bsonBytes, err := bson.Marshal(elem)
	if err != nil {
		return err
	}

	bson.Unmarshal(bsonBytes, dst)
	return nil
}
