package main

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetMongo() *mongo.Collection {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetMaxPoolSize(100),
	)

	if err != nil {
		log.Fatal(err)
	}

	return client.Database("perfdb").Collection("orders")
}
