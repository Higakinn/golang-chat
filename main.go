package main

import (
  "context"
  "time"
  "fmt"

  "go.mongodb.org/mongo-driver/bson"
  "go.mongodb.org/mongo-driver/mongo"
  "go.mongodb.org/mongo-driver/mongo/options"
  "go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
  defer fmt.Println("done.")

  ctx := context.Background()

  collection, err := connect(ctx)
  if err != nil {
      fmt.Println("failed to connect", err)
      return
  }
  fmt.Println("connected.")

  if err := follow(ctx, collection); err != nil {
      fmt.Println("failed to follow", err)
  }
}

func connect(ctx context.Context) (*mongo.Collection, error) {
  // use 3s timeout for connecting
  ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
  defer cancel()

  opts := options.Client().
      ApplyURI("mongodb://mongo1:27017")
  err := opts.Validate()
  if err != nil {
      return nil, err
  }

  client, err := mongo.NewClient(opts)
  if err != nil {
      return nil, err
  }

  if err := client.Connect(ctx); err != nil {
      return nil, err
  }

  if err := client.Ping(ctx, readpref.Primary()); err != nil {
      return nil, err
  }

  db := client.Database("local")
  return db.Collection("oplog.rs"), nil
}

func follow(ctx context.Context, collection *mongo.Collection) error {
  // create a tailable cursor on the collection
  ts := time.Now()
  fmt.Print(ts)  
  // filter := bson.D{{"ns", bson.D{{"$eq", "test.products"}}}}
  filter := bson.M{"wall": bson.M{"$gt": ts},"ns": bson.M{"$eq": "test.products"}}
  opts := options.Find().SetCursorType(options.TailableAwait)
  cursor, err := collection.Find(ctx, filter, opts)
  if err != nil {
      return err
  }
  defer cursor.Close(ctx)

  // load and print documents as they are inserted
  for cursor.Next(ctx) {
    // if cursor.Next(ctx) {
    var document bson.M
    if err := cursor.Decode(&document); err != nil {
        return err
    }
    fmt.Println("loaded document", document)
    // } else {
    //   fmt.Println("not data")
    // }
    time.Sleep(time.Second * 1)
  }
  return nil
}