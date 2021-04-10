package main

import (
	"context"
	"github.com/bluemediaapp/models"
	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
)

var (
	app    = fiber.New()
	client *mongo.Client
	config *Config

	mctx = context.Background()

	usersCollection *mongo.Collection
	videosCollection *mongo.Collection

)

func main() {
	config = &Config{
		port:     os.Getenv("port"),
		mongoUri: os.Getenv("mongo_uri"),
	}

	app.Get("/user/:user_id", func(ctx *fiber.Ctx) error {
		userId, err := strconv.ParseInt(ctx.Params("user_id"), 10, 64)
		if err != nil {
			return err
		}
		user, err := getUser(userId)
		if err != nil {
			return err
		}
		return ctx.JSON(user)
	})
	app.Get("/videos/:video_id", func(ctx *fiber.Ctx) error {
		videoId, err := strconv.ParseInt(ctx.Params("video_id"), 10, 64)
		if err != nil {
			return err
		}
		video, err := getVideo(videoId)
		if err != nil {
			return err
		}
		return ctx.JSON(video)
	})

	initDb()
	log.Fatal(app.Listen(config.port))
}

func initDb() {
	// Connect mongo
	var err error
	client, err = mongo.NewClient(options.Client().ApplyURI(config.mongoUri))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(mctx)
	if err != nil {
		log.Fatal(err)
	}

	// Setup tables
	db := client.Database("blue")
	usersCollection = db.Collection("users")
	videosCollection = db.Collection("video_metadata")
}

// Db utils
func getUser(userId int64) (models.DatabaseUser, error) {
	query := bson.D{{"_id", userId}}
	rawUser := usersCollection.FindOne(mctx, query)
	var user models.DatabaseUser
	err := rawUser.Decode(&user)
	if err != nil {
		return models.DatabaseUser{}, err
	}
	return user, nil
}

func getVideo(videoId int64) (models.DatabaseVideo, error) {
	query := bson.D{{"_id", videoId}}
	rawVideo := videosCollection.FindOne(mctx, query)
	var video models.DatabaseVideo
	err := rawVideo.Decode(&video)
	if err != nil {
		return models.DatabaseVideo{}, err
	}
	return video, nil
}
