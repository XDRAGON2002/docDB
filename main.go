package main

import (
	"fmt"

	"github.com/XDRAGON2002/docDB/pkg/db"
)

type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func main() {
	driver, err := db.New("./db")
	if err != nil {
		fmt.Printf(err.Error())
	}

	users := []User{{"A", 1}, {"B", 2}}

	for _, user := range users {
		driver.Collection("users").Write(user.Name, user)
	}

	records, err := driver.Collection("users").ReadAll()
	if err != nil {
		fmt.Printf(err.Error())
	}

	for _, record := range records {
		fmt.Printf("%+v\n", record)
	}

	var user User

	err = driver.Collection("users").Read("A", &user)

	fmt.Printf("%+v\n\n", user)

	driver.Collection("users").Delete("A")
}
