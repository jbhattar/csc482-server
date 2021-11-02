package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/gorilla/mux"
	//loggly "github.com/jamespearly/loggly"
	//"strconv"
)

type Results struct {
	Cache   Cache     `json:"cache"`
	RawData []RawData `json:"rawData"`
}
type Cache struct {
	LastUpdated          string `json:"lastUpdated"`
	Expires              string `json:"expires"`
	LastUpdatedTimestamp int64  `json:"lastUpdatedTimestamp"`
	ExpiresTimestamp     int64  `json:"expiresTimestamp"`
}

type RawData struct {
	FIPS              string `json:"FIPS"`
	Admin2            string `json:"Admin2"`
	ProvinceState     string `json:"Province_State"`
	CountryRegion     string `json:"Country_Region"`
	LastUpdate        string `json:"Last_Update"`
	Lat               string `json:"Lat"`
	Long              string `json:"Long_"`
	Confirmed         string `json:"Confirmed"`
	Deaths            string `json:"Deaths"`
	Recovered         string `json:"Recovered"`
	Active            string `json:"Active"`
	CombinedKey       string `json:"Combined_Key"`
	IncidentRate      string `json:"Incident_Rate"`
	CaseFatalityRatio string `json:"Case_Fatality_Ratio"`
}
type Status struct {
	Table       string `json:"table"`
	RecordCount *int64 `json:"recordCount"`
}

func getDataFromDynamoDB() (*int64, []RawData) {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	//Create DynamoDB client
	svc := dynamodb.New(sess)

	//using scan api
	params := &dynamodb.ScanInput{
		TableName: aws.String("jbhattar-covid-19-data"),
	}
	result, err := svc.Scan(params)
	if err != nil {
		fmt.Println("failed to make Query API call", err)
	}
	fmt.Println(result.Count)
	count := result.Count

	obj := []RawData{}
	err = dynamodbattribute.UnmarshalListOfMaps(result.Items, &obj)
	if err != nil {
		fmt.Println("failed to unmarshal Query result items", err)
	}

	return count, obj
}

func GetAllTheData(w http.ResponseWriter, r *http.Request) {
	_, countries := getDataFromDynamoDB()
	json.NewEncoder(w).Encode(countries)
}

func GetStatus(w http.ResponseWriter, r *http.Request) {
	count, _ := getDataFromDynamoDB()
	status := Status{Table: "jbhattar-covid-19-data", RecordCount: count}
	json.NewEncoder(w).Encode(status)
}

func SearchHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	searchCountry := mux.Vars(r)["country"]

	proper, err := regexp.MatchString(`[a-zA-Z][a-zA-Z ]+`, searchCountry)

	if err != nil {
		log.Fatal(err)
	}

	if proper {
		w.WriteHeader(http.StatusOK)

		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("us-east-1")},
		)
		if err != nil {
			log.Fatalf("Got error initializing AWS: %s", err)
		}


		svc := dynamodb.New(sess)


		filt := expression.Contains(expression.Name("Country_Region"), searchCountry)

		expr, err := expression.NewBuilder().WithFilter(filt).Build()
		if err != nil {
			log.Fatalf("Got error building expression: %s", err)
		}

		params := &dynamodb.ScanInput{
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
			ProjectionExpression:      expr.Projection(),
			TableName:                 aws.String("jbhattar-covid-19-data"),
		}


		out, err := svc.Scan(params)
		fmt.Println(params)
		if err != nil {
			log.Fatalf("Query API call failed: %s", err)
		}


		searchResponse := []RawData{}
		err = dynamodbattribute.UnmarshalListOfMaps(out.Items, &searchResponse)
		if err != nil {
			panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
		}


		json.NewEncoder(w).Encode(searchResponse)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		badMessage := "The format of the search is search?country=US"
		json.NewEncoder(w).Encode(badMessage)
	}
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/jbhattar/all", GetAllTheData).Methods("GET")
	r.HandleFunc("/jbhattar/status", GetStatus).Methods("GET")
	r.HandleFunc("/jbhattar/search", SearchHandler).Queries("country", "{country:.*}")
	log.Fatal(http.ListenAndServe(":8080", r))
}
