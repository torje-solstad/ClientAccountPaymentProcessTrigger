package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	collectionAccountCases []ViewCollectionAccount = nil
	connectionError        error                   = nil
	sess                   *session.Session        = nil
	DWH_CONSTR_DYNAMIC     string                  = os.Getenv("DWH_CONSTR_DYNAMIC")
	DWH_USERNAME           string                  = os.Getenv("DWH_USERNAME")
	DWH_PASSWORD           string                  = os.Getenv("DWH_PASSWORD")
	DWH_DB                 string                  = os.Getenv("DWH_DB")
	BPE_ENDPOINT           string                  = os.Getenv("BPE_ENDPOINT")
	SNS_TOPIC_NAME         string                  = os.Getenv("SNS_TOPIC_NAME")
	BUCKET                 string                  = os.Getenv("BUCKET")
	FILE_NAME              string                  = os.Getenv("FILE_NAME")
	// FROM               string                    = os.Getenv("FROM")
	// TO                 string                    = os.Getenv("TO")
	DB *sql.DB = nil
)

type PaymentIdModel struct {
	PaymentId int64
	Date      time.Time
}
type ViewCollectionAccount struct {
	CollectionAccountId int
	PaymentId           int64
}

type CollectionAccountCaseNumbers []int

type InputData struct {
	FirstTrigger int
}

func mapToInt(vc []ViewCollectionAccount) CollectionAccountCaseNumbers {
	var cases []int = []int{}

	for _, item := range vc {
		cases = append(cases, item.CollectionAccountId)
	}
	return cases
}

func uploadFile(data string) error {
	fmt.Printf("Upload file Recived -> %s", data)
	svc := s3.New(sess)
	f, err := os.Create(fmt.Sprintf("/tmp/%s", FILE_NAME))

	if err != nil {
		fmt.Println("Error opening file")
		fmt.Println(err)
		return err
	}
	defer f.Close()
	i, err := strconv.ParseInt(data, 10, 64)

	paymentIdData := PaymentIdModel{PaymentId: i, Date: time.Now()}

	json, err := json.Marshal(paymentIdData)
	if err != nil {
		panic(fmt.Sprintf("Could not marshal struct %v", paymentIdData))
	}

	bytelen, err := f.WriteString(string(json))

	if err != nil {
		fmt.Println("Error writing to file")
		fmt.Println(err)
		return err
	}

	fmt.Printf("Wrote %d bytes to file \n", bytelen)

	fmt.Println("Resetting file before upload")

	f.Seek(0, io.SeekStart)
	op, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(BUCKET),
		Body:   f,
		Key:    aws.String(FILE_NAME),
	})

	if err != nil {

		fmt.Println("Put object didnt work")
		fmt.Println(err)
		return err
	}
	fmt.Println(op.String())
	return nil

}
func getFileS3() (string, error) {
	fmt.Println("Trying to get a file")
	fmt.Println(fmt.Sprintf("FileName=%v", FILE_NAME))
	svc := s3.New(sess)
	f, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(FILE_NAME),
	})

	if err != nil {
		fmt.Println("Could not get file")
		fmt.Println(err)
		return "", err
	}

	bytes, e := ioutil.ReadAll(f.Body)

	if e != nil {
		fmt.Println("Could not read file")
		return "", e
	}

	return string(bytes), nil

}

func fetchCollectionAccountCasesHigherThanPaymentId(paymntid int64) ([]ViewCollectionAccount, error) {
	var col ViewCollectionAccount = ViewCollectionAccount{}

	rows, err := DB.Query(fmt.Sprintf(`
	SELECT distinct  v.collectionaccountid, max(dp.paymentid) pid from Aptic_Fact_payment dp
		JOIN dat_bankaccount db ON dp.bankaccountid = db.bankaccountid
		JOIN dat_trans dt ON dp.paymentid = dt.paymentid
		JOIN view_collectionaccount v ON dt.accountid = v.accountid
	WHERE db.ownertype = 1
	AND dp.paymentid > %d
	AND v.closureid is null
	GROUP BY v.collectionaccountid 
	ORDER BY pid desc`, paymntid))

	if err != nil {
		return nil, err
	}
	collectionAccounts := []ViewCollectionAccount{}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&col.CollectionAccountId, &col.PaymentId)
		if err != nil {
			panic(fmt.Sprintf("Error reading row %v", err.Error()))
		}
		collectionAccounts = append(collectionAccounts, ViewCollectionAccount{col.CollectionAccountId, col.PaymentId})
	}

	return collectionAccounts, nil
}

func fetchCollectionAccountCases() ([]ViewCollectionAccount, error) {
	var col ViewCollectionAccount = ViewCollectionAccount{}

	rows, err := DB.Query(`
		SELECT distinct  v.collectionaccountid, max(dp.paymentid) pid from Aptic_Fact_payment dp
			JOIN dat_bankaccount db ON dp.bankaccountid = db.bankaccountid
			JOIN dat_trans dt ON dp.paymentid = dt.paymentid
			JOIN view_collectionaccount v ON dt.accountid = v.accountid
		WHERE db.ownertype = 1
		AND cast(dp.loggedat as date) = cast(GetDate() as date)
		AND v.closureid is null
		GROUP BY v.collectionaccountid 
		ORDER BY pid desc`)

	if err != nil {
		return nil, err
	}
	collectionAccounts := []ViewCollectionAccount{}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&col.CollectionAccountId, &col.PaymentId)
		if err != nil {
			panic(fmt.Sprintf("Error reading row %v", err.Error()))
		}
		collectionAccounts = append(collectionAccounts, ViewCollectionAccount{col.CollectionAccountId, col.PaymentId})
	}

	return collectionAccounts, nil

}

func HandleRequest(ctx context.Context, inputData InputData) (string, error) {

	if connectionError != nil {
		fmt.Println("Could not connect to db")
		sendEmailNotification(aws.String(fmt.Sprintf("Could not open DB: %s", connectionError.Error())))
		return "", connectionError
	}
	var status string

	if inputData.FirstTrigger == 1 {
		data, err := fetchCollectionAccountCases()
		if err != nil {
			panic(fmt.Sprintf("%v", err.Error()))
		}

		collectionAccountCases = data

		fmt.Println(fmt.Sprintf("EVENT IS => %v", inputData.FirstTrigger))

		cases := mapToInt(collectionAccountCases)
		b, err := json.Marshal(cases)
		if err != nil {
			panic(fmt.Sprintf("Could not marshal %v", cases))
		}
		fmt.Println(fmt.Sprintf("%v", b))
		status, err = callEndPoint(b)

		if err != nil {
			panic(err.Error())
		}
		uploadFile(strconv.Itoa(int(collectionAccountCases[0].PaymentId)))

	} else if inputData.FirstTrigger == 0 {
		data, err := getFileS3()
		if err != nil {
			panic("No file found")
		}
		pim := PaymentIdModel{}

		err = json.Unmarshal([]byte(data), &pim)
		if err != nil {
			panic("Could not marshal filecontent")
		}

		isToday, err := isTimestampToday(formatDate(pim.Date), "2006-01-02")

		if err != nil {
			sendEmailNotification(aws.String("Unable to Parse tiemstamp. Does not look like morning run was started"))
			panic("Unable to parse tiemstamp")
		}

		if !isToday {
			sendEmailNotification(aws.String("Does not look like morning run was started"))
			msg := "Could not send secondary request because first trigger seem to have failed"
			return msg, errors.New(msg)
		}

		collectionAccountCases, err = fetchCollectionAccountCasesHigherThanPaymentId(pim.PaymentId)
		if err != nil {
			panic("COuld not fetch cases")
		}
		fmt.Println(len(collectionAccountCases))

		cases := mapToInt(collectionAccountCases)

		b, err := json.Marshal(cases)

		if err != nil {
			fmt.Println(fmt.Sprintf("Could not marshal data %v", cases))
		}

		status, err = callEndPoint(b)

	}

	fmt.Println(status)
	return status, nil

}

func setLocationGlobal() (*time.Location, error) {
	loc, err := time.LoadLocation("Europe/Oslo")
	if err != nil {
		fmt.Println("Unable to load locale Europe/Oslo")
		return nil, err
	} else {
		time.Local = loc

	}
	return loc, nil
}

func main() {

	DB, connectionError = initDB()
	fmt.Println("Tried initializing db")
	if connectionError == nil {

		defer DB.Close()
	}
	loc, _ := setLocationGlobal()
	fmt.Println(loc)

	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	lambda.Start(HandleRequest)
}

func isTimestampToday(timestamp string, layout string) (bool, error) {

	date, err := time.Parse(layout, timestamp)
	if err != nil {
		return false, err
	}

	fmt.Println(fmt.Printf("TIMESTAMP %v vs %v", timestamp, date))
	tdYear, tdMonth, tdDay := time.Now().Date()
	if tdYear == date.Year() && tdMonth == date.Month() && date.Day() == tdDay {
		return true, nil
	}
	return false, nil
}

func initDB() (*sql.DB, error) {
	constr := fmt.Sprintf(DWH_CONSTR_DYNAMIC, DWH_USERNAME, DWH_PASSWORD, DWH_DB)
	DB, err := sql.Open("mssql", constr)
	if err != nil {
		fmt.Println("Could not connect")
		return nil, err
	}
	return DB, nil
}

func formatDate(date time.Time) string {
	year := date.Year()
	month := date.Month()
	day := date.Day()

	formatDigit := func(n int) string {
		if n < 10 {
			return fmt.Sprintf("%d%d", 0, n)
		} else {
			return fmt.Sprintf("%d", n)
		}
	}

	return fmt.Sprintf("%v-%v-%v", year, formatDigit(int(month)), formatDigit(int(day)))
}

func callEndPoint(b []byte) (string, error) {
	body := bytes.NewReader(b)
	req, err := http.NewRequest(http.MethodPost, BPE_ENDPOINT, body)
	if err != nil {
		fmt.Println("Unable to set up request")
		fmt.Println(err)
		return fmt.Sprintf("Error creating request -> %v", err), err
	}

	req.Header.Add("Content-Type", "application/json")
	// fmt.Println(BPE_ENDPOINT)
	client := http.Client{}
	res, err := client.Do(req)

	if err != nil {
		fmt.Println("Error sending request...")
		fmt.Println(err)
	}

	if strings.Trim(strings.Split(res.Status, " ")[0], " ") != strconv.Itoa(200) {
		return fmt.Sprintf("Failed. Status = %s", res.Status), errors.New(res.Status)
	}
	resBytes, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return fmt.Sprintf("Error reading response - > %v", err), nil
	}

	return string(resBytes), err
}

func sendEmailNotification(msg *string) {
	fmt.Println("testing Topics...")

	svc := sns.New(sess)

	result, err := svc.ListTopics(nil)
	if err != nil {
		fmt.Println("No topics to list")
		panic(err)
	}
	topicName := getTopicName(SNS_TOPIC_NAME, result)

	fmt.Println(result.Topics)
	fmt.Println("Topicname -> " + *topicName)
	fmt.Println(fmt.Sprintf("Length -> %d", len(result.Topics)))
	output, err := svc.Publish(&sns.PublishInput{
		TopicArn: topicName,
		Message:  msg,
		Subject:  aws.String("BPE PaymentplanAgreement"),
	})
	if err != nil {
		fmt.Println("Could not publish notification...")
		fmt.Println(err)
	}

	fmt.Println(*output.MessageId)

}

func getTopicName(topicname string, topics *sns.ListTopicsOutput) *string {
	for _, topic := range topics.Topics {
		topicNameParts := strings.Split(*topic.TopicArn, ":")

		if topicNameParts[len(topicNameParts)-1] == topicname {
			return *&topic.TopicArn
		}
	}
	return nil
}
