/*
  JHA 07/06/16

  (c) Eliad Technologies, Inc.
*/

package main

import (
    "errors"
    "fmt"
  /*"strconv"
    "strings"*/
    "encoding/json"

  //"github.com/openblockchain/obc-peer/openchain/chaincode/shim"
    "github.com/hyperledger/fabric/core/chaincode/shim"
)

// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
}

const userPrefix = "usr:"
const diplomaPrefix = "dpl:"
const allUsersKey = "allUsers"
const allDiplomasKey = "allDiplomas"

type User struct {
    UserId    string `json:"user_id"`
    Email     string `json:"email"`
    FirstName string `json:"first_name"`
    LastName  string `json:"last_name"`
    FbId      string `json:"fb_id"`
    Diplomas  []string `json:"diplomas"`
}

type Diploma struct {
    UserId string `json:"user_id"`
    DiplomaId string `json:"diploma_id"`
    Label  string `json:"label"`
    Date   string `json:"date"`
}

type Record struct {
    Fields []string;
}

func (t *SimpleChaincode) Init(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    // Initialize the collection of commercial paper keys
    fmt.Println("Initializing kozyo")

    if err := t.createTableUsers(stub); err != nil {
        return nil, err
    }
    if err := t.createTableDiplomas(stub); err != nil {
        return nil, err
    }

    fmt.Println("Initialization complete")
    return nil, nil
}

func (t *SimpleChaincode) createTableUsers(stub *shim.ChaincodeStub) error {
    fmt.Println("createTableUsers()")
	var columnDefsUsers []*shim.ColumnDefinition
	columnUserIdDef    := shim.ColumnDefinition{Name: "UserId", Type: shim.ColumnDefinition_STRING, Key: true}
	columnEmailDef     := shim.ColumnDefinition{Name: "Email", Type: shim.ColumnDefinition_STRING, Key: false}
	columnFirstNameDef := shim.ColumnDefinition{Name: "FirstName", Type: shim.ColumnDefinition_STRING, Key: false}
	columnLastNameDef  := shim.ColumnDefinition{Name: "LastName", Type: shim.ColumnDefinition_STRING, Key: false}
	columnFbIdDef      := shim.ColumnDefinition{Name: "FbId", Type: shim.ColumnDefinition_STRING, Key: false}

	columnDefsUsers = append(columnDefsUsers, &columnUserIdDef)
	columnDefsUsers = append(columnDefsUsers, &columnEmailDef)
	columnDefsUsers = append(columnDefsUsers, &columnFirstNameDef)
	columnDefsUsers = append(columnDefsUsers, &columnLastNameDef)
	columnDefsUsers = append(columnDefsUsers, &columnFbIdDef)

    if err := stub.CreateTable("Users", columnDefsUsers); err != nil {
        msg := fmt.Sprintf("Error in CreateTable: %s", err)
        fmt.Println(msg)
    }
	return nil;
}

func (t *SimpleChaincode) createTableDiplomas(stub *shim.ChaincodeStub) error {
    fmt.Println("createTableDiplomas()")
	var columnDefsDiplomas []*shim.ColumnDefinition
	columnUserIdDef    := shim.ColumnDefinition{Name: "UserId", Type: shim.ColumnDefinition_STRING, Key: true}
	columnDiplomaIdDef := shim.ColumnDefinition{Name: "DiplomaId", Type: shim.ColumnDefinition_STRING, Key: true}
	columnLabelDef     := shim.ColumnDefinition{Name: "Label", Type: shim.ColumnDefinition_STRING, Key: false}
	columnDateDef      := shim.ColumnDefinition{Name: "Date", Type: shim.ColumnDefinition_STRING, Key: false}

	columnDefsDiplomas = append(columnDefsDiplomas, &columnUserIdDef)
	columnDefsDiplomas = append(columnDefsDiplomas, &columnDiplomaIdDef)
	columnDefsDiplomas = append(columnDefsDiplomas, &columnLabelDef)
	columnDefsDiplomas = append(columnDefsDiplomas, &columnDateDef)

    if err := stub.CreateTable("Diplomas", columnDefsDiplomas); err != nil {
        msg := fmt.Sprintf("Error in CreateTable: %s", err)
        fmt.Println(msg)
    }
	return nil;
}

func (t *SimpleChaincode) deleteTableUsers(stub *shim.ChaincodeStub) error {
    fmt.Println("deleteTableUsers()")

    if err := stub.DeleteTable("Users"); err != nil {
        msg := fmt.Sprintf("Error in DeleteTable: %s", err)
        fmt.Println(msg)
    }
	return nil;
}

func (t *SimpleChaincode) deleteTableDiplomas(stub *shim.ChaincodeStub) error {
    fmt.Println("deleteTableDiplomas()")

    if err := stub.DeleteTable("Diplomas"); err != nil {
        msg := fmt.Sprintf("Error in DeleteTable: %s", err)
        fmt.Println(msg)
    }
	return nil;
}

func (t *SimpleChaincode) insertRowUsers(stub *shim.ChaincodeStub, args []string) error {
    fmt.Printf("insertRowUsers(...,%v)\n",args)
    if len(args) != 5 {
        fmt.Printf("Error: insertRowUsers called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("insertRowUsers has 5 arguments")
    }

    var columns []*shim.Column
    for _,arg := range args {
        col := shim.Column{Value: &shim.Column_String_{String_: arg}}   // Note: All cols contain strings
        columns = append(columns, &col)
    }

    row := shim.Row{Columns: columns}
    ok, err := stub.InsertRow("Users", row)
    if err != nil {
        msg := fmt.Sprintf("insertRowUsers operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    if !ok {
        msg := "insertRowUsers operation failed. Row with given key already exists"
        fmt.Println(msg)
        return errors.New(msg)
    }
    return nil
}

func (t *SimpleChaincode) insertRowDiplomas(stub *shim.ChaincodeStub, args []string) error {
    fmt.Printf("insertRowDiplomas(...,%v)\n",args)
    if len(args) != 4 {
        fmt.Printf("Error: insertRowDiplomas called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("insertRowDiplomas has 4 arguments")
    }

    var columns []*shim.Column
    for _,arg := range args {
        col := shim.Column{Value: &shim.Column_String_{String_: arg}}   // Note: All cols contain strings
        columns = append(columns, &col)
    }

    row := shim.Row{Columns: columns}
    ok, err := stub.InsertRow("Diplomas", row)
    if err != nil {
        msg := fmt.Sprintf("insertRowDiplomas operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    if !ok {
        msg := "insertRowUsers operation failed. Row with given key already exists"
        fmt.Println(msg)
        return errors.New(msg)
    }
    return nil
}

func (t *SimpleChaincode) deleteRowUsers(stub *shim.ChaincodeStub, args []string) error {
    fmt.Printf("deleteRowUsers(...,%v)\n",args)
    if len(args) != 1 {
        fmt.Printf("Error: deleteRowUsers called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("deleteRowUsers has 1 arguments")
    }

    col1Val := args[0]
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
    columns = append(columns, col1)

    err := stub.DeleteRow("Users", columns)
    if err != nil {
        msg := fmt.Sprintf("deleteRowUsers operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }

    //XXX JHA : Effacer tous les diplomes avec userId == col1Val

    return nil
}

func (t *SimpleChaincode) deleteRowDiplomas(stub *shim.ChaincodeStub, args []string) error {
    fmt.Printf("deleteRowDiplomas(...,%v)\n",args)
    if len(args) != 1 {
        fmt.Printf("Error: deleteRowDiplomas called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("deleteRowDiplomas has 1 arguments")
    }

    col1Val := args[0]
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
    columns = append(columns, col1)

    err := stub.DeleteRow("Diplomas", columns)
    if err != nil {
        msg := fmt.Sprintf("deleteRowDiplomas operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }

    return nil
}

func (t *SimpleChaincode) replaceRowUsers(stub *shim.ChaincodeStub, args []string) error {
    fmt.Printf("replaceRowUsers(...,%v)\n",args)
    if len(args) != 5 {
        fmt.Printf("Error: replaceRowUsers called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("replaceRowUsers has 5 arguments")
    }

    var columns []*shim.Column
    for _,arg := range args {
        col := shim.Column{Value: &shim.Column_String_{String_: arg}}   // Note: All cols contain strings
        columns = append(columns, &col)
    }

    row := shim.Row{Columns: columns}
    ok, err := stub.ReplaceRow("Users", row)
    if err != nil {
        msg := fmt.Sprintf("replaceRowUsers operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    if !ok {
        return errors.New("replaceRowUsers operation failed. Row with key '"+args[0]+"' does not exist")
    }
    return nil
}

func (t *SimpleChaincode) replaceRowDiplomas(stub *shim.ChaincodeStub, args []string) error {
    fmt.Printf("replaceRowDiplomas(...,%v)\n",args)
    if len(args) != 4 {
        fmt.Printf("Error: replaceRowDiplomas called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("replaceRowDiplomas has 4 arguments")
    }

    var columns []*shim.Column
    for _,arg := range args {
        col := shim.Column{Value: &shim.Column_String_{String_: arg}}   // Note: All cols contain strings
        columns = append(columns, &col)
    }

    row := shim.Row{Columns: columns}
    ok, err := stub.ReplaceRow("Diplomas", row)
    if err != nil {
        msg := fmt.Sprintf("replaceRowDiplomas operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    if !ok {
        return errors.New("replaceRowDiplomas operation failed. Row with key '"+args[0]+"' does not exist")
    }
    return nil
}

// ======================================================================================================
// Delete - remove a key/value pair from state
// ======================================================================================================
func (t *SimpleChaincode) delete(stub *shim.ChaincodeStub, args []string) ([]byte, error) {
    if len(args) != 1 {
        fmt.Printf("Error: delete called with %d argument(s) (%v)\n",len(args),args)
        return nil, errors.New("delete has 1 arguments")
    }

    key := args[0]
    /* Check for additional clean-up to avoid references to deleted keys
    if strings.HasPrefix(key,userPrefix) {
        if err := userCleanup(stub,key); err != nil {
            return nil,err
        }
    } else if strings.HasPrefix(key,diplomaPrefix) {
        if err := diplomaCleanup(stub,key,true); err != nil {
            return nil,err
        }
    }
    */

    err := stub.DelState(key)    // Remove the key from chaincode state
    if err != nil {
        fmt.Println("Error: del state " + key + " => " + err.Error())
        return nil, err
    }
    fmt.Println("Del state '" + key + "' => OK")
    return nil, nil
}

/* Do additional clean-up when a user is deleted
func userCleanup(stub *shim.ChaincodeStub, userKey string) error 

// Do additional clean-up when a diploma is deleted
func diplomaCleanup(stub *shim.ChaincodeStub, diplomaKey string,doUserUpd bool) error 
*/

// Run callback representing the invocation of a chaincode
func (t *SimpleChaincode) Run(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    fmt.Printf("Run(...,'%s',%v)\n",function,args)
    return t.Invoke(stub, function, args)
}

func (t *SimpleChaincode) Invoke(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    fmt.Printf("Invoke(...,'%s',%v)\n",function,args)
    // Handle different functions
    switch function {
        case "init":                         // Initialize the chaincode
            return t.Init(stub, function, args)
        case "createTableUsers" :
            return nil, t.createTableUsers(stub);
        case "createTableDiplomas" :
            return nil, t.createTableDiplomas(stub);
        case "deleteTableUsers" :
            return nil, t.deleteTableUsers(stub);
        case "deleteTableDiplomas" :
            return nil, t.deleteTableDiplomas(stub);
        case "insertRowUsers" :
            return nil, t.insertRowUsers(stub, args)
        case "insertRowDiplomas" :
            return nil, t.insertRowDiplomas(stub, args)
        case "deleteRowUsers" :
            return nil, t.deleteRowUsers(stub, args)
        case "deleteRowDiplomas" :
            return nil, t.deleteRowDiplomas(stub, args)
        case "replaceRowUsers" :
            return nil, t.replaceRowUsers(stub, args)
        case "replaceRowDiplomas" :
            return nil, t.replaceRowDiplomas(stub, args)
        case "delete" :                // Remove args[0] from state
            return t.delete(stub, args)
    }

    return nil, errors.New("Received unknown function '"+function+"' invocation")
}

// Query callback representing the query of a chaincode
func (t *SimpleChaincode) Query(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    fmt.Printf("Query(...,'%s',%v)\n",function,args)
    if len(args) < 1 {
        return nil, errors.New("Incorrect number of arguments. Expecting 1 or more arguments")
    }
    switch function {
        case "query" :
            key := args[0]
            fmt.Println("Generic Query call, get state '"+key+"'")
            bytes, err := stub.GetState(key)
            if err != nil {
                fmt.Println("Error:  get state '"+key+"' => "+err.Error())
                return nil, err
            }

            fmt.Printf("Returning '%v' -> %v\n",key,bytes)
            return bytes, nil
        case "getRowUsers" :
            return t.getRowUsers(stub,args[0]);
        case "getRowDiplomas" :
            if len(args) < 2 {
                return nil, errors.New("Incorrect number of arguments. Expecting 2 or more arguments")
            }
            return t.getRowDiplomas(stub,args[0],args[1]);
        case "getRowsByUIdDiplomas" :
            return t.getRowsByUIdDiplomas(stub,args[0]);
        default:
            return nil, errors.New("Unsupported function '"+function+"'")
    }
}

// Get a user by UserId
func (t *SimpleChaincode) getRowUsers(stub *shim.ChaincodeStub, keyUId string) ([]byte, error) {
    fmt.Printf("getRowUsers(...,'%s')\n",keyUId)
    var columns []shim.Column
    col := shim.Column{Value: &shim.Column_String_{String_: keyUId}}
    columns = append(columns, col)

    row, err := stub.GetRow("Users", columns)
    if err != nil {
        return nil, fmt.Errorf("getRowUsers failed, %s", err)
    }

    if len(row.Columns) == 0 {
        fmt.Println("No matching rows")
        return nil,nil
    }

    userId    := row.Columns[0].GetString_()    // Note: We should have userId == keyUId
    email     := row.Columns[1].GetString_()
    firstName := row.Columns[2].GetString_()
    lastName  := row.Columns[3].GetString_()
    fbId      := row.Columns[4].GetString_()

    user := User{UserId: userId, Email: email, FirstName: firstName, LastName: lastName, FbId: fbId}
    fmt.Printf("user=%v\n",user)

    userBytes, err := json.Marshal(&user)
    if err != nil  {
        msg := "Error marshalling user " + userId
        fmt.Println(msg)
        return nil, errors.New(msg)
    }
    fmt.Printf("Marshall(user) -> %v\n",userBytes)
    return userBytes,nil
}

// Get a diploma by UserId & DiplomaId
func (t *SimpleChaincode) getRowDiplomas(stub *shim.ChaincodeStub, keyUId, keyDId string) ([]byte, error) {
    fmt.Printf("getRowDiplomas(...,'%s','%s')\n",keyDId,keyUId)
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: keyUId}}
    columns = append(columns, col1)
    col2 := shim.Column{Value: &shim.Column_String_{String_: keyDId}}
    columns = append(columns, col2)

    row, err := stub.GetRow("Diplomas", columns)
    if err != nil {
        return nil, fmt.Errorf("getRowDiplomas failed, %s", err)
    }

    if len(row.Columns) == 0 {
        fmt.Println("No matching rows")
        return nil,nil
    }

    userId    := row.Columns[0].GetString_()    // Note: We should have userId == keyUId
    diplomaId := row.Columns[1].GetString_()    // Note: We should have diplomaId == keyDId
    label     := row.Columns[2].GetString_()
    date      := row.Columns[3].GetString_()

    diploma := Diploma{DiplomaId: diplomaId, UserId: userId, Label: label, Date: date }
    fmt.Printf("diploma=%v\n",diploma)

    // Marshal the structure
    diplomaBytes, err := json.Marshal(&diploma)
    if err != nil  {
        msg := "Error marshalling diploma " + diplomaId
        fmt.Println(msg)
        return nil, errors.New(msg)
    }
    fmt.Printf("Marshall(diploma) -> %v\n",diplomaBytes)
    return diplomaBytes,nil
}

// Get all diplomas by UserId
func (t *SimpleChaincode) getRowsByUIdDiplomas(stub *shim.ChaincodeStub, keyUId string) ([]byte, error) {
    fmt.Printf("getRowsByUIdDiplomas(...,'%s')\n",keyUId)
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: keyUId}}
    columns = append(columns, col1)

    tableName := "Diplomas"
    rowChannel, err := stub.GetRows(tableName, columns)
    if err != nil {
        return nil, fmt.Errorf("getRowsByUIdDiplomas failed, %s", err)
    }

    var rows []shim.Row
    for {
        select {
        case row, ok := <-rowChannel:
            if !ok {
                rowChannel = nil
            } else {
                rows = append(rows, row)
            }
        }
        if rowChannel == nil {
            break
        }
    }

    if len(rows) == 0 {
        fmt.Println("No matching rows")
        return nil,nil
    }

    var diplomas []Diploma
    for i,_ := range rows {
        userId    := rows[i].Columns[0].GetString_()
        diplomaId := rows[i].Columns[1].GetString_()
        label     := rows[i].Columns[2].GetString_()
        date      := rows[i].Columns[3].GetString_()

        diploma := Diploma{DiplomaId: diplomaId, UserId: userId, Label: label, Date: date }
        fmt.Printf("diploma[%v]=%v\n",i,diploma)
        diplomas = append(diplomas,diploma)
    }

    // Marshal the array
    diplomasBytes, err := json.Marshal(&diplomas)
    if err != nil  {
        msg := "Error marshalling diplomas "
        fmt.Println(msg)
        return nil, errors.New(msg)
    }
    fmt.Printf("Marshall(diplomas) -> %v\n",diplomasBytes)
    return diplomasBytes,nil
}

func main() {
    err := shim.Start(new(SimpleChaincode))
    if err != nil {
        fmt.Printf("Error starting Simple chaincode: %s", err)
    }
}

