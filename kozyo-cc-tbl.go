/*
  JHA 07/06/16

  (c) Eliad Technologies, Inc.
*/

package main

import (
    "errors"
    "fmt"
  /*"strconv"
    "strings"
    "encoding/json"*/

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
    DiplomaId string `json:"diploma_id"`
    UserId string `json:"user_id"`
    Label  string `json:"label"`
    Date   string `json:"date"`
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

	return stub.CreateTable("Users", columnDefsUsers)
}

func (t *SimpleChaincode) createTableDiplomas(stub *shim.ChaincodeStub) error {
    fmt.Println("createTableDiplomas()")
	var columnDefsDiplomas []*shim.ColumnDefinition
	columnDiplomaIdDef := shim.ColumnDefinition{Name: "DiplomaId", Type: shim.ColumnDefinition_STRING, Key: true}
	columnUserIdDef    := shim.ColumnDefinition{Name: "UserId", Type: shim.ColumnDefinition_STRING, Key: true}
	columnLabelDef     := shim.ColumnDefinition{Name: "Label", Type: shim.ColumnDefinition_STRING, Key: false}
	columnDateDef      := shim.ColumnDefinition{Name: "Date", Type: shim.ColumnDefinition_STRING, Key: false}

	columnDefsDiplomas = append(columnDefsDiplomas, &columnDiplomaIdDef)
	columnDefsDiplomas = append(columnDefsDiplomas, &columnUserIdDef)
	columnDefsDiplomas = append(columnDefsDiplomas, &columnLabelDef)
	columnDefsDiplomas = append(columnDefsDiplomas, &columnDateDef)

	return stub.CreateTable("Diplomas", columnDefsDiplomas)
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
        return errors.New("insertRowUsers operation failed. Row with given key already exists")
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
        return errors.New("insertRowDiplomas operation failed. Row with given key already exists")
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
        return nil, errors.New("Incorrect number of arguments. Expecting 1 or more argumenst")
    }
    switch function {
        case "query" :
        /*if args[0] == "getAllUsers" {
            fmt.Println("Getting all users")
            allUsers, err := getAllUsers(stub)
            if err != nil {
                fmt.Println("Error from getAllUsers")   // Note: Error had printed in getAllUsers
                return nil, err
            } else {
                allUsersBytes, err1 := json.Marshal(&allUsers)
                if err1 != nil {
                    fmt.Println("Error marshalling allUsers => "+err.Error())
                    return nil, err1
                }
                fmt.Printf("Returning -> %v\n",allUsersBytes)
                return allUsersBytes, nil
            }
        } else { */
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
            return t.getRowDiplomas(stub,args[0]);
        case "getRowsDiplomas" :
            return t.getRowsDiplomas(stub,args[0],args[1]);
        default:
            return nil, errors.New("Unsupported function '"+function+"'")
    }
}

func (t *SimpleChaincode) getRowUsers(stub *shim.ChaincodeStub, key string) ([]byte, error) {
    fmt.Printf("getRowUsers(...,'%s')\n",key)
    var columns []shim.Column
    col := shim.Column{Value: &shim.Column_String_{String_: key}}
    columns = append(columns, col)

    row, err := stub.GetRow("Users", columns)
    if err != nil {
        return nil, fmt.Errorf("getRowUsers failed, %s", err)
    }

    rowString := fmt.Sprintf("%s", row)
    return []byte(rowString), nil
}

func (t *SimpleChaincode) getRowDiplomas(stub *shim.ChaincodeStub, key string) ([]byte, error) {
    fmt.Printf("getRowDiplomas(...,'%s')\n",key)
    var columns []shim.Column
    col := shim.Column{Value: &shim.Column_String_{String_: key}}
    columns = append(columns, col)

    row, err := stub.GetRow("Diplomas", columns)
    if err != nil {
        return nil, fmt.Errorf("getRowDiplomas failed, %s", err)
    }

    rowString := fmt.Sprintf("%s", row)
    return []byte(rowString), nil
}

func (t *SimpleChaincode) getRowsDiplomas(stub *shim.ChaincodeStub, key1, key2 string) ([]byte, error) {
    fmt.Printf("getRowsDiplomas(...,'%s','%s')\n",key1,key2)
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: key1}}
    columns = append(columns, col1)
    col2 := shim.Column{Value: &shim.Column_String_{String_: key2}}
    columns = append(columns, col2)

    row, err := stub.GetRow("Diplomas", columns)
    if err != nil {
        return nil, fmt.Errorf("getRowsDiplomas failed, %s", err)
    }

    rowString := fmt.Sprintf("%s", row)
    return []byte(rowString), nil
}

/*
func getAllUsers(stub *shim.ChaincodeStub) ([]User, error) {
    var allUsers []User
    // Get list of all the keys
    keysBytes, err := stub.GetState(allUsersKey)
    if err != nil {
        fmt.Println("Error get state "+allUsersKey+" => "+err.Error())
        return nil, err
    }
    fmt.Printf("GetState('%v') -> %v\n",allUsersKey,keysBytes)

    var keys []string
    err = json.Unmarshal(keysBytes, &keys)
    if err != nil {
        fmt.Println("Error unmarshalling "+allUsersKey+" => "+err.Error())
        return nil, err
    }
    fmt.Printf("Unmarshal(keysBytes) -> %v\n",keys)

    // Get all the Users
    for _, userKey := range keys {
        userBytes, err := stub.GetState(userKey)
        if err != nil {
            fmt.Println("Error: get state " + userKey+" => "+err.Error())
            return nil, err
        }
        fmt.Printf("GetState('%v') -> %v\n",userKey,userBytes)

        var user User
        err = json.Unmarshal(userBytes, &user)
        if err != nil {
            fmt.Println("Error: unmarshal " + userKey+" => "+err.Error())
            return nil, err
        }
        fmt.Printf("Unmarshal(userBytes) -> %v\n",user)

        // XXX JHA : ? convertir les clefs de user.Diplomas en structures ?

        fmt.Println("Appending " + userKey)
        allUsers = append(allUsers, user)
    }

    return allUsers, nil
}
*/

func main() {
    err := shim.Start(new(SimpleChaincode))
    if err != nil {
        fmt.Printf("Error starting Simple chaincode: %s", err)
    }
}

