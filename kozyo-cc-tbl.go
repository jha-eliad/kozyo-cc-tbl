/*
  JHA 07/06/16

  (c) Eliad Technologies, Inc.
*/

package main

import (
    "errors"
    "fmt"
    "strconv"
  //"strings"  
    "encoding/json"

  //"github.com/openblockchain/obc-peer/openchain/chaincode/shim"
    "github.com/hyperledger/fabric/core/chaincode/shim"
)

// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
}

const usersTblName = "Users"
const awardsTblName = "Awards"

type User struct {
    UserId    string `json:"user_id"`
    Email     string `json:"email"`
    FirstName string `json:"first_name"`
    LastName  string `json:"last_name"`
    FbId      string `json:"fb_id"`
    PictureUrl string `json:"picture_url"`
    PictureMd5 string `json:"picture_md5"`
}

type Award struct {
    UserId string `json:"user_id"`
    AwardId string `json:"award_id"`
    Prize string `json:"prize"`
    Label  string `json:"label"`
    Date   string `json:"date"`
    PictureUrl string `json:"picture_url"`
    PictureMd5 string `json:"picture_md5"`
}

type Record struct {
    Fields []string;
}

func (t *SimpleChaincode) Init(stub *shim.ChaincodeStub, function string, args []string) ([]byte, error) {
    // Initialize the collection of commercial paper keys
    fmt.Println("Initializing kozyo")

    usersTable, err := stub.GetTable(usersTblName)
    if usersTable != nil && err == nil {
        fmt.Printf("%s table already exists, deleting it\n",usersTblName)
        if err := t.deleteTable(stub,usersTblName); err != nil {
            return nil, err
        }
    }
    if err := t.createTableUsers(stub); err != nil {
        return nil, err
    }

    awardsTable, err := stub.GetTable(awardsTblName)
    if awardsTable != nil && err == nil {
        fmt.Printf("%s table already exists, deleting it\n",awardsTblName)
        if err := t.deleteTable(stub,awardsTblName); err != nil {
            return nil, err
        }
    }
    if err := t.createTableAwards(stub); err != nil {
        return nil, err
    }

    fmt.Println("Initialization complete")
    return nil, nil
}

func (t *SimpleChaincode) createTableUsers(stub *shim.ChaincodeStub) error {
    fmt.Println("createTableUsers()")
    var columnDefs []*shim.ColumnDefinition
    columnUserIdDef     := shim.ColumnDefinition{Name: "UserId", Type: shim.ColumnDefinition_STRING, Key: true}
    columnEmailDef      := shim.ColumnDefinition{Name: "Email", Type: shim.ColumnDefinition_STRING, Key: false}
    columnFirstNameDef  := shim.ColumnDefinition{Name: "FirstName", Type: shim.ColumnDefinition_STRING, Key: false}
    columnLastNameDef   := shim.ColumnDefinition{Name: "LastName", Type: shim.ColumnDefinition_STRING, Key: false}
    columnFbIdDef       := shim.ColumnDefinition{Name: "FbId", Type: shim.ColumnDefinition_STRING, Key: false}
    columnPictureUrlDef := shim.ColumnDefinition{Name: "PictureUrl", Type: shim.ColumnDefinition_STRING, Key: false}
    columnPictureMd5Def := shim.ColumnDefinition{Name: "PictureMd5", Type: shim.ColumnDefinition_STRING, Key: false}

    columnDefs = append(columnDefs, &columnUserIdDef)
    columnDefs = append(columnDefs, &columnEmailDef)
    columnDefs = append(columnDefs, &columnFirstNameDef)
    columnDefs = append(columnDefs, &columnLastNameDef)
    columnDefs = append(columnDefs, &columnFbIdDef)
    columnDefs = append(columnDefs, &columnPictureUrlDef)
    columnDefs = append(columnDefs, &columnPictureMd5Def)

    if err := stub.CreateTable(usersTblName, columnDefs); err != nil {
        msg := fmt.Sprintf("Error in CreateTable: %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    return nil;
}

func (t *SimpleChaincode) createTableAwards(stub *shim.ChaincodeStub) error {
    fmt.Println("createTableAwards()")
    var columnDefs []*shim.ColumnDefinition
    columnUserIdDef     := shim.ColumnDefinition{Name: "UserId", Type: shim.ColumnDefinition_STRING, Key: true}
    columnAwardIdDef    := shim.ColumnDefinition{Name: "AwardId", Type: shim.ColumnDefinition_STRING, Key: true}
    columnPrizeDef      := shim.ColumnDefinition{Name: "Prize", Type: shim.ColumnDefinition_STRING, Key: false}
    columnLabelDef      := shim.ColumnDefinition{Name: "Label", Type: shim.ColumnDefinition_STRING, Key: false}
    columnDateDef       := shim.ColumnDefinition{Name: "Date", Type: shim.ColumnDefinition_STRING, Key: false}
    columnPictureUrlDef := shim.ColumnDefinition{Name: "PictureUrl", Type: shim.ColumnDefinition_STRING, Key: false}
    columnPictureMd5Def := shim.ColumnDefinition{Name: "PictureMd5", Type: shim.ColumnDefinition_STRING, Key: false}

    columnDefs = append(columnDefs, &columnUserIdDef)
    columnDefs = append(columnDefs, &columnAwardIdDef)
    columnDefs = append(columnDefs, &columnPrizeDef)
    columnDefs = append(columnDefs, &columnLabelDef)
    columnDefs = append(columnDefs, &columnDateDef)
    columnDefs = append(columnDefs, &columnPictureUrlDef)
    columnDefs = append(columnDefs, &columnPictureMd5Def)

    if err := stub.CreateTable(awardsTblName, columnDefs); err != nil {
        msg := fmt.Sprintf("Error in CreateTable: %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    return nil;
}

func (t *SimpleChaincode) getTable(stub *shim.ChaincodeStub,tblName string) ([]byte, error) {
    fmt.Printf("getTable(%s)\n",tblName)

    table, err := stub.GetTable(tblName)
    if err != nil {
        msg := fmt.Sprintf("Error in GetTable(%s): %s", tblName,err)
        fmt.Println(msg)
        return nil, errors.New(msg)
    }
    fmt.Printf("table '%v', %d column(s):\n",table.Name,len(table.ColumnDefinitions));
    for i,col := range table.ColumnDefinitions {
        var isKey = ""
        if col.Key {
            isKey = ", key"
        }
        fmt.Printf("  [%d] '%s' %v%v\n",i,col.Name,col.Type,isKey)
    }

    tableDefBytes, err := json.Marshal(table)
    if err != nil  {
        msg := "Error marshalling table definition "
        fmt.Println(msg)
        return nil, errors.New(msg)
    }
    fmt.Printf("Marshall(table) -> %v\n",tableDefBytes)
    return tableDefBytes,nil
}

func (t *SimpleChaincode) deleteTable(stub *shim.ChaincodeStub,tblName string) error {
    fmt.Printf("deleteTable(%s)\n",tblName)

    if err := stub.DeleteTable(tblName); err != nil {
        msg := fmt.Sprintf("Error in DeleteTable(%s): %s", tblName,err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    return nil;
}

func (t *SimpleChaincode) insertRow(stub *shim.ChaincodeStub, tblName string, nbCols int, args []string) error {
    fmt.Printf("insertRow(...,%s,%d,%v)\n",tblName,nbCols,args)
    if len(args) != nbCols {
        fmt.Printf("Error: insertRow called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("insertRow has "+strconv.Itoa(nbCols)+" arguments")
    }

    var columns []*shim.Column
    for _,arg := range args {
        col := shim.Column{Value: &shim.Column_String_{String_: arg}}   // Note: All cols contain strings
        columns = append(columns, &col)
    }

    row := shim.Row{Columns: columns}
    ok, err := stub.InsertRow(tblName, row)
    if err != nil {
        msg := fmt.Sprintf("insertRow operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    if !ok {
        msg := "insertRow operation failed. Row with given key already exists"
        fmt.Println(msg)
        return errors.New(msg)
    }
    return nil
}

func (t *SimpleChaincode) deleteRow(stub *shim.ChaincodeStub, tblName string, nbCols int, args []string) error {
    fmt.Printf("deleteRow(...,%s,%d,%v)\n",tblName,nbCols,args)
    if len(args) != nbCols {
        fmt.Printf("Error: deleteRow called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("deleteRow has "+strconv.Itoa(nbCols)+" arguments")
    }

    var columns []shim.Column
    for _,arg := range args {
        col := shim.Column{Value: &shim.Column_String_{String_: arg}}   // Note: All cols contain strings
        columns = append(columns, col)
    }

    err := stub.DeleteRow(tblName, columns)
    if err != nil {
        msg := fmt.Sprintf("deleteRow operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    return nil
}

func (t *SimpleChaincode) deleteUsersAwards(stub *shim.ChaincodeStub, uidKey string) error {
    fmt.Printf("deleteUsersAwards(...,%v)\n",uidKey)
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: uidKey}}
    columns = append(columns, col1)
    rowChannel, err := stub.GetRows(awardsTblName, columns)
    if err != nil {
        msg := fmt.Sprintf("getRows '%s' failed, %s", awardsTblName, err)
        fmt.Println(msg)
        return errors.New(msg)
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
        fmt.Println("No award to delete")
        return nil
    }

    for i,_ := range rows {
        userId  := rows[i].Columns[0].GetString_()
        awardId := rows[i].Columns[1].GetString_()
        var argsBis []string;
        argsBis = append(argsBis,userId)
        argsBis = append(argsBis,awardId)
        if err := t.deleteRow(stub,awardsTblName,2,argsBis); err != nil {
            return err
        }
    }

    return nil
}

func (t *SimpleChaincode) replaceRow(stub *shim.ChaincodeStub, tblName string, nbCols int, args []string) error {
    fmt.Printf("replaceRow(...,%s,%d,%v)\n",tblName,nbCols,args)
    if len(args) != nbCols {
        fmt.Printf("Error: replaceRow called with %d argument(s) (%v)\n",len(args),args)
        return errors.New("replaceRow has "+strconv.Itoa(nbCols)+" arguments")
    }

    var columns []*shim.Column
    for _,arg := range args {
        col := shim.Column{Value: &shim.Column_String_{String_: arg}}   // Note: All cols contain strings
        columns = append(columns, &col)
    }

    row := shim.Row{Columns: columns}
    ok, err := stub.ReplaceRow(tblName, row)
    if err != nil {
        msg := fmt.Sprintf("replaceRow operation failed. %s", err)
        fmt.Println(msg)
        return errors.New(msg)
    }
    if !ok {
        return errors.New("replaceRow operation failed. Row with key '"+args[0]+"' does not exist")
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
    err := stub.DelState(key)    // Remove the key from chaincode state
    if err != nil {
        fmt.Println("Error: del state " + key + " => " + err.Error())
        return nil, err
    }
    fmt.Println("Del state '" + key + "' => OK")
    return nil, nil
}

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
        case "createTableAwards" :
            return nil, t.createTableAwards(stub);
        case "deleteTableUsers" :
            return nil, t.deleteTable(stub,usersTblName);
        case "deleteTableAwards" :
            return nil, t.deleteTable(stub,awardsTblName);
        case "insertRowUsers" :
            return nil, t.insertRow(stub, usersTblName, 7, args)
        case "insertRowAwards" :
            return nil, t.insertRow(stub, awardsTblName, 7, args)
        case "deleteRowUsers" :
            if err := t.deleteRow(stub, usersTblName, 1, args); err != nil {
                return nil, err;
            }
            return nil, t.deleteUsersAwards(stub,args[0]);
        case "deleteRowAwards" :
            return nil, t.deleteRow(stub, awardsTblName, 2, args)
        case "replaceRowUsers" :
            return nil, t.replaceRow(stub, usersTblName, 7, args)
        case "replaceRowAwards" :
            return nil, t.replaceRow(stub, awardsTblName, 7, args)
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
        case "getRowAwards" :
            if len(args) < 2 {
                return nil, errors.New("Incorrect number of arguments. Expecting 2 or more arguments")
            }
            return t.getRowAwards(stub,args[0],args[1]);
        case "getRowsByUIdAwards" :
            return t.getRowsByUIdAwards(stub,args[0]);
        case "getTable" :
            return t.getTable(stub,args[0]);
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

    row, err := stub.GetRow(usersTblName, columns)
    if err != nil {
        msg := fmt.Sprintf("getRowUsers failed, %s", err)
        fmt.Println(msg)
        return nil, errors.New(msg)
    }

    if len(row.Columns) == 0 {
        fmt.Println("No matching rows")
        return nil,nil
    }

    userId     := row.Columns[0].GetString_()    // Note: We should have userId == keyUId
    email      := row.Columns[1].GetString_()
    firstName  := row.Columns[2].GetString_()
    lastName   := row.Columns[3].GetString_()
    fbId       := row.Columns[4].GetString_()
    pictureUrl := row.Columns[5].GetString_()
    pictureMd5 := row.Columns[6].GetString_()

    user := User{UserId: userId, Email: email, FirstName: firstName, LastName: lastName, FbId: fbId, PictureUrl: pictureUrl, PictureMd5: pictureMd5}
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

// Get a award by UserId & AwardId
func (t *SimpleChaincode) getRowAwards(stub *shim.ChaincodeStub, keyUId, keyDId string) ([]byte, error) {
    fmt.Printf("getRowAwards(...,'%s','%s')\n",keyUId,keyDId)
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: keyUId}}
    columns = append(columns, col1)
    col2 := shim.Column{Value: &shim.Column_String_{String_: keyDId}}
    columns = append(columns, col2)

    row, err := stub.GetRow(awardsTblName, columns)
    if err != nil {
        msg := fmt.Sprintf("getRowAwards failed, %s", err)
        fmt.Println(msg)
        return nil, errors.New(msg)
    }

    if len(row.Columns) == 0 {
        fmt.Println("No matching rows")
        return nil,nil
    }

    userId     := row.Columns[0].GetString_()    // Note: We should have userId == keyUId
    awardId    := row.Columns[1].GetString_()    // Note: We should have awardId == keyDId
    prize      := row.Columns[2].GetString_()
    label      := row.Columns[3].GetString_()
    date       := row.Columns[4].GetString_()
    pictureUrl := row.Columns[5].GetString_()
    pictureMd5 := row.Columns[6].GetString_()

    award := Award{AwardId: awardId, Prize: prize, UserId: userId, Label: label, Date: date, PictureUrl: pictureUrl, PictureMd5: pictureMd5 }
    fmt.Printf("award=%v\n",award)

    // Marshal the structure
    awardBytes, err := json.Marshal(&award)
    if err != nil  {
        msg := "Error marshalling award " + awardId
        fmt.Println(msg)
        return nil, errors.New(msg)
    }
    fmt.Printf("Marshall(award) -> %v\n",awardBytes)
    return awardBytes,nil
}

// Get all awards by UserId
func (t *SimpleChaincode) getRowsByUIdAwards(stub *shim.ChaincodeStub, keyUId string) ([]byte, error) {
    fmt.Printf("getRowsByUIdAwards(...,'%s')\n",keyUId)
    var columns []shim.Column
    col1 := shim.Column{Value: &shim.Column_String_{String_: keyUId}}
    columns = append(columns, col1)

    rowChannel, err := stub.GetRows(awardsTblName, columns)
    if err != nil {
        msg := fmt.Sprintf("getRows '%s' failed, %s", awardsTblName, err)
        fmt.Println(msg)
        return nil, errors.New(msg)
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

    var awards []Award
    for i,_ := range rows {
        userId     := rows[i].Columns[0].GetString_()
        awardId    := rows[i].Columns[1].GetString_()
        prize      := rows[i].Columns[2].GetString_()
        label      := rows[i].Columns[3].GetString_()
        date       := rows[i].Columns[4].GetString_()
        pictureUrl := rows[i].Columns[5].GetString_()
        pictureMd5 := rows[i].Columns[6].GetString_()

        award := Award{UserId: userId, AwardId: awardId, Prize: prize, Label: label, Date: date,
                       PictureUrl: pictureUrl, PictureMd5: pictureMd5}
        fmt.Printf("award[%v]=%v\n",i,award)
        awards = append(awards,award)
    }

    // Marshal the array
    awardsBytes, err := json.Marshal(&awards)
    if err != nil  {
        msg := "Error marshalling awards "
        fmt.Println(msg)
        return nil, errors.New(msg)
    }
    fmt.Printf("Marshall(awards) -> %v\n",awardsBytes)
    return awardsBytes,nil
}

func main() {
    err := shim.Start(new(SimpleChaincode))
    if err != nil {
        fmt.Printf("Error starting Simple chaincode: %s", err)
    }
}

