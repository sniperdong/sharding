package sharding

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrTxHasBegan    = errors.New("<orm.Begin> transaction already begin")
	ErrTxDone        = errors.New("<orm.Commit/Rollback> transaction not begin")
    ErrMultiRows     = errors.New("<QuerySeter> return multi rows")
    ErrNoRows        = errors.New("<QuerySeter> no row found")
    ErrStmtCloser    = errors.New("<QuerySeter> stmt already closed")
	ErrArgs          = errors.New("<orm> args error may be empty")
	ErrNotImplement  = errors.New("have not implement")
    ErrUnkownModel   = errors.New("no model found")
    ErrNoModel       = errors.New("<Query2Obj> must model slice ptr")
    ErrUnkownColumn  = errors.New("<Query2Obj> no mathch column found, do you use alias name")
)

type orm struct {
	db 	*sql.DB
	tx 	*sql.Tx
	aliasName string
	isTx  bool
}

func (o *orm) Using(aliasName string) error {
	if o.isTx {
		panic(fmt.Errorf("<orm.Using> transaction has been start, cannot change db"))
	}

	if db, ok := dbConn[aliasName]; ok {
		o.db = db
		o.aliasName = aliasName
	} else {
		return fmt.Errorf("<orm.Using> unknown db alias name `%s`", aliasName)
	}

	return nil
}

func (o *orm) Read(md interface{}, cols ...string) error {
	var (
		whereCols []string
		argsCols []interface{}
		model *modelInfo
		row *sql.Row
	)

	fullName := getFullName(md)
	if _, ok := models[fullName]; !ok {
		return fmt.Errorf("<orm.Read> unknown model name `%s`", fullName)
	}

	model = models[fullName]

	val := reflect.ValueOf(md)
	ind := reflect.Indirect(val)
	if len(cols) > 0 {
		whereCols = make([]string, 0, len(cols))
		argsCols = make([]interface{}, 0, len(cols))
		for _, column := range cols {
			if _,ok := model.c2n[column];!ok {
				if v,ok := model.n2c[column]; ok {
					column = v
				} else {
					return fmt.Errorf("<orm.Read> unknown column name `%s`", column)
				}
			}
			value := reflect.Indirect(ind.FieldByName(model.c2n[column])).Interface()
			whereCols, argsCols = append(whereCols, column), append(argsCols, value)
		}
	} else {
		whereCols = make([]string, 0, 1)
		argsCols = make([]interface{}, 0, 1)
		if len(model.uk) > 0 {
			value := reflect.Indirect(ind.FieldByName(model.c2n[model.uk])).Interface()
			whereCols, argsCols = append(whereCols, model.uk), append(argsCols, value)
		} else if len(model.pk) > 0 {
			value := reflect.Indirect(ind.FieldByName(model.c2n[model.pk])).Interface()
			whereCols, argsCols = append(whereCols, model.pk), append(argsCols, value)
		} else {
			return fmt.Errorf("<orm.Read> unknown condition column name `%s`", fullName)
		}
	}
	wheres := strings.Join(whereCols, Sep)

	table := getTableName(md)

	query := fmt.Sprintf("SELECT %s FROM %s%s%s WHERE %s%s%s = ?", model.columns, TableQuote, table, TableQuote, TableQuote, wheres, TableQuote)

	refs := make([]interface{}, len(model.c2n))
	for i := range refs {
		var ref interface{}
		refs[i] = &ref
	}
	if !o.isTx {
		row = o.db.QueryRow(query, argsCols...)
	} else {
		query += ForUp
		row = o.tx.QueryRow(query, argsCols...)
	}
	
	if err := row.Scan(refs...); err != nil {
		if err == sql.ErrNoRows {
			return ErrNoRows
		}
		return err
	}

	for k,_ := range model.c2n {
		fieldDes := model.fields[k]
		valRaw := reflect.Indirect(reflect.ValueOf(refs[fieldDes.fieldIndex])).Interface()

		field := ind.FieldByName(model.c2n[k])

		val := convertValueFromDB(fieldDes, valRaw)

		setFieldValue(fieldDes, val, field)
	}

	return nil
}

func (o *orm) Insert(md interface{}) (int64, error){
	var (
		err error
		insertCols []string
		argsCols []interface{}
		qmarks string
		res sql.Result
	)
	fullName := getFullName(md)
	if _, ok := models[fullName]; !ok {
		panic(fmt.Errorf("<orm.Read> unknown model name `%s`", fullName))
	}

	model := models[fullName]

	val := reflect.ValueOf(md)
	ind := reflect.Indirect(val)

	insertCols = make([]string, 0, len(model.c2n))
	argsCols = make([]interface{}, 0, len(model.c2n))
	for k,v := range model.c2n {
		if k == model.pk {
			continue
		}

		value := reflect.Indirect(ind.FieldByName(v)).Interface()
		insertCols, argsCols = append(insertCols, k), append(argsCols, value)
		qmarks += PrepareDelim + ColumnDelim
	}

	table := getTableName(md)
	sep := fmt.Sprintf("%s, %s", TableQuote, TableQuote)
	columns := strings.Join(insertCols, sep)
	qmarks = strings.TrimRight(qmarks, ColumnDelim)
	query := fmt.Sprintf("INSERT INTO  %s%s%s (%s%s%s) VALUES (%s) ", TableQuote, table, TableQuote, TableQuote, columns, TableQuote, qmarks)

	if !o.isTx {
		res, err = o.db.Exec(query, argsCols...)
	} else {
		res, err = o.tx.Exec(query, argsCols...)
	}
	if err == nil {
		return res.LastInsertId()
	}
	return 0, err
}

func (o *orm) Update(md interface{}, cols ...string) (int64, error){
	var (
		values []interface{}
		setNames []string
		err error
		whereCon string
		whereVal interface{}
		res sql.Result
	)
	fullName := getFullName(md)
	if _, ok := models[fullName]; !ok {
		err = fmt.Errorf("<orm.Update> unknown model name `%s`", fullName)
		return 0,err
	}

	model := models[fullName]
	val := reflect.ValueOf(md)
	ind := reflect.Indirect(val)

	if len(model.uk) >0 {
		whereCon = model.uk
	} else if len(model.pk) >0 {
		whereCon = model.pk
	} else {
		err = fmt.Errorf("<orm.Update> unknown unique key `%s`", fullName)
		return 0,err
	}
	whereVal = reflect.Indirect(ind.FieldByName(model.c2n[whereCon])).Interface()

	if len(cols) == 0 {
		setNames = make([]string, 0, len(model.c2n)-1)
		for column, name := range model.c2n {
			value := reflect.Indirect(ind.FieldByName(name)).Interface()
			setNames, values = append(setNames, column), append(values, value)
		}
	} else {
		setNames = make([]string, 0, len(cols))
		for _, column := range cols {
			if _,ok := model.c2n[column];!ok {
				if v,ok := model.n2c[column]; ok {
					column = v
				} else {
					err = fmt.Errorf("<orm.Update> unknown column name `%s`", column)
					return 0,err
				}
			}
			if column == model.uk || column == model.pk {
				err = fmt.Errorf("<orm.Update> can't update unique key `%s`", column)
				return 0,err
			}
			value := reflect.Indirect(ind.FieldByName(model.c2n[column])).Interface()
			setNames, values = append(setNames, column), append(values, value)
		}
	}

	table := getTableName(md)
	sep := fmt.Sprintf("%s = ?, %s", TableQuote, TableQuote)
	setColumns := strings.Join(setNames, sep)
	query := fmt.Sprintf("UPDATE %s%s%s SET %s%s%s = ? WHERE %s%s%s = ?", TableQuote, table, TableQuote, TableQuote, setColumns, TableQuote, TableQuote, whereCon, TableQuote)
	values = append(values, whereVal)
	if !o.isTx {
		res, err = o.db.Exec(query, values...)
	} else {
		res, err = o.tx.Exec(query, values...)
	}
	
	if err == nil {
		return res.RowsAffected()
	}

	return 0,err
}

func (o *orm) Delete(md interface{}) (int64, error){
	var (
		column string
		value interface{}
		res sql.Result
		err error
		num int64
	)
	fullName := getFullName(md)
	if _, ok := models[fullName]; !ok {
		panic(fmt.Errorf("<orm.Read> unknown model name `%s`", fullName))
	}

	model := models[fullName]

	ind := reflect.Indirect(reflect.ValueOf(md))

	if len(model.uk) > 0 {
		column = model.uk
		value = reflect.Indirect(ind.FieldByName(model.c2n[column])).Interface()
	} else if len(model.pk) > 0 {
		column = model.pk
		value = reflect.Indirect(ind.FieldByName(model.c2n[column])).Interface()
	} else {
		panic(fmt.Errorf("<orm.Read> unknown condition column name `%s`", fullName))
	}

	table := getTableName(md)
	query := fmt.Sprintf("DELETE FROM %s%s%s WHERE %s%s%s = ? ", TableQuote, table, TableQuote, TableQuote, column, TableQuote)

	if !o.isTx {
		res, err = o.db.Exec(query, value)
	} else {
		res, err = o.tx.Exec(query, value)
	}

	if err == nil {
		num, err = res.RowsAffected()
		if err != nil {
			return 0, err
		}
		return num, err
	}
	return 0, err
}

func (o *orm) Exec(query string, args ...interface{}) (sql.Result, error) {
	if !o.isTx {
		return o.db.Exec(query, args...)
	} else {
		return o.tx.Exec(query, args...)
	}
}

func (o *orm) Query(query string, args ...interface{}) (*sql.Rows, error){
	if !o.isTx {
		return o.db.Query(query, args...)
	} else {
		return o.tx.Query(query, args...)
	}
}

func (o *orm) Query2Obj(res interface{},query string, args ...interface{}) error {
    var (
        ok bool
        m *modelInfo
        err error
        columns []string
        col string
        i   int
    )
    v:=reflect.ValueOf(res)
    if v.Kind() != reflect.Ptr || reflect.Indirect(v).Kind() != reflect.Slice {
        return ErrNoModel
    }
    modelsType := reflect.Indirect(v).Type().Elem().String()
    if m,ok = models[modelsType]; !ok {
        return ErrUnkownModel
    }  
    inds := reflect.Indirect(v)
    slice := inds
    rows, err := o.Query(query, args...)
    if err != nil {
        return err
    }
    defer rows.Close()
    columns, err = rows.Columns()
    if err != nil {
        return err
    }
    refs := make([]interface{}, len(columns))
    for i := range refs {
        var ref interface{}
        refs[i] = &ref
    }

    for rows.Next() {
        obj := reflect.New(reflect.Indirect(v).Type().Elem())
        if err = rows.Scan(refs...); err != nil {
            return err
        }
        ind := reflect.Indirect(obj)
        for i=0; i<len(columns); i++ {
            col = columns[i]
            if _,ok = m.c2n[col]; !ok {
                return ErrUnkownColumn
            }
            fieldDes := m.fields[col]
            valRaw := reflect.Indirect(reflect.ValueOf(refs[i])).Interface()
            field := ind.FieldByName(m.c2n[col])
            val := convertValueFromDB(fieldDes, valRaw)
            setFieldValue(fieldDes, val, field)
        }
        slice = reflect.Append(slice,ind)
    }
    inds.Set(slice)
    return nil
}

func (o *orm) Begin() error {
	if o.isTx {
		return ErrTxHasBegan
	}

	tx, err := o.db.Begin()
	if err != nil {
		return err
	}
	o.isTx = true
	o.tx = tx
	return nil
}
func (o *orm) Commit() error {
	var err error
	if o.isTx == false {
		return ErrTxDone
	}
	err = o.tx.Commit()
	if err == nil {
		o.isTx = false
		o.tx = nil
		//err = o.Using(o.aliasName)
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}
func (o *orm) Rollback() error {
	var err error
	if o.isTx == false {
		return ErrTxDone
	}
	err = o.tx.Rollback()
	if err == nil {
		o.isTx = false
		o.tx = nil
		//err = o.Using(o.aliasName)
	} else if err == sql.ErrTxDone {
		return ErrTxDone
	}
	return err
}
