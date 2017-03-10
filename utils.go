package sharding

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"reflect"
	"strings"
	"strconv"
)

const (
	DriverName = "mysql"
	StructFieldTagName  = "orm"
	StructFieldTagDelim = ";"
	ColumnDelim = ","
	PrepareDelim = "?"
	TableQuote = "`"
	Sep = "` = ? AND `"
	ForUp = " FOR UPDATE "
)

var (
	dbConn map[string]*sql.DB
	models map[string]*modelInfo
	supportTag = map[string]int{
		"pk":           1,
		"uk":       1,
		"column":       2,
	}
)

func init() {
	dbConn = make(map[string]*sql.DB)
	models = make(map[string]*modelInfo)
}

//create an orm with model  
func NewOrm(md interface{}) (Eorm, error) {
    o := new(orm)
    o.isTx = false
    v := reflect.ValueOf(md).MethodByName("DB")
    var err error
    if v.IsValid() {
        sAlias := v.Call([]reflect.Value{})
        err = o.Using(sAlias[0].String())
    } else {
        err = fmt.Errorf("The func `DB` undefine in `%s`", reflect.Indirect(reflect.ValueOf(md)).Type())
    }

    return o, err
}

//build db connection
func RegisterDataBase(aliasName, dataSource string, params ...int) error {
	var (
		err error
		db  *sql.DB
	)

	//验证是否已注册
	if _, ok := dbConn[aliasName]; ok {
		return fmt.Errorf("alias name `%s` have been registered", aliasName)
	} else {
		db, err = sql.Open(DriverName, dataSource)
		if err != nil {
			err = fmt.Errorf("register db open `%s` , %s", aliasName, err.Error())
			goto end
		}	
		dbConn[aliasName] = db
	}

	//联通新验证
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("register db Ping `%s`, %s", aliasName, err.Error())
		goto end
	}

	for i, v := range params {
		switch i {
		case 0:
			db.SetMaxIdleConns(v)
		case 1:
			db.SetMaxOpenConns(v)
		}
	}

end:
	if err != nil {
		if db != nil {
			db.Close()
		}
	}

	return err
}

//must register modelinfo before used
func RegisterModel(md interface{}) {
	fullName := getFullName(md)
	if _,ok := models[fullName]; ok {
		panic(fmt.Errorf("<sharding.RegisterModel> model `%s` repeat register ", fullName))
	} 

	model := &modelInfo{}
	model.fullName = fullName
	model.name = getName(md)
	model.fields = make(map[string]*fieldInfo)
	model.c2n = make(map[string]string)
	model.n2c = make(map[string]string)
	model.columns = ``

	ind := reflect.Indirect(reflect.ValueOf(md))

	var (
		attrs     map[string]bool
		tags      map[string]string
		sf  	  reflect.StructField
	)
	for i := 0; i < ind.NumField(); i++ {
		sf = ind.Type().Field(i)
		parseStructTag(sf.Tag.Get(StructFieldTagName), &attrs, &tags)
		fi := new(fieldInfo)
		fi.fieldIndex = i
		fi.fieldType = getFieldType(ind.Field(i))
		fi.name = sf.Name

		if v,ok := tags["column"]; ok {
			fi.colume = v
		} else {
			fi.colume = sf.Name
		}

		model.columns += TableQuote + fi.colume + TableQuote + ColumnDelim

		if v,ok := attrs["pk"]; ok && v {
			fi.pk = true
			model.pk = fi.colume
		}

		if v,ok := attrs["uk"]; ok && v {
			fi.uk = true
			model.uk = fi.colume
		}

		model.fields[fi.colume] = fi
		model.c2n[fi.colume] = fi.name
		model.n2c[fi.name] = fi.colume
	}
	model.columns = strings.TrimRight(model.columns, ColumnDelim)

	if len(model.pk) == 0 && len(model.uk) ==0 {
		panic(fmt.Errorf("<sharding.RegisterModel> model `%s` must have primary or unique key  ", fullName))
	}

	models[fullName] = model
}

//parse table struct setting
func parseStructTag(data string, attrs *map[string]bool, tags *map[string]string) {
	attr := make(map[string]bool)
	tag := make(map[string]string)
	for _, v := range strings.Split(data, StructFieldTagDelim) {
		v = strings.TrimSpace(v)
		if t := strings.ToLower(v); supportTag[t] == 1 {
			attr[t] = true
		} else if i := strings.Index(v, "("); i > 0 && strings.Index(v, ")") == len(v)-1 {
			name := t[:i]
			if supportTag[name] == 2 {
				v = v[i+1 : len(v)-1]
				tag[name] = v
			}
		}
	}
	*attrs = attr
	*tags = tag
}

// get table name. method
func getTableName(md interface{}) string {
	val := reflect.ValueOf(md)
	fun := val.MethodByName("Table")
	if fun.IsValid() {
		vals := fun.Call([]reflect.Value{})
		if len(vals) > 0 {
			val := vals[0]
			if val.Kind() == reflect.String {
				return val.String()
			}
		}
	}
	
	return reflect.Indirect(reflect.ValueOf(md)).Type().Name()
}
func getName(md interface{}) string {
	return reflect.Indirect(reflect.ValueOf(md)).Type().Name()
}
func getFullName(md interface{}) string {
	typ := reflect.Indirect(reflect.ValueOf(md)).Type()
	return typ.PkgPath() + "." + typ.Name()
}

// return field type as type constant from reflect.Value
func getFieldType(val reflect.Value) (ft int) {
	switch val.Type() {
		case reflect.TypeOf(new(int64)):
			ft = TypeBigIntegerField
		case reflect.TypeOf(new(uint64)):
			ft = TypePositiveBigIntegerField
		case reflect.TypeOf(new(float64)):
			ft = TypeFloatField
		case reflect.TypeOf(new(string)):
			ft = TypeTextField
		default:
			elm := reflect.Indirect(val)
			switch elm.Kind() {
				case reflect.Int64 :
					ft = TypeBigIntegerField
				case reflect.Uint64:
					ft = TypePositiveBigIntegerField
				case reflect.Float64:
					ft = TypeFloatField
				case reflect.String:
					ft = TypeTextField
				default:
					panic(fmt.Errorf("unsupport field type %s, may be miss setting tag", val))
		}
	}
	return
}
func setFieldValue(fi *fieldInfo, val interface{}, field reflect.Value) {
	switch fi.fieldType{
	case TypeBigIntegerField:
		v := val.(int64)
		field.Set(reflect.ValueOf(v))
	case TypePositiveBigIntegerField:
		v := val.(uint64)
		field.Set(reflect.ValueOf(v))
	case TypeFloatField:
		v := val.(float64)
		field.Set(reflect.ValueOf(v))
	default:
		field.SetString(val.(string))
	}
}
func convertValueFromDB(fi *fieldInfo, val interface{}) interface{} {
	var value interface{}
	var str *string
	switch v := val.(type) {
	case []byte:
		s := string(v)
		str = &s
	case string:
		s := string(v)
		str = &s
	}

	switch fi.fieldType {
	case TypeBigIntegerField:
		if str == nil {
			s := string(ToStr(val))
			str = &s
		}
		v, _ := strconv.ParseInt(*str, 10, 64)
		value = v
	case TypePositiveBigIntegerField:
		if str == nil {
			s := string(ToStr(val))
			str = &s
		}
		v, _ := strconv.ParseUint(*str, 10, 64)
		value = v
	case TypeFloatField:
		if str == nil {
			switch v := val.(type) {
			case float64:
				value = v
			default:
				s := string(ToStr(v))
				str = &s
			}
		}
		if str != nil {
			v, _ := strconv.ParseFloat(*str, 64)
			value = v
		}
	default:
		value = *str
	}

	return value
}

type argInt []int
// get int by index from int slice
func (a argInt) Get(i int, args ...int) (r int) {
	if i >= 0 && i < len(a) {
		r = a[i]
	}
	if len(args) > 0 {
		r = args[0]
	}
	return
}

// ToStr interface to string
func ToStr(value interface{}, args ...int) (s string) {
	switch v := value.(type) {
	case bool:
		s = strconv.FormatBool(v)
	case float32:
		s = strconv.FormatFloat(float64(v), 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 32))
	case float64:
		s = strconv.FormatFloat(v, 'f', argInt(args).Get(0, -1), argInt(args).Get(1, 64))
	case int:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int8:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int16:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int32:
		s = strconv.FormatInt(int64(v), argInt(args).Get(0, 10))
	case int64:
		s = strconv.FormatInt(v, argInt(args).Get(0, 10))
	case uint:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint8:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint16:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint32:
		s = strconv.FormatUint(uint64(v), argInt(args).Get(0, 10))
	case uint64:
		s = strconv.FormatUint(v, argInt(args).Get(0, 10))
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		s = fmt.Sprintf("%v", v)
	}
	return s
}