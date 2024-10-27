package hell

import (
	"danilov/pkg/models"
	"fmt"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"reflect"
)

func BornShit(fieldsInfo []models.SchemaDTO) reflect.Type {
	var fieldType reflect.Type

	fieldsLen := len(fieldsInfo)
	structFields := make([]reflect.StructField, 0, fieldsLen)

	caser := cases.Title(language.AmericanEnglish)

	for i := 0; i < fieldsLen; i++ {
		switch fieldsInfo[i].Type {
		case "int":
			fieldType = reflect.TypeOf(0)
		case "string":
			fieldType = reflect.TypeOf("")
		case "bool":
			fieldType = reflect.TypeOf(false)
		case "uint64":
			fieldType = reflect.TypeOf(uint64(0))
		case "uint8":
			fieldType = reflect.TypeOf(uint8(0))
		}

		structFields = append(structFields, reflect.StructField{
			Name: caser.String(fieldsInfo[i].Name),
			Type: fieldType,
			Tag:  reflect.StructTag(fmt.Sprintf(`yson:"%s"`, fieldsInfo[i].Name)),
		})
	}

	childStruct := reflect.StructOf(structFields)

	return childStruct
}
