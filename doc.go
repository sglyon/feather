/*
Package feather provides an implementation of the Apache Feather file format
*/
package feather

//go:generate go run _tools/tmpl/main.go -i -data=columns.tmpldata column.go.tmpl dictencoding.go.tmpl
