package xbuilder_ozzo_dbx

import (
	"github.com/go-xbuilder/config"
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

var c config.Config
var xBuilder *XBuilder

func init() {
	c = config.NewConfig("xbuilder.yml")
	xBuilder = NewBuilder(c)
}

func TestXBuilder_OrderByColumns(t *testing.T) {
	testCases := []struct {
		tag           string
		table         string
		params        url.Values
		defaultOrder  []string
		expectedValue []string
	}{
		{"t1", "user", url.Values{"orderBy": []string{"id.asc"}}, []string{}, []string{"user.id ASC"}},
		{"t2", "user", url.Values{"orderBy": []string{"idx.asc"}}, []string{}, []string{}},
		{"t3", "user", url.Values{"orderBy": []string{"idx.asc"}}, []string{"id DESC"}, []string{"id DESC"}},
		{"t4", "userxxx", url.Values{"orderBy": []string{"id.asc"}}, []string{}, []string{}},
	}
	for _, testCase := range testCases {
		cols := xBuilder.OrderBy(testCase.table, testCase.params, testCase.defaultOrder...)
		assert.Equal(t, testCase.expectedValue, cols, testCase.tag)
	}
}
