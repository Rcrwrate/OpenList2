package middlewares

import (
	"encoding/json"
	"net/netip"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type filter struct {
	CIDR   *netip.Prefix `json:"cidr,omitempty"`
	Path   *string       `json:"path,omitempty"`
	Method *string       `json:"method,omitempty"`
}

var filterList []*filter

func initFilterList() {
	for _, s := range conf.Conf.Log.Filter.Filters {
		f := new(filter)

		err := json.Unmarshal([]byte(s), &f)
		if err != nil {
			log.Warnf("failed to parse filter %s: %v", s, err)
			continue
		}

		if f.CIDR == nil && f.Path == nil && f.Method == nil {
			log.Warnf("filter %s is empty, skipping", s)
			continue
		}

		filterList = append(filterList, f)
		log.Debugf("added filter: %+v", f)
	}

	log.Infof("Loaded %d log filters.", len(filterList))
}

func skiperDecider(c *gin.Context) bool {
	// every filter need metch all condithon as filter match
	// so if any condithon not metch, skip this filter
	// all filters misatch, log this request

	for _, f := range filterList {
		if f.CIDR != nil {
			cip := netip.MustParseAddr(c.ClientIP())
			if !f.CIDR.Contains(cip) {
				continue
			}
		}
		if f.Path != nil {
			// match path as prefix
			if !strings.HasPrefix(c.Request.URL.Path, *f.Path) {
				continue
			}
		}

		if f.Method != nil {
			if *f.Method != c.Request.Method {
				continue
			}
		}

		return true
	}

	return false
}

func FilteredLogger() gin.HandlerFunc {
	initFilterList()

	return gin.LoggerWithConfig(gin.LoggerConfig{
		Output: log.StandardLogger().Out,
		Skip:   skiperDecider,
	})
}
