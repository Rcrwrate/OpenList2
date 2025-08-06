package handles

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/gin-gonic/gin"
)

type ManifestIcon struct {
	Src   string `json:"src"`
	Sizes string `json:"sizes"`
	Type  string `json:"type"`
}

type Manifest struct {
	Display  string         `json:"display"`
	Scope    string         `json:"scope"`
	StartURL string         `json:"start_url"`
	Name     string         `json:"name"`
	Icons    []ManifestIcon `json:"icons"`
}

// getBasePath returns the cleaned base path, following the same logic as static.go
func getBasePath() string {
	basePath := conf.URL.Path
	if basePath != "" {
		basePath = utils.FixAndCleanPath(basePath)
	}
	if basePath == "" {
		basePath = "/"
	}
	return basePath
}

func ManifestJSON(c *gin.Context) {
	// Get site title from settings
	siteTitle := setting.GetStr(conf.SiteTitle)
	
	// Get logo from settings, use the first line (light theme logo)
	logoSetting := setting.GetStr(conf.Logo)
	logoUrl := strings.Split(logoSetting, "\n")[0]

	// Determine scope and start_url based on CDN configuration
	var scope, startURL string
	if conf.Conf.Cdn != "" {
		// When using CDN, don't add custom subdirectory as CDN resources don't need our base path
		scope = "/"
		startURL = "/"
	} else {
		// When not using CDN, use the configured base path
		basePath := getBasePath()
		scope = basePath
		startURL = basePath
	}

	manifest := Manifest{
		Display:  "standalone",
		Scope:    scope,
		StartURL: startURL,
		Name:     siteTitle,
		Icons: []ManifestIcon{
			{
				Src:   logoUrl,
				Sizes: "512x512",
				Type:  "image/png",
			},
		},
	}

	c.Header("Content-Type", "application/json")
	c.Header("Cache-Control", "public, max-age=3600") // cache for 1 hour
	
	if err := json.NewEncoder(c.Writer).Encode(manifest); err != nil {
		utils.Log.Errorf("Failed to encode manifest.json: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate manifest"})
		return
	}
}
