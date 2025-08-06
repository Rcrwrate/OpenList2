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
	// Get the base path using the same logic as static.go
	basePath := getBasePath()
	
	// Get site title from settings
	siteTitle := setting.GetStr(conf.SiteTitle)
	
	// Get logo from settings, use the first line (light theme logo)
	logoSetting := setting.GetStr(conf.Logo)
	logoUrl := strings.Split(logoSetting, "\n")[0]

	manifest := Manifest{
		Display:  "standalone",
		Scope:    basePath,
		StartURL: basePath,
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
