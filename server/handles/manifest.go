package handles

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
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

func ManifestJSON(c *gin.Context) {
	// Get the base path
	basePath := conf.URL.Path
	if basePath == "" {
		basePath = "/"
	}
	
	// Make sure the path ends with /
	if basePath != "/" && basePath[len(basePath)-1] != '/' {
		basePath += "/"
	}

	manifest := Manifest{
		Display:  "standalone",
		Scope:    basePath,
		StartURL: basePath,
		Name:     "OpenList",
		Icons: []ManifestIcon{
			{
				Src:   "https://cdn.oplist.org/gh/OpenListTeam/Logo@main/logo/512x512.png",
				Sizes: "512x512",
				Type:  "image/png",
			},
		},
	}

	c.Header("Content-Type", "application/json")
	c.Header("Cache-Control", "public, max-age=3600") // 缓存1小时
	
	if err := json.NewEncoder(c.Writer).Encode(manifest); err != nil {
		utils.Log.Errorf("Failed to encode manifest.json: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate manifest"})
		return
	}
}