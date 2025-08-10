package cnb_releases

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	driver.RootPath
	Repo       string `json:"repo" type:"string" required:"true"`
	Token      string `json:"token" type:"string" required:"true"`
	UseTagName bool   `json:"use_tag_name" type:"bool" default:"false" help:"Use tag name instead of release name"`
}

var config = driver.Config{
	Name:        "CNB Releases",
	LocalSort:   true,
	DefaultRoot: "/",
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &CnbReleases{}
	})
}
