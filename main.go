package main

import (
	"encoding/json"
	"github.com/Mimoja/MFT-Common"
	"os"
	"time"
)

var Bundle MFTCommon.AppBundle

func main() {
	Bundle = MFTCommon.Init("Reindexer")

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")

	ticker := time.NewTicker(time.Duration(Bundle.Config.App.Reindexer.ReindexTimeInHours) * time.Hour)

	reindex();

	func() {
		for {
			<-ticker.C
			reindex();
		}
	}()

}
