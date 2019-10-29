package main

import (
	"MimojaFirmwareToolkit/pkg/Common"
	"context"
	"encoding/json"
	"github.com/olivere/elastic"
	"os"
)

var Bundle MFTCommon.AppBundle

func main() {
	Bundle = MFTCommon.Init("FlashCatalog")

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", " ")

	esQuery := elastic.NewBoolQuery()
	esQuery.Must(elastic.NewTermQuery("MetaData.Vendor.keyword", "ASUS"))

	result, err := Bundle.DB.ES.Search().
		Index("imports").
		Query(esQuery).
		From(0).Size(0).
		Do(context.Background())

	if err != nil {
		Bundle.Log.WithError(err).Error("Could not get hit count")
		return
	}

	hitcount := result.TotalHits()
	Bundle.Log.Info("Found ", hitcount, " hits")
	getSize := int64(100)

	var ientry MFTCommon.ImportEntry

	i := int64(0)
	for ; i+getSize <= hitcount; i += getSize {
		Bundle.Log.Info("Getting entries ", i, " to ", i+getSize)
		result, err = Bundle.DB.ES.Search().
			Index("imports").
			Query(esQuery).
			From(int(i)).Size(int(getSize)).
			Do(context.Background())
		if err != nil {
			Bundle.Log.WithError(err).Error("Could not get hits")
			return
		}
		for _, hit := range result.Hits.Hits {
			jsonbytes, err := hit.Source.MarshalJSON()
			if err != nil {
				Bundle.Log.WithError(err).Error("Could not marshall json")
				continue
			}
			err = json.Unmarshal(jsonbytes, &ientry)
			if err != nil {
				Bundle.Log.WithError(err).Error("Could not unmarshall json into ImportEntry")
				continue
			}
			Bundle.MessageQueue.DownloadedQueue.MarshalAndSend(ientry.MetaData)
		}
	}
	if hitcount-i == 0 {
		return
	}

	Bundle.Log.Info("Getting entries ", i, " to ", hitcount-i)
	result, err = Bundle.DB.ES.Search().
		Index("imports").
		Query(esQuery).
		From(int(i)).Size(int(hitcount - i)).
		Do(context.Background())

	if err != nil {
		Bundle.Log.WithError(err).Error("Could not get hits")
		return
	}
	for _, hit := range result.Hits.Hits {
		jsonbytes, err := hit.Source.MarshalJSON()
		if err != nil {
			Bundle.Log.WithError(err).Error("Could not marshall json")
			continue
		}
		err = json.Unmarshal(jsonbytes, &ientry)
		if err != nil {
			Bundle.Log.WithError(err).Error("Could not unmarshall json into ImportEntry")
			continue
		}
		Bundle.MessageQueue.DownloadedQueue.MarshalAndSend(ientry.MetaData)
	}

}
