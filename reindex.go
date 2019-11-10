package main

import (
	"context"
	"encoding/json"
	"github.com/Mimoja/MFT-Common"
	"github.com/olivere/elastic"
	"math"
)

func reindex(){

	esQuery := elastic.NewBoolQuery()

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


	for i := int64(0) ; i < hitcount; i += getSize {
		getSize = int64(math.Min(float64(getSize), float64(hitcount -i)));
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
			Bundle.MessageQueue.DownloadedQueue.MarshalAndSend(MFTCommon.DownloadWrapper{ientry.MetaData, true})
		}
	}
}