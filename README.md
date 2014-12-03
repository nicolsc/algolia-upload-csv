#Upload CSV dataset to Algolia


##Use

````
$ npm install
$ ALGOLIA_APPID=xxxx ALGOLIA_SECRET=xxxxx node upload.js path [encoding] indexName
`````

##How

* Create a readable stream from the csv file
* process through csv-streamify
* push to algolia index

