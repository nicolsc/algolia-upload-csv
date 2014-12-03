var fs      = require('fs'),
    csv     = require('csv-streamify'),
    crypto  = require('crypto'),
    Algolia = require('algolia-search'),
    colors  = require('colors'),
    //params
    batchSize     = 1000, //push to algolia every n items
    //vars
    algoliaCreds  = {},
    algoliaClient,
    algoliaIndex,
    path      = '',
    encoding  = '',
    indexName     = '',
    batch         = [];
    

colors.setTheme({
  info    : 'blue',
  notice  : 'grey',
  success : 'green',
  warn    : 'yellow',
  error   : 'red'
});

var toAlgoliaItem       = function (item) {
  if (typeof item.objectID === 'undefined') {
    var alternatives = ['Id', 'ID', 'id', '_id'];
    alternatives.forEach(function(alt){
      if (typeof item[alt] !== 'undefined') {
        item.objectID = item[alt]; 
      }
    });
    if (typeof item.objectID === 'undefined') {
      //hash the item itself to generate a unique id
      item.objectID = crypto.createHash('sha1').update(JSON.stringify(item)).digest('hex');
    }
  }
  return item;
};
var pushToAlgoliaIndex  = function (index, data) {
  console.log(colors.info('Trying to push %d items'), data.length);
  
  index.saveObjects(data, function (err, res) {
      if (err) {
        console.error(colors.error('Error while pushing to Algolia : %s'), res.message);
      }
      else{
        console.log(colors.success('Algolia push successful — %s'), res);
      }
    });
};


//Args
//[node, upload.js, {fileName}, {indexName}]
if (process.argv.length < 4) {
  console.error(colors.error('Invalid syntax, please use:'));
  console.log(colors.notice('$ node upload.js path [encoding] indexNameToCreate'));
  process.exit(1);
}
//creds retrieved from env
algoliaCreds = {
  appId : process.env.ALGOLIA_APPID,
  privateKey : process.env.ALGOLIA_SECRET
};
if (!algoliaCreds.appId || !algoliaCreds.privateKey) {
  console.error(colors.error('Algolia creds not set in process env'));
  process.exit(1); 
}

//Process input
path      = process.argv[2],
encoding  = process.argv.length === 5 ? process.argv[3] : 'utf-8',
indexName     = process.argv[4] || process.argv[3];


algoliaClient = new Algolia(algoliaCreds.appId, algoliaCreds.privateKey);
algoliaIndex = algoliaClient.initIndex(indexName);

var stream = fs.createReadStream(path, {encoding:encoding}),
    parser = csv({objectMode:true, columns:true}, function(err, docs){
      if (err){ 
        console.warn(colors.warn('An error occured - %s'), err.message); 
      } 
      else {
        console.log(colors.success('processed %d docs'), docs.length);
      }
      if (batch.length){ 
        console.log(colors.notice('Push the remaining %d'), batch.length);
        pushToAlgoliaIndex(algoliaIndex, batch);
      }
                                  
    });
parser.on('readable', function () {
  batch.push(toAlgoliaItem(parser.read()));
    if (batch.length === batchSize) {
      pushToAlgoliaIndex(algoliaIndex, batch);
      batch = [];
    }
  
  
  console.log(colors.notice('Line #%d'), parser.lineNo);
});


stream.pipe(parser).pipe(process.stdout);