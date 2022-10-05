const { Client } = require("@opensearch-project/opensearch");
const cheerio = require('cheerio');
const axios = require('axios');

exports.handler = function(event, context) {
    event.Records.forEach(function(record) {
        var payload = Buffer.from(record.kinesis.data, 'base64').toString('ascii');
        console.log('Decoded payload:', payload);
        var payloadJson = JSON.parse(payload);
        console.log('keys', Object.keys(payloadJson));
        console.log('values', Object.values(payloadJson));
        console.log(`Ingesting data: ${payloadJson.length}`);
        var index_name = "branddataprocessor";
        
        var client = new Client({
                node: 'https://vpc-brand-protection-es-1-2tqcgmdeohhml3hcvjyo6go4va.us-east-1.es.amazonaws.com/',
        });
        const resp =  client.indices.exists({
        index: index_name
 }).then(function (exists) {
  if (!exists) {
   console.log('Creating index %s ...', index_name)
   return client.indices.create({
    index: index_name
   }).then(function (r) {
    console.log('Index %s created:', index_name, r)
    return Promise.resolve(r)
   })
  } else {
   console.log('Index %s exists', index_name)
   return Promise.resolve()
  }
 }).catch(function (err) {
  console.log('Unable to create index index %s ...', index_name, err)
  return Promise.reject(err)
 })
   console.log("Creating index:");
      
    // Add a document to the index.
   var document = {
      "doc": payloadJson,
      "doc_as_upsert": true
   };

   var id = "1";
   var response = client.index({
     id: id,
     index: index_name,
     body: document,
     refresh: true,
   });

   console.log("Adding document:");
   console.log(JSON.stringify(response.body));
   
   
   // crawl
        var crawlUrl = 'http://webcode.me';
        axios.get(crawlUrl).then(resp => {
          console.log('resp');
          console.log(resp);
          var responseData=resp;
          const foundURLs = [] // Discovered URLs from the page
          console.log('crawl started: ', crawlUrl)
          const $ = cheerio.load(responseData.data, {
    withDomLvl1: true,
    normalizeWhitespace: false,
    xmlMode: false,
    decodeEntities: false
  })

  // Iterate through all hrefs on the crawled page
  $('a').each((i, link) => {
    const linkUrl = $(link).attr('href')
    console.log(i, linkUrl);
    foundURLs.push(linkUrl)
  });
        }, (error) => {
        console.log(error);
        });
        
  

  });
};
