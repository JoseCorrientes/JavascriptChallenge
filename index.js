require('dotenv').config();
const { AWS_ACCESS_KEY,
        AWS_SECRET_ACCESS_KEY, 
        AWS_REGION, 
        AWS_ENDPOINT, 
        SERP_API_KEY, 
        AWS_TABLE_NAME
    } = process.env;
    
const SerpApi=require('google-search-results-nodejs');
const ApiKey=SERP_API_KEY;
const search = new SerpApi.GoogleSearch(ApiKey);
const AWS =require('aws-sdk');

AWS.config.update({ 
    accessKeyId: AWS_ACCESS_KEY,   
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
    region: AWS_REGION,
    endpoint: AWS_ENDPOINT
 });

 


//Funcion que crea la tabla en DynamoDB y la llena con los datos que vienen en el briefArray
 const createGoogleDynamoDB = async(briefArray)=>{            
     var scrapingDB = new AWS.DynamoDB();
     var params = {
         TableName : AWS_TABLE_NAME,
         KeySchema: [       
             { AttributeName: "Url", KeyType: "HASH"},
             { AttributeName: "Title", KeyType: "RANGE" },
            ],
            AttributeDefinitions: [       
                { AttributeName: "Url", AttributeType: "S" },
                { AttributeName: "Title", AttributeType: "S" },
            ],
            ProvisionedThroughput: {       
                ReadCapacityUnits: 10, 
                WriteCapacityUnits: 10
            }
        };
        
    scrapingDB.createTable(params, function(err, data) {
                if(err) {
                    console.log("Can't create Table. Error JSON: ", JSON.stringify(err,null,2));
                } else {
                    console.log("Table created. Table JSON description: ", JSON.stringify(data, null, 2))
                }
    })
 
    var dynamoFinalDB = new AWS.DynamoDB.DocumentClient();
    
    briefArray.forEach( function(article) {
        var params2 = {
            TableName: AWS_TABLE_NAME,
            Item: {
                "Url": article.url,
                "Title":article.title,
                "info": article.info
                }
        }
        
        dynamoFinalDB.put(params2, function(err, data) {
            if(err) {
                console.log("Article wasn't added: ", article.tile);
            } else {
                console.log("Article was added: ", article.title);
            }
        })
    });
}


//Funcion que extrae la info de GoogleSearchApi y la guarda en un arreglo resultsArray
const getInternet = () => {
    return new Promise((resolve, reject)=> {
        search.json({
            q: "covid",
            gl: "ca",
            as_qdr: "d2",
            tbm: "nws",
            num: "100"                           //defino esta cantidad como maximo, para manejarlo
        }, (data)=>{
                    let resultsArray = data.news_results;
                    resolve(resultsArray);
                });
            })
}

// Funcion principal
async function googleToAWSDynamoDB() {                
    try {
        let resultado = await getInternet();
        let briefArray = resultado.map(x=> { return {
            url: x.link,
            title: x.title,
            info: {
                publicationName: x.source,
                date: x.date
            }    
        }
        });
        createGoogleDynamoDB(briefArray);                      
    }catch (e) {
        console.log(e);
    }
}

googleToAWSDynamoDB();
