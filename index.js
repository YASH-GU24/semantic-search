const express =  require('express')
const app = express()
const path = require('path')
const bodyParser = require('body-parser');
const weaviate = require("weaviate-client");
app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'views')));
let initial_path = path.join(__dirname, "views");
const dotenv = require("dotenv");
dotenv.config();

const client = weaviate.client({
  scheme: 'http',
  host: process.env.WEAVIATE_TOKEN,
  headers: {'X-OpenAI-Api-Key': process.env.OPEN_AI_TOKEN},
});

//rendering home page
app.get('/', (req, res) => {
    res.render(path.join(initial_path, "search.ejs"),{obj_info:{}});
})

//perform query for seached text
app.post('/', (req, res) => {
    let text = req.body['searched_data'];
    console.log(text)
    client.graphql
      .get()
      .withClassName('Document')
      .withFields(['paragraph',"part","index","filename","_additional { certainty }"])
      .withNearText({
        concepts: [text],
        certainty: 0.7
      })
      .withLimit(10)
      .do()    
      .then(info => {
        // console.log(info['data']['Get']['Document'][0]['_additional']['certainty'])
        res.render(path.join(initial_path, "search.ejs"),{obj_info:info['data']['Get']['Document']});
      })
      .catch(err => {
        console.error(err)
      })
})



app.listen(process.env.PORT || 3000,
  () => console.log(`The app is running on: http://localhost:${process.env.PORT || 3000}`)
) 