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
  scheme: 'https',
  host: process.env.WEAVIATE_TOKEN,
  headers: {'X-OpenAI-Api-Key': process.env.OPEN_AI_TOKEN},
  authClientSecret: new weaviate.AuthUserPasswordCredentials({
    username: process.env.USER_ID,
    password: process.env.USER_PASSWORD
  })
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
      .withFields(["index","holder","production","episodenumber","part","aititle","aisubtitle","aikeywords","bibleverses","biblecharacters","bibleconcepts","famouspeople","booksmentioned","lifeissues","biblicallesson","questionanswered","bookofthebible","aifirstgrader","aisimple","aielegant","aicreative","aibiblical","aicasual","aiformal","ainewsanchor","ailoving","importantphrase","christiantopics","biblicalconcepts","describingwords","biblereferences","biblephrases","aiphdstudent","booknumber","episodetitle","filename","paragraph","summary","productionimage","publisher","publisherimage","testament","type","booktitle","_additional { certainty }"])
      .withNearText({
        concepts: [text],
        certainty: 0.7
      })
      .withLimit(20)
      .do()    
      .then(info => {
        // console.log(info['data']['Get']['Document'][0]['_additional']['certainty'])
        res.render(path.join(initial_path, "search.ejs"),{obj_info:info['data']['Get']['Document']});
      })
      .catch(err => {
        console.error(err)
      })
})

app.get('/:idx',(req,res)=>{
  const {idx}=req.params;
  console.log(idx)
  client.graphql
      .get()
      .withClassName('Document')
      .withFields(["_additional{id}"])
      .withWhere({
        operator: 'Equal',
        path: ['index'],
        valueNumber: parseInt(idx),
      })
      .withLimit(1)
      .do()    
      .then(info => {
          id = info['data']['Get']['Document'][0]['_additional']['id']
            client.graphql
            .get()
            .withClassName('Document')
            .withFields(["index","holder","production","episodenumber","part","aititle","aisubtitle","aikeywords","bibleverses","biblecharacters","bibleconcepts","famouspeople","booksmentioned","lifeissues","biblicallesson","questionanswered","bookofthebible","aifirstgrader","aisimple","aielegant","aicreative","aibiblical","aicasual","aiformal","ainewsanchor","ailoving","importantphrase","christiantopics","biblicalconcepts","describingwords","biblereferences","biblephrases","aiphdstudent","booknumber","episodetitle","filename","paragraph","summary","productionimage","publisher","publisherimage","testament","type","booktitle","_additional { certainty }"])
            .withNearObject({id: id})
            .do()
            .then(info2 => {
              info2['data']['Get']['Document'].shift()
              res.render(path.join(initial_path, "search.ejs"),{obj_info:info2['data']['Get']['Document']});
            })
            .catch(err => {
              console.error(err)
            });
      })
      .catch(err => {
        console.error(err)
      })
})

app.get('/bible/:idx',(req,res)=>{
  const {idx}=req.params;
  console.log(idx)
  client.graphql
      .get()
      .withClassName('Document')
      .withFields(["_additional{id}"])
      .withWhere({
        operator: 'Equal',
        path: ['index'],
        valueNumber: parseInt(idx),
      })
      .withLimit(1)
      .do()    
      .then(info => {
          id = info['data']['Get']['Document'][0]['_additional']['id']
            client.graphql
            .get()
            .withClassName('Document')
            .withFields(["index","holder","production","episodenumber","part","aititle","aisubtitle","aikeywords","bibleverses","biblecharacters","bibleconcepts","famouspeople","booksmentioned","lifeissues","biblicallesson","questionanswered","bookofthebible","aifirstgrader","aisimple","aielegant","aicreative","aibiblical","aicasual","aiformal","ainewsanchor","ailoving","importantphrase","christiantopics","biblicalconcepts","describingwords","biblereferences","biblephrases","aiphdstudent","booknumber","episodetitle","filename","paragraph","summary","productionimage","publisher","publisherimage","testament","type","booktitle","_additional { certainty }"])
            .withWhere({
              operator: 'GreaterThan',
              path: ['index'],
              valueNumber: 99999900,
            })
            .withNearObject({id: id})
            .do()
            .then(info2 => {
              info2['data']['Get']['Document'].shift()
              res.render(path.join(initial_path, "bible_results.ejs"),{obj_info:info2['data']['Get']['Document']});
            })
            .catch(err => {
              console.error(err)
            });
      })
      .catch(err => {
        console.error(err)
      })
})
app.get('/all/:text', (req, res) => {
  const {text}=req.params;
  console.log(text)
  client.graphql
      .get()
      .withClassName('Document')
      .withFields(["index","holder","production","episodenumber","part","aititle","aisubtitle","aikeywords","bibleverses","biblecharacters","bibleconcepts","famouspeople","booksmentioned","lifeissues","biblicallesson","questionanswered","bookofthebible","aifirstgrader","aisimple","aielegant","aicreative","aibiblical","aicasual","aiformal","ainewsanchor","ailoving","importantphrase","christiantopics","biblicalconcepts","describingwords","biblereferences","biblephrases","aiphdstudent","booknumber","episodetitle","filename","paragraph","summary","productionimage","publisher","publisherimage","testament","type","booktitle","_additional { certainty }"])
      .withWhere({
        operator: 'LessThan',
        path: ['index'],
        valueNumber: 99999900,
      })
      .withNearText({
        concepts: [text],
        certainty: 0.7
      })
      .withLimit(20)
      .do()    
      .then(info => {
        // console.log(info['data']['Get']['Document'][0]['_additional']['certainty'])
        res.send(info['data']['Get']['Document']);
      })
      .catch(err => {
        console.error(err)
      })
})
app.get('/only_bible/:text', (req, res) => {
  const {text}=req.params;
  console.log(text)
  client.graphql
      .get()
      .withClassName('Document')
      .withFields(["index","holder","production","episodenumber","part","aititle","aisubtitle","aikeywords","bibleverses","biblecharacters","bibleconcepts","famouspeople","booksmentioned","lifeissues","biblicallesson","questionanswered","bookofthebible","aifirstgrader","aisimple","aielegant","aicreative","aibiblical","aicasual","aiformal","ainewsanchor","ailoving","importantphrase","christiantopics","biblicalconcepts","describingwords","biblereferences","biblephrases","aiphdstudent","booknumber","episodetitle","filename","paragraph","summary","productionimage","publisher","publisherimage","testament","type","booktitle","_additional { certainty }"])
      .withWhere({
        operator: 'GreaterThan',
        path: ['index'],
        valueNumber: 99999900,
      })
      .withNearText({
        concepts: [text],
        certainty: 0.7
      })
      .withLimit(20)
      .do()    
      .then(info => {
        // console.log(info['data']['Get']['Document'][0]['_additional']['certainty'])
        res.send(info['data']['Get']['Document']);
      })
      .catch(err => {
        console.error(err)
      })
})
app.get('/get_recommendation/:idx',(req,res)=>{
  const {idx}=req.params;
  console.log(idx)
  client.graphql
      .get()
      .withClassName('Document')
      .withFields(["_additional{id}"])
      .withWhere({
        operator: 'Equal',
        path: ['index'],
        valueNumber: parseInt(idx),
      })
      .withLimit(1)
      .do()    
      .then(info => {
          id = info['data']['Get']['Document'][0]['_additional']['id']
          console.log(id)
            client.graphql
            .get()
            .withClassName('Document')
            .withFields(["index","holder","production","episodenumber","part","aititle","aisubtitle","aikeywords","bibleverses","biblecharacters","bibleconcepts","famouspeople","booksmentioned","lifeissues","biblicallesson","questionanswered","bookofthebible","aifirstgrader","aisimple","aielegant","aicreative","aibiblical","aicasual","aiformal","ainewsanchor","ailoving","importantphrase","christiantopics","biblicalconcepts","describingwords","biblereferences","biblephrases","aiphdstudent","booknumber","episodetitle","filename","paragraph","summary","productionimage","publisher","publisherimage","testament","type","booktitle","_additional { certainty }"])
            .withNearObject({id: id})
            .do()
            .then(info2 => {
              info2['data']['Get']['Document'].shift()
              res.send(info2['data']['Get']['Document']);
            })
            .catch(err => {
              console.error(err)
            });
      })
      .catch(err => {
        console.error(err)
      })
})
app.listen(process.env.PORT || 3000,
  () => console.log(`The app is running on: http://localhost:${process.env.PORT || 3000}`)
) 