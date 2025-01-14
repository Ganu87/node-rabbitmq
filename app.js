const express = require('express');
const bodyParser = require('body-parser');
const routes = require('./src/routes.js');

const PORT = process.env.PORT || 3001;

const app = express();

app.use(bodyParser.json());

app.use('/api',routes);

app.listen(PORT,() => {console.log(`RabbitQ app running on Port ${PORT}`)});