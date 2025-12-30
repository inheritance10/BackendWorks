const express = require('express');
const axios = require('axios');

const app = express();
const PORT = 3000;


// I/O ağırlıklı endpoint
app.get('/ping', async (req, res) => {
    res.send('PONG!!!');
});


//Cpu isi go servisine gönderilir
app.get('/cpu', async (req, res) => {
    const response = await axios.get("http://service-go:4000/cpu");
    res.send(response.data);
});


//Asyn job (worker)

app.get('/job', async (req, res) => {
    await axios.get('http://worker-go:5000/job');
    res.send('Job sent to worker');
});


app.listen(PORT, () => {
  console.log("Node Gateway running on 3000");
});
