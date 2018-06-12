
const express = require('express');
const bodyParser = require('body-parser');
const request = require("request");
const async = require('async');
const app = express();
const logger = require('morgan');
const moment = require('moment'); 
var schedule = require('node-schedule');

const { Client } = require('pg');
const connectionString = 'postgres://postgres:Softcityplan2018@innovacity.cessmvb4skx1.sa-east-1.rds.amazonaws.com:5432/postgres'

var client_auth = "975EAD79";
var token = "a0d123d9u2eweklyf8dasdasdoej0j672bidkdsd";
var secret = "zcvcjk4fddfvnsh170fn47dbf45623ffdnd6hjs";

var rule = new schedule.RecurrenceRule();
rule.dayOfWeek = [new schedule.Range(0, 6)];
rule.hour = 16; // set at 6 am
rule.minute = 36;

var port     = process.env.PORT || 8080; // set our port
app.use(logger('dev')); 
app.use(bodyParser.urlencoded({ extended: true }))

app.use(bodyParser.json())


app.get('/', (req, res) => {
    /* var dateFilter = {
        data_de: null,
        data_ate: null
    }
    scheduleNewEmissions(dateFilter).then(resultSchedule => {
        console.log(">>>resultSchedule"); 
      }); */
    res.status(200).send('Innovacity');
});

app.listen(port, () => {
    console.log("Server is listening on port"+port);
});

function consultarDocRequest(data,callback){
    var options = { method: 'POST',
    url: 'https://api.1doc.com.br/',
    headers:
        { 'Content-Type': 'application/x-www-form-urlencoded',
            'Postman-Token': '20678118-a3c9-4fba-ab00-34acdf73234c',
            'Cache-Control': 'no-cache',
            'Content-Typ': 'application/x-www-form-urlencoded' },
    form: { data: data } };

request(options, function (error, response, body) {
    if (error){
        console.log(">>>error:",error)
        callback(error);
    }else{
        callback(body);
    }
});
}
function listEmissions(currentPage,dateFilter,callback) {
    var listEmissionsData = [
        {
            "method": "listEmissions",
            "client_auth": client_auth,
            "token": token,
            "secret": secret,
            "emissao": {
                "grupo": "3",
                "id_documento": "4",
                "data_de": dateFilter.data_de ? dateFilter.data_de : null,
				"data_ate": dateFilter.data_ate ? dateFilter.data_ate : null
            },
            "num_pagina": currentPage
        }
    ]

    console.log(listEmissionsData)
    var encoded = new Buffer(JSON.stringify(listEmissionsData)).toString('base64');


    var options = { method: 'POST',
        url: 'https://api.1doc.com.br/',
        headers:
            { 'Content-Type': 'application/x-www-form-urlencoded',
                'Cache-Control': 'no-cache',
                'Content-Typ': 'application/x-www-form-urlencoded' },
        form: { data: encoded } };

    request(options, function (error, response, body) {
        if (error){
            console.log(">>>error:",error)
            callback(error);
        }else{
            console.log(body)
            callback(body);
        }
    });
}
async function scheduleNewEmissions(dateFilter) {
    var emissoes = [];
    var emissionsSaveDb = []
    var currentPage = 1,
    lastPage = 1;
    async.whilst(function () {
        return currentPage <= lastPage;
    },function (next) {
        listEmissions(currentPage, dateFilter?dateFilter:null, function (listEmissionsRes) {
            var listEmissionsResJson = JSON.parse(listEmissionsRes);
            if(listEmissionsResJson.total <= 0 || !listEmissionsResJson.emissoes){
                return;
            }
            lastPage = listEmissionsResJson.total /30;
            listEmissionsResJson.emissoes.forEach(function(item){
                emissoes.push(item);
              });
            currentPage++;
            console.log(">>PAGE",currentPage)
            next();
            if(currentPage > lastPage){
                async.map(emissoes, consultarDoc, function (err, results) {
                    if (err) {
                        console.log("!!!!!",err)
                    }
                    var resultConsultarDoc = results;
                    async.forEach(resultConsultarDoc, function (item) {
                        if(item.numero_atendimento){
                            emissionsSaveDb.push(item)
                        }
                    })
                    const client = new Client({
                        connectionString: connectionString,
                    });
                    client.on('error', error => {
                        console.log("ERROR",error)
                        return error
                    });
                    client.connect((err, client) => {
                        if(err) {
                           console.log(">>>",err)
                           return err
                        }
                        const query = client.query(
                            buildStatement('INSERT INTO ocorrencias (endereco, position, data_abertura, origem, tipo, nome_solicitante, descricao, status_id, anexos, usuario_fiscal_id, numero_atendimento, numero_documento_solicitante, lida) VALUES ', emissionsSaveDb),
                            function (err, result) {
                                console.log("-------err",err)
                                console.log("-------result",result)
                                if(err || result.rowCount == 0) {
                                    client.end()
                                    return err
                                }else{
                                    client.end()
                                    return result
                                }
                            });
                    })
                })
            }
        })
    },
    function (err) {
        console.log(err);
        return err
    });
}


function buildStatement (insert, rows) {
    const params = []
    const chunks = []
    rows.forEach(row => {
      const valueClause = []
      Object.keys(row).forEach(p => {
        params.push(row[p])
        valueClause.push('$' + params.length)
      })
      chunks.push('(' + valueClause.join(', ') + ')')
    })
    return {
      text: insert + chunks.join(', '),
      values: params
    }
  }

function consultarDoc (data, callback){
    if(data.contem_anexo) {
        var emissaoAnexo = [
            {
                "method": "consultarDoc",
                "client_auth": client_auth,
                "token": token,
                "secret": secret,
                "emissao": {"hash": data.hash}
            }
        ];

        var encoded = new Buffer(JSON.stringify(emissaoAnexo)).toString('base64');

        consultarDocRequest(encoded, function (consultarDocRes) {
            var jsonResult = JSON.parse(consultarDocRes).emissao;
            var arrayAnexos = [];
            jsonResult.anexos.forEach(function(anexos){
                arrayAnexos.push(anexos.url)
              });
            if(jsonResult.data == '0000-00-00' || !jsonResult.data){
                jsonResult.data = new Date();
            }
            var newEmission = {
                endereco: jsonResult.endereco ? jsonResult.endereco : "",
                position: '(-27.63785, -48.68030)',
                data_abertura: jsonResult.data,
                origem: jsonResult.origem,
                tipo:jsonResult.assunto_txt,
                nome_solicitante:jsonResult.origem_pessoa,
                descricao:jsonResult.resumo,
                status_id:1,
                anexos: JSON.stringify(arrayAnexos),
                usuario_fiscal_id:null,
                numero_atendimento:jsonResult.num_formatado,
                numero_documento_solicitante:null,
                lida:false
            }
            callback(null, newEmission)
        })
    }else{
        if(data.data == '0000-00-00' || !data.data){
            data.data = new Date();
        }
        var newEmission = {
            endereco: data.endereco ? data.endereco : "",
            position: '(-27.63785, -48.68030)',
            data_abertura: data.data,
            origem: data.origem,
            tipo:data.assunto_txt,
            nome_solicitante:data.origem_pessoa,
            descricao:data.resumo,
            status_id:1,
            anexos: null,
            usuario_fiscal_id:null,
            numero_atendimento:data.num_formatado,
            numero_documento_solicitante:null,
            lida:false
        }
        return callback(null,newEmission);
    }

}

schedule.scheduleJob(rule, function(){
    console.log('scheduleJob');
    const client = new Client({
        connectionString: connectionString,
    });
    client.on('error', error => {
        console.log("ERROR",error)
        return error
    });
    client.connect((err, client) => {
        if(err) {
           console.log(">>>",err)
           return err
        }
        var sql="select * from ocorrencias ORDER  BY data_abertura DESC LIMIT 1";
        const query = client.query(sql,function (err, result) {
                if(err || result.rowCount == 0) {
                    client.end()
                    console.log(err)
                }else{
                    //client.end()
                    var resultRows =  result.rows;
                    var dateFilter = {
                        data_de: moment(resultRows[0].data_abertura).format('YYYY/MM/DD'),
                        data_ate:moment(new Date()).format('YYYY/MM/DD')
                    }
                    scheduleNewEmissions(dateFilter).then(resultSchedule => {
                        console.log(resultSchedule); 
                        sql="INSERT INTO schedule(date) VALUES ($1)"
                        client.query(sql,
                            [new Date()], function (err, resultUpdate) {
                                if (err) {
                                    client.end();
                                } else {
                                    client.end();
                                }
                            });
                      });
                }
            });
    });
});

