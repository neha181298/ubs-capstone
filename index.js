//create table tradelist (firmclient varchar(30), Tradeid serial primary key, tradetype varchar(10), security varchar(30), quantity integer, priceperunit integer, brokername varchar(10));
//create table securities (security varchar(30) primary key, price integer);
//alter table tradelist add column timestamp varchar(20);
//--sample queries to understand data being processed
//insert into tradelist values ('firm', default, 'buy', 'Apple', '400', '10', 'B1', '3-03');
//insert into tradelist values ('client', default, 'buy', 'Apple', '3000', '10', 'B1', '3-05');

const express = require('express')
const app = express();
const knex = require('knex');
const ArrayList=require('ArrayList');
const csv = require('csv-parser');
const fs = require('fs');

const db = knex({
  client: 'pg',
  connection: {
    host : '127.0.0.1',
    user : 'postgres',
    password : 'admin',
    database : 'ubsproject'
  }
});



function calcTime(data){
	let timeString= data.split(':');
	let hour=parseInt(timeString[0]);
	let minute=parseInt(timeString[1]);
	let sec=parseInt(timeString[2]);
	let mainTime=(hour*60*60)+(minute*60)+sec;
	return mainTime;
}

//--------------------------------------------General APIs--------------------------------------------------------------

app.get('/getall', (req, res)=> {
	db.select('*').from('tradelist')
	.then(data=>{
			console.log(data);
			res.send(data);
	})
})

app.post('/insert', (req, res)=> {
  db('tradelist').insert({
    firmclient: req.body.firmclient,
    security: req.body.security,
    quantity: req.body.quantity,
    priceperunit: req.body.priceperunit,
    brokername: req.body.brokername,
    timestamp: req.body.timestamp,
    tradetype: req.body.tradetype
  })
  .then(response=>{
  	console.log("Inserting new row");
  	res.json(response);
  })
	.catch(err=>res.status(400).json('error getting data'))
})

app.get('/clear', (req, res)=> {
	db.select('*').from('tradelist').del()
	.then(data=>{
			console.log('All rows cleared from tradelist');
			res.send('All rows cleared from tradelist');
	})
  .catch(err=>res.status(400).json(err))
})


//--------------------------------------------Import Dummy Data--------------------------------------------------------------

app.get('/import', (req, res)=>{
  require('child_process').fork('csv-pg.js');
  res.send('imported data from csv file')
})

//--------------------------------------------Update Prices--------------------------------------------------------------

app.get('/updatePrices', (req, res)=> {
  var result = updatePrices();
	console.log("Updating prices");
  res.send(result);
})

app.get('/resetPrices', (req, res)=> {
  var result = resetPrices();
	console.log("Resetting prices. Uber - 100, Facebook - 50, Apple - 250");
  res.send("Resetting prices. Uber - 100, Facebook - 50, Apple - 250");
  res.send(result);
})

const updatePrices=async()=>{
	var data= await db.select('*').from('tradelist').orderBy('timestamp');
	for(let i=0;i<data.length;i++){
    var sec= await db.select('*').from('securities').where({security: data[i].security}).first();
    console.log(sec.price);
		var result = await db.select('*').from('tradelist').where({ tradeid: data[i].tradeid }).update({ priceperunit: sec.price });
    if(data[i].quantity>1000 && data[i].tradetype=='buy'){
      var result1 = await db.select('*').from('securities').where({security: data[i].security}).update({ price: Math.round(sec.price*1.1)});
    }
    if(data[i].quantity>1000 && data[i].tradetype=='sell'){
      var result1 = await db.select('*').from('securities').where({security: data[i].security}).update({ price: Math.round(sec.price*0.9)});
    }
	}
  return result;
}

const resetPrices=async()=>{
  var securities = [['Uber',100],['Facebook',50],['Apple',250]];
  for (var i =0;i<securities.length;i++){
    var result1 = await db.select('*').from('securities').where({security: securities[i][0]}).update({ price: securities[i][1]});
  }
  return
}

//--------------------------------------------Front running scenario 1--------------------------------------------------------------


app.get('/1', async(req, res) => {
	var result=await frontRunningScenario1();
	console.log("Front running scenario 1-",result)
	res.send(result)
});

//execution starts here
const frontRunningScenario1=async()=>{
	var resList=[];
	var data1= await db.select('*').from('tradelist').where({firmclient:'client',tradetype:'buy'}).where('quantity','>',1000)
	if(data1.length>0)
	{
		for(let i=0;i<data1.length;i++)
		{
			var mainTime1=await calcTime(data1[i].timestamp);
			var resultInside1= await frontRunningScenario1Inside1(data1[i], mainTime1);
			resList.push(resultInside1)
		}
	}
	return resList;
}

//function searches for firm buy trades before client buy
const frontRunningScenario1Inside1=async(data1, mainTime1)=>{
	var resList=[];
	var data2=await db.select('*').from('tradelist').where({firmclient:'firm',tradetype:'buy',security:data1.security, brokername:data1.brokername}).where('timestamp','<',data1.timestamp)
	if(data2.length>0)
	{
		for(let i=0;i<data2.length;i++)
		{
			var mainTime2=await calcTime(data2[i].timestamp);
			if((mainTime1-mainTime2)>0)
				data2.splice(i,i)
		}
	}
	if(data2.length>0)
	{
		for (let i=0;i<data2.length;i++)
		{
			var resultInside2= await frontRunningScenario1Inside2(data1, data2, mainTime1)
			resList.push(resultInside2)//creating a 2D array of trades involved in frontrunning
		}
	}
	return resList;
}

//if firm buys are found for the respective client buy, this function searches for firm sell
const frontRunningScenario1Inside2=async(data1,data2,mainTime1)=>{
	var resList=[];
	var data3= await db.select('*').from('tradelist').where({firmclient:'firm',tradetype:'sell',security:data1.security, brokername:data1.brokername}).where('timestamp','>',data1.timestamp)
	if(data3.length>0)
	{
		for(let i=0;i<data3.length;i++)
		{
			var mainTime3= await calcTime(data3[i].timestamp)
			if((mainTime3-mainTime1)>900)
				data3.splice(i,i)
		}
	}
	//creating a 1D array of trades involved in front running
	if(data3.length>0)
	{
		for(let i=0;i<data3.length;i++)
			resList.push(data3[i].tradeid)
		resList.push(data2[0].tradeid)
		resList.push(data1.tradeid)
	}
	return resList;

}

//----------------------------------------------------------------------------------------------------------------------------------














//--------------------------------------------Front running scenario 2--------------------------------------------------------------
app.get('/2', async(req, res) => {
	var result=await frontRunningScenario2();
	console.log("Front running scenario 2-",result)
	res.send(result)
});

const frontRunningScenario2=async()=>{
	var resList=[];
	var data1=await db.select('*').from('tradelist').where({firmclient:'client', tradetype:'sell'}).where('quantity','>',1000)
	if(data1.length>0)
	{
		for(let i=0;i<data1.length;i++)
		{
			var mainTime1=await calcTime(data1[i].timestamp);
			var resultInside1=await frontRunningScenario2Inside1(data1[i], mainTime1);
			resList.push(resultInside1);
		}
	}
	return resList;
}


const frontRunningScenario2Inside1=async(data1, mainTime1)=>{
	var resList=[];
	var data2=await db.select('*').from('tradelist').where({firmclient:'firm',tradetype:'sell',security:data1.security,brokername:data1.brokername}).where('timestamp','<',data1.timestamp)
	if(data2.length>0)
	{
		for(let i=0;i<data2.length;i++)
		{
			var mainTime2=await calcTime(data2[i].timestamp);
			if((mainTime1-mainTime2)>900)
				data2.splice(i,i)
		}
	}
	if(data2.length>0)
	{
		for(let i=0;i<data2.length;i++)
		{
			resList.push(data2[i].tradeid)
		}
		resList.push(data1.tradeid)
	}
	return resList;
}


//----------------------------------------------------------------------------------------------------------------------------------













//--------------------------------------------Front running scenario 3--------------------------------------------------------------
app.get('/3',async(req,res)=>{
	var result=await frontRunningScenario3();
	console.log("Front running scenario 3-",result)
	res.send(result);
})

//structure-
//firm buys at certain price and quantity
//client buys same quantity and greater price
//firm sells to customer at the greater price and same quantity

//function gets all trades where firm buys
const frontRunningScenario3=async()=>{
	var resList=[];
	var data1=await db.select('*').from('tradelist').where({firmclient:'firm',tradetype:'buy'}).where('quantity','>','1000')
	if(data1.length>0)
	{
		for(let i=0;i<data1.length;i++)
		{
			var mainTime1=await calcTime(data1[i].timestamp);
			var resultInside1=await frontRunningScenario3Inside1(data1[i],mainTime1);
			resList.push(resultInside1);
		}
	}
	return resList;
}

//function gets trades of client buying at higher price after firm buys
const frontRunningScenario3Inside1=async(data1,mainTime1)=>{
	var resultInside2;
	var data2=await db.select('*').from('tradelist').where({firmclient:'client',tradetype:'buy',security:data1.security,brokername:data1.brokername,quantity:data1.quantity}).where('priceperunit','>',data1.priceperunit).where('timestamp','>',data1.timestamp)
	if(data2.length>0)
	{
		for(let i=0;i<data2.length;i++)
		{
			var mainTime2=await calcTime(data2[i].timestamp)
			if((mainTime2-mainTime1)>900)
				data2.splice(i,i)
		}
	}
	if(data2.length>0)
	{
		for(let i=0;i<data2.length;i++)
		{
			resultInside2=await frontRunningScenario3Inside2(data1,data2[i],mainTime1,mainTime2);
		}
	}
	return resultInside2;
}

//function gets all data for firm selling to the client the same quantity of shares and at higher price
const frontRunningScenario3Inside2=async(data1,data2,mainTime1,mainTime2)=>{
	 var resList=[];
	var data3=await db.select('*').from('tradelist').where({'firmclient':'firm',tradetype:'sell',security:data2.security,brokername:data2.brokername,quantity:data2.quantity,'priceperunit':data2.priceperunit}).where('timestamp','>',data1.timestamp)
	if(data3.length>0)
	{
		for(let i=0;i<data3.length;i++)
		{
			var mainTime3=await calcTime(data3[i].timestamp);
			if((mainTime3-mainTime1)>900)
				data3.splice(i,i)
		}
	}
	if(data3.length>0)
	{
		for(let i=0;i<data3.length;i++)
		{
			resList.push(data3[i].tradeid);
		}
		resList.push(data2.tradeid)
		resList.push(data1.tradeid)
	}
	return resList;
}
//----------------------------------------------------------------------------------------------------------------------------------

app.listen(8000, () => {
  console.log('Example app listening on port 8000!')
});
