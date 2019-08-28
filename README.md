# Big Data 230 Final Project

## Project Description
This project creates a real time dashboard hosted on a local server and does some basic analysis.

## Data Source
The data sources will be provided https://www.alphavantage.co/.  
Example API Call: https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=MSFT&apikey=demo
Example JSON returned: 
```
{  
    "Global Quote": {  
        "01. symbol": "MSFT",  
        "02. open": "134.8800",  
        "03. high": "135.2200",  
        "04. low": "133.5500",  
        "05. price": "134.0900",  
        "06. volume": "1126931",  
        "07. latest trading day": "2019-08-28",  
        "08. previous close": "135.7400",  
        "09. change": "-1.6500",  
        "10. change percent": "-1.2156%"  
    }  
}  
```

## Tools
The tools used in this project are:
*  Kafka through https://www.cloudkarafka.com/ 
*  Python
*  Kafka Producer/Consumer
*  Bokeh for dashboard

## Questions Answered
Does a particular stock move indepently of the market?  
Plots comparing the percent change to the SP500 and percent changed normalized by the SP500 are present in the dashboar.  
Should you buy?  
A plot of price and volume slope are presented in order to help make that decision. If the volume of trading goes down, then one would not expect the price to rise. 
