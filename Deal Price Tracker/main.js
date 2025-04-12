/**
 *@author: - Nehal Sateesh Kumar (nehal.kumar@mydeal.com.au)
 */

function onOpen(){

  SpreadsheetApp.getUi()
    .createMenu('MYD Price Tracker')
    .addItem('Run Report','sendAlert')
    .addToUi()

}

function getProductData() {
  /**
   * This function is used to fetch dealid+variantid available on the Deal Price Tracker sheet 
   * This function doesn't take any parameters 
   * @param {[None]} This function takes no input parameters
   * @return {[String]} 
   
   */
  const product_data_sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Input ").getRange("C1:C");
  const product_data = product_data_sheet.getValues();
  let product_id = ''
  for(var i=1;i < product_data.length;i++){
    if(product_data[i][0] != ''){
    product_id += "'"+product_data[i][0]+"'" +","
      }};
  /* */
  return product_id.replace(/,\s*$/,"");
}

function ClearContents(sheetName){
  data_clean_sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName).clearContents()
}


function bigQueryResult() {
/**
 * This function is used to return order status and write data back to the sheet
 * @param {[None]} This function takes no input parameters
 * @return {[string]} The output is a string indicating if data was successfully written to the sheet
 * 
 *
 */
const projectID = 'myd-dev-396904';
const request = {

  query : 'with sell_price_tracker as (select * from(select date(datetimestamp) as date,dealid,variantid,sellprice,lag(sellprice,1) over(partition by dealid,ifnull(variantid,001001) order by date(datetimestamp) asc) as pre_sell_price,lag(date(datetimestamp),1) over(partition by dealid,ifnull(variantid,001001) order by date(datetimestamp) asc) as pre_sell_price_date from mydeal-bigquery.dbt_data_studio.price_history_tracker)where pre_sell_price  is not null and abs(sellprice-pre_sell_price)>0 qualify row_number() over (partition by dealid,ifnull(variantid,001001) order by date desc) = 1),promo_price_stats as (select *,lead(promotionalsellprice,1) over (partition by dealid,ifnull(variantid,001001) order by date(promotion_start_date) desc) as prev_promo_price,lead(promotion_start_date,1) over (partition by dealid,ifnull(variantid,001001) order by date(promotion_start_date) desc) as prev_promo_startdate,lead(promotion_end_date,1) over (partition by dealid,ifnull(variantid,001001) order by date(promotion_start_date) desc) as prev_promo_enddate from(select date(datetimestamp) as date,dealid,variantid,promotionalsellprice,date(promotionalstartdatetimestamp) as promotion_start_date,date(promotionalenddatetimestamp) as promotion_end_date from mydeal-bigquery.dbt_data_studio.price_history_tracker where date(promotionalstartdatetimestamp) is not null) qualify row_number() over (partition by dealid,ifnull(variantid,001001) order by promotion_start_date desc ) = 1),scheduled_promo as (select dealid,variantid,promotionalprice,date(promotionstarttime) as promotion_start_date,date(promotionendtime) as promotion_end_date from mydeal-bigquery.sql_server_rds_dbo.dealpromotionalprice where date(promotionstarttime)>= current_date("Australia/Melbourne") qualify row_number() over (partition by dealid,ifnull(variantid,001001) order by promotion_start_date desc ) = 1),crm_stock_price AS (select dl.dealid,v.variantid,concat(cast(dl.dealid as string),"-",ifnull(cast(v.variantid as string),"")) as key,case when v.sellprice is null then dl.customerprice else v.sellprice end as product_price,case when v.stocklevel is null then dl.quota else v.stocklevel end as product_stock,case when date(v.promotionstartdate) is null then date(dl.promotionstartdate) else date(v.promotionstartdate) end promostartdate,case when date(v.promotionenddate) is null then  date(dl.promotionenddate) else date(v.promotionenddate) end promoenddate from mydeal-bigquery.sql_server_rds_dbo.deal dl left join mydeal-bigquery.sql_server_rds_dbo.product as p on (dl.dealid = p.dealid)left join mydeal-bigquery.sql_server_rds_dbo.variant as v on (p.productid = v.productid)where concat(cast(dl.dealid as string),"-",ifnull(cast(v.variantid as string),"")) in'+'('+getProductData()+')'+'),base as(select pdm.edm_product_id,pdm.edm_product_created_date,pdm.edm_active_status,csp.dealid,csp.variantid,csp.key as prouct_key,csp.product_price as current_price,spt.date as price_change_date,case when spt.date is not null then date_diff(current_date("Australia/Melbourne"),spt.date,day) else null end as days_on_curr_price,csp.product_stock,coalesce(csp.promostartdate,sp.promotion_start_date) as promostartdate,coalesce(csp.promoenddate,sp.promotion_end_date) as promoenddate,pps.prev_promo_price,pps.prev_promo_startdate,pps.prev_promo_enddate,spt.sellprice as updated_price,spt.pre_sell_price as prior_updated_price,spt.pre_sell_price_date as last_price_change_date from crm_stock_price as csp left join sell_price_tracker as spt on(csp.dealid = spt.dealid and ifnull(csp.variantid,001001)= ifnull(spt.variantid,001001))left join promo_price_stats as pps on(csp.dealid = pps.dealid and ifnull(csp.variantid,001001)= ifnull(pps.variantid,001001))left join (select * from mydeal-bigquery.dbt_data_studio.product_detail_edm where last_updated_date = (select max(last_updated_date) from mydeal-bigquery.dbt_data_studio.product_detail_edm)) as pdm on(csp.dealid = pdm.myd_dealid and ifnull(csp.variantid,001001)= ifnull(pdm.myd_variantid,001001))left join scheduled_promo as sp on(csp.dealid = sp.dealid and ifnull(csp.variantid,001001)= ifnull(sp.variantid,001001)))select edm_product_id,edm_product_created_date,edm_active_status,base.dealid,base.variantid,prouct_key,current_price,price_change_date,days_on_curr_price,product_stock,dp.promotionalprice AS promo_price_scheduled,promostartdate,promoenddate,prev_promo_price,prev_promo_startdate,prev_promo_enddate,updated_price,prior_updated_price,last_price_change_date from base left join mydeal-bigquery.sql_server_rds_dbo.dealpromotionalprice AS dp on (base.dealid = dp.dealid and ifnull(base.variantid,00100) = ifnull(dp.variantid,00100) and base.promostartdate = date(dp.promotionstarttime)and base.promoenddate = date(dp.promotionendtime))group by all'
  ,useLegacySql: false
};

let queryResults = BigQuery.Jobs.query(request,projectID);
const jobId = queryResults.jobReference.jobId;

let sleepTimeMs = 500;
while(!queryResults.jobComplete){
  Utilities.sleep(sleepTimeMs);
  sleepTimeMs *=2;
  queryResults = BigQuery.Jobs.query(request,projectID)
}

const headers = queryResults.schema.fields.map((x) => x.name);
rows = queryResults.rows;

/* 
rows are in this format
{ f: 
   [ { v: '7457221' },
     { v: 'order approved' },
     { v: '564.48' },
     { v: '-400.83' } ] }

*/

if(rows){
  
  const data = new Array(rows.length);

  for (let i=0;i<rows.length;i++){
    const cols = rows[i].f;
    data[i] = new Array(cols.length);
    for(j=0;j<cols.length;j++){
      data[i][j] = cols[j].v
    }
  }

 const product_price_results = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Output");

 if(headers){
    ClearContents('Output');
    product_price_results.appendRow(headers);
    product_price_results.getRange(2,1,rows.length,headers.length).setValues(data);

  }
  return "Data has been successfully written to the sheet!"
}else{
  return "Error:Data not written to sheet! Check logs!"
}
}

function sendAlert(){
  /**
   * This function is used to send a slack alert when the validation is complete or failed
   * @params : [{None}]
   * @return : [{None}] - Just output stats wriiten to slack 
   */

let data = 
{
	"blocks": [
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": ":information_source: *Deal Price Tracker - Marnie*"
			}
		},
		{
			"type": "divider"
		},
		{
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": bigQueryResult()
			}
		}
	]
}

  const url = "https://hooks.slack.com/services/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";
  const options = {
    "method" : "post",
    "contentType" : "application/json",
    "muteHttpExceptions" : true,
    "payload" : JSON.stringify(data)
  };
  try {
         UrlFetchApp.fetch(url,options);
    
  }catch(e){
    Logger.log(e);
  }
}
