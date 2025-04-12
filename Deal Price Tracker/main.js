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
  data_clean_sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName).getRange("A2:K")
  data_clean_sheet.clearContent()
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

  query : 'with promo_schedule as (select * from (select dealid,variantid,sellprice,date(datetimestamp) last_change_date,date_diff(current_date("Australia/Sydney"),date(datetimestamp) , DAY ) days_since_change,promotionalsellprice scheduled_promo_price,date(promotionalstartdatetimestamp) promo_start_date,date(promotionalenddatetimestamp) promo_end_date,row_number() over (partition by CONCAT(cast(dealid as string), IFNULL(cast(variantid as string),"")) order by date(datetimestamp)  desc) rn FROM mydeal-bigquery.dbt_data_studio.price_history_tracker) where rn=1 and current_date("Australia/Sydney") <= promo_end_date),promo_prev as (select * from (select dealid,variantid,sellprice,date(datetimestamp) last_change_date,date_diff(current_date("Australia/Sydney"),date(datetimestamp) , DAY ) days_since_change,promotionalsellprice prev_promo_price,date(promotionalstartdatetimestamp) promo_start_date,date(promotionalenddatetimestamp) promo_end_date,row_number() over (partition by CONCAT(cast(dealid as string), IFNULL(cast(variantid as string),"")) order by date(datetimestamp)  desc) rn FROM mydeal-bigquery.dbt_data_studio.price_history_tracker)where rn=1 and current_date("Australia/Sydney") > promo_start_date ),promo_stock as (SELECT DISTINCT d.dealid,v.variantid,case when v.variantid is not null and current_date("Australia/Sydney") between date(v.promotionstartdate) and date(v.promotionenddate) then v.promotionalsellprice when v.variantid is not null and ( (current_date("Australia/Sydney") not between date(v.promotionstartdate) and date(v.promotionenddate)) or date(v.promotionstartdate) is null) then v.sellprice when v.variantid is null and current_date("Australia/Sydney") between date(d.promotionstartdate) and date(d.promotionenddate) then d.promotionalcustomerprice when v.variantid is null and ( ( current_date("Australia/Sydney") not between date(d.promotionstartdate) and date(d.promotionenddate)) or d.promotionstartdate is null)  then d.customerprice else d.customerprice end as  product_price,case when v.stocklevel is null then d.quota else v.stocklevel end as product_stock,FROM mydeal-bigquery.sql_server_rds_dbo.deal d LEFT JOIN mydeal-bigquery.sql_server_rds_dbo.product AS prd ON d.dealid=prd.dealid LEFT JOIN mydeal-bigquery.sql_server_rds_dbo.variant AS v ON prd.productid = v.productid where concat(cast(d.dealid as string),"-",ifnull(cast(v.variantid as string),"")) in'+'('+getProductData()+')'+')select distinct lp.dealid, lp.variantid,lp.product_price current_price,coalesce(ps.last_change_date,pp.last_change_date) last_price_date,date_diff(current_date("Australia/Sydney"),date(coalesce(ps.last_change_date,pp.last_change_date) ), DAY) days_on_curr_price,lp.product_stock,ps.scheduled_promo_price promo_price_scheduled,ps.promo_start_date promo_start,ps.promo_end_date promo_end,pp.promo_end_date last_promo_end_date,pp.prev_promo_price last_promo_price from  promo_stock lp left join promo_schedule ps on CONCAT(cast(lp.dealid as string), IFNULL(cast(lp.variantid as string),""))=CONCAT(cast(ps.dealid as string),IFNULL(CAST(ps.variantid AS string),"")) left join promo_prev pp on CONCAT(cast(lp.dealid as string), IFNULL(cast(lp.variantid as string),""))=CONCAT(cast(pp.dealid as string),IFNULL(CAST(pp.variantid AS string),""))'
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
