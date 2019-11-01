select ticker_code, max(date), min(date) from "historical_price" 
group by ticker_code
having max(date) > (CURRENT_DATE + INTERVAL '-1 year')
order by min(date) desc;